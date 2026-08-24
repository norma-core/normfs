use std::path::Path;

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use bytes::Bytes;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use normfs_types::{EncryptionType, QueueId};
use rand_chacha::ChaCha20Rng;
use rand_core::SeedableRng;
use sha2::{Digest, Sha256};
use uintn::UintN;
use zeroize::{ZeroizeOnDrop, Zeroizing};

use crate::kdf::{self, KdfError, AES_KEY_SIZE, GCM_NONCE_SIZE};
use crate::seed::{Seed, SeedError};

#[derive(Debug)]
pub enum CryptoError {
    Seed(SeedError),
    Kdf(KdfError),
    UnsupportedEncryption(EncryptionType),
    KeyDerivation,
    Encryption,
    Decryption,
    InvalidNonce,
    Verification,
}

impl std::fmt::Display for CryptoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CryptoError::Seed(e) => write!(f, "Seed error: {}", e),
            CryptoError::Kdf(e) => write!(f, "Key derivation error: {}", e),
            CryptoError::UnsupportedEncryption(t) => {
                write!(f, "No key derivation for encryption type {:?}", t)
            }
            CryptoError::KeyDerivation => write!(f, "Key derivation failed"),
            CryptoError::Encryption => write!(f, "Encryption failed"),
            CryptoError::Decryption => write!(f, "Decryption failed"),
            CryptoError::InvalidNonce => write!(f, "Invalid nonce size"),
            CryptoError::Verification => write!(f, "Signature verification failed"),
        }
    }
}

impl std::error::Error for CryptoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CryptoError::Seed(e) => Some(e),
            CryptoError::Kdf(e) => Some(e),
            _ => None,
        }
    }
}

impl From<SeedError> for CryptoError {
    fn from(e: SeedError) -> Self {
        CryptoError::Seed(e)
    }
}

#[derive(ZeroizeOnDrop)]
pub struct CryptoContext {
    seed: Seed,
    signing_key: SigningKey,
    instance_id: [u8; 32],
    #[zeroize(skip)]
    instance_id_hex: String,
}

impl CryptoContext {
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self, CryptoError> {
        let seed = Seed::open(data_dir)?;

        let mut rng = ChaCha20Rng::from_seed(*seed.as_bytes());
        let signing_key = SigningKey::generate(&mut rng);
        let verifying_key = signing_key.verifying_key();

        let instance_id: [u8; 32] = Sha256::digest(verifying_key.as_bytes()).into();
        let instance_id_hex = instance_id.iter().map(|b| format!("{:02x}", b)).collect();

        Ok(Self {
            seed,
            signing_key,
            instance_id,
            instance_id_hex,
        })
    }

    pub fn instance_id(&self) -> &[u8; 32] {
        &self.instance_id
    }

    pub fn instance_id_hex(&self) -> &str {
        &self.instance_id_hex
    }

    pub fn instance_id_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.instance_id)
    }

    pub fn sign(&self, data: &[u8]) -> Signature {
        self.signing_key.sign(data)
    }

    pub fn verify(&self, data: &[u8], signature: &[u8; 64]) -> Result<(), CryptoError> {
        let sig = Signature::from_bytes(signature);
        let verifying_key = self.signing_key.verifying_key();
        verifying_key
            .verify(data, &sig)
            .map_err(|_| CryptoError::Verification)
    }

    /// The AES-256 key and GCM nonce for one file. Replaces a `ChaCha20Rng`
    /// that existed only to draw these 44 bytes, in this order, from its first
    /// keystream block.
    fn derive_file_key(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        encryption: EncryptionType,
    ) -> Result<(Zeroizing<[u8; AES_KEY_SIZE]>, [u8; GCM_NONCE_SIZE]), CryptoError> {
        let info = match encryption {
            EncryptionType::Aes => info_v1(queue_id, file_id),
            EncryptionType::AesV2 => info_v2(queue_id, file_id),
            EncryptionType::None => return Err(CryptoError::UnsupportedEncryption(encryption)),
        };

        kdf::derive_file_key(self.seed.as_bytes(), &info).map_err(CryptoError::Kdf)
    }

    pub fn encrypt(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        encryption: EncryptionType,
        content: &Bytes,
    ) -> Result<(Bytes, Bytes), CryptoError> {
        let (aes_key, nonce_bytes) = self.derive_file_key(queue_id, file_id, encryption)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(aes_key.as_ref()));
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, content.as_ref())
            .map_err(|_| CryptoError::Encryption)?;

        Ok((
            Bytes::copy_from_slice(&nonce_bytes),
            Bytes::from(ciphertext),
        ))
    }

    pub fn decrypt(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        encryption: EncryptionType,
        nonce: &Bytes,
        ciphertext: &Bytes,
    ) -> Result<Bytes, CryptoError> {
        if nonce.len() != GCM_NONCE_SIZE {
            return Err(CryptoError::InvalidNonce);
        }

        // The nonce comes off disk, so the derived one is discarded here --
        // exactly as the old code discarded it by drawing only the key.
        let (aes_key, _derived_nonce) = self.derive_file_key(queue_id, file_id, encryption)?;

        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(aes_key.as_ref()));

        let nonce_array: [u8; GCM_NONCE_SIZE] = nonce.as_ref().try_into().unwrap();
        let nonce = Nonce::from_slice(&nonce_array);

        let plaintext = cipher
            .decrypt(nonce, ciphertext.as_ref())
            .map_err(|_| CryptoError::Decryption)?;

        Ok(Bytes::from(plaintext))
    }
}

/// Ambiguous: `("/x/q", 0x3141)` and `("/x/qA", 0x31)` both encode to
/// `/x/qA1`, so two files share a key and a nonce. Kept unchanged because the
/// keys of every file already on disk come from it.
fn info_v1(queue_id: &QueueId, file_id: &UintN) -> Vec<u8> {
    let base = queue_id.to_key_derivation_base().as_bytes();
    let file_id_bytes = file_id.value_to_bytes();

    let mut info = Vec::with_capacity(base.len() + file_id_bytes.len());
    info.extend_from_slice(base);
    info.extend_from_slice(&file_id_bytes);
    info
}

/// Length-prefixed, so no two inputs encode alike. `file_id` is narrowed first
/// to keep the bytes a function of the value, not of the `UintN` variant held.
fn info_v2(queue_id: &QueueId, file_id: &UintN) -> Vec<u8> {
    let base = queue_id.to_key_derivation_base().as_bytes();
    let file_id_bytes = file_id.shrink_to_fit().value_to_bytes();

    let mut info = Vec::with_capacity(8 + base.len() + file_id_bytes.len());
    info.extend_from_slice(&(base.len() as u32).to_le_bytes());
    info.extend_from_slice(base);
    info.extend_from_slice(&(file_id_bytes.len() as u32).to_le_bytes());
    info.extend_from_slice(&file_id_bytes);
    info
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_creates_context() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();

        assert_eq!(ctx.instance_id().len(), 32);
    }

    #[test]
    fn test_instance_id_is_deterministic() {
        let temp_dir = tempfile::tempdir().unwrap();

        let ctx1 = CryptoContext::open(temp_dir.path()).unwrap();
        let id1 = *ctx1.instance_id();

        let ctx2 = CryptoContext::open(temp_dir.path()).unwrap();
        let id2 = *ctx2.instance_id();

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_sign_produces_signature() {
        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();

        let data = b"test message";
        let signature = ctx.sign(data);

        assert_eq!(signature.to_bytes().len(), 64);
    }

    #[test]
    fn test_different_contexts_have_different_instance_ids() {
        let temp_dir1 = tempfile::tempdir().unwrap();
        let temp_dir2 = tempfile::tempdir().unwrap();

        let ctx1 = CryptoContext::open(temp_dir1.path()).unwrap();
        let ctx2 = CryptoContext::open(temp_dir2.path()).unwrap();

        assert_ne!(ctx1.instance_id(), ctx2.instance_id());
    }

    #[test]
    fn test_encrypt_decrypt() {
        use normfs_types::QueueIdResolver;

        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();
        let resolver = QueueIdResolver::new("test_instance");
        let queue_id = resolver.resolve("test_queue");
        let file_id = UintN::from(42u64);
        let plaintext = Bytes::from("Hello, normfs!");

        let (nonce, ciphertext) = ctx
            .encrypt(&queue_id, &file_id, EncryptionType::AesV2, &plaintext)
            .unwrap();
        let decrypted = ctx
            .decrypt(
                &queue_id,
                &file_id,
                EncryptionType::AesV2,
                &nonce,
                &ciphertext,
            )
            .unwrap();

        assert_eq!(plaintext, decrypted);
    }

    #[test]
    fn test_encryption_is_deterministic() {
        use normfs_types::QueueIdResolver;

        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();
        let resolver = QueueIdResolver::new("test_instance");
        let queue_id = resolver.resolve("test_queue");
        let file_id = UintN::from(42u64);
        let plaintext = Bytes::from("test data");

        let (nonce1, ciphertext1) = ctx
            .encrypt(&queue_id, &file_id, EncryptionType::AesV2, &plaintext)
            .unwrap();
        let (nonce2, ciphertext2) = ctx
            .encrypt(&queue_id, &file_id, EncryptionType::AesV2, &plaintext)
            .unwrap();

        assert_eq!(nonce1, nonce2);
        assert_eq!(ciphertext1, ciphertext2);
    }

    #[test]
    fn test_different_files_produce_different_ciphertexts() {
        use normfs_types::QueueIdResolver;

        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();
        let resolver = QueueIdResolver::new("test_instance");
        let queue_id = resolver.resolve("test_queue");
        let plaintext = Bytes::from("same data");

        let (nonce1, ciphertext1) = ctx
            .encrypt(
                &queue_id,
                &UintN::from(1u64),
                EncryptionType::AesV2,
                &plaintext,
            )
            .unwrap();
        let (nonce2, ciphertext2) = ctx
            .encrypt(
                &queue_id,
                &UintN::from(2u64),
                EncryptionType::AesV2,
                &plaintext,
            )
            .unwrap();

        assert_ne!(nonce1, nonce2);
        assert_ne!(ciphertext1, ciphertext2);
    }

    #[test]
    fn test_decrypt_with_wrong_nonce_fails() {
        use normfs_types::QueueIdResolver;

        let temp_dir = tempfile::tempdir().unwrap();
        let ctx = CryptoContext::open(temp_dir.path()).unwrap();
        let resolver = QueueIdResolver::new("test_instance");
        let queue_id = resolver.resolve("test_queue");
        let file_id = UintN::from(42u64);
        let plaintext = Bytes::from("test");

        let (_, ciphertext) = ctx
            .encrypt(&queue_id, &file_id, EncryptionType::AesV2, &plaintext)
            .unwrap();
        let wrong_nonce = Bytes::from(vec![0u8; 12]);

        let result = ctx.decrypt(
            &queue_id,
            &file_id,
            EncryptionType::AesV2,
            &wrong_nonce,
            &ciphertext,
        );
        assert!(result.is_err());
    }
}
