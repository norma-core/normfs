use std::path::Path;

use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use bytes::Bytes;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use hkdf::Hkdf;
use normfs_types::QueueId;
use rand_chacha::ChaCha20Rng;
use rand_core::{RngCore, SeedableRng};
use sha2::{Digest, Sha256};
use uintn::UintN;
use zeroize::ZeroizeOnDrop;

use crate::seed::{Seed, SeedError};

#[derive(Debug)]
pub enum CryptoError {
    Seed(SeedError),
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

    fn derive_rng(&self, queue_id: &QueueId, file_id: &UintN) -> Result<ChaCha20Rng, CryptoError> {
        let hkdf = Hkdf::<Sha256>::new(None, self.seed.as_bytes());

        let file_id_bytes = file_id.value_to_bytes();

        let mut info = Vec::new();
        info.extend_from_slice(queue_id.to_key_derivation_base().as_bytes());
        info.extend_from_slice(&file_id_bytes);

        let mut rng_seed = [0u8; 32];
        hkdf.expand(&info, &mut rng_seed)
            .map_err(|_| CryptoError::KeyDerivation)?;

        Ok(ChaCha20Rng::from_seed(rng_seed))
    }

    pub fn encrypt(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        content: &Bytes,
    ) -> Result<(Bytes, Bytes), CryptoError> {
        let mut rng = self.derive_rng(queue_id, file_id)?;

        let mut aes_key = [0u8; 32];
        rng.fill_bytes(&mut aes_key);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&aes_key));

        let mut nonce_bytes = [0u8; 12];
        rng.fill_bytes(&mut nonce_bytes);
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
        nonce: &Bytes,
        ciphertext: &Bytes,
    ) -> Result<Bytes, CryptoError> {
        if nonce.len() != 12 {
            return Err(CryptoError::InvalidNonce);
        }

        let mut rng = self.derive_rng(queue_id, file_id)?;

        let mut aes_key = [0u8; 32];
        rng.fill_bytes(&mut aes_key);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&aes_key));

        let nonce_array: [u8; 12] = nonce.as_ref().try_into().unwrap();
        let nonce = Nonce::from_slice(&nonce_array);

        let plaintext = cipher
            .decrypt(nonce, ciphertext.as_ref())
            .map_err(|_| CryptoError::Decryption)?;

        Ok(Bytes::from(plaintext))
    }
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

        let (nonce, ciphertext) = ctx.encrypt(&queue_id, &file_id, &plaintext).unwrap();
        let decrypted = ctx
            .decrypt(&queue_id, &file_id, &nonce, &ciphertext)
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

        let (nonce1, ciphertext1) = ctx.encrypt(&queue_id, &file_id, &plaintext).unwrap();
        let (nonce2, ciphertext2) = ctx.encrypt(&queue_id, &file_id, &plaintext).unwrap();

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
            .encrypt(&queue_id, &UintN::from(1u64), &plaintext)
            .unwrap();
        let (nonce2, ciphertext2) = ctx
            .encrypt(&queue_id, &UintN::from(2u64), &plaintext)
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

        let (_, ciphertext) = ctx.encrypt(&queue_id, &file_id, &plaintext).unwrap();
        let wrong_nonce = Bytes::from(vec![0u8; 12]);

        let result = ctx.decrypt(&queue_id, &file_id, &wrong_nonce, &ciphertext);
        assert!(result.is_err());
    }
}
