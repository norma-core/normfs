use bytes::Bytes;
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use uintn::UintN;

use crate::StoreError;
use crate::header::{CompressionType, FileAuthentication, StoreHeader};

pub fn extract_wal_header(
    store_file_bytes: &[u8],
    crypto_ctx: &CryptoContext,
    queue_id: &QueueId,
    file_id: &UintN,
    verify_signatures: bool,
) -> Result<normfs_wal::WalHeader, StoreError> {
    let (file_auth, auth_size) = FileAuthentication::from_bytes(store_file_bytes)?;

    let content_after_auth = &store_file_bytes[auth_size..];
    let (store_header, header_size) = StoreHeader::from_bytes(content_after_auth)?;

    if verify_signatures {
        let header_bytes = &content_after_auth[..header_size];
        crypto_ctx
            .verify(header_bytes, &file_auth.header_signature)
            .map_err(|_| StoreError::SignatureVerificationFailed)?;

        let content_bytes = &content_after_auth[header_size..];
        crypto_ctx
            .verify(content_bytes, &file_auth.content_signature)
            .map_err(|_| StoreError::SignatureVerificationFailed)?;
    }

    let content_bytes = &content_after_auth[header_size..];

    let wal_content_bytes = if store_header.is_encrypted() {
        if content_bytes.len() < 12 {
            return Err(StoreError::Decrypt);
        }

        let store_bytes = Bytes::from(content_bytes.to_vec());
        let nonce = store_bytes.slice(0..12);
        let ciphertext = store_bytes.slice(12..);

        crypto_ctx
            .decrypt(queue_id, file_id, &nonce, &ciphertext)
            .map_err(|_| StoreError::Decrypt)?
    } else {
        Bytes::from(content_bytes.to_vec())
    };

    // Handle decompression if needed
    let wal_content_bytes = if store_header.is_compressed() {
        match store_header.compression {
            CompressionType::Gzip => {
                crate::compression::zstd_decompress(wal_content_bytes.as_ref())?
            }
            CompressionType::Xz => crate::compression::xz_decompress(wal_content_bytes.as_ref())?,
            CompressionType::Zstd => {
                crate::compression::zstd_decompress(wal_content_bytes.as_ref())?
            }
            CompressionType::None => wal_content_bytes.to_vec(),
        }
    } else {
        wal_content_bytes.to_vec()
    };

    // Read WAL header from the decrypted/decompressed content
    let wal_header = normfs_wal::get_wal_header(&Bytes::from(wal_content_bytes))
        .map_err(StoreError::WalHeaderError)?;

    Ok(wal_header)
}
