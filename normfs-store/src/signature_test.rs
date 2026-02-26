use crate::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use bytes::{Bytes, BytesMut};
use normfs_crypto::CryptoContext;
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use uintn::UintN;

#[test]
fn test_file_authentication_signature_verification() {
    // Create a simple store file with FileAuthentication
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Create crypto context
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let crypto_ctx = CryptoContext::open(&temp_dir).unwrap();

    // Sign header and content
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Serialize FileAuthentication
    let mut auth_bytes = BytesMut::new();
    file_auth.write_to_bytes(&mut auth_bytes);

    // Verify header signature
    crypto_ctx
        .verify(&header_bytes, &file_auth.header_signature)
        .unwrap();

    // Verify content signature
    crypto_ctx
        .verify(&content, &file_auth.content_signature)
        .unwrap();

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn test_signature_verification_fails_on_tampered_header() {
    // Create a simple store file with FileAuthentication
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Create crypto context
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let crypto_ctx = CryptoContext::open(&temp_dir).unwrap();

    // Sign header and content
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Tamper with header
    let mut tampered_header = header_bytes.to_vec();
    if !tampered_header.is_empty() {
        tampered_header[0] ^= 0xFF;
    }

    // Verify should fail
    let result = crypto_ctx.verify(&tampered_header, &file_auth.header_signature);
    assert!(result.is_err(), "Should detect tampered header");

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn test_signature_verification_fails_on_tampered_content() {
    // Create a simple store file with FileAuthentication
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Create crypto context
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let crypto_ctx = CryptoContext::open(&temp_dir).unwrap();

    // Sign header and content
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Tamper with content
    let mut tampered_content = content.to_vec();
    if !tampered_content.is_empty() {
        tampered_content[0] ^= 0xFF;
    }

    // Verify should fail
    let result = crypto_ctx.verify(&tampered_content, &file_auth.content_signature);
    assert!(result.is_err(), "Should detect tampered content");

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn test_extract_wal_bytes_with_verification() {
    use crate::{PersistStore, StoreWriteConfig};
    use normfs_wal::WalStore;

    // Create temp directory
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    // Create crypto context
    let crypto_ctx = Arc::new(CryptoContext::open(&temp_dir).unwrap());

    // Create queue
    let resolver = QueueIdResolver::new(crypto_ctx.instance_id_hex());
    let queue = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);

    // Create WAL and PersistStore (needed for extract_wal_bytes method)
    let (wal_entry_send, _) = tokio::sync::mpsc::unbounded_channel();
    let (wal_complete_send, _) = tokio::sync::mpsc::unbounded_channel();
    let wal_store = Arc::new(WalStore::new(&temp_dir, wal_entry_send, wal_complete_send));

    let config = StoreWriteConfig {
        num_workers: 1,
        verify_signatures: true,
    };

    let store = PersistStore::new(&temp_dir, config, crypto_ctx.clone(), wal_store);

    // Create test data (unencrypted, uncompressed)
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Sign both
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Assemble complete store file
    let mut complete_file = BytesMut::new();
    file_auth.write_to_bytes(&mut complete_file);
    complete_file.extend_from_slice(&header_bytes);
    complete_file.extend_from_slice(&content);

    let store_bytes = complete_file.freeze();

    // Test extraction with verification enabled
    let result = store.extract_wal_bytes(&queue, &file_id, store_bytes.clone(), true);
    assert!(
        result.is_ok(),
        "Extraction with verification should succeed"
    );

    // Test extraction with verification disabled
    let result = store.extract_wal_bytes(&queue, &file_id, store_bytes, false);
    assert!(
        result.is_ok(),
        "Extraction without verification should succeed"
    );

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn test_extract_wal_bytes_detects_tampered_header() {
    use crate::{PersistStore, StoreWriteConfig};
    use normfs_wal::WalStore;

    // Create temp directory
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    // Create crypto context
    let crypto_ctx = Arc::new(CryptoContext::open(&temp_dir).unwrap());

    // Create queue
    let resolver = QueueIdResolver::new(crypto_ctx.instance_id_hex());
    let queue = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);

    // Create WAL and PersistStore
    let (wal_entry_send, _) = tokio::sync::mpsc::unbounded_channel();
    let (wal_complete_send, _) = tokio::sync::mpsc::unbounded_channel();
    let wal_store = Arc::new(WalStore::new(&temp_dir, wal_entry_send, wal_complete_send));

    let config = StoreWriteConfig {
        num_workers: 1,
        verify_signatures: true,
    };

    let store = PersistStore::new(&temp_dir, config, crypto_ctx.clone(), wal_store);

    // Create test data
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Sign both
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Assemble complete store file
    let mut complete_file = BytesMut::new();
    file_auth.write_to_bytes(&mut complete_file);
    complete_file.extend_from_slice(&header_bytes);
    complete_file.extend_from_slice(&content);

    // Tamper with header (after FileAuthentication)
    let mut tampered = complete_file.to_vec();
    let auth_size = 152; // FileAuthentication size
    if tampered.len() > auth_size {
        tampered[auth_size] ^= 0xFF;
    }

    let tampered_store_bytes = Bytes::from(tampered);

    // Test extraction with verification enabled - should fail
    let result = store.extract_wal_bytes(&queue, &file_id, tampered_store_bytes, true);
    assert!(
        result.is_err(),
        "Should detect tampered header with verification"
    );

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn test_extract_wal_bytes_detects_tampered_content() {
    use crate::{PersistStore, StoreWriteConfig};
    use normfs_wal::WalStore;

    // Create temp directory
    let temp_dir = std::env::temp_dir().join(format!("normfs_test_{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&temp_dir).unwrap();

    // Create crypto context
    let crypto_ctx = Arc::new(CryptoContext::open(&temp_dir).unwrap());

    // Create queue
    let resolver = QueueIdResolver::new(crypto_ctx.instance_id_hex());
    let queue = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);

    // Create WAL and PersistStore
    let (wal_entry_send, _) = tokio::sync::mpsc::unbounded_channel();
    let (wal_complete_send, _) = tokio::sync::mpsc::unbounded_channel();
    let wal_store = Arc::new(WalStore::new(&temp_dir, wal_entry_send, wal_complete_send));

    let config = StoreWriteConfig {
        num_workers: 1,
        verify_signatures: true,
    };

    let store = PersistStore::new(&temp_dir, config, crypto_ctx.clone(), wal_store);

    // Create test data
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(0u64),
        UintN::from(10u64),
    );

    let mut header_bytes = BytesMut::new();
    header.write_to_bytes(&mut header_bytes);

    let content = Bytes::from(vec![42u8; 1024]);

    // Sign both
    let header_signature = crypto_ctx.sign(&header_bytes);
    let content_signature = crypto_ctx.sign(&content);

    // Create FileAuthentication
    let file_auth =
        FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

    // Assemble complete store file
    let mut complete_file = BytesMut::new();
    file_auth.write_to_bytes(&mut complete_file);
    complete_file.extend_from_slice(&header_bytes);
    complete_file.extend_from_slice(&content);

    // Tamper with content (after FileAuthentication + StoreHeader)
    let mut tampered = complete_file.to_vec();
    let auth_size = 152; // FileAuthentication size
    let header_size = header_bytes.len();
    let content_start = auth_size + header_size;
    if tampered.len() > content_start {
        tampered[content_start] ^= 0xFF;
    }

    let tampered_store_bytes = Bytes::from(tampered);

    // Test extraction with verification enabled - should fail
    let result = store.extract_wal_bytes(&queue, &file_id, tampered_store_bytes, true);
    assert!(
        result.is_err(),
        "Should detect tampered content with verification"
    );

    // Cleanup
    std::fs::remove_dir_all(&temp_dir).unwrap();
}
