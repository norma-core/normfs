use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};
use normfs_crypto::CryptoContext;
use normfs_store::header::{FileAuthentication, SignatureType};
use normfs_store::store_header_v1::AnyStoreHeader;
use std::sync::Arc;
use tempfile::TempDir;
use uintn::UintN;

fn get_queue_store_path(
    base_path: &std::path::Path,
    instance_id: &str,
    queue_name: &str,
) -> std::path::PathBuf {
    // For test helper: manually construct path since test uses raw QueueId::from()
    // In real code, QueueId would already have instance_id embedded via resolver
    let full_path = format!("{}/{}", instance_id, queue_name);
    base_path.join(full_path).join("store")
}

#[tokio::test]
async fn test_signature_verification() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let queue_name = "test_queue";

    let settings = NormFsSettings::default();
    let normfs = NormFS::new(data_dir.clone(), settings).await.unwrap();
    let normfs = Arc::new(normfs);

    let queue_id = normfs.resolve(queue_name);

    normfs
        .ensure_queue_exists_for_write(&queue_id)
        .await
        .unwrap();

    let entry_size = 1024 * 1024; // 1 MB per entry
    let num_entries = 150; // 150 MB total
    let data = Bytes::from(vec![0u8; entry_size]);

    for _ in 0..num_entries {
        normfs.enqueue(&queue_id, data.clone()).unwrap();
    }

    let instance_id = normfs.get_instance_id().to_string();

    let store_dir = get_queue_store_path(&data_dir, &instance_id, queue_name);
    let file_id = UintN::from(1u64);
    let store_file_path = file_id.to_file_path(store_dir.to_str().unwrap(), "store");

    // Read while the instance is still alive: the flusher that writes store
    // files stops once it is closed, and there is no guarantee that a file
    // which merely exists at some instant is still there (or unchanged) by
    // the time a later, separate read happens, so retry the read itself
    // rather than checking existence and reading in two separate steps.
    let store_bytes = read_store_file_when_ready(&store_file_path).await;

    normfs.close().await.unwrap();
    drop(normfs);

    let crypto_ctx = CryptoContext::open(&data_dir).unwrap();

    // Parse FileAuthentication
    let (file_auth, auth_size) = FileAuthentication::from_bytes(&store_bytes).unwrap();

    assert_eq!(file_auth.header_signature_type, SignatureType::Ed25519);
    assert_eq!(file_auth.content_signature_type, SignatureType::Ed25519);

    // Parse StoreHeader
    let content_after_auth = &store_bytes[auth_size..];
    let (_header, header_size) = AnyStoreHeader::from_bytes(content_after_auth).unwrap();

    // Verify header signature
    let header_bytes = &content_after_auth[..header_size];
    crypto_ctx
        .verify(header_bytes, &file_auth.header_signature)
        .unwrap();

    // Verify content signature
    let content_bytes = &content_after_auth[header_size..];
    crypto_ctx
        .verify(content_bytes, &file_auth.content_signature)
        .unwrap();

    println!("✓ Signature verification successful!");
}

#[tokio::test]
async fn test_signature_verification_fails_on_tampered_header() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let queue_name = "test_queue";

    let settings = NormFsSettings::default();
    let normfs = NormFS::new(data_dir.clone(), settings).await.unwrap();
    let normfs = Arc::new(normfs);

    let queue_id = normfs.resolve(queue_name);

    normfs
        .ensure_queue_exists_for_write(&queue_id)
        .await
        .unwrap();

    let entry_size = 1024 * 1024; // 1 MB per entry
    let num_entries = 150; // 150 MB total
    let data = Bytes::from(vec![0u8; entry_size]);

    for _ in 0..num_entries {
        normfs.enqueue(&queue_id, data.clone()).unwrap();
    }

    let instance_id = normfs.get_instance_id().to_string();

    let store_dir = get_queue_store_path(&data_dir, &instance_id, queue_name);
    let file_id = UintN::from(1u64);
    let store_file_path = file_id.to_file_path(store_dir.to_str().unwrap(), "store");

    // Read while the instance is still alive: the flusher that writes store
    // files stops once it is closed, and there is no guarantee that a file
    // which merely exists at some instant is still there (or unchanged) by
    // the time a later, separate read happens, so retry the read itself
    // rather than checking existence and reading in two separate steps.
    let store_bytes = read_store_file_when_ready(&store_file_path).await;

    normfs.close().await.unwrap();
    drop(normfs);

    let crypto_ctx = CryptoContext::open(&data_dir).unwrap();

    // Parse FileAuthentication
    let (file_auth, auth_size) = FileAuthentication::from_bytes(&store_bytes).unwrap();

    // Parse StoreHeader
    let content_after_auth = &store_bytes[auth_size..];
    let (_, header_size) = AnyStoreHeader::from_bytes(content_after_auth).unwrap();

    // Tamper with header
    let mut tampered_header = content_after_auth[..header_size].to_vec();
    if !tampered_header.is_empty() {
        tampered_header[0] ^= 0xFF;
    }

    let result = crypto_ctx.verify(&tampered_header, &file_auth.header_signature);

    assert!(
        result.is_err(),
        "Signature verification should fail on tampered header"
    );

    println!("✓ Header signature verification correctly rejected tampered data!");
}

#[tokio::test]
async fn test_signature_verification_fails_on_tampered_content() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path().to_path_buf();

    let queue_name = "test_queue";

    let settings = NormFsSettings::default();
    let normfs = NormFS::new(data_dir.clone(), settings).await.unwrap();
    let normfs = Arc::new(normfs);

    let queue_id = normfs.resolve(queue_name);

    normfs
        .ensure_queue_exists_for_write(&queue_id)
        .await
        .unwrap();

    let entry_size = 1024 * 1024; // 1 MB per entry
    let num_entries = 150; // 150 MB total
    let data = Bytes::from(vec![0u8; entry_size]);

    for _ in 0..num_entries {
        normfs.enqueue(&queue_id, data.clone()).unwrap();
    }

    let instance_id = normfs.get_instance_id().to_string();

    let store_dir = get_queue_store_path(&data_dir, &instance_id, queue_name);
    let file_id = UintN::from(1u64);
    let store_file_path = file_id.to_file_path(store_dir.to_str().unwrap(), "store");

    // Read while the instance is still alive: the flusher that writes store
    // files stops once it is closed, and there is no guarantee that a file
    // which merely exists at some instant is still there (or unchanged) by
    // the time a later, separate read happens, so retry the read itself
    // rather than checking existence and reading in two separate steps.
    let store_bytes = read_store_file_when_ready(&store_file_path).await;

    normfs.close().await.unwrap();
    drop(normfs);

    let crypto_ctx = CryptoContext::open(&data_dir).unwrap();

    // Parse FileAuthentication
    let (file_auth, auth_size) = FileAuthentication::from_bytes(&store_bytes).unwrap();

    // Parse StoreHeader
    let content_after_auth = &store_bytes[auth_size..];
    let (_, header_size) = AnyStoreHeader::from_bytes(content_after_auth).unwrap();

    // Tamper with content
    let mut tampered_content = content_after_auth[header_size..].to_vec();
    if !tampered_content.is_empty() {
        tampered_content[0] ^= 0xFF;
    }

    let result = crypto_ctx.verify(&tampered_content, &file_auth.content_signature);

    assert!(
        result.is_err(),
        "Signature verification should fail on tampered content"
    );

    println!("✓ Content signature verification correctly rejected tampered data!");
}

/// Waits for the flusher to produce a store file.
///
/// The store write is asynchronous: the WAL rotates, then the content is
/// compressed, encrypted and signed before the file appears. A fixed sleep is
/// a race that holds on a fast machine and loses on a shared CI runner.
///
/// This must be awaited, not blocked on: these are `#[tokio::test]`, so the
/// runtime is current-thread and the flusher only makes progress while the
/// test is awaiting.
async fn read_store_file_when_ready(path: &std::path::Path) -> Vec<u8> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        match std::fs::read(path) {
            Ok(bytes) => return bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "store file never appeared: {}",
                    path.display()
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => panic!("reading {}: {}", path.display(), e),
        }
    }
}
