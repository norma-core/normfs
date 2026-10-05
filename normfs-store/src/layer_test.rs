use crate::backend::{Body, LocalStore, StoreBackend};
use crate::header::{CompressionType, EncryptionType};
use crate::layer::Layer;
use crate::{DiskUsage, store_file};
use bytes::Bytes;
use normfs_crypto::CryptoContext;
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use uintn::UintN;

fn local(root: &std::path::Path) -> Arc<dyn StoreBackend> {
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    Arc::new(LocalStore::new(
        fs,
        root,
        false,
        Arc::new(DiskUsage::default()),
    ))
}

#[tokio::test]
async fn a_forgotten_range_is_read_from_disk_again() {
    let temp = tempfile::TempDir::new().unwrap();
    let layer = Layer::new(local(temp.path()), None, false);
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let file_id = UintN::from(7u64);

    layer.record_range(&queue, &file_id, &UintN::from(10u64), &UintN::from(19u64));
    assert_eq!(
        layer.get_file_range(&queue, &file_id).await.unwrap(),
        Some((UintN::from(10u64), UintN::from(19u64)))
    );

    // No file on disk behind the entry, so after forgetting there is nothing.
    layer.forget(&queue, &file_id);
    assert_eq!(layer.get_file_range(&queue, &file_id).await.unwrap(), None);
}

#[tokio::test]
async fn the_range_and_queue_start_come_from_the_file_header() {
    let temp = tempfile::TempDir::new().unwrap();
    let crypto = Arc::new(CryptoContext::open(temp.path()).unwrap());
    let backend = local(temp.path());
    let layer = Layer::new(backend.clone(), Some(crypto.clone()), false);
    let queue = QueueIdResolver::new("inst").resolve("cam");

    for (id, before) in [(3u64, 30u64), (4, 40)] {
        let file_id = UintN::from(id);
        let file = store_file::build(
            &queue,
            &file_id,
            CompressionType::Zstd,
            EncryptionType::Aes,
            UintN::from(before),
            UintN::from(5u64),
            &Bytes::from_static(b"entries"),
            &crypto,
        )
        .unwrap();
        let body = Body::Runs(vec![file.to_bytes()]);
        backend.put(&queue, &file_id, body).await.unwrap();
    }

    assert_eq!(
        layer
            .get_file_range(&queue, &UintN::from(4u64))
            .await
            .unwrap(),
        Some((UintN::from(40u64), UintN::from(44u64)))
    );
    assert_eq!(
        layer.get_queue_start(&queue).await.unwrap(),
        Some(UintN::from(30u64))
    );
}
