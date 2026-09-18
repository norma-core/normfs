use crate::ranges::RangeStore;
use normfs_crypto::CryptoContext;
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use uintn::UintN;

#[tokio::test]
async fn a_forgotten_range_is_read_from_disk_again() {
    let temp = tempfile::TempDir::new().unwrap();
    let crypto = Arc::new(CryptoContext::open(temp.path()).unwrap());
    let store = RangeStore::new(temp.path(), crypto, false);
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let file_id = UintN::from(7u64);

    store
        .record_range(&queue, &file_id, &UintN::from(10u64), &UintN::from(19u64))
        .await
        .unwrap();
    assert_eq!(
        store.get_range(&queue, &file_id).await.unwrap(),
        Some((UintN::from(10u64), UintN::from(19u64)))
    );

    // No file on disk behind the entry, so after forgetting there is nothing.
    store.forget(&queue, &file_id);
    assert_eq!(store.get_range(&queue, &file_id).await.unwrap(), None);
}
