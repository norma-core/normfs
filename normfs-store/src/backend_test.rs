use crate::DiskUsage;
use crate::backend::{Body, End, LocalStore, StoreBackend};
use bytes::Bytes;
use normfs_types::{DataSource, QueueIdResolver};
use std::sync::Arc;
use uintn::UintN;

fn local(root: &std::path::Path, usage: Arc<DiskUsage>) -> LocalStore {
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    LocalStore::new(fs, root, false, usage)
}

#[tokio::test]
async fn a_local_put_reads_back_whole_and_in_ranges() {
    let temp = tempfile::TempDir::new().unwrap();
    let usage = Arc::new(DiskUsage::default());
    let store = local(temp.path(), usage.clone());
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let id = UintN::from(3u64);

    let runs = vec![Bytes::from_static(b"head"), Bytes::from_static(b"-body")];
    store.put(&queue, &id, Body::Runs(runs)).await.unwrap();

    assert_eq!(store.source(), DataSource::DiskStore);
    assert_eq!(
        store.get(&queue, &id).await.unwrap().unwrap(),
        &b"head-body"[..]
    );
    assert_eq!(
        store.get_range(&queue, &id, 2, 4).await.unwrap().unwrap(),
        &b"ad-b"[..]
    );
    assert_eq!(
        store.get_range(&queue, &id, 6, 100).await.unwrap().unwrap(),
        &b"ody"[..]
    );
    assert_eq!(store.size(&queue, &id).await.unwrap(), Some(9));
    assert_eq!(usage.queue(&queue).bytes(), 9);
}

#[tokio::test]
async fn a_missing_local_file_is_none() {
    let temp = tempfile::TempDir::new().unwrap();
    let store = local(temp.path(), Arc::new(DiskUsage::default()));
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let id = UintN::from(1u64);

    assert!(store.get(&queue, &id).await.unwrap().is_none());
    assert!(store.get_range(&queue, &id, 0, 10).await.unwrap().is_none());
    assert!(store.size(&queue, &id).await.unwrap().is_none());
    assert!(store.find(&queue, End::Min).await.unwrap().is_none());
    assert!(!queue.to_store_dir(temp.path()).exists());
}

#[tokio::test]
async fn local_find_returns_the_ends() {
    let temp = tempfile::TempDir::new().unwrap();
    let store = local(temp.path(), Arc::new(DiskUsage::default()));
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in [4u64, 9, 6] {
        let body = Body::Runs(vec![Bytes::from_static(b"x")]);
        store.put(&queue, &UintN::from(id), body).await.unwrap();
    }

    assert_eq!(
        store.find(&queue, End::Min).await.unwrap(),
        Some(UintN::from(4u64))
    );
    assert_eq!(
        store.find(&queue, End::Max).await.unwrap(),
        Some(UintN::from(9u64))
    );
}
