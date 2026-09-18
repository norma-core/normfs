use crate::DiskUsage;
use normfs_types::QueueIdResolver;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn publication_accounts_replacements_and_leaves_failures_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = DiskUsage::default();
    let temp_path = dir.path().join("temp");
    let store_path = dir.path().join("001.store");
    for size in [100, 40, 200] {
        std::fs::write(&temp_path, vec![0; size]).unwrap();
        usage
            .publish(&queue, &temp_path, &store_path, size as u64)
            .await
            .unwrap();
        assert_eq!(*usage.queue(&queue).lock().await, size as u64);
        assert_eq!(std::fs::metadata(&store_path).unwrap().len(), size as u64);
    }
    assert!(
        usage
            .publish(&queue, &temp_path, &store_path, 500)
            .await
            .is_err()
    );
    assert_eq!(*usage.queue(&queue).lock().await, 200);
}

#[tokio::test]
async fn publication_waits_for_the_scan_lock() {
    let dir = tempfile::tempdir().unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = Arc::new(DiskUsage::default());
    let temp_path = dir.path().join("temp");
    let store_path = dir.path().join("001.store");
    std::fs::write(&temp_path, vec![0; 100]).unwrap();
    let tracked = usage.queue(&queue);
    let mut scan = tracked.lock().await;
    let publish_path = store_path.clone();
    let mut publish = tokio::spawn(async move {
        usage
            .publish(&queue, &temp_path, &publish_path, 100)
            .await
            .unwrap();
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut publish)
            .await
            .is_err()
    );
    assert!(!store_path.exists());
    *scan = 50;
    drop(scan);
    publish.await.unwrap();
    assert_eq!(*tracked.lock().await, 150);
    assert!(store_path.exists());
}
