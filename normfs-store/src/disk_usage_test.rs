use crate::DiskUsage;
use bytes::Bytes;
use normfs_fs::{Fs, FsConfig, PublishSpec, Runs, TmpMode};
use normfs_types::QueueIdResolver;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

fn spec(tmp: &Path, dst: &Path, size: usize) -> PublishSpec {
    PublishSpec {
        tmp: tmp.to_path_buf(),
        dst: dst.to_path_buf(),
        runs: Runs(vec![Bytes::from(vec![0u8; size])]),
        tmp_mode: TmpMode::Trunc,
        sync: false,
    }
}

fn synced(tmp: &Path, dst: &Path, size: usize) -> PublishSpec {
    PublishSpec {
        sync: true,
        ..spec(tmp, dst, size)
    }
}

#[tokio::test]
async fn publication_accounts_replacements_and_leaves_failures_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = DiskUsage::default();
    let temp_path = dir.path().join("temp");
    let store_path = dir.path().join("001.store");
    for size in [100, 40, 200] {
        usage
            .publish(&fs, &queue, spec(&temp_path, &store_path, size))
            .await
            .unwrap();
        assert_eq!(usage.queue(&queue).bytes(), size as u64);
        assert_eq!(std::fs::metadata(&store_path).unwrap().len(), size as u64);
    }
    let missing_dir = dir.path().join("missing").join("002.store");
    assert!(
        usage
            .publish(&fs, &queue, spec(&temp_path, &missing_dir, 500))
            .await
            .is_err()
    );
    assert_eq!(usage.queue(&queue).bytes(), 200);
}

#[tokio::test]
async fn publication_waits_for_the_scan_lock() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = Arc::new(DiskUsage::default());
    let temp_path = dir.path().join("temp");
    let store_path = dir.path().join("001.store");
    let tracked = usage.queue(&queue);
    let scan = tracked.clone().exclusive().await;
    let publish_spec = spec(&temp_path, &store_path, 100);
    let mut publish = tokio::spawn(async move {
        usage.publish(&fs, &queue, publish_spec).await.unwrap();
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut publish)
            .await
            .is_err()
    );
    assert!(!store_path.exists());
    scan.set(50);
    drop(scan);
    publish.await.unwrap();
    assert_eq!(tracked.bytes(), 150);
    assert!(store_path.exists());
}

#[tokio::test]
async fn publications_of_one_queue_do_not_wait_for_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig {
        threads: 2,
        ..FsConfig::default()
    })
    .unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = Arc::new(DiskUsage::default());
    // A publish whose accounting is still pending holds the shared side: a
    // second publish goes through, a rescan waits. (tokio's lock is
    // write-preferring, so the rescan must not be queued before the publish.)
    let tracked = usage.queue(&queue);
    let held = tracked.gate.clone().read_owned().await;
    usage
        .publish(
            &fs,
            &queue,
            spec(&dir.path().join("temp"), &dir.path().join("001.store"), 100),
        )
        .await
        .unwrap();
    assert_eq!(tracked.bytes(), 100);
    let mut rescan = tokio::spawn(tracked.clone().exclusive());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut rescan)
            .await
            .is_err()
    );
    drop(held);
    assert_eq!(rescan.await.unwrap().get(), 100);
}

#[tokio::test]
async fn a_rename_is_counted_even_when_the_directory_sync_fails() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = DiskUsage::default();
    let first = dir.path().join("001.store");
    usage
        .publish(&fs, &queue, synced(&dir.path().join("a.tmp"), &first, 200))
        .await
        .unwrap();

    let sealed = dir.path().join("sealed");
    std::fs::create_dir(&sealed).unwrap();
    let second = sealed.join("002.store");
    // Write and search without read: the rename goes through, opening the
    // directory to sync it does not.
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o300)).unwrap();
    let failed = usage
        .publish(&fs, &queue, synced(&sealed.join("b.tmp"), &second, 100))
        .await;
    std::fs::set_permissions(&sealed, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(failed.is_err());
    assert_eq!(std::fs::metadata(&second).unwrap().len(), 100);
    assert_eq!(usage.queue(&queue).bytes(), 300);

    usage
        .publish(&fs, &queue, synced(&sealed.join("c.tmp"), &second, 100))
        .await
        .unwrap();
    assert_eq!(usage.queue(&queue).bytes(), 300);
}
