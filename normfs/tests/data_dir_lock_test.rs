use bytes::Bytes;
use normfs::{CloudSettings, Error, NormFS, NormFsSettings, Persist, QueueSettings};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn a_data_dir_takes_one_instance_at_a_time() {
    let dir = TempDir::new().unwrap();
    let first = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();

    match NormFS::new(dir.path().to_path_buf(), NormFsSettings::default()).await {
        Err(Error::DataDirInUse) => {}
        Err(e) => panic!("expected DataDirInUse, got {e}"),
        Ok(_) => panic!("a second instance opened a data dir in use"),
    }

    first.close().await.unwrap();
    drop(first);
    let second = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    // Dropped without a close: the lock goes once its tasks have ended.
    drop(second);
    let third = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    third.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_lock_file_that_is_a_link_is_refused() {
    let dir = TempDir::new().unwrap();
    let outside = TempDir::new().unwrap();
    let target = outside.path().join("elsewhere");
    std::os::unix::fs::symlink(&target, dir.path().join(".lock")).unwrap();

    assert!(
        NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
            .await
            .is_err()
    );
    assert!(!target.exists());
}

fn memory_only(flush_interval: Duration) -> NormFsSettings {
    NormFsSettings {
        queue_settings: QueueSettings::default().with_default_persist(Persist::MEMORY),
        memory_pointers_flush_interval: flush_interval,
        ..NormFsSettings::default()
    }
}

// A drop used to free the directory while the pointer flusher of the dropped
// instance kept running, and its late write rolled the next ids back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_instance_cannot_roll_back_the_next_ones_ids() {
    let dir = TempDir::new().unwrap();
    let a = NormFS::new(
        dir.path().to_path_buf(),
        memory_only(Duration::from_millis(1500)),
    )
    .await
    .unwrap();
    let queue = a.resolve("probe/ids");
    a.ensure_queue_exists_for_write(&queue).await.unwrap();
    for _ in 0..3 {
        a.enqueue(&queue, Bytes::from_static(b"a")).await.unwrap();
    }
    drop(a);

    let b = NormFS::new(
        dir.path().to_path_buf(),
        memory_only(Duration::from_secs(3600)),
    )
    .await
    .unwrap();
    b.ensure_queue_exists_for_write(&queue).await.unwrap();
    let mut last = 0;
    for _ in 0..10 {
        last = b
            .enqueue(&queue, Bytes::from_static(b"b"))
            .await
            .unwrap()
            .to_u64()
            .unwrap();
    }
    b.close().await.unwrap();
    drop(b);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let c = NormFS::new(
        dir.path().to_path_buf(),
        memory_only(Duration::from_secs(3600)),
    )
    .await
    .unwrap();
    c.ensure_queue_exists_for_write(&queue).await.unwrap();
    let first = c
        .enqueue(&queue, Bytes::from_static(b"c"))
        .await
        .unwrap()
        .to_u64()
        .unwrap();
    c.close().await.unwrap();
    assert!(first > last, "id {first} handed out twice");
}

#[tokio::test]
async fn a_failed_open_leaves_the_data_dir_free() {
    let dir = TempDir::new().unwrap();
    let bad = NormFsSettings {
        cloud_settings: Some(CloudSettings {
            endpoint: "not a url".into(),
            bucket: "b".into(),
            region: "r".into(),
            access_key: "a".into(),
            secret_key: "s".into(),
            prefix: "p".into(),
        }),
        ..NormFsSettings::default()
    };
    assert!(NormFS::new(dir.path().to_path_buf(), bad).await.is_err());
    let fs = NormFS::new(dir.path().to_path_buf(), NormFsSettings::default())
        .await
        .unwrap();
    fs.close().await.unwrap();
}
