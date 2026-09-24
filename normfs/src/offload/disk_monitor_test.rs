use super::*;
use normfs_types::QueueIdResolver;
use std::sync::Mutex;

fn write_store_file(root: &Path, queue: &QueueId, id: u64, len: usize) {
    let path = queue.to_store_path(root, &UintN::from(id));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, vec![0u8; len]).unwrap();
}

fn store_file_exists(root: &Path, queue: &QueueId, id: u64) -> bool {
    queue.to_store_path(root, &UintN::from(id)).exists()
}

#[tokio::test]
async fn the_tracked_size_follows_completions_and_deletions() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=4 {
        write_store_file(root, &queue, id, 100);
    }

    let forgotten = Arc::new(Mutex::new(Vec::new()));
    let log = forgotten.clone();
    let forget: ForgetRange =
        Arc::new(move |_: &QueueId, id: &UintN| log.lock().unwrap().push(id.clone()));
    let monitor = DiskMonitor::new(
        root,
        None,
        None,
        Some(forget),
        Arc::new(DiskUsage::default()),
    )
    .await
    .unwrap();

    let config = DiskMonitorConfig {
        max_size: 250,
        check_interval: Duration::from_secs(60),
        wal_settings: WalSettings {
            max_file_size: 10,
            ..Default::default()
        },
    };
    monitor.add_queue(&queue, config).await.unwrap();

    assert!(!store_file_exists(root, &queue, 1));
    assert!(!store_file_exists(root, &queue, 2));
    assert!(store_file_exists(root, &queue, 3));
    assert_eq!(
        *forgotten.lock().unwrap(),
        vec![UintN::from(1u64), UintN::from(2u64)]
    );

    let monitors = monitor.monitors.read().await;
    let queue_monitor = monitors.get(&queue).unwrap();
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    // A file nobody reported is not counted: the directory is not walked.
    write_store_file(root, &queue, 6, 100);
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    let temp_file = root.join("new-store-file");
    std::fs::write(&temp_file, vec![0; 100]).unwrap();
    monitor
        .disk_usage
        .publish(
            &queue,
            &temp_file,
            &queue.to_store_path(root, &UintN::from(5u64)),
            100,
        )
        .await
        .unwrap();
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 300);

    queue_monitor.check_and_cleanup(false).await.unwrap();
    assert!(!store_file_exists(root, &queue, 3));
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);

    // The periodic rescan picks up the unreported file.
    queue_monitor.check_and_cleanup(true).await.unwrap();
    assert!(!store_file_exists(root, &queue, 4));
    assert!(store_file_exists(root, &queue, 5));
    assert!(store_file_exists(root, &queue, 6));
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 200);
}

#[test]
fn the_c_layout_matches_to_file_path() {
    let store = Path::new("/data/inst/cam/store");
    let wal = Path::new("/data/inst/cam/wal");
    let mut ids: Vec<UintN> = [0u64, 1, 0xfff, 0x1000, 0xabcdef, u64::MAX]
        .into_iter()
        .map(UintN::from)
        .collect();
    ids.push(UintN::from(u128::MAX));
    ids.push(UintN::from_hex_digits(&"f".repeat(48)).unwrap());

    for id in &ids {
        assert_eq!(
            file_path(store, FileKind::Store, id).unwrap(),
            id.to_file_path(store.to_str().unwrap(), "store")
        );
        assert_eq!(
            file_path(wal, FileKind::Wal, id).unwrap(),
            id.to_file_path(wal.to_str().unwrap(), "wal")
        );
    }

    let past_the_layout = UintN::from_hex_digits(&format!("1{}", "0".repeat(48))).unwrap();
    assert!(file_path(store, FileKind::Store, &past_the_layout).is_err());
}

#[tokio::test]
async fn eviction_never_passes_the_offloaded_bound() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=4 {
        write_store_file(root, &queue, id, 100);
    }
    let store = queue.to_store_dir(root);
    let wal = queue.to_wal_dir(root);

    let bound = UintN::from(2u64);
    let eviction = evict(&store, &wal, &UintN::from(1u64), Some(&bound), 1000).unwrap();
    assert_eq!(eviction.stop, Stop::Bound);
    assert_eq!(eviction.next, UintN::from(3u64));
    assert_eq!(eviction.events.len(), 2);
    assert!(eviction.events.iter().all(|e| e.result.is_ok()));
    assert!(!store_file_exists(root, &queue, 2));
    assert!(store_file_exists(root, &queue, 3));

    let eviction = evict(&store, &wal, &UintN::from(3u64), None, 150).unwrap();
    assert_eq!(eviction.stop, Stop::Freed);
    assert_eq!(eviction.next, UintN::from(5u64));
    assert!(!store_file_exists(root, &queue, 4));

    let eviction = evict(&store, &wal, &UintN::from(5u64), None, 150).unwrap();
    assert_eq!(eviction.stop, Stop::Gap);
    assert!(eviction.events.is_empty());
}

#[tokio::test]
async fn delayed_and_duplicate_completions_do_not_count_scanned_files_again() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=2 {
        write_store_file(root, &queue, id, 100);
    }
    let usage = Arc::new(DiskUsage::default());
    let monitor = DiskMonitor::new(root, None, None, None, usage.clone())
        .await
        .unwrap();
    monitor
        .add_queue(
            &queue,
            DiskMonitorConfig {
                max_size: 300,
                check_interval: Duration::from_secs(60),
                wal_settings: WalSettings {
                    max_file_size: 10,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    monitor
        .store_file_done(&queue, UintN::from(1u64))
        .await
        .unwrap();
    let temp_file = root.join("new-store-file");
    std::fs::write(&temp_file, vec![0; 100]).unwrap();
    usage
        .publish(
            &queue,
            &temp_file,
            &queue.to_store_path(root, &UintN::from(3u64)),
            100,
        )
        .await
        .unwrap();
    let monitors = monitor.monitors.read().await;
    let queue_monitor = monitors.get(&queue).unwrap();
    queue_monitor.rescan_store().await.unwrap();
    drop(monitors);
    for _ in 0..2 {
        monitor
            .store_file_done(&queue, UintN::from(3u64))
            .await
            .unwrap();
    }
    let monitors = monitor.monitors.read().await;
    let queue_monitor = monitors.get(&queue).unwrap();
    assert_eq!(queue_monitor.get_queue_size().await.unwrap(), 300);
    queue_monitor.check_and_cleanup(false).await.unwrap();
    for id in 1..=3 {
        assert!(store_file_exists(root, &queue, id));
    }
}

#[tokio::test]
async fn concurrent_rescans_and_out_of_order_publications_preserve_usage() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let usage = Arc::new(DiskUsage::default());
    let monitor = QueueMonitor::new(
        queue.clone(),
        DiskMonitorConfig {
            max_size: 10_000,
            check_interval: Duration::from_secs(60),
            wal_settings: WalSettings {
                max_file_size: 10,
                ..Default::default()
            },
        },
        root.to_path_buf(),
        None,
        None,
        None,
        usage.clone(),
    )
    .await
    .unwrap();
    std::fs::create_dir_all(queue.to_store_dir(root)).unwrap();
    let publish = async {
        for id in (1..=32u64).rev() {
            let temp_file = root.join("new-store-file");
            tokio::fs::write(&temp_file, vec![0; 100]).await.unwrap();
            usage
                .publish(
                    &queue,
                    &temp_file,
                    &queue.to_store_path(root, &UintN::from(id)),
                    100,
                )
                .await
                .unwrap();
        }
    };
    let rescan = async {
        for _ in 0..32 {
            monitor.rescan_store().await.unwrap();
        }
    };
    tokio::join!(publish, rescan);
    assert_eq!(monitor.get_queue_size().await.unwrap(), 3200);
    assert_eq!(
        scan(&queue.to_store_dir(root), FileKind::Store)
            .unwrap()
            .total,
        3200
    );
}

async fn seeded_monitor(root: &Path, queue: &QueueId, max_size: usize) -> QueueMonitor {
    QueueMonitor::new(
        queue.clone(),
        DiskMonitorConfig {
            max_size,
            check_interval: Duration::from_secs(60),
            wal_settings: WalSettings {
                max_file_size: 10,
                ..Default::default()
            },
        },
        root.to_path_buf(),
        None,
        None,
        None,
        Arc::new(DiskUsage::default()),
    )
    .await
    .unwrap()
}

async fn publish_store_file(monitor: &QueueMonitor, root: &Path, queue: &QueueId, id: u64) {
    let temp_file = root.join("new-store-file");
    std::fs::write(&temp_file, vec![0; 100]).unwrap();
    let path = queue.to_store_path(root, &UintN::from(id));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::rename(&temp_file, &path).unwrap();
    *monitor.store_bytes.lock().await += 100;
}

#[tokio::test]
async fn cleanup_resumes_at_the_cursor_without_walking_the_store() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=4 {
        write_store_file(root, &queue, id, 100);
    }
    let monitor = seeded_monitor(root, &queue, 250).await;
    monitor.check_and_cleanup(false).await.unwrap();
    assert!(!store_file_exists(root, &queue, 2));

    // Behind the cursor and never reported: only a walk would find it.
    write_store_file(root, &queue, 1, 100);
    publish_store_file(&monitor, root, &queue, 5).await;
    monitor.check_and_cleanup(false).await.unwrap();

    assert!(store_file_exists(root, &queue, 1));
    assert!(!store_file_exists(root, &queue, 3));
    assert!(store_file_exists(root, &queue, 4));
    assert_eq!(*monitor.cursor.lock().unwrap(), Some(UintN::from(4u64)));
}

#[tokio::test]
async fn a_cursor_left_on_a_gap_is_moved_to_the_oldest_file() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    for id in 1..=6 {
        write_store_file(root, &queue, id, 100);
    }
    let monitor = seeded_monitor(root, &queue, 350).await;
    monitor.check_and_cleanup(false).await.unwrap();
    assert!(!store_file_exists(root, &queue, 3));
    assert!(store_file_exists(root, &queue, 4));

    std::fs::remove_file(queue.to_store_path(root, &UintN::from(4u64))).unwrap();
    publish_store_file(&monitor, root, &queue, 7).await;
    monitor.check_and_cleanup(false).await.unwrap();

    assert!(!store_file_exists(root, &queue, 5));
    assert!(store_file_exists(root, &queue, 6));
    assert_eq!(*monitor.cursor.lock().unwrap(), Some(UintN::from(6u64)));
}

#[tokio::test]
async fn a_file_that_cannot_be_deleted_holds_cleanup_at_its_id() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    // 0x1fff and 0x2000 sit in different chunk directories.
    for id in [0x1fffu64, 0x2000, 0x2001] {
        write_store_file(root, &queue, id, 100);
    }
    let locked = queue
        .to_store_path(root, &UintN::from(0x1fffu64))
        .parent()
        .unwrap()
        .to_path_buf();
    let probe = locked.join("probe");
    std::fs::write(&probe, b"").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o500)).unwrap();
    if std::fs::remove_file(&probe).is_ok() {
        // root unlinks regardless of the directory mode.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }

    let monitor = seeded_monitor(root, &queue, 150).await;
    let result = monitor.check_and_cleanup(false).await;
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
    result.unwrap();

    assert!(store_file_exists(root, &queue, 0x1fff));
    assert!(store_file_exists(root, &queue, 0x2000));
    assert_eq!(
        *monitor.cursor.lock().unwrap(),
        Some(UintN::from(0x1fffu64))
    );
    assert_eq!(monitor.get_queue_size().await.unwrap(), 300);

    monitor.check_and_cleanup(false).await.unwrap();
    assert!(!store_file_exists(root, &queue, 0x1fff));
    assert!(!store_file_exists(root, &queue, 0x2000));
    assert!(store_file_exists(root, &queue, 0x2001));
}
