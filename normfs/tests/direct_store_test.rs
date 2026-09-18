//! A store queue (`Persist::STORE`) turns each sealed memory page into one
//! store file with no `.wal` and no timer in between. Its contract: a file is
//! born when a page fills, on `flush_queue`, on `close_queue` and on
//! `NormFS::close`, and a crash loses at most the open page.

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use normfs::{
    ConfigError, DataSource, Error, NormFS, NormFsSettings, Persist, QueueConfig, QueueSettings,
    ReadPosition,
};
use normfs_wal::WalSettings;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

/// Four records of `RECORD_LEN` fit a page; the fifth opens the next.
const PAGE: usize = 4096;
const RECORD_LEN: usize = 1000;
const PER_PAGE: u64 = 4;

fn settings(queue_settings: QueueSettings) -> NormFsSettings {
    NormFsSettings {
        mem_page_size: PAGE,
        max_memory_usage: 64 * 1024,
        wal_settings: WalSettings {
            write_interval: Duration::from_millis(20),
            max_file_size: 1 << 20,
            ..Default::default()
        },
        queue_settings,
        ..NormFsSettings::all_active()
    }
}

fn store_settings() -> NormFsSettings {
    settings(QueueSettings::all_active().with_default_persist(Persist::STORE))
}

async fn open(root: &Path, settings: NormFsSettings) -> NormFS {
    NormFS::new(root.to_path_buf(), settings).await.unwrap()
}

async fn write(fs: &NormFS, queue: &normfs::QueueId, count: u64) {
    for _ in 0..count {
        fs.enqueue(queue, Bytes::from(vec![0x5A; RECORD_LEN]))
            .await
            .unwrap();
    }
}

fn files_with(dir: &Path, ext: &str) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().and_then(|s| s.to_str()) == Some(ext) {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

async fn wait_for_store_files(dir: &Path, want: usize) {
    for _ in 0..200 {
        if files_with(dir, "store").len() >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "expected {want} store files, found {}",
        files_with(dir, "store").len()
    );
}

async fn read_all(fs: &NormFS, queue: &normfs::QueueId, from: u64, count: u64) -> Vec<DataSource> {
    // `read` delivers before it returns, so the channel must hold the lot.
    let (tx, mut rx) = mpsc::channel(count as usize + 1);
    fs.read(
        queue,
        ReadPosition::Absolute(UintN::from(from)),
        count,
        1,
        tx,
    )
    .await
    .unwrap();
    let mut sources = Vec::new();
    for i in from..from + count {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived"))
            .expect("stream ended early");
        assert_eq!(entry.id.to_u64().unwrap(), i);
        assert_eq!(entry.data.len(), RECORD_LEN);
        sources.push(entry.source);
    }
    sources
}

#[tokio::test]
async fn a_full_page_is_one_store_file_and_there_is_no_wal() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), store_settings()).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    // More pages than the arena holds, so the first ones are evicted from
    // memory once they have landed.
    let pages = 40u64;
    write(&fs, &queue, pages * PER_PAGE).await;
    fs.flush_queue(&queue).await.unwrap();

    let store_dir = queue.to_store_dir(temp.path());
    assert_eq!(files_with(&store_dir, "store").len(), pages as usize);
    assert!(!queue.to_wal_dir(temp.path()).exists());

    // Early pages left memory, so the first records come off the store files,
    // and the ids are one unbroken run across the file boundaries.
    let sources = read_all(&fs, &queue, 0, pages * PER_PAGE).await;
    assert_eq!(sources[0], DataSource::DiskStore);
    assert_eq!(*sources.last().unwrap(), DataSource::Memory);

    fs.close().await.unwrap();
}

#[tokio::test]
async fn nothing_lands_until_a_page_fills_or_a_flush_asks() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), store_settings()).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    write(&fs, &queue, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        files_with(&queue.to_store_dir(temp.path()), "store").is_empty(),
        "well past write_interval, still nothing: this mode has no timer"
    );

    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(
        files_with(&queue.to_store_dir(temp.path()), "store").len(),
        1
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn records_after_a_flush_continue_the_page_and_the_ids() {
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), store_settings()).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 2).await;
        fs.flush_queue(&queue).await.unwrap();
        write(&fs, &queue, 2).await;
        fs.flush_queue(&queue).await.unwrap();
        assert_eq!(
            files_with(&queue.to_store_dir(temp.path()), "store").len(),
            2
        );
        fs.close().await.unwrap();
    }

    // A fresh instance has nothing in memory, so this crosses the file
    // boundary on disk: file 2 must say it starts at 2.
    let fs = open(temp.path(), store_settings()).await;
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 3);
    let sources = read_all(&fs, &queue, 0, 4).await;
    assert!(sources.iter().all(|s| *s == DataSource::DiskStore));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn close_queue_lands_the_tail_and_a_restart_continues_from_it() {
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), store_settings()).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, PER_PAGE + 1).await;
        fs.close_queue(&queue).await.unwrap();
        assert!(queue.to_fs_path(temp.path()).join("closed").is_file());
        assert_eq!(
            files_with(&queue.to_store_dir(temp.path()), "store").len(),
            2
        );
        fs.close().await.unwrap();
    }

    let fs = open(temp.path(), store_settings()).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), PER_PAGE);
    let id = fs
        .enqueue(&queue, Bytes::from(vec![1u8; RECORD_LEN]))
        .await
        .unwrap();
    assert_eq!(id.to_u64().unwrap(), PER_PAGE + 1);
    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(
        files_with(&queue.to_store_dir(temp.path()), "store").len(),
        3,
        "the new life writes the next file id, not over the old one"
    );
    fs.close().await.unwrap();
}

#[tokio::test]
async fn instance_close_lands_every_store_queues_tail() {
    let temp = tempfile::TempDir::new().unwrap();
    let (a, b);
    {
        let fs = open(temp.path(), store_settings()).await;
        a = fs.resolve("cam0");
        b = fs.resolve("cam1");
        for q in [&a, &b] {
            fs.ensure_queue_exists_for_write(q).await.unwrap();
            write(&fs, q, 1).await;
        }
        fs.close().await.unwrap();
    }
    let fs = open(temp.path(), store_settings()).await;
    for q in [&a, &b] {
        assert_eq!(read_all(&fs, q, 0, 1).await, [DataSource::DiskStore]);
    }
    fs.close().await.unwrap();
}

#[tokio::test]
async fn an_instance_dropped_without_close_loses_at_most_the_open_page() {
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        let fs = open(temp.path(), store_settings()).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, PER_PAGE + 2).await;
        wait_for_store_files(&queue.to_store_dir(temp.path()), 1).await;
        // No close: the two records on the open page never leave memory.
    }
    let fs = open(temp.path(), store_settings()).await;
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    assert_eq!(
        fs.get_last_id(&queue).unwrap().to_u64().unwrap(),
        PER_PAGE - 1,
        "the sealed page survived; the open page is the loss"
    );
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_wal_queue_and_a_store_queue_share_an_instance() {
    let temp = tempfile::TempDir::new().unwrap();
    let mixed = settings(
        QueueSettings::new(
            vec![(
                "**/cam*".to_string(),
                QueueConfig {
                    persist: Persist::STORE,
                    ..QueueConfig::active()
                },
            )],
            QueueConfig::active(),
        )
        .unwrap(),
    );
    let fs = open(temp.path(), mixed).await;
    let cam = fs.resolve("cam0");
    let tele = fs.resolve("telemetry");
    for q in [&cam, &tele] {
        fs.ensure_queue_exists_for_write(q).await.unwrap();
        write(&fs, q, 2).await;
    }
    fs.flush_queue(&cam).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(!cam.to_wal_dir(temp.path()).exists());
    assert_eq!(files_with(&cam.to_store_dir(temp.path()), "store").len(), 1);
    assert_eq!(files_with(&tele.to_wal_dir(temp.path()), "wal").len(), 1);
    assert!(files_with(&tele.to_store_dir(temp.path()), "store").is_empty());
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_moved_from_wal_to_store_archives_what_the_wal_left() {
    let temp = tempfile::TempDir::new().unwrap();
    let queue;
    {
        // A WAL life that ends without closing the queue leaves its file.
        let fs = open(temp.path(), settings(QueueSettings::all_active())).await;
        queue = fs.resolve("cam0");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write(&fs, &queue, 3).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(files_with(&queue.to_wal_dir(temp.path()), "wal").len(), 1);
    }

    let fs = open(temp.path(), store_settings()).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 2);
    wait_for_store_files(&queue.to_store_dir(temp.path()), 1).await;
    for _ in 0..200 {
        if files_with(&queue.to_wal_dir(temp.path()), "wal").is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(files_with(&queue.to_wal_dir(temp.path()), "wal").is_empty());

    write(&fs, &queue, 1).await;
    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(
        files_with(&queue.to_store_dir(temp.path()), "store").len(),
        2
    );
    assert_eq!(read_all(&fs, &queue, 0, 4).await.len(), 4);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_store_queue_registers_with_the_disk_monitor() {
    let temp = tempfile::TempDir::new().unwrap();
    let with_monitor = NormFsSettings {
        max_disk_usage_per_queue: Some(3 << 20),
        ..store_settings()
    };
    let fs = open(temp.path(), with_monitor).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    // A page ends when the next one opens, so one record past a full page.
    write(&fs, &queue, PER_PAGE + 1).await;
    wait_for_store_files(&queue.to_store_dir(temp.path()), 1).await;
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_rule_lives_beside_a_store_rule() {
    let temp = tempfile::TempDir::new().unwrap();
    let mixed = settings(
        QueueSettings::new(
            vec![(
                "**/scratch*".to_string(),
                QueueConfig {
                    persist: Persist::MEMORY,
                    ..QueueConfig::active()
                },
            )],
            QueueConfig {
                persist: Persist::STORE,
                ..QueueConfig::active()
            },
        )
        .unwrap(),
    );
    let (scratch, cam);
    {
        let fs = open(temp.path(), mixed.clone()).await;
        scratch = fs.resolve("scratch0");
        cam = fs.resolve("cam0");
        for q in [&scratch, &cam] {
            fs.ensure_queue_exists_for_write(q).await.unwrap();
            write(&fs, q, 2).await;
        }
        fs.close().await.unwrap();
        assert!(!scratch.to_store_dir(temp.path()).exists());
        assert!(!scratch.to_wal_dir(temp.path()).exists());
        assert_eq!(files_with(&cam.to_store_dir(temp.path()), "store").len(), 1);
    }
    let fs = open(temp.path(), mixed).await;
    fs.ensure_queue_exists_for_read(&scratch).await.unwrap();
    assert_eq!(fs.get_last_id(&scratch).unwrap().to_u64().unwrap(), 1);
    let (tx, _rx) = mpsc::channel(1);
    let err = fs
        .read(&scratch, ReadPosition::Absolute(UintN::zero()), 1, 1, tx)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound), "got {err:?}");
    assert_eq!(read_all(&fs, &cam, 0, 2).await, [DataSource::DiskStore; 2]);
    fs.close().await.unwrap();
}

#[test]
fn wal_without_store_is_refused() {
    let err = QueueSettings::new(
        Vec::new(),
        QueueConfig {
            persist: Persist {
                wal: true,
                store: false,
                cloud: false,
            },
            ..QueueConfig::default()
        },
    )
    .unwrap_err();
    assert!(
        matches!(err, ConfigError::WalWithoutStore { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn cloud_without_cloud_settings_is_refused() {
    let temp = tempfile::TempDir::new().unwrap();
    let wants_cloud = settings(QueueSettings::all_active().with_default_persist(Persist {
        cloud: true,
        ..Persist::WAL_STORE
    }));
    let err = NormFS::new(temp.path().to_path_buf(), wants_cloud)
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(err, Error::Config(ConfigError::CloudWithoutSettings { .. })),
        "got {err:?}"
    );
}
