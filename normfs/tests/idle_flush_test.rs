//! A queue nobody writes to often must still reach disk.
//!
//! Both of the WAL's volume-driven flush triggers -- the pool's unwritten-bytes
//! watermark and the writer's buffer filling -- are reached by writing more.
//! A queue that takes one record a week reaches neither, so without a timer its
//! record would sit in memory until the next one arrived. `write_interval` is
//! that timer, and these tests are what say it is wired to the thing that
//! actually puts bytes on disk rather than merely running.

use std::time::{Duration, Instant};

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};
use normfs_types::{QueueId, QueueIdResolver};
use tempfile::TempDir;

fn wal_dir(base: &std::path::Path, queue: &QueueId) -> std::path::PathBuf {
    base.join(queue.to_string().trim_start_matches('/'))
        .join("wal")
}

/// Whether the queue's WAL files contain `needle` -- the record itself, not a
/// count of bytes. A byte count cannot tell the record from the header the
/// writer lays down when it opens the file, which is a way for this test to
/// pass while nothing was ever flushed.
fn wal_contains(dir: &std::path::Path, needle: &[u8]) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.filter_map(|e| e.ok()).any(|e| {
        std::fs::read(e.path())
            .map(|bytes| bytes.windows(needle.len()).any(|w| w == needle))
            .unwrap_or(false)
    })
}

/// One record, then silence. It must be on disk within a bounded wait, without
/// the queue being closed and without a second record arriving to push it.
#[tokio::test]
async fn a_lone_record_on_a_silent_queue_reaches_disk() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let mut settings = NormFsSettings::all_active();
    settings.wal_settings.write_interval = Duration::from_millis(20);

    let fs = NormFS::new(path.clone(), settings).await.unwrap();
    let queue = fs.resolve("weekly");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let resolver = QueueIdResolver::new(fs.get_instance_id());
    let dir = wal_dir(&path, &resolver.resolve("weekly"));

    // A tokio interval yields its first tick at once, and the writer's task is
    // spawned when the queue opens -- so without this the record could be
    // caught by a tick that was already due rather than by one the timer went
    // on to produce, and the test would pass with any interval at all.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let record = Bytes::from_static(b"one record, then a week of nothing");
    assert!(!wal_contains(&dir, &record), "nothing is written yet");
    fs.enqueue(&queue, record.clone()).await.unwrap();

    // No close, no second record: only the timer can put this on disk.
    let deadline = Instant::now() + Duration::from_secs(5);
    while !wal_contains(&dir, &record) {
        assert!(
            Instant::now() < deadline,
            "a record on a silent queue never reached disk: the flush timer is the only \
             trigger it has, and nothing else will come along to push it"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // And it reads back, so what landed is the record rather than a header the
    // writer happened to widen.
    fs.close().await.unwrap();
    drop(fs);
    let fs = NormFS::new(path.clone(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("weekly");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    fs.read(
        &queue,
        normfs::ReadPosition::Absolute(uintn::UintN::zero()),
        1,
        1,
        tx,
    )
    .await
    .unwrap();
    let got = rx.recv().await.expect("the record must read back");
    assert_eq!(got.data, record);
    fs.close().await.unwrap();
}

/// The timer keeps running after a long quiet stretch.
///
/// An interval armed by a write, or one whose ticks are swallowed while idle,
/// looks identical to a working one until the queue goes quiet for longer than
/// anyone tests. This goes quiet first and writes afterwards.
#[tokio::test]
async fn the_flush_timer_survives_a_long_idle_stretch() {
    let temp = TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let mut settings = NormFsSettings::all_active();
    settings.wal_settings.write_interval = Duration::from_millis(20);

    let fs = NormFS::new(path.clone(), settings).await.unwrap();
    let queue = fs.resolve("quiet");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let resolver = QueueIdResolver::new(fs.get_instance_id());
    let dir = wal_dir(&path, &resolver.resolve("quiet"));

    // Far longer than the interval, so any tick the writer was going to miss it
    // has missed by now.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let record = Bytes::from_static(b"after the quiet");
    assert!(!wal_contains(&dir, &record));
    fs.enqueue(&queue, record.clone()).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while !wal_contains(&dir, &record) {
        assert!(
            Instant::now() < deadline,
            "the flush timer stopped after an idle stretch"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    fs.close().await.unwrap();
}
