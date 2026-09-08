//! The risk is not that a refused record is lost -- the caller asked for that
//! -- but that refusing it moves the sequence. An id taken with no record
//! behind it is a gap the pool would later re-seed over, discarding records it
//! still holds.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, PersistenceMode, ReadPosition};
use tokio::sync::mpsc;
use uintn::UintN;

fn settings() -> NormFsSettings {
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;
    settings.wal_settings.max_file_size = 128 * 1024;
    settings
}

async fn read_all(normfs: &NormFS, queue: &normfs::QueueId, limit: u64) -> Vec<Bytes> {
    let (tx, mut rx) = mpsc::channel(limit as usize + 1);
    normfs
        .read(
            queue,
            ReadPosition::Absolute(UintN::from(0u64)),
            limit,
            1,
            tx,
        )
        .await
        .expect("read");
    let mut out = Vec::new();
    while let Some(entry) = rx.recv().await {
        out.push(entry.data);
    }
    out
}

#[tokio::test]
async fn try_enqueue_and_enqueue_share_one_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let normfs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .expect("normfs");
    let queue = normfs.resolve("mixed");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");

    let mut ids = Vec::new();
    for i in 0u8..16 {
        let data = Bytes::from(vec![i]);
        let id = if i % 2 == 0 {
            normfs
                .try_enqueue(&queue, data)
                .expect("room in an idle pool")
        } else {
            normfs.enqueue(&queue, data).await.expect("enqueue")
        };
        ids.push(id.to_u64().unwrap());
    }

    assert_eq!(ids, (0..16).collect::<Vec<u64>>());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        read_all(&normfs, &queue, 16).await,
        (0u8..16).map(|i| Bytes::from(vec![i])).collect::<Vec<_>>()
    );
}

/// Nothing here can hold the pool full, so the refusal comes from a record no
/// page can hold.
#[tokio::test]
async fn a_record_no_page_can_hold_is_refused_without_taking_an_id() {
    let dir = tempfile::tempdir().unwrap();
    let normfs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .expect("normfs");
    let queue = normfs.resolve("too-large");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");

    let first = normfs
        .try_enqueue(&queue, Bytes::from_static(b"small"))
        .expect("accepted");

    let too_wide = Bytes::from(vec![0u8; 512 * 1024]);
    assert!(matches!(
        normfs.try_enqueue(&queue, too_wide),
        Err(Error::RecordTooLarge(_))
    ));

    let next = normfs
        .try_enqueue(&queue, Bytes::from_static(b"after"))
        .expect("accepted");
    assert_eq!(
        next.to_u64().unwrap(),
        first.to_u64().unwrap() + 1,
        "the refused record left the sequence alone"
    );
}

#[tokio::test]
async fn a_closed_queue_refuses_a_try_the_same_way_it_refuses_a_wait() {
    let dir = tempfile::tempdir().unwrap();
    let normfs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .expect("normfs");
    let queue = normfs.resolve("closing");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");
    normfs
        .try_enqueue(&queue, Bytes::from_static(b"before"))
        .expect("accepted");
    normfs.close_queue(&queue).await.expect("close");

    assert!(matches!(
        normfs.try_enqueue(&queue, Bytes::from_static(b"after")),
        Err(Error::QueueClosed)
    ));
}

#[tokio::test]
async fn memory_only_accepts_a_try_because_it_never_waits() {
    let dir = tempfile::tempdir().unwrap();
    let settings = NormFsSettings {
        persistence_mode: PersistenceMode::MemoryOnly,
        max_disk_usage_per_queue: None,
        ..settings()
    };
    let normfs = NormFS::new(dir.path().to_path_buf(), settings)
        .await
        .expect("normfs");
    let queue = normfs.resolve("in-memory");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");

    for i in 0u8..32 {
        normfs
            .try_enqueue(&queue, Bytes::from(vec![i]))
            .expect("memory-only never refuses for want of a page");
    }
}

#[tokio::test]
async fn a_write_back_into_the_notifying_queue_is_refused_not_deadlocked() {
    let dir = tempfile::tempdir().unwrap();
    let normfs = Arc::new(
        NormFS::new(dir.path().to_path_buf(), settings())
            .await
            .expect("normfs"),
    );
    let queue = normfs.resolve("reentrant");
    normfs
        .ensure_queue_exists_for_write(&queue)
        .await
        .expect("queue");

    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let callback_normfs = Arc::clone(&normfs);
    let callback_queue = queue.clone();
    let callback_seen = Arc::clone(&seen);
    normfs
        .subscribe(
            &queue,
            Box::new(move |entries: &[(UintN, Bytes)]| {
                for _ in entries {
                    let outcome =
                        callback_normfs.try_enqueue(&callback_queue, Bytes::from_static(b"echo"));
                    callback_seen
                        .lock()
                        .unwrap()
                        .push(matches!(outcome, Err(Error::WouldBlock)));
                }
                true
            }),
        )
        .expect("subscribe");

    normfs
        .try_enqueue(&queue, Bytes::from_static(b"first"))
        .expect("accepted");

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![true],
        "the re-entrant write is refused, not hung"
    );
}
