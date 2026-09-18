//! Close is a promise with three parts: no write is accepted until the queue
//! is started for write again, everything written stays readable, and a
//! follower is told "that was the last record" instead of waiting forever.
//! Each test pins one part.

use std::time::Duration;

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition};
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

async fn write_records(fs: &NormFS, queue: &normfs::QueueId, count: u64) {
    for i in 0..count {
        fs.enqueue(queue, Bytes::from(format!("record-{i}")))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn a_closed_queue_refuses_writes_and_keeps_its_data_readable() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 5).await;

    fs.close_queue(&queue).await.unwrap();

    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "a write after close must be refused as QueueClosed, got {refused:?}"
    );

    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 5, 1, tx)
        .await
        .unwrap();
    for i in 0..5u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} must stay readable after close"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_closed_queue_starts_again_and_continues_its_ids() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("device");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();
    let marker = queue.to_fs_path(temp.path()).join("closed");
    assert!(marker.is_file(), "close must leave its marker");

    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert!(!marker.exists(), "a reopen must remove the marker");
    let id = fs
        .enqueue(&queue, Bytes::from_static(b"record-3"))
        .await
        .unwrap();
    assert_eq!(
        id,
        UintN::from(3u64),
        "ids must continue where the close left them"
    );

    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 4, 1, tx)
        .await
        .unwrap();
    for i in 0..4u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived"))
            .expect("the stream ended early");
        assert_eq!(entry.id, UintN::from(i));
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close_queue(&queue).await.unwrap();
    assert!(
        marker.is_file(),
        "a second close must leave its marker again"
    );
    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "got {refused:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_only_queue_starts_again_after_a_close() {
    let temp = tempfile::TempDir::new().unwrap();
    let settings = NormFsSettings {
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::MEMORY),
        max_disk_usage_per_queue: None,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("device");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 2).await;
    fs.close_queue(&queue).await.unwrap();

    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let id = fs
        .enqueue(&queue, Bytes::from_static(b"record-2"))
        .await
        .unwrap();
    assert_eq!(
        id,
        UintN::from(2u64),
        "ids must continue where the close left them"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_close_survives_a_restart_until_the_next_write_start() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");

    fs.ensure_queue_exists_for_read(&queue).await.unwrap();
    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "a restart must not resurrect a closed queue on its own, got {refused:?}"
    );
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 3, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} must stay readable across the restart"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let id = fs
        .enqueue(&queue, Bytes::from_static(b"record-3"))
        .await
        .unwrap();
    assert_eq!(
        id,
        UintN::from(3u64),
        "ids must continue across the restart and the reopen"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_follow_on_a_closed_queue_ends_at_the_last_record() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();

    // Limit 0 is a follow. On a live queue it would subscribe and wait; on a
    // closed one it must deliver the backlog and end.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("backlog record {i} never arrived"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(i));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the follow must end after the backlog of a closed queue, not wait forever");
    assert!(
        end.is_none(),
        "nothing can follow the last record of a closed queue, got {end:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_live_follower_ends_when_the_queue_closes() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 2).await;

    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..2u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} never arrived on the follow"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(i));
    }

    fs.close_queue(&queue).await.unwrap();

    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("closing the queue must end the live follow, not leave it waiting");
    assert!(
        end.is_none(),
        "no record can arrive after close, got {end:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn an_enqueue_on_a_never_started_queue_is_an_error_not_a_panic() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("never-started");

    // No ensure call: this is the map-miss path a racing close also takes.
    let refused = fs.enqueue(&queue, Bytes::from_static(b"lost")).await;
    assert!(
        matches!(refused, Err(Error::QueueNotFound)),
        "a record the map cannot place must be refused, got {refused:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_tail_follow_on_a_closed_queue_ends_at_the_last_record() {
    let temp = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("doomed");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        write_records(&fs, &queue, 3).await;
        fs.close_queue(&queue).await.unwrap();
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_read(&queue).await.unwrap();

    // A follow one record back from the tail: ids 1 and 2, then the end.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(
        &queue,
        ReadPosition::ShiftFromTail(UintN::from(1u64)),
        0,
        1,
        tx,
    )
    .await
    .unwrap();
    for expected in [1u64, 2] {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {expected} never arrived on the tail follow"))
            .expect("the stream ended inside the backlog");
        assert_eq!(entry.id, UintN::from(expected));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the tail follow must end after the last record of a closed queue");
    assert!(
        end.is_none(),
        "nothing can follow the last record, got {end:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_follow_after_an_in_process_close_delivers_the_backlog() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Same process, no restart, no re-ensure: the queue is gone from the
    // memory map and only the closed record knows where the sequence ended.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("backlog record {i} never arrived"))
            .unwrap_or_else(|| panic!("the stream ended at record {i}, swallowing the backlog"));
        assert_eq!(entry.id, UintN::from(i));
    }
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the follow must end after the backlog");
    assert!(end.is_none());

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_batch_after_close_is_refused() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close_queue(&queue).await.unwrap();

    let refused = fs
        .enqueue_batch(
            &queue,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        )
        .await;
    assert!(
        matches!(refused, Err(Error::QueueClosed)),
        "a batch after close must be refused, got {refused:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_second_close_is_ok_and_a_never_written_queue_closes() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    let queue = fs.resolve("silent");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close_queue(&queue).await.unwrap();
    fs.close_queue(&queue)
        .await
        .expect("a second close has nothing left to do and succeeds");

    let refused = fs.enqueue(&queue, Bytes::from_static(b"late")).await;
    assert!(matches!(refused, Err(Error::QueueClosed)));

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_closed_queues_file_migrates_to_the_store() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Close is this file's only completion: no writer will ever rotate it
    // out, so if close does not migrate it, nothing ever will.
    let wal_dir = temp
        .path()
        .join(fs.get_instance_id())
        .join("doomed")
        .join("wal");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let wal_files = std::fs::read_dir(&wal_dir)
            .map(|d| d.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        if wal_files == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the closed queue's final WAL file never migrated to the store"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // And the data survives the migration.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 3, 1, tx)
        .await
        .unwrap();
    for i in 0..3u64 {
        let entry = timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| panic!("record {i} unreadable after migration"))
            .expect("the stream ended early");
        assert_eq!(entry.data, Bytes::from(format!("record-{i}")));
    }

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_queue_named_like_a_marker_does_not_close_its_parent() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    // "parent/closed" is a legitimate queue name; its *directory* must not
    // read as parent's closed *marker*.
    let inner = fs.resolve("parent/closed");
    fs.ensure_queue_exists_for_write(&inner).await.unwrap();
    fs.enqueue(&inner, Bytes::from_static(b"data"))
        .await
        .unwrap();

    let parent = fs.resolve("parent");
    fs.ensure_queue_exists_for_write(&parent)
        .await
        .expect("a child queue named 'closed' must not close its parent");
    fs.enqueue(&parent, Bytes::from_static(b"alive"))
        .await
        .unwrap();

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_zero_offset_tail_follow_on_a_closed_queue_delivers_nothing() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("doomed");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write_records(&fs, &queue, 3).await;
    fs.close_queue(&queue).await.unwrap();

    // Live semantics for ShiftFromTail(0) + follow are "from the next
    // record"; on a closed queue the next record can never exist.
    let (tx, mut rx) = mpsc::channel(16);
    fs.read(&queue, ReadPosition::ShiftFromTail(UintN::zero()), 0, 1, tx)
        .await
        .unwrap();
    let end = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a zero-offset tail follow on a closed queue must end at once");
    assert!(
        end.is_none(),
        "nothing follows the tail of a closed queue, got {end:?}"
    );

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_close_of_an_unknown_name_is_refused_not_certified() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    let typo = fs.resolve("tpyo");
    let refused = fs.close_queue(&typo).await;
    assert!(
        matches!(refused, Err(Error::QueueNotFound)),
        "closing a name nothing ever wrote must be refused, got {refused:?}"
    );

    // The real spelling is not bricked: the name still opens for writing.
    fs.ensure_queue_exists_for_write(&typo)
        .await
        .expect("a refused close must leave nothing behind");
    fs.enqueue(&typo, Bytes::from_static(b"first"))
        .await
        .unwrap();

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_colliding_marker_path_fails_the_close_and_leaves_the_queue_writable() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();

    let child = fs.resolve("parent/closed");
    fs.ensure_queue_exists_for_write(&child).await.unwrap();
    fs.enqueue(&child, Bytes::from_static(b"child"))
        .await
        .unwrap();

    let parent = fs.resolve("parent");
    fs.ensure_queue_exists_for_write(&parent).await.unwrap();
    fs.enqueue(&parent, Bytes::from_static(b"one"))
        .await
        .unwrap();

    let refused = fs.close_queue(&parent).await;
    assert!(
        refused.is_err(),
        "the child dir occupies the marker path; close must refuse, got Ok"
    );

    // Refused before anything changed: the parent still accepts writes.
    fs.enqueue(&parent, Bytes::from_static(b"two"))
        .await
        .expect("a refused close must not leave the queue half closed");

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_close_of_a_never_written_queue_migrates_nothing() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("silent");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close_queue(&queue).await.unwrap();

    // A header-only file has nothing for the store worker: completing it
    // writes an empty store file and a last id computed from nothing.
    let store_dir = temp
        .path()
        .join(fs.get_instance_id())
        .join("silent")
        .join("store");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let store_files = std::fs::read_dir(&store_dir)
        .map(|d| d.filter_map(|e| e.ok()).count())
        .unwrap_or(0);
    assert_eq!(
        store_files, 0,
        "a file with no entries must not migrate to the store"
    );

    fs.close().await.unwrap();
}

/// The marker means "everything this queue accepted is on disk", which is not
/// true while a retrier still holds a file's records -- however durable the
/// pages look, and they do look durable, because the retry took an owned copy
/// and let the pool recycle them.
#[tokio::test]
async fn a_close_waits_for_a_file_whose_flush_failed() {
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().to_path_buf();

    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;
    // Below one page, so every page that opens rotates the file.
    settings.wal_settings.max_file_size = 1024;
    // Not the default thousand: watch it give up, don't wait ten seconds.
    settings.wal_settings.flush_max_retries = 2;
    settings.wal_settings.flush_retry_delay = Duration::from_millis(1);

    let fs = NormFS::new(path.clone(), settings).await.unwrap();
    let queue = fs.resolve("torn");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let wal_dir = queue.to_wal_dir(&path);
    let first = UintN::from(1u64).to_file_path(wal_dir.to_str().unwrap(), "wal");
    normfs_wal::fail_flushes(&first, u32::MAX);

    for i in 0..300u64 {
        fs.enqueue(
            &queue,
            Bytes::from(format!("r-{i:04}-{}", "x".repeat(1500))),
        )
        .await
        .unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match fs.close_queue(&queue).await {
            Err(Error::Wal(normfs_wal::WalError::CloseIncomplete)) => break,
            other => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "file 1's flush was made to fail, so a close must not certify this \
                     queue; got {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    normfs_wal::heal(&first);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match fs.close_queue(&queue).await {
            Ok(()) => break,
            other => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the retrier never landed file 1's tail after the writes started \
                     working, so the close never became truthful; last answer {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
