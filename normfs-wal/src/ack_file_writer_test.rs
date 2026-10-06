fn test_fs() -> normfs_fs::Fs {
    normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap()
}

use crate::ack_file_writer::{AckFileWriter, AckFileWriterSettings};

use super::*;
use bytes::Bytes;
use normfs_types::QueueIdResolver;
use std::fs;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::timeout;

async fn read_file_content(path: &Path) -> Vec<u8> {
    fs::read(path).unwrap()
}

#[tokio::test]
async fn test_write_single_entry_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
        ..Default::default()
    };

    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::new(),
        None,
        0,
    )
    .await
    .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("queue_1");
    let entry_id = UintN::from(1u64);
    let data = Bytes::from("hello world");

    writer
        .write(queue_id.clone(), entry_id.clone(), data.clone())
        .await;
    writer.close().await.unwrap();

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, data);

    let ack = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack, (queue_id, entry_id));
}

#[tokio::test]
async fn test_write_multiple_entries_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(100),
        fsync: true,
        ..Default::default()
    };

    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::new(),
        None,
        0,
    )
    .await
    .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id_1 = resolver.resolve("queue_1");
    let entry_id_1 = UintN::from(1u64);
    let data_1 = Bytes::from("entry 1");

    let queue_id_2 = resolver.resolve("queue_2");
    let entry_id_2 = UintN::from(10u64);
    let data_2 = Bytes::from("entry 2");

    writer
        .write(queue_id_1.clone(), entry_id_1.clone(), data_1.clone())
        .await;
    writer
        .write(queue_id_2.clone(), entry_id_2.clone(), data_2.clone())
        .await;

    writer.close().await.unwrap();

    let mut expected_content = Vec::new();
    expected_content.extend_from_slice(&data_1);
    expected_content.extend_from_slice(&data_2);

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(ack, (queue_id_2, entry_id_2));
}

#[tokio::test]
async fn test_buffer_full_triggers_write_and_ack() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test2.wal");

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 20,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_secs(5), // Long interval to ensure buffer full is the trigger
        fsync: true,
        ..Default::default()
    };

    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::new(),
        None,
        0,
    )
    .await
    .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id_1 = resolver.resolve("queue_1");
    let entry_id_1 = UintN::from(1u64);
    let data_1 = Bytes::from("1234567890");

    let queue_id_2 = resolver.resolve("queue_2");
    let entry_id_2 = UintN::from(2u64);
    let data_2 = Bytes::from("abcdefghij");

    let queue_id_3 = resolver.resolve("queue_3");
    let entry_id_3 = UintN::from(3u64);
    let data_3 = Bytes::from("last_one");

    writer
        .write(queue_id_1.clone(), entry_id_1.clone(), data_1.clone())
        .await;
    writer
        .write(queue_id_2.clone(), entry_id_2.clone(), data_2.clone())
        .await;

    // The first two writes should fill the buffer and trigger a flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut expected_content = Vec::new();
    expected_content.extend_from_slice(&data_1);
    expected_content.extend_from_slice(&data_2);

    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack2 = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack2, (queue_id_2, entry_id_2));

    // Write one more and close
    writer
        .write(queue_id_3.clone(), entry_id_3.clone(), data_3.clone())
        .await;
    writer.close().await.unwrap();

    expected_content.extend_from_slice(&data_3);
    let file_content = read_file_content(&file_path).await;
    assert_eq!(file_content, expected_content);

    let ack3 = timeout(Duration::from_secs(1), ack_receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack3, (queue_id_3, entry_id_3));
}

#[tokio::test]
async fn test_writer_with_header() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("test_writer_with_header.log");

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024,
        write_interval: Duration::from_millis(100),
        fsync: true,
        ..Default::default()
    };

    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();
    let header = Bytes::from_static(b"test header");

    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        header.clone(),
        None,
        0,
    )
    .await
    .unwrap();

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("test_queue");
    let entry_id = UintN::from(1u64);
    let entry_data = Bytes::from_static(b"test data");
    writer
        .write(queue_id.clone(), entry_id.clone(), entry_data.clone())
        .await;

    writer.close().await.unwrap();

    let file_content = tokio::fs::read(&file_path).await.unwrap();
    assert_eq!(&file_content[..header.len()], header.as_ref());
    assert_eq!(&file_content[header.len()..], entry_data.as_ref());

    let ack = ack_receiver.recv().await.unwrap();
    assert_eq!(ack, (queue_id, entry_id));
}

/// The page-write path: bytes reach the file straight from the pages they were
/// appended into, and the durable watermark moves only once they are there.
#[tokio::test]
async fn pages_reach_the_file_before_the_watermark_moves() {
    use crate::page_pool::PagePool;
    use crate::wal_ring_v1::AppendOutcome;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("paged.wal");
    let (ack_sender, _ack_receiver) = mpsc::unbounded_channel();

    let pool = Arc::new(PagePool::new(4, 4096, 0));
    let settings = AckFileWriterSettings {
        max_buffer_size: 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
        ..Default::default()
    };

    let header = Bytes::from_static(b"HDR!");
    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        header.clone(),
        Some(Arc::clone(&pool)),
        0,
    )
    .await
    .unwrap();

    // Nothing is durable before anything has been appended.
    assert_eq!(pool.durable_before(), 0);

    let record = b"a paged record";
    for _ in 0..3 {
        assert!(matches!(pool.try_append(record), AppendOutcome::Cached(_)));
    }
    let appended_through = pool.next_entry_id();

    // A flush writes nothing the writer has not taken responsibility for, in
    // the id order the writer's ordered buffer restores, so a record already in
    // its page cannot be written ahead of an earlier one still in flight. This
    // is that handover, and it is the same call the WAL writer makes.
    let queue = QueueIdResolver::new("test_instance").resolve("paged");
    for id in 0..appended_through {
        writer
            .write_maybe_pooled(queue.clone(), UintN::from(id), Bytes::new(), true)
            .await;
    }
    // The WAL writer marks the handover once per batch; these tests stand in
    // for it, so they mark what they handed.
    pool.note_handed_over(appended_through.saturating_sub(1));

    // The flush is on a timer; wait for the watermark rather than for a
    // duration, so the test asserts the ordering instead of racing it.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < appended_through {
        assert!(
            tokio::time::Instant::now() < deadline,
            "writer never reported the pages durable"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    writer.close().await.unwrap();

    // Once the watermark has moved, the bytes really are in the file: header
    // first, then each record's payload in append order.
    let content = read_file_content(&file_path).await;
    assert!(content.starts_with(&header), "header should lead the file");
    let occurrences = content
        .windows(record.len())
        .filter(|w| *w == record)
        .count();
    assert_eq!(
        occurrences, 3,
        "every appended record should appear exactly once in the file"
    );
}

/// Ported from feat/v1-page-pool 97fbd98: an entry still on its way to the
/// writer is not overtaken by later entries whose bytes are already in pages.
#[tokio::test]
async fn a_record_in_flight_is_not_overtaken_by_the_pages_behind_it() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("order.wal");
    let (ack_sender, _ack_receiver) = mpsc::unbounded_channel();

    let pool = Arc::new(PagePool::new(4, 4096, 0));
    pool.set_drainer();
    pool.arm_file_fill(10 * 1024 * 1024, 4);
    let settings = AckFileWriterSettings {
        max_buffer_size: 1024 * 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
        ..Default::default()
    };

    let header = Bytes::from_static(b"HDR!");
    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        header.clone(),
        Some(Arc::clone(&pool)),
        0,
    )
    .await
    .unwrap();

    let queue = QueueIdResolver::new("test_instance").resolve("order");
    let first = b"AAAA-entry-zero".as_slice();
    // Wide enough to fill its page on its own, so entries 1 and 2 land on
    // different pages and reach the file as two separate runs.
    let second = vec![b'B'; crate::page_pool::max_record_len(4096)];
    let third = b"CCCC-entry-two".as_slice();

    pool.place(0, first).await.unwrap();
    writer
        .write_maybe_pooled(queue.clone(), UintN::from(0u64), Bytes::new(), true)
        .await;
    pool.note_handed_over(0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "entry 0 never reached the file"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Entry 1 fills a page and entry 2 opens the next one. Neither is handed to
    // the writer yet, and several flush ticks pass while they wait: nothing may
    // reach the file in that window.
    pool.place(1, &second).await.unwrap();
    pool.place(2, third).await.unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        pool.durable_before(),
        1,
        "a flush wrote entries the writer was never handed"
    );

    writer
        .write_maybe_pooled(queue.clone(), UintN::from(1u64), Bytes::new(), true)
        .await;
    writer
        .write_maybe_pooled(queue.clone(), UintN::from(2u64), Bytes::new(), true)
        .await;
    pool.note_handed_over(2);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < 3 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "entries 1-2 never reached the file"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    writer.close().await.unwrap();

    let content = read_file_content(&file_path).await;
    let at = |needle: &[u8]| {
        content
            .windows(needle.len())
            .position(|w| w == needle)
            .unwrap_or_else(|| panic!("a record is missing from the file"))
    };
    assert!(
        at(first) < at(&second) && at(&second) < at(third),
        "records reached the file out of id order: 0 at {}, 1 at {}, 2 at {}",
        at(first),
        at(&second),
        at(third)
    );
}

/// After a failed attempt, the next write lands exactly at the known-good
/// length: no torn prefix left behind, and no hole where it was.
#[tokio::test]
async fn a_failed_commit_cuts_back_to_the_known_good_length() {
    use crate::ack_file_writer::{FileTail, commit};
    use normfs_fs::Runs;

    let dir = tempdir().unwrap();
    let path = dir.path().join("tail.wal");
    let fs = test_fs();
    let (file, inode) = fs
        .create_durable_with_inode(&path, Runs::default(), normfs_fs::TmpMode::Excl, true)
        .await
        .unwrap();
    let mut tail = FileTail {
        inode,
        needs_restore: false,
        file: std::sync::Arc::new(file),
        flushed_len: 0,
    };
    let settings = AckFileWriterSettings {
        max_retries: 1,
        ..AckFileWriterSettings::default()
    };

    let good = Runs(vec![Bytes::from_static(b"GOOD")]);
    assert!(commit(&fs, &path, &mut tail, good, &settings).await);
    assert_eq!(tail.flushed_len, 4);

    // A torn attempt: bytes reach the file, but the sync fails.
    normfs_fs::fault::fail_flushes(&path, 1);
    let torn = Runs(vec![Bytes::from_static(b"TORN-PREFIX")]);
    assert!(!commit(&fs, &path, &mut tail, torn, &settings).await);
    assert_eq!(tail.flushed_len, 4);

    let retry = Runs(vec![Bytes::from_static(b"RETRY")]);
    assert!(commit(&fs, &path, &mut tail, retry, &settings).await);

    let content = tokio::fs::read(&path).await.unwrap();
    assert_eq!(
        content, b"GOODRETRY",
        "the retry must start at the known-good length, with the torn prefix gone"
    );
}

/// A buffered entry is never acked from beyond a pooled one still in its page.
///
/// An ack is a durability watermark and the reader of the channel keeps the
/// highest it has seen, so acking entry 1 says entry 0 is on disk as well. The
/// two entries take different roads to the file -- one through the buffer, one
/// from the pool's pages -- and only the flush that carried each may report it.
/// Draining the ack list wholesale reported both durable when only one was.
#[tokio::test]
async fn a_buffered_ack_does_not_report_a_pooled_record_durable() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("mixed.wal");
    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let pool = Arc::new(PagePool::new(4, 4096, 0));
    pool.set_drainer();
    pool.arm_file_fill(10 * 1024 * 1024, 4);

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024 * 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
        ..Default::default()
    };
    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::from_static(b"HDR!"),
        Some(Arc::clone(&pool)),
        0,
    )
    .await
    .unwrap();

    let queue = QueueIdResolver::new("test_instance").resolve("mixed");

    // Entry 0 is in a page and is deliberately never handed over, so no flush
    // may write it: `take_pending` stops at the handover bound.
    pool.place(0, b"pooled-entry-zero".as_slice())
        .await
        .unwrap();
    writer
        .write_maybe_pooled(queue.clone(), UintN::from(0u64), Bytes::new(), true)
        .await;

    // Entry 1 goes through the buffer, so a flush does write it.
    writer
        .write_maybe_pooled(
            queue.clone(),
            UintN::from(1u64),
            Bytes::from_static(b"buffered-entry-one"),
            false,
        )
        .await;

    // Several flush ticks. Entry 1 reaches the file; entry 0 cannot.
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        pool.durable_before(),
        0,
        "entry 0 was never handed over, so nothing may have written it"
    );
    assert!(
        read_file_content(&file_path)
            .await
            .windows(18)
            .any(|w| w == b"buffered-entry-one"),
        "entry 1 should have been written from the buffer"
    );
    assert!(
        ack_receiver.try_recv().is_err(),
        "acking entry 1 reports entry 0 durable as well, and entry 0 is still in its page"
    );

    // Once entry 0 is handed over and flushed, both are durable and the
    // watermark may move -- to the highest, which covers both.
    pool.note_handed_over(0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "entry 0 never reached the file"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    writer.close().await.unwrap();

    let mut highest = None;
    while let Ok((_, id)) = ack_receiver.try_recv() {
        highest = Some(id);
    }
    assert_eq!(
        highest,
        Some(UintN::from(1u64)),
        "both entries are on disk now, so the watermark should have reached entry 1"
    );
}

/// An entry buffered while a flush's file I/O is in flight is not acked by
/// that flush. The one-byte buffer bound starts a flush on every write, and
/// fsync yields mid-I/O on this single-threaded runtime, so writing the next
/// entry the moment the previous one appears in the file lands it inside the
/// window where the flush has written but not yet drained its acks.
#[tokio::test]
async fn an_entry_buffered_during_a_flush_is_not_acked_by_it() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("during.wal");
    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_secs(3600),
        fsync: true,
        ..Default::default()
    };
    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::from_static(b"HDR!"),
        None,
        0,
    )
    .await
    .unwrap();
    let queue = QueueIdResolver::new("test_instance").resolve("during");

    let payload = |id: u64| format!("entry-{id:03}-payload");
    let assert_acked_bytes_on_disk = |id: UintN| {
        let id = id.to_u64().unwrap();
        let pat = payload(id);
        let content = fs::read(&file_path).unwrap();
        assert!(
            content.windows(pat.len()).any(|w| w == pat.as_bytes()),
            "entry {id} was acked while its bytes were still in the buffer"
        );
    };

    const ENTRIES: u64 = 20;
    for i in 0..ENTRIES {
        let body = Bytes::from(payload(i));
        writer
            .write_maybe_pooled(queue.clone(), UintN::from(i), body.clone(), false)
            .await;
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let on_disk = fs::read(&file_path)
                .unwrap()
                .windows(body.len())
                .any(|w| w == &body[..]);
            while let Ok((_, id)) = ack_receiver.try_recv() {
                assert_acked_bytes_on_disk(id);
            }
            if on_disk {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "entry {i} never reached the file"
            );
            tokio::task::yield_now().await;
        }
    }

    writer.close().await.unwrap();
    let mut highest = None;
    while let Ok((_, id)) = ack_receiver.try_recv() {
        assert_acked_bytes_on_disk(id.clone());
        highest = Some(id.to_u64().unwrap());
    }
    assert_eq!(
        highest,
        Some(ENTRIES - 1),
        "the closing flush reports the last entry once its bytes are down"
    );
}

/// A durable buffered entry's ack arrives without another write and without a
/// close. The stranding shape: the entry's bytes are written while a pooled
/// ack ahead of it holds the cut at zero, the pool's flush later drains only
/// its own ids, and the ack is left at the head, durable, waiting for a flush
/// with no other reason to run.
#[tokio::test]
async fn a_durable_buffered_ack_is_sent_without_another_write_or_a_close() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    let dir = tempdir().unwrap();
    let file_path = dir.path().join("stranded.wal");
    let (ack_sender, mut ack_receiver) = mpsc::unbounded_channel();

    let pool = Arc::new(PagePool::new(4, 4096, 0));
    pool.set_drainer();
    pool.arm_file_fill(10 * 1024 * 1024, 4);

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024 * 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_millis(10),
        fsync: true,
        ..Default::default()
    };
    let writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::from_static(b"HDR!"),
        Some(Arc::clone(&pool)),
        0,
    )
    .await
    .unwrap();
    let queue = QueueIdResolver::new("test_instance").resolve("stranded");

    // Entry 0 in a page, not yet handed over; entry 1 through the buffer.
    pool.place(0, b"pooled-entry-zero".as_slice())
        .await
        .unwrap();
    writer
        .write_maybe_pooled(queue.clone(), UintN::from(0u64), Bytes::new(), true)
        .await;
    writer
        .write_maybe_pooled(
            queue.clone(),
            UintN::from(1u64),
            Bytes::from_static(b"buffered-entry-one"),
            false,
        )
        .await;

    // Entry 1's bytes reach the file behind the held cut; then entry 0 is
    // handed over and becomes durable, draining its own ack.
    pool.note_handed_over(0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while pool.durable_before() < 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "entry 0 never reached the file"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // No further write and no close: the ack for entry 1 must still arrive.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let watermark = loop {
        if let Ok((_, id)) = ack_receiver.try_recv() {
            if id == UintN::from(1u64) {
                break id;
            }
            continue;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ack for entry 1 is stranded: its bytes are durable but nothing \
             delivers it until the next write or the close"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(watermark, UintN::from(1u64));
    drop(writer);
}

/// A close whose final flush cannot write reports the failure instead of
/// returning Ok: `rotate` keys wal_complete on this, and an Ok here would
/// have the store worker archive a truncated file. /dev/full accepts the
/// empty header (nothing to flush) and then refuses every write.
#[tokio::test]
async fn a_close_that_cannot_flush_says_so() {
    if !Path::new("/dev/full").exists() {
        return;
    }
    let (ack_sender, _ack_receiver) = mpsc::unbounded_channel();
    let settings = AckFileWriterSettings {
        max_buffer_size: 1024 * 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_secs(3600),
        fsync: false,
        ..Default::default()
    };
    let mut writer = AckFileWriter::new(
        test_fs(),
        "/dev/full",
        settings,
        ack_sender,
        Bytes::new(),
        None,
        0,
    )
    .await
    .expect("an empty header has nothing to flush, so construction succeeds");

    let queue = QueueIdResolver::new("test_instance").resolve("full");
    writer
        .write_maybe_pooled(
            queue,
            UintN::from(0u64),
            Bytes::from_static(b"doomed"),
            false,
        )
        .await;

    assert!(
        writer.close().await.is_err(),
        "the closing flush lost this entry's bytes, and close() must say so"
    );
}

/// The same claim as `a_close_that_cannot_flush_says_so`, on a real WAL path
/// and on every platform: `/dev/full` is Linux only and cannot be a WAL path,
/// so the branch the rotation-retry design keys on was never covered on a
/// developer's machine.
#[tokio::test]
async fn an_injected_flush_failure_makes_a_close_fail() {
    let tmp_dir = tempdir().unwrap();
    let file_path = tmp_dir.path().join("injected.wal");
    let (ack_sender, _ack_receiver) = mpsc::unbounded_channel();

    let settings = AckFileWriterSettings {
        max_buffer_size: 1024 * 1024,
        max_file_size: 10 * 1024 * 1024,
        write_interval: Duration::from_secs(3600),
        fsync: false,
        // Not the default thousand: watch it give up, don't wait ten seconds.
        max_retries: 2,
        retry_delay: Duration::from_millis(1),
    };
    crate::fail_flushes(&file_path, u32::MAX);

    let mut writer = AckFileWriter::new(
        test_fs(),
        &file_path,
        settings,
        ack_sender,
        Bytes::from_static(b"hdr"),
        None,
        0,
    )
    .await
    .unwrap();

    let queue = QueueIdResolver::new("test_instance").resolve("injected");
    writer
        .write_maybe_pooled(
            queue,
            UintN::from(0u64),
            Bytes::from_static(b"doomed"),
            false,
        )
        .await;

    assert!(
        writer.close().await.is_err(),
        "every flush attempt was made to fail, so the close owes the file bytes it never wrote"
    );

    crate::heal(&file_path);
    assert_eq!(read_file_content(&file_path).await, b"hdr");
}

#[tokio::test]
async fn failed_restore_blocks_later_batches_until_the_tail_is_repaired() {
    use crate::ack_file_writer::{FileTail, commit};
    use normfs_fs::{Runs, TmpMode};
    let dir = tempdir().unwrap();
    let path = dir.path().join("unrestored.wal");
    let fs = test_fs();
    let (file, inode) = fs
        .create_durable_with_inode(&path, Runs::default(), TmpMode::Excl, true)
        .await
        .unwrap();
    let mut tail = FileTail {
        inode,
        file: std::sync::Arc::new(file),
        flushed_len: 0,
        needs_restore: false,
    };
    let settings = AckFileWriterSettings {
        max_retries: 1,
        ..Default::default()
    };
    normfs_fs::fault::fail_flushes(&path, 1);
    normfs_fs::fault::fail_truncates(&path, 2);
    assert!(
        !commit(
            &fs,
            &path,
            &mut tail,
            Runs(vec![Bytes::from_static(b"long torn batch")]),
            &settings
        )
        .await
    );
    assert!(tail.needs_restore);
    assert!(
        !commit(
            &fs,
            &path,
            &mut tail,
            Runs(vec![Bytes::from_static(b"new")]),
            &settings
        )
        .await
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"long torn batch");
    assert_eq!(tail.flushed_len, 0);
    assert!(
        commit(
            &fs,
            &path,
            &mut tail,
            Runs(vec![Bytes::from_static(b"new")]),
            &settings
        )
        .await
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert!(!tail.needs_restore);
}
