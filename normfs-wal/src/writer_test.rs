fn test_fs() -> normfs_fs::Fs {
    static FS: std::sync::OnceLock<normfs_fs::Fs> = std::sync::OnceLock::new();
    FS.get_or_init(|| normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap())
        .clone()
}

use std::sync::Once;
use std::time::Duration;

use crate::{
    Placement, WalSettings, WalStore,
    reader::{
        ReadRangeResult, get_wal_content, get_wal_range, read_wal_bytes_range, read_wal_file_range,
    },
    wal_entry::WalEntryHeader,
    wal_header::WalHeader,
};
use bytes::{Bytes, BytesMut};
use normfs_types::{DataSource, QueueIdResolver};
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

static INIT: Once = Once::new();

fn init_logger() {
    INIT.call_once(|| {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .init();
    });
}

#[tokio::test]
async fn test_enqueue_and_read() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from("hello");
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from("world");
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &test_fs(),
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));
    assert_eq!(read_entry1.source, DataSource::DiskWal);

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
    assert_eq!(read_entry2.source, DataSource::DiskWal);
}

#[tokio::test]
async fn test_enqueue_batch_and_read() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entries = vec![
        (UintN::from(0u64), Bytes::from("hello"), Placement::legacy()),
        (UintN::from(1u64), Bytes::from("world"), Placement::legacy()),
    ];
    store.enqueue_batch(&queue_id, entries).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &test_fs(),
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::Complete));

    let read_entry1 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry1.id, UintN::from(0u64));
    assert_eq!(read_entry1.data, Bytes::from("hello"));
    assert_eq!(read_entry1.source, DataSource::DiskWal);

    let read_entry2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_entry2.id, UintN::from(1u64));
    assert_eq!(read_entry2.data, Bytes::from("world"));
    assert_eq!(read_entry2.source, DataSource::DiskWal);
}

#[tokio::test]
async fn test_size_based_rotation() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut wal_complete_receiver) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default();
    let settings = WalSettings {
        max_file_size: 128,
        write_buffer_size: 64,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from(vec![0; 64]);
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from(vec![0; 64]);
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    store.close().await.unwrap();

    let received = wal_complete_receiver.recv().await.unwrap();
    assert_eq!(received.queue_id, queue_id);
    assert_eq!(received.file_id, file_id);

    let content1 = get_wal_content(&test_fs(), &queue_id.to_wal_dir(tmp_dir.path()), &file_id)
        .await
        .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = get_wal_content(
        &test_fs(),
        &queue_id.to_wal_dir(tmp_dir.path()),
        &file_id.increment(),
    )
    .await
    .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
}

/// End-to-end V1: the writer emits `[varint][record][crc]` entries with no id,
/// and every reader path derives the id from num_entries_before + index.
#[tokio::test]
async fn test_v1_enqueue_read_and_scan() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("v1_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader::default(); // num_entries_before = 0
    let settings = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    // Empty and varying-width records; ids must be enqueued as num_entries_before + i.
    let records: Vec<Bytes> = vec![
        Bytes::from_static(b"alpha"),
        Bytes::from_static(b""),
        Bytes::from_static(
            b"a longer gamma record crossing a two byte varint boundary ..............",
        ),
    ];
    for (i, r) in records.iter().enumerate() {
        store
            .enqueue(&queue_id, UintN::from(i as u64), r.clone())
            .unwrap();
    }
    store.close().await.unwrap();

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    // The file is genuinely V1: the header version word (u64 LE) is 1.
    let raw = tokio::fs::read(file_id.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    assert_eq!(raw[0], 1, "new file must carry the V1 header version");

    // read_wal_file_range: ids are 0,1,2 derived from position, data intact.
    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        &test_fs(),
        &wal_dir,
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(2u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    assert!(matches!(result, ReadRangeResult::Complete));
    for (i, r) in records.iter().enumerate() {
        let e = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(e.id, UintN::from(i as u64));
        assert_eq!(e.data, *r);
    }

    // get_wal_content + get_wal_range agree on the count and the id range.
    let content = get_wal_content(&test_fs(), &wal_dir, &file_id)
        .await
        .unwrap();
    assert_eq!(content.num_entries, UintN::from(3u64));
    assert_eq!(content.entries_before, UintN::from(0u64));

    let (_, range) = get_wal_range(&test_fs(), &wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(2u64))));

    // read_wal_bytes_range over the same content, with a step, hits the V1
    // in-memory path and its zero-copy record slices.
    let (tx2, mut rx2) = mpsc::channel(10);
    read_wal_bytes_range(
        &content.content,
        &UintN::from(0u64),
        &Some(UintN::from(2u64)),
        2, // step: ids 0 and 2
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);
    let first = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.id, UintN::from(0u64));
    assert_eq!(first.data, records[0]);
    let third = timeout(Duration::from_secs(1), rx2.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.id, UintN::from(2u64));
    assert_eq!(third.data, records[2]);
}

/// A truncated V1 tail is dropped: the scan stops at the last whole entry so
/// the derived ids of the surviving prefix stay correct.
#[tokio::test]
async fn test_v1_truncated_tail_is_dropped() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("v1_trunc");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();
    for i in 0..3u64 {
        store
            .enqueue(
                &queue_id,
                UintN::from(i),
                Bytes::from(format!("record-{i}")),
            )
            .unwrap();
    }
    store.close().await.unwrap();

    // Lop 3 bytes off the end, corrupting the last entry's CRC/frame.
    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());
    let path = file_id.to_file_path(wal_dir.to_str().unwrap(), "wal");
    let bytes = tokio::fs::read(&path).await.unwrap();
    tokio::fs::write(&path, &bytes[..bytes.len() - 3])
        .await
        .unwrap();

    // Only the first two entries survive; ids 0 and 1.
    let (_, range) = get_wal_range(&test_fs(), &wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(1u64))));

    let content = get_wal_content(&test_fs(), &wal_dir, &file_id)
        .await
        .unwrap();
    assert_eq!(content.num_entries, UintN::from(2u64));
}

/// Write `n` records of `payload` bytes as V1 into one file; returns the wal
/// dir and file id for tests that then poke at the bytes.
async fn build_v1_file(
    root: &std::path::Path,
    name: &str,
    n: u64,
    payload: usize,
) -> (std::path::PathBuf, UintN) {
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(root, written_sender, wal_complete_sender);

    let queue_id = QueueIdResolver::new("test_instance").resolve(name);
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        max_file_size: 1 << 30, // one file: the reader scans per file
        write_buffer_size: 64 * 1024,
        enable_fsync: true,
        ..Default::default()
    };
    store
        .start_writer(&queue_id, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();

    let record = Bytes::from(vec![0xCDu8; payload]);
    for i in 0..n {
        store
            .enqueue(&queue_id, UintN::from(i), record.clone())
            .unwrap();
    }
    store.close().await.unwrap();

    (queue_id.to_wal_dir(root), file_id)
}

/// Entries spanning a window boundary are what breaks a naive decoder. With
/// 12 KiB records and a 64 KiB window, roughly every fifth entry straddles one.
#[tokio::test]
async fn test_v1_entries_straddle_window_boundary() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (wal_dir, file_id) = build_v1_file(tmp_dir.path(), "v1_straddle", 40, 12 * 1024).await;

    let (_, range) = get_wal_range(&test_fs(), &wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(39u64))));

    let content = get_wal_content(&test_fs(), &wal_dir, &file_id)
        .await
        .unwrap();
    assert_eq!(content.num_entries, UintN::from(40u64));
}

/// An entry larger than the window must grow it, or the scan reports it
/// truncated and drops the rest of the file. 200 KiB against a 64 KiB window.
#[tokio::test]
async fn test_v1_entry_larger_than_window() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (wal_dir, file_id) = build_v1_file(tmp_dir.path(), "v1_big", 3, 200 * 1024).await;

    let (_, range) = get_wal_range(&test_fs(), &wal_dir, &file_id).await.unwrap();
    assert_eq!(range, Some((UintN::from(0u64), UintN::from(2u64))));

    let content = get_wal_content(&test_fs(), &wal_dir, &file_id)
        .await
        .unwrap();
    assert_eq!(content.num_entries, UintN::from(3u64));
}

/// Cutting inside the last entry must drop exactly that entry; cutting on its
/// boundary must drop nothing extra. A 200 byte record gives a two-byte prefix,
/// so the offsets below land mid-CRC, mid-record and mid-prefix in turn.
#[tokio::test]
async fn test_v1_truncation_offsets_across_a_frame() {
    init_logger();
    let payload = 200usize;
    // [prefix 2][record 200][crc 4]
    let entry_len = 2 + payload + 4;

    for cut in [1usize, 4, 5, 100, 203, 204, entry_len] {
        let tmp_dir = tempdir().unwrap();
        let (wal_dir, file_id) = build_v1_file(tmp_dir.path(), "v1_cut", 4, payload).await;
        let path = file_id.to_file_path(wal_dir.to_str().unwrap(), "wal");
        let bytes = tokio::fs::read(&path).await.unwrap();
        tokio::fs::write(&path, &bytes[..bytes.len() - cut])
            .await
            .unwrap();

        // Either way the fourth entry is gone and the first three remain.
        let (_, range) = get_wal_range(&test_fs(), &wal_dir, &file_id).await.unwrap();
        assert_eq!(
            range,
            Some((UintN::from(0u64), UintN::from(2u64))),
            "cut of {cut} bytes should leave ids 0..=2"
        );
    }
}

/// A queue may hold a V0 file and a V1 file side by side; the reader dispatches
/// on each file's header version, so both read back correctly.
#[tokio::test]
async fn test_mixed_v0_and_v1_files_in_one_queue() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("mixed_queue");
    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    let settings = WalSettings {
        max_file_size: 4096,
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };

    // File 1: a legacy V0 file, entries 0 and 1, written by hand — the writer
    // only produces V1, so the file a running deployment would have left behind
    // has to be constructed here.
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    let file1 = UintN::from(1u64);
    let header1 = WalHeader::new(8, 4, UintN::from(0u64)).unwrap();
    let mut v0_bytes = BytesMut::new();
    header1.write_to_bytes(&mut v0_bytes);
    for (id, record) in [
        (0u64, Bytes::from_static(b"v0-zero")),
        (1u64, Bytes::from_static(b"v0-one")),
    ] {
        WalEntryHeader::new(UintN::from(id), &record)
            .write_to_bytes(&mut v0_bytes, &header1)
            .unwrap();
        v0_bytes.extend_from_slice(&record);
    }
    tokio::fs::write(
        file1.to_file_path(wal_dir.to_str().unwrap(), "wal"),
        &v0_bytes,
    )
    .await
    .unwrap();

    // File 2: V1, entries 2 and 3 (num_entries_before = 2).
    let file2 = UintN::from(2u64);
    let header2 = WalHeader::new(8, 4, UintN::from(2u64)).unwrap();
    store
        .start_writer(
            &queue_id,
            &file2,
            header2,
            settings,
            Some(UintN::from(1u64)),
        )
        .await
        .unwrap();
    store
        .enqueue(&queue_id, UintN::from(2u64), Bytes::from_static(b"v1-two"))
        .unwrap();
    store
        .enqueue(
            &queue_id,
            UintN::from(3u64),
            Bytes::from_static(b"v1-three"),
        )
        .unwrap();
    store.close().await.unwrap();

    // Both files read back correctly via their own format.
    let raw1 = tokio::fs::read(file1.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    let raw2 = tokio::fs::read(file2.to_file_path(wal_dir.to_str().unwrap(), "wal"))
        .await
        .unwrap();
    assert_eq!(raw1[0], 0, "file 1 must be V0");
    assert_eq!(raw2[0], 1, "file 2 must be V1");

    let (_, range1) = get_wal_range(&test_fs(), &wal_dir, &file1).await.unwrap();
    assert_eq!(range1, Some((UintN::from(0u64), UintN::from(1u64))));

    let (_, range2) = get_wal_range(&test_fs(), &wal_dir, &file2).await.unwrap();
    assert_eq!(range2, Some((UintN::from(2u64), UintN::from(3u64))));

    // Stream file 2's V1 entries and confirm the derived ids and data.
    let (tx, mut rx) = mpsc::channel(10);
    read_wal_file_range(
        &test_fs(),
        &wal_dir,
        &file2,
        &UintN::from(2u64),
        &Some(UintN::from(3u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    let e2 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e2.id, UintN::from(2u64));
    assert_eq!(e2.data, Bytes::from_static(b"v1-two"));
    let e3 = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(e3.id, UintN::from(3u64));
    assert_eq!(e3.data, Bytes::from_static(b"v1-three"));
}

/// V1 counterpart to `test_data_size_based_rotation`: no width to overflow, so
/// the oversized record stays put and `max_file_size` is the only trigger left.
#[tokio::test]
async fn test_v1_rotates_on_file_size_not_field_width() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, _) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader {
        data_size_bytes: 1,
        ..Default::default()
    };
    let settings = WalSettings {
        max_file_size: 4096, // far above the total written below
        write_buffer_size: 128,
        write_interval: std::time::Duration::from_millis(50),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
        ..Default::default()
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    store
        .enqueue(&queue_id, UintN::from(0u64), Bytes::from("a"))
        .unwrap();
    // 512 bytes does not fit a 1-byte-wide size field. Under V0 this rotates.
    store
        .enqueue(&queue_id, UintN::from(1u64), Bytes::from(vec![b'x'; 512]))
        .unwrap();

    store.close().await.unwrap();

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    let content1 = get_wal_content(&test_fs(), &wal_dir, &file_id)
        .await
        .unwrap();
    assert_eq!(
        content1.num_entries,
        UintN::from(2u64),
        "V1 has no fixed-width size field to overflow, so both entries belong to file 1"
    );

    let file_2 = UintN::from(2u64).to_file_path(wal_dir.to_str().unwrap(), "wal");
    assert!(
        tokio::fs::metadata(&file_2).await.is_err(),
        "no rotation should have happened, but file 2 exists"
    );

    let (tx, mut rx) = mpsc::channel(10);
    read_wal_file_range(
        &test_fs(),
        &wal_dir,
        &file_id,
        &UintN::from(1u64),
        &Some(UintN::from(1u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    let entry = timeout(Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.id, UintN::from(1u64));
    assert_eq!(entry.data, Bytes::from(vec![b'x'; 512]));
}

/// A rotation that cannot open its next file waits for one that can, and the
/// queue drains once it does.
///
/// The two sides count files independently, and the enqueue side counts first:
/// it stamps a record's page with the new file before the writer has closed the
/// old one. A rotation that gave up half-way used to leave them apart for good,
/// and `take_pending` skips pages stamped above the writer's epoch -- so those
/// pages never reached a file, never became durable, never became reusable, and
/// the queue stopped on a pool nothing could drain.
///
/// The failure is injected with a directory standing where the next file has to
/// go, which `open` refuses; removing it is what lets the rotation through.
#[tokio::test]
async fn a_rotation_that_cannot_open_its_file_waits_rather_than_desyncing() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _written) = mpsc::unbounded_channel();
    let (wal_complete_sender, _complete) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("rotate_fault");
    let first_file = UintN::from(1u64);

    const PAGE_SIZE: usize = 1024;
    let record = Bytes::from(vec![b'r'; 200]);

    let pool = Arc::new(PagePool::new(8, PAGE_SIZE, 0));
    let settings = WalSettings {
        // Below a page, so every page that opens rotates the file.
        max_file_size: 256,
        write_buffer_size: 128,
        write_interval: Duration::from_millis(10),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        ..Default::default()
    };

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();

    // A directory where file 2 has to be created. `open` cannot write to it, so
    // the first rotation fails and has to retry.
    let blocked = UintN::from(2u64).to_file_path(wal_dir.to_str().unwrap(), "wal");
    tokio::fs::create_dir_all(&blocked).await.unwrap();

    store
        .start_writer_with_pool(
            &queue_id,
            &first_file,
            WalHeader::default(),
            settings,
            None,
            Some(Arc::clone(&pool)),
        )
        .await
        .unwrap();

    // Enough records to cross the file threshold several times over, so the
    // blocked rotation is reached and later ones queue up behind it.
    const COUNT: u64 = 24;
    for id in 0..COUNT {
        let placement = pool.place(id, &record).await.expect("a record fits a page");
        store
            .enqueue_pooled(&queue_id, UintN::from(id), Bytes::new(), placement)
            .unwrap();
    }

    // Blocked: the writer is retrying, so nothing past the first file can be
    // durable. If the rotation had given up instead, this would stay true for
    // ever -- which is the bug.
    tokio::time::sleep(Duration::from_millis(120)).await;
    let stuck = pool.durable_before();
    assert!(
        stuck < COUNT,
        "the whole queue drained with file 2 blocked, so this test proves nothing"
    );

    // Clear the way. The rotation must then complete on its own.
    tokio::fs::remove_dir(&blocked).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while pool.durable_before() < COUNT {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the queue never drained after the rotation was unblocked: durable up to {} of \
             {COUNT}, which is the writer and the pool left on different files",
            pool.durable_before()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    store.close().await.unwrap();

    // Every record on disk, once, in order -- the epochs reconverged rather
    // than the pages behind the failure being skipped.
    let mut seen = 0u64;
    for file in 1..=16u64 {
        let file_id = UintN::from(file);
        let Ok(content) = get_wal_content(&test_fs(), &wal_dir, &file_id).await else {
            continue;
        };
        assert_eq!(
            content.entries_before,
            UintN::from(seen),
            "file {file} claims a different starting id than the files before it hold"
        );
        seen += content.num_entries.to_u64().unwrap();
    }
    assert_eq!(
        seen, COUNT,
        "records reached no file at all: the writer and the pool never agreed again"
    );
}

/// A close interrupts a rotation that is stuck retrying. The retry loop runs
/// on the writer task, the only consumer of the request channel, so without
/// the closing flag a Close request would sit undequeued for as long as the
/// disk stayed broken.
#[tokio::test]
async fn a_close_interrupts_a_rotation_that_is_stuck_retrying() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _written) = mpsc::unbounded_channel();
    let (wal_complete_sender, _complete) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("rotate_close");
    let first_file = UintN::from(1u64);

    const PAGE_SIZE: usize = 1024;
    let record = Bytes::from(vec![b'r'; 200]);

    let pool = Arc::new(PagePool::new(8, PAGE_SIZE, 0));
    let settings = WalSettings {
        max_file_size: 256,
        write_buffer_size: 128,
        write_interval: Duration::from_millis(10),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        ..Default::default()
    };

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());
    tokio::fs::create_dir_all(&wal_dir).await.unwrap();

    // A directory where file 2 has to be created, so the rotation retries.
    // It is never removed: the only way out of the retry is the close.
    let blocked = UintN::from(2u64).to_file_path(wal_dir.to_str().unwrap(), "wal");
    tokio::fs::create_dir_all(&blocked).await.unwrap();

    store
        .start_writer_with_pool(
            &queue_id,
            &first_file,
            WalHeader::default(),
            settings,
            None,
            Some(Arc::clone(&pool)),
        )
        .await
        .unwrap();

    const COUNT: u64 = 24;
    for id in 0..COUNT {
        let placement = pool.place(id, &record).await.expect("a record fits a page");
        store
            .enqueue_pooled(&queue_id, UintN::from(id), Bytes::new(), placement)
            .unwrap();
    }

    // Let the writer reach the blocked rotation and start retrying.
    tokio::time::sleep(Duration::from_millis(120)).await;

    // The blocker stays. Close must still return.
    tokio::time::timeout(Duration::from_secs(5), store.close())
        .await
        .expect("close() hung behind a rotation that can only retry")
        .expect("close() reported an error");
}

/// The handover mark used to be published only after the whole released run,
/// so a rotation part-way through it closed the file while the pool still
/// believed the earlier entries of that run were not the writer's. They were
/// cut off, the file was reported complete, and the store archived it without
/// them. No failing hardware involved.
#[tokio::test]
async fn a_rotation_inside_a_released_run_keeps_the_entries_before_it() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _written) = mpsc::unbounded_channel();
    let (wal_complete_sender, _complete) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("straddling_run");
    let first_file = UintN::from(1u64);

    const PAGE_SIZE: usize = 1024;
    const COUNT: u64 = 7;
    let record = Bytes::from(vec![b'r'; 200]);

    let pool = Arc::new(PagePool::new(8, PAGE_SIZE, 0));
    let settings = WalSettings {
        // Crossed by the first record, so the next page to open rotates.
        max_file_size: 256,
        write_buffer_size: 128,
        write_interval: Duration::from_millis(10),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        ..Default::default()
    };

    store
        .start_writer_with_pool(
            &queue_id,
            &first_file,
            WalHeader::default(),
            settings,
            None,
            Some(Arc::clone(&pool)),
        )
        .await
        .unwrap();

    let mut placements = Vec::new();
    for id in 0..COUNT {
        placements.push(pool.place(id, &record).await.expect("a record fits a page"));
    }

    let rotating = placements
        .iter()
        .position(|p| p.rotate == crate::RotateHint::Before)
        .expect("one of these records must open a file") as u64;
    assert!(
        rotating >= 2 && rotating < COUNT - 1,
        "the rotation has to fall inside a run with an entry either side of it; it is at {rotating}"
    );

    // A released run starts at the lowest parked id, and an id that is exactly
    // the next one expected never parks -- so `rotating - 1` must be sent while
    // the writer is two behind, which means withholding `rotating - 2` until
    // after it. One more id then releases them together, with an entry ahead of
    // the rotation.
    let held = rotating - 2;
    let mut order: Vec<u64> = (0..held).collect();
    order.extend([rotating - 1, rotating, held]);
    order.extend((rotating + 1)..COUNT);

    for id in order {
        store
            .enqueue_pooled(
                &queue_id,
                UintN::from(id),
                Bytes::new(),
                placements[id as usize].clone(),
            )
            .unwrap();
    }

    store.close().await.unwrap();

    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());
    let mut seen = 0u64;
    for file in 1..=16u64 {
        let file_id = UintN::from(file);
        let Ok(content) = get_wal_content(&test_fs(), &wal_dir, &file_id).await else {
            continue;
        };
        assert_eq!(
            content.entries_before,
            UintN::from(seen),
            "file {file} claims to start at {:?}, but the files before it hold {seen} entries: \
             the entries released alongside the rotation reached no file",
            content.entries_before
        );
        seen += content.num_entries.to_u64().unwrap();
    }
    assert_eq!(seen, COUNT, "{} of {COUNT} entries reached a file", seen);
}

/// The rotation cannot wait for the file it is leaving, so its unflushed
/// records used to reach no file and were then overwritten in memory once the
/// next file's fsync advanced the watermark past them.
///
/// Injected on file 1's path only and healed part-way, which is what separates
/// "the write failed" from "the disk is gone for good".
#[tokio::test]
async fn a_close_that_cannot_flush_hands_its_tail_to_a_retrier() {
    use crate::page_pool::PagePool;
    use std::sync::Arc;

    init_logger();
    let tmp_dir = tempdir().unwrap();
    let (written_sender, _written) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut complete) = mpsc::unbounded_channel();
    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new("test_instance");
    let queue_id = resolver.resolve("torn_close");
    let first_file = UintN::from(1u64);
    let wal_dir = queue_id.to_wal_dir(tmp_dir.path());

    const PAGE_SIZE: usize = 1024;
    const COUNT: u64 = 12;
    let record = Bytes::from(vec![b'r'; 200]);

    let pool = Arc::new(PagePool::new(8, PAGE_SIZE, 0));
    let settings = WalSettings {
        max_file_size: 256,
        write_buffer_size: 128,
        write_interval: Duration::from_millis(10),
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::None,
        compression_type: normfs_types::CompressionType::None,
        // Not the default thousand: watch it give up, don't wait ten seconds.
        flush_max_retries: 2,
        flush_retry_delay: Duration::from_millis(1),
    };

    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    let torn = first_file.to_file_path(wal_dir.to_str().unwrap(), "wal");
    crate::fail_flushes(&torn, u32::MAX);

    store
        .start_writer_with_pool(
            &queue_id,
            &first_file,
            WalHeader::default(),
            settings,
            None,
            Some(Arc::clone(&pool)),
        )
        .await
        .unwrap();

    for id in 0..COUNT {
        let placement = pool.place(id, &record).await.expect("a record fits a page");
        store
            .enqueue_pooled(&queue_id, UintN::from(id), Bytes::new(), placement)
            .unwrap();
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !pool.has_stranded() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "file 1's closing flush was made to fail, so it must have left records owed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !pool.is_fully_durable(),
        "a file is still owed its tail, so nothing may certify this queue durable"
    );

    crate::fail_flushes(&torn, 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while pool.has_stranded() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the retrier never landed file 1's tail after the writes started working"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    store.close().await.unwrap();

    let mut completed: Vec<u64> = Vec::new();
    while let Ok(file) = complete.try_recv() {
        completed.push(file.file_id.to_u64().unwrap());
    }
    assert!(
        completed.contains(&1),
        "file 1 landed its tail, so it must be reported complete; got {completed:?}"
    );

    let mut seen = 0u64;
    for file in 1..=32u64 {
        let file_id = UintN::from(file);
        let Ok(content) = get_wal_content(&test_fs(), &wal_dir, &file_id).await else {
            continue;
        };
        assert_eq!(
            content.entries_before,
            UintN::from(seen),
            "file {file} claims to start at {:?} but the files before it hold {seen} entries",
            content.entries_before
        );
        seen += content.num_entries.to_u64().unwrap();
    }
    assert_eq!(seen, COUNT, "{seen} of {COUNT} records reached a file");
}
