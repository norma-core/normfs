use std::sync::Once;
use std::time::Duration;

use crate::{
    WalSettings, WalStore,
    reader::{ReadRangeResult, get_wal_content, read_wal_file_range},
    wal_header::WalHeader,
};
use bytes::Bytes;
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
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
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
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entries = vec![
        (UintN::from(0u64), Bytes::from("hello")),
        (UintN::from(1u64), Bytes::from("world")),
    ];
    store.enqueue_batch(&queue_id, entries).unwrap();

    store.close().await.unwrap();

    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
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
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
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

    let content1 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id)
        .await
        .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id.increment())
        .await
        .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
}

#[tokio::test]
async fn test_data_size_based_rotation() {
    init_logger();
    let tmp_dir = tempdir().unwrap();
    let instance_id = "test_instance";
    let (written_sender, _) = mpsc::unbounded_channel();
    let (wal_complete_sender, mut wal_complete_receiver) = mpsc::unbounded_channel();

    let store = WalStore::new(tmp_dir.path(), written_sender, wal_complete_sender);

    let resolver = QueueIdResolver::new(instance_id);
    let queue_id = resolver.resolve("test_queue");
    let file_id = UintN::from(1u64);
    let header = WalHeader {
        data_size_bytes: 1, // Set data size limit to 1 byte
        ..Default::default()
    };
    let settings = WalSettings {
        max_file_size: 1024,
        write_buffer_size: 128,
        enable_fsync: true,
        encryption_type: normfs_types::EncryptionType::Aes,
        compression_type: normfs_types::CompressionType::Zstd,
    };

    store
        .start_writer(&queue_id, &file_id, header, settings, None)
        .await
        .unwrap();

    let entry_id1 = UintN::from(0u64);
    let data1 = Bytes::from("a"); // 1 byte - fits in data_size_bytes = 1
    store.enqueue(&queue_id, entry_id1, data1).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let entry_id2 = UintN::from(1u64);
    let data2 = Bytes::from("data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file,
    data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file
    data too large for writing into file, data too large for writing into file, data too large for writing into file, data too large for writing into file"); // > 1 byte - triggers rotation
    store.enqueue(&queue_id, entry_id2, data2).unwrap();

    // Wait for the rotation to complete and receive the completion signal
    let received = timeout(Duration::from_secs(5), wal_complete_receiver.recv())
        .await
        .expect("wal_complete_receiver.recv() timed out")
        .unwrap();
    assert_eq!(received.queue_id, queue_id);
    assert_eq!(received.file_id, file_id);

    // Now close the store
    timeout(Duration::from_secs(5), store.close())
        .await
        .expect("store.close() timed out")
        .unwrap();

    let content1 = timeout(
        Duration::from_secs(5),
        get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id),
    )
    .await
    .expect("get_wal_content for file_id timed out")
    .unwrap();
    assert_eq!(content1.num_entries, UintN::from(1u64));

    let content2 = timeout(
        Duration::from_secs(5),
        get_wal_content(&queue_id.to_wal_dir(tmp_dir.path()), &file_id.increment()),
    )
    .await
    .expect("get_wal_content for file_id.increment() timed out")
    .unwrap();
    assert_eq!(content2.num_entries, UintN::from(1u64));
    assert_eq!(content2.entries_before, UintN::from(1u64));
}
