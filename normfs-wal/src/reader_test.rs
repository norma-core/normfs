use crate::wal_header::WalHeader;
use crate::{
    errors::WalError,
    reader::{
        ReadRangeResult, get_wal_content, get_wal_range, read_wal_bytes_range, read_wal_file_range,
    },
    wal_entry,
};
use bytes::{Bytes, BytesMut};
use normfs_types::DataSource;
use tempfile::tempdir;
use tokio::{
    fs,
    sync::mpsc,
    time::{Duration, timeout},
};
use uintn::UintN;

#[tokio::test]
async fn test_get_wal_range() {
    let dir = tempdir().unwrap();
    let file_id = UintN::from(1u64);
    let wal_path = file_id.to_file_path(dir.path().to_str().unwrap(), "wal");

    let wal_header = WalHeader::new(8, 4, UintN::from(0u64)).unwrap();
    let mut buffer = BytesMut::new();
    wal_header.write_to_bytes(&mut buffer);

    for i in 1..=5 {
        let entry_id = UintN::from(i as u64);
        let record_data = format!("record {}", i);
        let record_bytes = Bytes::from(record_data);

        let entry_header = wal_entry::WalEntryHeader::new(entry_id.clone(), &record_bytes);

        entry_header
            .write_to_bytes(&mut buffer, &wal_header)
            .unwrap();
        buffer.extend_from_slice(&record_bytes);
    }

    fs::write(&wal_path, &buffer).await.unwrap();

    let (header, range) = get_wal_range(dir.path(), &file_id).await.unwrap();

    assert_eq!(header.num_entries_before, UintN::from(0u64));
    assert_eq!(range, Some((UintN::from(1u64), UintN::from(5u64))));

    let wal_content = get_wal_content(dir.path(), &file_id).await.unwrap();
    assert_eq!(wal_content.entries_before, UintN::from(0u64));
    assert_eq!(wal_content.num_entries, UintN::from(5u64));
    assert_eq!(wal_content.content, buffer.clone().freeze());

    // Test read_wal_file_range
    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        dir.path(),
        &file_id,
        &UintN::from(1u64),
        &Some(UintN::from(5u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx); // Close the sender to terminate the loop

    // Since we're reading all entries (1-5) and reaching EOF, we expect Complete
    assert!(matches!(result, ReadRangeResult::Complete));

    let mut i = 1;
    while let Some(entry) = rx.recv().await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 6);

    // Test read_wal_bytes_range
    let (tx2, mut rx2) = mpsc::channel(10);
    let result2 = read_wal_bytes_range(
        &buffer.freeze(),
        &UintN::from(1u64),
        &Some(UintN::from(5u64)),
        1,
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);

    assert!(matches!(result2, ReadRangeResult::Complete));

    let mut i = 1;
    while let Some(entry) = rx2.recv().await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 6);
}

#[tokio::test]
async fn test_get_wal_range_empty_file() {
    let dir = tempdir().unwrap();
    let file_id = UintN::from(1u64);
    let wal_path = file_id.to_file_path(dir.path().to_str().unwrap(), "wal");

    fs::write(&wal_path, &[]).await.unwrap();

    let result = get_wal_range(dir.path(), &file_id).await;

    // Empty file should return WalEmpty error
    assert!(matches!(result, Err(WalError::WalEmpty(_))));

    let result_content = get_wal_content(dir.path(), &file_id).await;
    assert!(matches!(
        result_content,
        Err(WalError::WalEntryError(
            wal_entry::WalEntryError::SliceTooShort
        ))
    ));

    // Test read_wal_file_range with empty file
    let (tx, mut rx) = mpsc::channel(1);
    let result = read_wal_file_range(
        dir.path(),
        &file_id,
        &UintN::from(1u64),
        &Some(UintN::from(10u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);
    assert!(matches!(result, ReadRangeResult::PartialRead { .. }));
    assert!(rx.recv().await.is_none());

    // Test read_wal_bytes_range with empty bytes
    let (tx2, mut rx2) = mpsc::channel(1);
    let result2 = read_wal_bytes_range(
        &Bytes::new(),
        &UintN::from(1u64),
        &Some(UintN::from(10u64)),
        1,
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);
    assert!(matches!(result2, ReadRangeResult::PartialRead { .. }));
    assert!(rx2.recv().await.is_none());
}

#[tokio::test]
async fn test_get_wal_range_header_only() {
    let dir = tempdir().unwrap();
    let file_id = UintN::from(1u64);
    let wal_path = file_id.to_file_path(dir.path().to_str().unwrap(), "wal");

    let wal_header = WalHeader::new(8, 4, UintN::from(0u64)).unwrap();
    let mut buffer = BytesMut::new();
    wal_header.write_to_bytes(&mut buffer);

    fs::write(&wal_path, &buffer).await.unwrap();

    let (header, range) = get_wal_range(dir.path(), &file_id).await.unwrap();

    assert_eq!(header.num_entries_before, UintN::from(0u64));
    assert_eq!(range, None);

    let wal_content = get_wal_content(dir.path(), &file_id).await.unwrap();
    assert_eq!(wal_content.entries_before, UintN::from(0u64));
    assert_eq!(wal_content.num_entries, UintN::from(0u64));
    assert_eq!(wal_content.content, buffer.clone().freeze());

    // Test read_wal_file_range with header only
    let (tx, _rx) = mpsc::channel(1);
    match read_wal_file_range(
        dir.path(),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(u64::MAX)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap()
    {
        ReadRangeResult::PartialRead { .. } => {
            // No entries should be sent for header-only file, returns PartialRead
        }
        _ => panic!("Expected PartialRead result for header-only file"),
    }

    // Test read_wal_bytes_range with header only
    let (tx2, _rx2) = mpsc::channel(1);
    match read_wal_bytes_range(
        &buffer.freeze(),
        &UintN::from(0u64),
        &Some(UintN::from(u64::MAX)),
        1,
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap()
    {
        ReadRangeResult::PartialRead { .. } => {
            // No entries should be sent for header-only file, returns PartialRead
        }
        _ => panic!("Expected PartialRead result for header-only file"),
    }
}

#[tokio::test]
async fn test_get_wal_range_corrupted_end() {
    let dir = tempdir().unwrap();
    let file_id = UintN::from(1u64);
    let wal_path = file_id.to_file_path(dir.path().to_str().unwrap(), "wal");

    let wal_header = WalHeader::new(8, 4, UintN::from(0u64)).unwrap();
    let mut buffer = BytesMut::new();
    wal_header.write_to_bytes(&mut buffer);

    for i in 1..=3 {
        let entry_id = UintN::from(i as u64);
        let record_data = format!("record {}", i);
        let record_bytes = Bytes::from(record_data);

        let entry_header = wal_entry::WalEntryHeader::new(entry_id.clone(), &record_bytes);

        entry_header
            .write_to_bytes(&mut buffer, &wal_header)
            .unwrap();
        buffer.extend_from_slice(&record_bytes);
    }

    // Add corrupted data
    let corrupted_data: Vec<u8> = (0..50).map(|_| rand::random::<u8>()).collect();
    buffer.extend_from_slice(&corrupted_data);

    fs::write(&wal_path, &buffer).await.unwrap();

    let (header, range) = get_wal_range(dir.path(), &file_id).await.unwrap();

    assert_eq!(header.num_entries_before, UintN::from(0u64));
    assert_eq!(range, Some((UintN::from(1u64), UintN::from(3u64))));

    let wal_content = get_wal_content(dir.path(), &file_id).await.unwrap();
    assert_eq!(wal_content.entries_before, UintN::from(0u64));
    assert_eq!(wal_content.num_entries, UintN::from(3u64));
    assert_eq!(wal_content.content, buffer.clone().freeze());

    // Test read_wal_file_range with corrupted end
    let (tx, mut rx) = mpsc::channel(10);
    read_wal_file_range(
        dir.path(),
        &file_id,
        &UintN::from(0u64),
        &Some(UintN::from(u64::MAX)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    let mut i = 1;
    while let Some(entry) = rx.recv().await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 4);

    // Test read_wal_bytes_range with corrupted end
    let (tx2, mut rx2) = mpsc::channel(10);
    read_wal_bytes_range(
        &buffer.freeze(),
        &UintN::from(0u64),
        &Some(UintN::from(u64::MAX)),
        1,
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);

    let mut i = 1;
    while let Some(entry) = rx2.recv().await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 4);
}

#[tokio::test]
async fn test_get_wal_range_cropped_last_entry() {
    let dir = tempdir().unwrap();
    let file_id = UintN::from(1u64);
    let wal_path = file_id.to_file_path(dir.path().to_str().unwrap(), "wal");

    let wal_header = WalHeader::new(8, 4, UintN::from(0u64)).unwrap();
    let mut buffer = BytesMut::new();
    wal_header.write_to_bytes(&mut buffer);

    for i in 1..=3 {
        let entry_id = UintN::from(i as u64);
        let record_data = format!("record {}", i);
        let record_bytes = Bytes::from(record_data);

        let entry_header = wal_entry::WalEntryHeader::new(entry_id.clone(), &record_bytes);

        entry_header
            .write_to_bytes(&mut buffer, &wal_header)
            .unwrap();
        buffer.extend_from_slice(&record_bytes);
    }

    // Add a cropped last entry
    let entry_id = UintN::from(4u64);
    let record_data = "this is a long record that will be cropped";
    let record_bytes = Bytes::from(record_data);
    let entry_header = wal_entry::WalEntryHeader::new(entry_id.clone(), &record_bytes);
    let mut entry_buffer = BytesMut::new();
    entry_header
        .write_to_bytes(&mut entry_buffer, &wal_header)
        .unwrap();
    entry_buffer.extend_from_slice(&record_bytes);

    // Crop the entry
    let cropped_len = entry_buffer.len() / 2;
    buffer.extend_from_slice(&entry_buffer[..cropped_len]);

    fs::write(&wal_path, &buffer).await.unwrap();

    let (header, range) = get_wal_range(dir.path(), &file_id).await.unwrap();

    assert_eq!(header.num_entries_before, UintN::from(0u64));
    assert_eq!(range, Some((UintN::from(1u64), UintN::from(3u64))));

    let wal_content = get_wal_content(dir.path(), &file_id).await.unwrap();
    assert_eq!(wal_content.entries_before, UintN::from(0u64));
    assert_eq!(wal_content.num_entries, UintN::from(3u64));
    assert_eq!(wal_content.content, buffer.clone().freeze());

    // Test read_wal_file_range with cropped last entry
    let (tx, mut rx) = mpsc::channel(10);
    let result = read_wal_file_range(
        dir.path(),
        &file_id,
        &UintN::from(1u64),
        &Some(UintN::from(10u64)),
        1,
        &tx,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx);

    assert!(matches!(result, ReadRangeResult::PartialRead { .. }));

    let mut i = 1;
    while let Ok(Some(entry)) = timeout(Duration::from_secs(1), rx.recv()).await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 4);

    // Test read_wal_bytes_range with cropped last entry
    let (tx2, mut rx2) = mpsc::channel(10);
    let result = read_wal_bytes_range(
        &buffer.freeze(),
        &UintN::from(1u64),
        &Some(UintN::from(10u64)),
        1,
        &tx2,
        DataSource::DiskWal,
    )
    .await
    .unwrap();
    drop(tx2);

    assert!(matches!(result, ReadRangeResult::PartialRead { .. }));

    let mut i = 1;
    while let Ok(Some(entry)) = timeout(Duration::from_secs(1), rx2.recv()).await {
        assert_eq!(entry.id, UintN::from(i as u64));
        let record_data = format!("record {}", i);
        assert_eq!(entry.data, Bytes::from(record_data));
        assert_eq!(entry.source, DataSource::DiskWal);
        i += 1;
    }
    assert_eq!(i, 4);
}
