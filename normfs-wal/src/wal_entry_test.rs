use super::wal_entry::WalEntryHeader;
use super::wal_header::WalHeader;
use bytes::BytesMut;
use tokio::io::BufReader;
use uintn::UintN;

#[test]
fn test_wal_entry_header_serialization_deserialization() {
    // --- Test Case 1 ---
    let wal_header1 = WalHeader::new(2, 4, UintN::from(123u64)).unwrap();
    let record_data1 = "hello world";
    let go_bytes1: [u8; 22] = [
        0, 0, 0, 0, 0, 0, 0, 0, // Version
        200, 1, 0, 0, // EntryID (456)
        11, 0, // RecordSize (11)
        104, 105, 30, 178, 52, 103, 171, 69, // XXHash
    ];

    // Test Deserialization
    let parsed_header1 = WalEntryHeader::from_bytes(&go_bytes1, &wal_header1).unwrap();
    assert_eq!(parsed_header1.entry_id, UintN::from(456u16));
    assert_eq!(
        parsed_header1.record_size,
        UintN::from(record_data1.len() as u16)
    );
    assert_eq!(parsed_header1.xxhash, 5020219685658847592);

    // Test Serialization
    let new_header1 = WalEntryHeader::new(UintN::from(456u16), record_data1.as_bytes());
    let mut buffer1 = BytesMut::new();
    new_header1
        .write_to_bytes(&mut buffer1, &wal_header1)
        .unwrap();
    assert_eq!(buffer1.as_ref(), &go_bytes1[..]);

    // --- Test Case 2 ---
    let wal_header2 = WalHeader::new(8, 8, UintN::from(9876543210u64)).unwrap();
    let record_data2 = "another record with much more data to see how it behaves";
    let go_bytes2: [u8; 32] = [
        0, 0, 0, 0, 0, 0, 0, 0, // Version
        21, 205, 91, 7, 0, 0, 0, 0, // EntryID (123456789)
        56, 0, 0, 0, 0, 0, 0, 0, // RecordSize (56)
        90, 105, 6, 101, 177, 48, 60, 111, // XXHash
    ];

    // Test Deserialization
    let parsed_header2 = WalEntryHeader::from_bytes(&go_bytes2, &wal_header2).unwrap();
    assert_eq!(parsed_header2.entry_id, UintN::from(123456789u64));
    assert_eq!(
        parsed_header2.record_size,
        UintN::from(record_data2.len() as u64)
    );
    assert_eq!(parsed_header2.xxhash, 8015334975274903898);

    // Test Serialization
    let new_header2 = WalEntryHeader::new(UintN::from(123456789u64), record_data2.as_bytes());
    let mut buffer2 = BytesMut::new();
    new_header2
        .write_to_bytes(&mut buffer2, &wal_header2)
        .unwrap();
    assert_eq!(buffer2.as_ref(), &go_bytes2[..]);

    // --- Test Case 3 ---
    let wal_header3 = WalHeader::new(1, 1, UintN::from(255u64)).unwrap();
    let record_data3 = "short";
    let go_bytes3: [u8; 18] = [
        0, 0, 0, 0, 0, 0, 0, 0,  // Version
        10, // EntryID (10)
        5,  // RecordSize (5)
        164, 169, 97, 65, 41, 179, 219, 164, // XXHash
    ];

    // Test Deserialization
    let parsed_header3 = WalEntryHeader::from_bytes(&go_bytes3, &wal_header3).unwrap();
    assert_eq!(parsed_header3.entry_id, UintN::from(10u8));
    assert_eq!(
        parsed_header3.record_size,
        UintN::from(record_data3.len() as u8)
    );
    assert_eq!(parsed_header3.xxhash, 11879285431891765668);

    // Test Serialization
    let new_header3 = WalEntryHeader::new(UintN::from(10u8), record_data3.as_bytes());
    let mut buffer3 = BytesMut::new();
    new_header3
        .write_to_bytes(&mut buffer3, &wal_header3)
        .unwrap();
    assert_eq!(buffer3.as_ref(), &go_bytes3[..]);

    // --- Test Case 4 ---
    let wal_header4 = WalHeader::new(4, 2, UintN::from(1000u64)).unwrap();
    let record_data4 = "medium data";
    let go_bytes4: [u8; 22] = [
        0, 0, 0, 0, 0, 0, 0, 0, // Version
        44, 1, // EntryID (300)
        11, 0, 0, 0, // RecordSize (11)
        19, 154, 69, 252, 55, 53, 122, 17, // XXHash
    ];

    // Test Deserialization
    let parsed_header4 = WalEntryHeader::from_bytes(&go_bytes4, &wal_header4).unwrap();
    assert_eq!(parsed_header4.entry_id, UintN::from(300u16));
    assert_eq!(
        parsed_header4.record_size,
        UintN::from(record_data4.len() as u32)
    );
    assert_eq!(parsed_header4.xxhash, 1259377560375368211);

    // Test Serialization
    let new_header4 = WalEntryHeader::new(UintN::from(300u16), record_data4.as_bytes());
    let mut buffer4 = BytesMut::new();
    new_header4
        .write_to_bytes(&mut buffer4, &wal_header4)
        .unwrap();
    assert_eq!(buffer4.as_ref(), &go_bytes4[..]);
}

#[tokio::test]
async fn test_wal_entry_header_from_reader() {
    let header = WalHeader {
        data_size_bytes: 8,
        id_size_bytes: 4,
        num_entries_before: UintN::from(12345u64),
    };

    let entry_header = WalEntryHeader {
        entry_id: UintN::from(123u32),
        record_size: UintN::from(456u64),
        xxhash: 0,
    };

    let mut buffer = BytesMut::new();
    entry_header.write_to_bytes(&mut buffer, &header).unwrap();

    let mut reader = BufReader::new(buffer.as_ref());
    let deserialized = WalEntryHeader::from_reader(&mut reader, &header)
        .await
        .unwrap();

    assert_eq!(entry_header, deserialized);
}
