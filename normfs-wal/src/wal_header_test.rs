use super::wal_header::WalHeader;
use bytes::BytesMut;
use tokio::io::BufReader;
use uintn::UintN;

#[test]
fn test_wal_header_serialization_deserialization() {
    // From Go test: Go WAL Header 1 (dataSize: 2, idSize: 4, numEntriesBefore: 123)
    let go_bytes1: [u8; 40] = [
        0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0,
        0, 0, 123, 0, 0, 0, 0, 0, 0, 0,
    ];

    let (header1, _) = WalHeader::from_bytes(&go_bytes1[..]).unwrap();
    assert_eq!(header1.data_size_bytes, 2);
    assert_eq!(header1.id_size_bytes, 4);
    assert_eq!(header1.num_entries_before, UintN::from(123u8));

    let mut buffer1 = BytesMut::new();
    header1.write_to_bytes(&mut buffer1);
    assert_eq!(
        buffer1.as_ref(),
        [
            0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 123
        ]
    );

    // From Go test: Go WAL Header 2 (dataSize: 8, idSize: 8, numEntriesBefore: 9876543210)
    let go_bytes2: [u8; 40] = [
        0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0,
        0, 0, 234, 22, 176, 76, 2, 0, 0, 0,
    ];

    let (header2, _) = WalHeader::from_bytes(&go_bytes2[..]).unwrap();
    assert_eq!(header2.data_size_bytes, 8);
    assert_eq!(header2.id_size_bytes, 8);
    assert_eq!(header2.num_entries_before, UintN::from(9876543210u64));

    let mut buffer2 = BytesMut::new();
    header2.write_to_bytes(&mut buffer2);
    assert_eq!(buffer2.as_ref(), go_bytes2);

    // From Go test: Go WAL Header 3 (dataSize: 1, idSize: 1, numEntriesBefore: 255)
    let go_bytes3: [u8; 40] = [
        0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0,
        0, 0, 255, 0, 0, 0, 0, 0, 0, 0,
    ];

    let (header3, _) = WalHeader::from_bytes(&go_bytes3[..]).unwrap();
    assert_eq!(header3.data_size_bytes, 1);
    assert_eq!(header3.id_size_bytes, 1);
    assert_eq!(header3.num_entries_before, UintN::from(255u64));

    let mut buffer3 = BytesMut::new();
    header3.write_to_bytes(&mut buffer3);
    assert_eq!(
        buffer3.as_ref(),
        [
            0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0,
            0, 0, 0, 255
        ]
    );

    // From Go test: Go WAL Header 4 (dataSize: 4, idSize: 2, numEntriesBefore: 1000)
    let go_bytes4: [u8; 40] = [
        0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0,
        0, 0, 232, 3, 0, 0, 0, 0, 0, 0,
    ];

    let (header4, _) = WalHeader::from_bytes(&go_bytes4[..]).unwrap();
    assert_eq!(header4.data_size_bytes, 4);
    assert_eq!(header4.id_size_bytes, 2);
    assert_eq!(header4.num_entries_before, UintN::from(1000u64));

    let mut buffer4 = BytesMut::new();
    header4.write_to_bytes(&mut buffer4);
    assert_eq!(
        buffer4.as_ref(),
        [
            0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0,
            0, 0, 0, 232, 3
        ]
    );
}

#[tokio::test]
async fn test_wal_header_from_reader() {
    let header = WalHeader::new(8, 4, UintN::from(12345u64)).unwrap();

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);

    let mut reader = BufReader::new(buffer.as_ref());
    let (deserialized, bytes_read) = WalHeader::from_reader(&mut reader).await.unwrap();

    assert_eq!(header, deserialized);
    assert_eq!(buffer.len(), bytes_read);
}
