use super::wal_header::{WalHeader, WalHeaderError};
use super::wal_header_v1::{
    AnyWalHeader, AnyWalHeaderError, WAL_HEADER_V0_VERSION, WAL_HEADER_V1_MAX_SIZE,
    WAL_HEADER_V1_MIN_SIZE, WAL_HEADER_V1_VERSION, WAL_HEADER_VERSION_SIZE, WalHeaderV1,
    WalHeaderV1Error, peek_version,
};
use bytes::BytesMut;
use tokio::io::BufReader;
use uintn::UintN;

// From Go test: Go WAL Header 1 (dataSize: 2, idSize: 4, numEntriesBefore: 123)
const GO_BYTES_1: [u8; 40] = [
    0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
    123, 0, 0, 0, 0, 0, 0, 0,
];

// From Go test: Go WAL Header 2 (dataSize: 8, idSize: 8, numEntriesBefore: 9876543210)
const GO_BYTES_2: [u8; 40] = [
    0, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
    234, 22, 176, 76, 2, 0, 0, 0,
];

/// A V1 header with the given version word, so unsupported versions can be
/// built the same way a real writer would lay them out.
fn v1_bytes_with_version(version: u64, tail: &[u8]) -> Vec<u8> {
    let mut buffer = version.to_le_bytes().to_vec();
    buffer.extend_from_slice(tail);
    buffer
}

#[test]
fn test_wal_header_v0_still_parses_through_dispatch() {
    let (header1, bytes_read1) = AnyWalHeader::from_bytes(&GO_BYTES_1[..]).unwrap();
    assert_eq!(header1.version(), WAL_HEADER_V0_VERSION);
    assert_eq!(header1.data_size_bytes(), 2);
    assert_eq!(header1.id_size_bytes(), 4);
    assert_eq!(header1.num_entries_before(), UintN::from(123u8));
    assert_eq!(bytes_read1, 40);

    let (header2, bytes_read2) = AnyWalHeader::from_bytes(&GO_BYTES_2[..]).unwrap();
    assert_eq!(header2.version(), WAL_HEADER_V0_VERSION);
    assert_eq!(header2.num_entries_before(), UintN::from(9876543210u64));
    assert_eq!(bytes_read2, 40);

    assert!(matches!(header1, AnyWalHeader::V0(_)));
    assert!(matches!(header2, AnyWalHeader::V0(_)));
}

#[test]
fn test_wal_header_v0_write_output_is_unchanged() {
    let (header, _) = WalHeader::from_bytes(&GO_BYTES_2[..]).unwrap();

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);

    assert_eq!(buffer.as_ref(), GO_BYTES_2);
    assert_eq!(
        peek_version(buffer.as_ref()).unwrap(),
        WAL_HEADER_V0_VERSION
    );
}

#[tokio::test]
async fn test_wal_header_v0_still_parses_from_reader_through_dispatch() {
    let header = WalHeader::new(8, 4, UintN::from(12345u64)).unwrap();

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);

    let mut reader = BufReader::new(buffer.as_ref());
    let (deserialized, bytes_read) = AnyWalHeader::from_reader(&mut reader).await.unwrap();

    assert_eq!(deserialized, AnyWalHeader::V0(header));
    assert_eq!(buffer.len(), bytes_read);
}

/// The reason the version word keeps its V0 encoding: a reader that only knows
/// V0 must reject a V1 file by reporting its version, not by misparsing it.
#[test]
fn test_v0_parser_rejects_v1_header_with_its_version() {
    let header = WalHeaderV1::new(8, 4, 12345).unwrap();
    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer).unwrap();

    // On disk the V0 parser is handed the whole rest of the file, not just the
    // header, so give it entries to read past as a real reader would.
    buffer.extend_from_slice(&[0xAA; 64]);

    assert_eq!(
        WalHeader::from_bytes(&buffer),
        Err(WalHeaderError::UnsupportedVersion(WAL_HEADER_V1_VERSION))
    );
}

fn roundtrip(data_size_bytes: u64, id_size_bytes: u64, num_entries_before: u64, expected: usize) {
    let header = WalHeaderV1::new(data_size_bytes, id_size_bytes, num_entries_before).unwrap();
    assert_eq!(header.size(), expected);

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, expected);
    assert_eq!(buffer.len(), expected);

    assert_eq!(
        &buffer[..WAL_HEADER_VERSION_SIZE],
        &WAL_HEADER_V1_VERSION.to_le_bytes()
    );
    assert_eq!(buffer[8] as u64, data_size_bytes);
    assert_eq!(buffer[9] as u64, id_size_bytes);

    let (decoded, bytes_read) = WalHeaderV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(bytes_read, expected);
}

#[test]
fn test_wal_header_v1_roundtrip_across_varint_widths() {
    roundtrip(1, 1, 0, 11);
    roundtrip(2, 4, 127, 11);
    roundtrip(8, 4, 128, 12);
    roundtrip(8, 8, 16_383, 12);
    roundtrip(8, 8, 16_384, 13);
    roundtrip(16, 16, 9_876_543_210, 15);
    roundtrip(8, 4, 0x7fff_ffff_ffff_ffff, 19);

    assert_eq!(
        WalHeaderV1::new(1, 1, 0).unwrap().size(),
        WAL_HEADER_V1_MIN_SIZE
    );
}

#[tokio::test]
async fn test_wal_header_v1_roundtrip_through_reader() {
    let header = WalHeaderV1::new(8, 4, 12345).unwrap();

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();

    let mut reader = BufReader::new(buffer.as_ref());
    let (deserialized, bytes_read) = AnyWalHeader::from_reader(&mut reader).await.unwrap();

    assert_eq!(deserialized, AnyWalHeader::V1(header));
    assert_eq!(bytes_read, written);
}

#[test]
fn test_wal_header_v1_roundtrip_through_dispatch() {
    let header = WalHeaderV1::new(8, 4, 987_654).unwrap();

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();

    let (decoded, bytes_read) = AnyWalHeader::from_bytes(&buffer).unwrap();
    assert_eq!(decoded, AnyWalHeader::V1(header));
    assert_eq!(decoded.version(), WAL_HEADER_V1_VERSION);
    assert_eq!(bytes_read, written);
}

#[test]
fn test_wal_header_v1_rejects_malformed_varints() {
    let header = WalHeaderV1::new(8, 4, u64::MAX).unwrap();
    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();

    for len in 0..written {
        assert_eq!(
            WalHeaderV1::from_bytes(&buffer[..len]),
            Err(WalHeaderV1Error::Truncated),
            "length {} should be truncated",
            len
        );
    }

    // data_size_bytes padded to two bytes instead of the canonical one
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(1, &[0x88, 0x00, 4, 0])),
        Err(WalHeaderV1Error::NonCanonical)
    );

    // num_entries_before needs an eleventh byte to be represented
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(
            1,
            &[
                8, 4, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02
            ]
        )),
        Err(WalHeaderV1Error::Overflow)
    );

    // field sizes outside the {1, 2, 4, 8, 16} set
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(1, &[3, 4, 0])),
        Err(WalHeaderV1Error::InvalidDataSizeBytes(3))
    );
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(1, &[8, 7, 0])),
        Err(WalHeaderV1Error::InvalidIdSizeBytes(7))
    );
}

#[tokio::test]
async fn test_wal_header_v1_reader_rejects_malformed_varints() {
    let truncated = v1_bytes_with_version(1, &[8, 4]);
    let mut reader = BufReader::new(&truncated[..]);
    assert_eq!(
        WalHeaderV1::from_reader(&mut reader).await,
        Err(WalHeaderV1Error::Truncated)
    );

    let non_canonical = v1_bytes_with_version(1, &[0x88, 0x00, 4, 0]);
    let mut reader = BufReader::new(&non_canonical[..]);
    assert_eq!(
        WalHeaderV1::from_reader(&mut reader).await,
        Err(WalHeaderV1Error::NonCanonical)
    );

    let overflowing = v1_bytes_with_version(
        1,
        &[
            8, 4, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02,
        ],
    );
    let mut reader = BufReader::new(&overflowing[..]);
    assert_eq!(
        WalHeaderV1::from_reader(&mut reader).await,
        Err(WalHeaderV1Error::Overflow)
    );
}

#[test]
fn test_wal_header_v1_rejects_unsupported_versions() {
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(2, &[8, 4, 0])),
        Err(WalHeaderV1Error::UnsupportedVersion(2))
    );
    assert_eq!(
        WalHeaderV1::from_bytes(&v1_bytes_with_version(u64::MAX, &[8, 4, 0])),
        Err(WalHeaderV1Error::UnsupportedVersion(u64::MAX))
    );

    assert_eq!(
        AnyWalHeader::from_bytes(&v1_bytes_with_version(2, &[8, 4, 0])),
        Err(AnyWalHeaderError::V1(WalHeaderV1Error::UnsupportedVersion(
            2
        )))
    );
}

#[tokio::test]
async fn test_wal_header_v1_reader_rejects_unsupported_versions() {
    let unsupported = v1_bytes_with_version(2, &[8, 4, 0]);
    let mut reader = BufReader::new(&unsupported[..]);
    assert_eq!(
        WalHeaderV1::from_reader(&mut reader).await,
        Err(WalHeaderV1Error::UnsupportedVersion(2))
    );
}

#[test]
fn test_wal_header_v1_u64_bounds() {
    let header = WalHeaderV1::new(8, 4, u64::MAX).unwrap();
    assert_eq!(header.size(), WAL_HEADER_V1_MAX_SIZE);

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, WAL_HEADER_V1_MAX_SIZE);

    let (decoded, bytes_read) = WalHeaderV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded.num_entries_before, u64::MAX);
    assert_eq!(bytes_read, WAL_HEADER_V1_MAX_SIZE);

    // A V0 header whose counter fits in a u64 converts; one above it does not.
    let convertible = WalHeader::new(8, 4, UintN::from(u64::MAX)).unwrap();
    assert_eq!(
        WalHeaderV1::from_v0(&convertible)
            .unwrap()
            .num_entries_before,
        u64::MAX
    );

    let too_large = WalHeader::new(8, 4, UintN::from(u128::MAX)).unwrap();
    assert_eq!(
        WalHeaderV1::from_v0(&too_large),
        Err(WalHeaderV1Error::ValueTooLarge)
    );
}
