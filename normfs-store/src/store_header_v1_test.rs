use super::header::{StoreHeader, StoreHeaderVersion};
use super::store_header_v1::{
    AnyStoreHeader, AnyStoreHeaderError, STORE_HEADER_V0_VERSION, STORE_HEADER_V1_VERSION,
    StoreHeaderV1, StoreHeaderV1Error, peek_version,
};
use bytes::BytesMut;
use normfs_types::{CompressionType, EncryptionType};
use uintn::UintN;

fn v0_bytes(num_entries_before: UintN, num_entries: UintN) -> BytesMut {
    let header = StoreHeader::new(
        CompressionType::Zstd,
        EncryptionType::Aes,
        num_entries_before,
        num_entries,
    );

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);
    buffer
}

#[test]
fn test_store_header_v0_still_parses_through_dispatch() {
    let buffer = v0_bytes(UintN::from(10u64), UintN::from(20u64));

    let (header, bytes_read) = AnyStoreHeader::from_bytes(&buffer).unwrap();
    assert_eq!(header.version(), STORE_HEADER_V0_VERSION);
    assert_eq!(header.compression(), CompressionType::Zstd);
    assert_eq!(header.encryption(), EncryptionType::Aes);
    assert_eq!(header.num_entries_before(), UintN::from(10u64));
    assert_eq!(header.num_entries(), UintN::from(20u64));
    assert_eq!(bytes_read, buffer.len());

    assert!(matches!(header, AnyStoreHeader::V0(_)));
}

#[test]
fn test_store_header_v0_write_output_is_unchanged() {
    let buffer = v0_bytes(UintN::from(10u64), UintN::from(20u64));

    let (header, _) = StoreHeader::from_bytes(&buffer).unwrap();
    assert_eq!(header.version, StoreHeaderVersion::V0);

    let mut reserialized = BytesMut::new();
    header.write_to_bytes(&mut reserialized);

    assert_eq!(reserialized.as_ref(), buffer.as_ref());
    assert_eq!(
        peek_version(reserialized.as_ref()).unwrap(),
        STORE_HEADER_V0_VERSION
    );
}

fn roundtrip(
    compression: CompressionType,
    encryption: EncryptionType,
    num_entries_before: u64,
    num_entries: u64,
    expected: usize,
) {
    let header = StoreHeaderV1::new(compression, encryption, num_entries_before, num_entries);
    assert_eq!(header.size(), expected);

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, expected);
    assert_eq!(buffer.len(), expected);
    assert_eq!(buffer[0] as u64, STORE_HEADER_V1_VERSION);

    let (decoded, bytes_read) = StoreHeaderV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(bytes_read, expected);
}

#[test]
fn test_store_header_v1_roundtrip_across_varint_widths() {
    roundtrip(CompressionType::None, EncryptionType::None, 0, 0, 5);
    roundtrip(CompressionType::Zstd, EncryptionType::Aes, 127, 127, 5);
    roundtrip(CompressionType::Xz, EncryptionType::Aes, 128, 127, 6);
    roundtrip(CompressionType::Xz, EncryptionType::Aes, 16_383, 16_384, 8);
    roundtrip(
        CompressionType::Zstd,
        EncryptionType::Aes,
        9_876_543_210,
        1_000,
        10,
    );
}

#[test]
fn test_store_header_v1_roundtrip_through_dispatch() {
    let header = StoreHeaderV1::new(CompressionType::Gzip, EncryptionType::None, 5, 7);

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();

    let (decoded, bytes_read) = AnyStoreHeader::from_bytes(&buffer).unwrap();
    assert_eq!(decoded, AnyStoreHeader::V1(header));
    assert_eq!(decoded.version(), STORE_HEADER_V1_VERSION);
    assert_eq!(decoded.num_entries_before(), UintN::from(5u64));
    assert_eq!(decoded.num_entries(), UintN::from(7u64));
    assert_eq!(bytes_read, written);

    assert!(!decoded.compression().eq(&CompressionType::None));
    assert_eq!(decoded.encryption(), EncryptionType::None);
}

#[test]
fn test_store_header_v1_rejects_malformed_varints() {
    let header = StoreHeaderV1::new(
        CompressionType::Zstd,
        EncryptionType::Aes,
        u64::MAX,
        u64::MAX,
    );
    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();

    for len in 0..written {
        assert_eq!(
            StoreHeaderV1::from_bytes(&buffer[..len]),
            Err(StoreHeaderV1Error::Truncated),
            "length {} should be truncated",
            len
        );
    }

    // num_entries_before padded to two bytes instead of the canonical one.
    assert_eq!(
        StoreHeaderV1::from_bytes(&[1, 0, 0, 0x83, 0x00, 0]),
        Err(StoreHeaderV1Error::NonCanonical)
    );

    // num_entries_before needs an eleventh byte to be represented.
    assert_eq!(
        StoreHeaderV1::from_bytes(&[
            1, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02, 0
        ]),
        Err(StoreHeaderV1Error::Overflow)
    );
}

#[test]
fn test_store_header_v1_rejects_unsupported_types() {
    assert_eq!(
        StoreHeaderV1::from_bytes(&[1, 9, 0, 0, 0]),
        Err(StoreHeaderV1Error::UnsupportedCompression(9))
    );
    assert_eq!(
        StoreHeaderV1::from_bytes(&[1, 0, 9, 0, 0]),
        Err(StoreHeaderV1Error::UnsupportedEncryption(9))
    );
}

#[test]
fn test_store_header_v1_rejects_unsupported_versions() {
    assert_eq!(
        StoreHeaderV1::from_bytes(&[2, 0, 0, 0, 0]),
        Err(StoreHeaderV1Error::UnsupportedVersion(2))
    );

    // 255 encoded as a two byte varint.
    assert_eq!(
        StoreHeaderV1::from_bytes(&[0xff, 0x01, 0, 0, 0, 0]),
        Err(StoreHeaderV1Error::UnsupportedVersion(255))
    );

    assert_eq!(
        AnyStoreHeader::from_bytes(&[2, 0, 0, 0, 0]),
        Err(AnyStoreHeaderError::V1(
            StoreHeaderV1Error::UnsupportedVersion(2)
        ))
    );
}

#[test]
fn test_store_header_v1_u64_bounds() {
    // The largest values a V1 header can carry occupy ten byte varints each.
    let header = StoreHeaderV1::new(
        CompressionType::None,
        EncryptionType::None,
        u64::MAX,
        u64::MAX,
    );
    assert_eq!(header.size(), 23);

    let mut buffer = BytesMut::new();
    let written = header.write_to_bytes(&mut buffer).unwrap();
    assert_eq!(written, 23);

    let (decoded, bytes_read) = StoreHeaderV1::from_bytes(&buffer).unwrap();
    assert_eq!(decoded.num_entries_before, u64::MAX);
    assert_eq!(decoded.num_entries, u64::MAX);
    assert_eq!(bytes_read, 23);

    // A V0 header whose counters fit in a u64 converts; one above it does not.
    let convertible = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(u64::MAX),
        UintN::from(0u64),
    );
    assert_eq!(
        StoreHeaderV1::from_v0(&convertible)
            .unwrap()
            .num_entries_before,
        u64::MAX
    );

    let too_large = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        UintN::from(u128::MAX),
        UintN::from(0u64),
    );
    assert_eq!(
        StoreHeaderV1::from_v0(&too_large),
        Err(StoreHeaderV1Error::ValueTooLarge)
    );
}
