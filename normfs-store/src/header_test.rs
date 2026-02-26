use super::header::{
    CompressionType, EncryptionType, FileAuthVersion, FileAuthentication, SignatureType,
    StoreHeader, StoreHeaderError, StoreHeaderVersion,
};
use bytes::BytesMut;
use uintn::UintN;

#[test]
fn test_store_header_serialization_deserialization() {
    let num_entries_before = UintN::from(10u64);
    let num_entries = UintN::from(20u64);
    let header = StoreHeader::new(
        CompressionType::None,
        EncryptionType::None,
        num_entries_before,
        num_entries,
    );

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);

    let (deserialized_header, bytes_read) = StoreHeader::from_bytes(&buffer).unwrap();

    assert_eq!(header.size(), bytes_read);
    assert_eq!(header.version, deserialized_header.version);
    assert_eq!(StoreHeaderVersion::V0, deserialized_header.version);
    assert_eq!(
        header.num_entries_before,
        deserialized_header.num_entries_before
    );
    assert_eq!(header.num_entries, deserialized_header.num_entries);
    assert!(!deserialized_header.is_compressed());
    assert!(!deserialized_header.is_encrypted());
}

#[test]
fn test_store_header_with_compression_and_encryption() {
    let num_entries_before = UintN::from(10u64);
    let num_entries = UintN::from(20u64);
    let header = StoreHeader::new(
        CompressionType::Xz,
        EncryptionType::Aes,
        num_entries_before,
        num_entries,
    );

    let mut buffer = BytesMut::new();
    header.write_to_bytes(&mut buffer);

    let (deserialized_header, bytes_read) = StoreHeader::from_bytes(&buffer).unwrap();

    assert_eq!(header.size(), bytes_read);
    assert_eq!(header.version, deserialized_header.version);
    assert_eq!(StoreHeaderVersion::V0, deserialized_header.version);
    assert_eq!(
        header.num_entries_before,
        deserialized_header.num_entries_before
    );
    assert_eq!(header.num_entries, deserialized_header.num_entries);
    assert!(deserialized_header.is_compressed());
    assert!(deserialized_header.is_encrypted());
    assert_eq!(deserialized_header.compression, CompressionType::Xz);
    assert_eq!(deserialized_header.encryption, EncryptionType::Aes);
}

#[test]
fn test_store_header_from_too_short_slice() {
    let data = vec![0; 7];
    let result = StoreHeader::from_bytes(&data);
    assert!(matches!(result, Err(StoreHeaderError::SliceTooShort)));
}

#[test]
fn test_store_header_unsupported_version() {
    let mut buffer = BytesMut::new();
    buffer.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]); // Version 1 (unsupported)
    buffer.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // Compression
    buffer.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]); // Encryption
    let num_entries_before = UintN::from(10u64);
    let num_entries = UintN::from(20u64);
    num_entries_before.write_to_buffer(&mut buffer);
    num_entries.write_to_buffer(&mut buffer);

    let result = StoreHeader::from_bytes(&buffer);
    assert!(matches!(
        result,
        Err(StoreHeaderError::UnsupportedVersion(1))
    ));
}

#[test]
fn test_file_authentication_serialization_deserialization() {
    let header_signature = [42u8; 64];
    let content_signature = [99u8; 64];
    let file_auth = FileAuthentication::new(header_signature, content_signature);

    let mut buffer = BytesMut::new();
    file_auth.write_to_bytes(&mut buffer);

    let (deserialized_auth, bytes_read) = FileAuthentication::from_bytes(&buffer).unwrap();

    assert_eq!(file_auth.size(), bytes_read);
    assert_eq!(152, bytes_read);
    assert_eq!(file_auth.version, deserialized_auth.version);
    assert_eq!(FileAuthVersion::V0, deserialized_auth.version);
    assert_eq!(
        file_auth.header_signature_type,
        deserialized_auth.header_signature_type
    );
    assert_eq!(
        file_auth.content_signature_type,
        deserialized_auth.content_signature_type
    );
    assert_eq!(
        file_auth.header_signature,
        deserialized_auth.header_signature
    );
    assert_eq!(
        file_auth.content_signature,
        deserialized_auth.content_signature
    );
    assert_eq!(
        deserialized_auth.header_signature_type,
        SignatureType::Ed25519
    );
    assert_eq!(
        deserialized_auth.content_signature_type,
        SignatureType::Ed25519
    );
}

#[test]
fn test_file_authentication_from_too_short_slice() {
    let data = vec![0; 151];
    let result = FileAuthentication::from_bytes(&data);
    assert!(matches!(result, Err(StoreHeaderError::SliceTooShort)));
}

#[test]
fn test_file_authentication_size() {
    let header_signature = [1u8; 64];
    let content_signature = [2u8; 64];
    let file_auth = FileAuthentication::new(header_signature, content_signature);

    assert_eq!(file_auth.size(), 152);
}
