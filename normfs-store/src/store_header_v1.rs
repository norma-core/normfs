use bytes::BytesMut;
use std::os::raw::c_int;
use uintn::{UintN, varint};

use super::header::{StoreHeader, StoreHeaderError};
use normfs_types::{CompressionType, EncryptionType};

pub const STORE_HEADER_V0_VERSION: u64 = 0;
pub const STORE_HEADER_V1_VERSION: u64 = 1;

/// The version word keeps its V0 encoding, a fixed 8 byte little-endian u64,
/// so that readers predating V1 still report an unsupported version instead of
/// misparsing the file.
pub const STORE_HEADER_VERSION_SIZE: usize = 8;
pub const STORE_HEADER_V1_MIN_SIZE: usize = 12;
pub const STORE_HEADER_V1_MAX_SIZE: usize = 48;

const NORMFS_STORE_HEADER_OK: c_int = 0;
const NORMFS_STORE_HEADER_ERR_TRUNCATED: c_int = 1;
const NORMFS_STORE_HEADER_ERR_OVERFLOW: c_int = 2;
const NORMFS_STORE_HEADER_ERR_NON_CANONICAL: c_int = 3;
const NORMFS_STORE_HEADER_ERR_NO_SPACE: c_int = 4;
const NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION: c_int = 5;
const NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION: c_int = 6;
const NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION: c_int = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreHeaderV1Error {
    Truncated,
    Overflow,
    NonCanonical,
    NoSpace,
    UnsupportedVersion(u64),
    UnsupportedCompression(u64),
    UnsupportedEncryption(u64),
    ValueTooLarge,
    UnknownStatus(c_int),
}

impl std::fmt::Display for StoreHeaderV1Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreHeaderV1Error::Truncated => write!(f, "Truncated V1 store header"),
            StoreHeaderV1Error::Overflow => write!(f, "V1 store header varint exceeds u64"),
            StoreHeaderV1Error::NonCanonical => write!(f, "Non-canonical V1 store header varint"),
            StoreHeaderV1Error::NoSpace => write!(f, "Buffer too small for V1 store header"),
            StoreHeaderV1Error::UnsupportedVersion(version) => {
                write!(f, "Unsupported store version: {}", version)
            }
            StoreHeaderV1Error::UnsupportedCompression(c) => {
                write!(f, "Unsupported compression type: {}", c)
            }
            StoreHeaderV1Error::UnsupportedEncryption(e) => {
                write!(f, "Unsupported encryption type: {}", e)
            }
            StoreHeaderV1Error::ValueTooLarge => {
                write!(f, "Value does not fit in a u64 V1 store header field")
            }
            StoreHeaderV1Error::UnknownStatus(status) => {
                write!(f, "Unknown V1 store header status: {}", status)
            }
        }
    }
}

impl std::error::Error for StoreHeaderV1Error {}

impl From<varint::VarintError> for StoreHeaderV1Error {
    fn from(e: varint::VarintError) -> Self {
        match e {
            varint::VarintError::Truncated => StoreHeaderV1Error::Truncated,
            varint::VarintError::Overflow => StoreHeaderV1Error::Overflow,
            varint::VarintError::NonCanonical => StoreHeaderV1Error::NonCanonical,
            varint::VarintError::NoSpace => StoreHeaderV1Error::NoSpace,
            varint::VarintError::UnknownStatus(status) => StoreHeaderV1Error::UnknownStatus(status),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CStoreHeaderV1 {
    compression: u64,
    encryption: u64,
    num_entries_before: u64,
    num_entries: u64,
}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

#[repr(C)]
struct CDecodeResult {
    header: CStoreHeaderV1,
    version: u64,
    consumed: usize,
    status: c_int,
}

#[repr(C)]
struct CVersionResult {
    version: u64,
    consumed: usize,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_store_header_v1_size(header: *const CStoreHeaderV1) -> usize;

    fn normfs_store_header_v1_encode(
        header: *const CStoreHeaderV1,
        out: *mut u8,
        out_len: usize,
    ) -> CEncodeResult;

    fn normfs_store_header_v1_decode(buf: *const u8, len: usize) -> CDecodeResult;

    fn normfs_store_header_peek_version(buf: *const u8, len: usize) -> CVersionResult;
}

fn map_status(status: c_int, version: u64) -> Result<(), StoreHeaderV1Error> {
    match status {
        NORMFS_STORE_HEADER_OK => Ok(()),
        NORMFS_STORE_HEADER_ERR_TRUNCATED => Err(StoreHeaderV1Error::Truncated),
        NORMFS_STORE_HEADER_ERR_OVERFLOW => Err(StoreHeaderV1Error::Overflow),
        NORMFS_STORE_HEADER_ERR_NON_CANONICAL => Err(StoreHeaderV1Error::NonCanonical),
        NORMFS_STORE_HEADER_ERR_NO_SPACE => Err(StoreHeaderV1Error::NoSpace),
        NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION => {
            Err(StoreHeaderV1Error::UnsupportedVersion(version))
        }
        other => Err(StoreHeaderV1Error::UnknownStatus(other)),
    }
}

/// Same as [`map_status`], but for the statuses that name a rejected field, so
/// the error can carry the offending code back.
fn map_field_status(
    status: c_int,
    version: u64,
    header: &CStoreHeaderV1,
) -> Result<(), StoreHeaderV1Error> {
    match status {
        NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION => Err(
            StoreHeaderV1Error::UnsupportedCompression(header.compression),
        ),
        NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION => Err(
            StoreHeaderV1Error::UnsupportedEncryption(header.encryption),
        ),
        other => map_status(other, version),
    }
}

/// Maps a compression code that the C decoder has already accepted.
///
/// This is not a second validation pass. `normfs_store_header_v1_validate`
/// proves `0 <= compression <= NORMFS_STORE_COMPRESSION_MAX`, and both
/// `normfs_store_header_v1_encode` and `_decode` return `OK` only when it
/// holds, so the mapping is total on every value that reaches here.
fn compression_from_c(value: u64) -> CompressionType {
    match value {
        0 => CompressionType::None,
        1 => CompressionType::Gzip,
        2 => CompressionType::Xz,
        3 => CompressionType::Zstd,
        _ => unreachable!("C decoder returned OK with compression {}", value),
    }
}

/// Maps an encryption code that the C decoder has already accepted. See
/// [`compression_from_c`] for why no error case is needed.
fn encryption_from_c(value: u64) -> EncryptionType {
    match value {
        0 => EncryptionType::None,
        1 => EncryptionType::Aes,
        _ => unreachable!("C decoder returned OK with encryption {}", value),
    }
}

/// Reads the version word of a header without consuming the rest of it.
///
/// Both versions carry the version as a `u64` little-endian word at offset 0,
/// which is what lets a reader tell them apart, and what lets a V0-only reader
/// reject a V1 file cleanly.
pub fn peek_version(data: &[u8]) -> Result<u64, StoreHeaderV1Error> {
    let result = unsafe { normfs_store_header_peek_version(data.as_ptr(), data.len()) };
    map_status(result.status, result.version)?;
    Ok(result.version)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreHeaderV1 {
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    pub num_entries_before: u64,
    pub num_entries: u64,
}

impl StoreHeaderV1 {
    pub fn new(
        compression: CompressionType,
        encryption: EncryptionType,
        num_entries_before: u64,
        num_entries: u64,
    ) -> Self {
        Self {
            compression,
            encryption,
            num_entries_before,
            num_entries,
        }
    }

    /// Re-encodes a V0 header as V1. Fails when either entry count does not fit
    /// in a u64, which V1 headers are bounded to.
    pub fn from_v0(header: &StoreHeader) -> Result<Self, StoreHeaderV1Error> {
        let num_entries_before = header
            .num_entries_before
            .to_u64()
            .map_err(|_| StoreHeaderV1Error::ValueTooLarge)?;
        let num_entries = header
            .num_entries
            .to_u64()
            .map_err(|_| StoreHeaderV1Error::ValueTooLarge)?;

        Ok(Self::new(
            header.compression,
            header.encryption,
            num_entries_before,
            num_entries,
        ))
    }

    fn as_c(&self) -> CStoreHeaderV1 {
        CStoreHeaderV1 {
            compression: self.compression.into(),
            encryption: self.encryption.into(),
            num_entries_before: self.num_entries_before,
            num_entries: self.num_entries,
        }
    }

    pub fn size(&self) -> usize {
        let header = self.as_c();
        unsafe { normfs_store_header_v1_size(&header) }
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> Result<usize, StoreHeaderV1Error> {
        let header = self.as_c();
        let mut buf = [0u8; STORE_HEADER_V1_MAX_SIZE];

        let result = unsafe { normfs_store_header_v1_encode(&header, buf.as_mut_ptr(), buf.len()) };
        map_field_status(result.status, STORE_HEADER_V1_VERSION, &header)?;

        dest.extend_from_slice(&buf[..result.written]);
        Ok(result.written)
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), StoreHeaderV1Error> {
        let result = unsafe { normfs_store_header_v1_decode(data.as_ptr(), data.len()) };
        map_field_status(result.status, result.version, &result.header)?;

        Ok((
            Self {
                compression: compression_from_c(result.header.compression),
                encryption: encryption_from_c(result.header.encryption),
                num_entries_before: result.header.num_entries_before,
                num_entries: result.header.num_entries,
            },
            result.consumed,
        ))
    }

    pub fn is_encrypted(&self) -> bool {
        self.encryption != EncryptionType::None
    }

    pub fn is_compressed(&self) -> bool {
        self.compression != CompressionType::None
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AnyStoreHeaderError {
    V0(StoreHeaderError),
    V1(StoreHeaderV1Error),
}

impl std::fmt::Display for AnyStoreHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyStoreHeaderError::V0(e) => write!(f, "{}", e),
            AnyStoreHeaderError::V1(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for AnyStoreHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AnyStoreHeaderError::V0(e) => Some(e),
            AnyStoreHeaderError::V1(e) => Some(e),
        }
    }
}

impl From<StoreHeaderError> for AnyStoreHeaderError {
    fn from(e: StoreHeaderError) -> Self {
        AnyStoreHeaderError::V0(e)
    }
}

impl From<StoreHeaderV1Error> for AnyStoreHeaderError {
    fn from(e: StoreHeaderV1Error) -> Self {
        AnyStoreHeaderError::V1(e)
    }
}

/// A store header of either on-disk version.
#[derive(Debug, PartialEq, Eq)]
pub enum AnyStoreHeader {
    V0(StoreHeader),
    V1(StoreHeaderV1),
}

impl AnyStoreHeader {
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), AnyStoreHeaderError> {
        if peek_version(data)? == STORE_HEADER_V0_VERSION {
            let (header, bytes_read) = StoreHeader::from_bytes(data)?;
            return Ok((AnyStoreHeader::V0(header), bytes_read));
        }

        let (header, bytes_read) = StoreHeaderV1::from_bytes(data)?;
        Ok((AnyStoreHeader::V1(header), bytes_read))
    }

    pub fn version(&self) -> u64 {
        match self {
            AnyStoreHeader::V0(_) => STORE_HEADER_V0_VERSION,
            AnyStoreHeader::V1(_) => STORE_HEADER_V1_VERSION,
        }
    }

    pub fn compression(&self) -> CompressionType {
        match self {
            AnyStoreHeader::V0(header) => header.compression,
            AnyStoreHeader::V1(header) => header.compression,
        }
    }

    pub fn encryption(&self) -> EncryptionType {
        match self {
            AnyStoreHeader::V0(header) => header.encryption,
            AnyStoreHeader::V1(header) => header.encryption,
        }
    }

    pub fn num_entries_before(&self) -> UintN {
        match self {
            AnyStoreHeader::V0(header) => header.num_entries_before.clone(),
            AnyStoreHeader::V1(header) => UintN::from(header.num_entries_before),
        }
    }

    pub fn num_entries(&self) -> UintN {
        match self {
            AnyStoreHeader::V0(header) => header.num_entries.clone(),
            AnyStoreHeader::V1(header) => UintN::from(header.num_entries),
        }
    }

    pub fn is_encrypted(&self) -> bool {
        self.encryption() != EncryptionType::None
    }

    pub fn is_compressed(&self) -> bool {
        self.compression() != CompressionType::None
    }
}
