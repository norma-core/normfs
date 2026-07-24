use bytes::BytesMut;
use std::io::Cursor;
use std::os::raw::c_int;
use tokio::io::{AsyncRead, AsyncReadExt};
use uintn::{UintN, varint};

use super::wal_header::{WalHeader, WalHeaderError};

pub const WAL_HEADER_V0_VERSION: u64 = 0;
pub const WAL_HEADER_V1_VERSION: u64 = 1;
pub const WAL_HEADER_V1_MAX_SIZE: usize = 13;

const NORMFS_WAL_HEADER_OK: c_int = 0;
const NORMFS_WAL_HEADER_ERR_TRUNCATED: c_int = 1;
const NORMFS_WAL_HEADER_ERR_OVERFLOW: c_int = 2;
const NORMFS_WAL_HEADER_ERR_NON_CANONICAL: c_int = 3;
const NORMFS_WAL_HEADER_ERR_NO_SPACE: c_int = 4;
const NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION: c_int = 5;
const NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE: c_int = 6;
const NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE: c_int = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalHeaderV1Error {
    Truncated,
    Overflow,
    NonCanonical,
    NoSpace,
    UnsupportedVersion(u64),
    InvalidDataSizeBytes(u64),
    InvalidIdSizeBytes(u64),
    ValueTooLarge,
    UnknownStatus(c_int),
}

impl std::fmt::Display for WalHeaderV1Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalHeaderV1Error::Truncated => write!(f, "Truncated V1 WAL header"),
            WalHeaderV1Error::Overflow => write!(f, "V1 WAL header varint exceeds u64"),
            WalHeaderV1Error::NonCanonical => write!(f, "Non-canonical V1 WAL header varint"),
            WalHeaderV1Error::NoSpace => write!(f, "Buffer too small for V1 WAL header"),
            WalHeaderV1Error::UnsupportedVersion(version) => {
                write!(f, "Unsupported WAL version: {}", version)
            }
            WalHeaderV1Error::InvalidDataSizeBytes(size) => {
                write!(f, "Invalid data size bytes: {}", size)
            }
            WalHeaderV1Error::InvalidIdSizeBytes(size) => {
                write!(f, "Invalid ID size bytes: {}", size)
            }
            WalHeaderV1Error::ValueTooLarge => {
                write!(f, "Value does not fit in a u64 V1 WAL header field")
            }
            WalHeaderV1Error::UnknownStatus(status) => {
                write!(f, "Unknown V1 WAL header status: {}", status)
            }
        }
    }
}

impl std::error::Error for WalHeaderV1Error {}

impl From<varint::VarintError> for WalHeaderV1Error {
    fn from(e: varint::VarintError) -> Self {
        match e {
            varint::VarintError::Truncated => WalHeaderV1Error::Truncated,
            varint::VarintError::Overflow => WalHeaderV1Error::Overflow,
            varint::VarintError::NonCanonical => WalHeaderV1Error::NonCanonical,
            varint::VarintError::NoSpace => WalHeaderV1Error::NoSpace,
            varint::VarintError::UnknownStatus(status) => WalHeaderV1Error::UnknownStatus(status),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CWalHeaderV1 {
    data_size_bytes: u64,
    id_size_bytes: u64,
    num_entries_before: u64,
}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

#[repr(C)]
struct CDecodeResult {
    header: CWalHeaderV1,
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
    fn normfs_wal_header_v1_size(header: *const CWalHeaderV1) -> usize;

    fn normfs_wal_header_v1_encode(
        header: *const CWalHeaderV1,
        out: *mut u8,
        out_len: usize,
    ) -> CEncodeResult;

    fn normfs_wal_header_v1_decode(buf: *const u8, len: usize) -> CDecodeResult;

    fn normfs_wal_header_peek_version(buf: *const u8, len: usize) -> CVersionResult;
}

fn map_status(status: c_int, header: &CWalHeaderV1, version: u64) -> Result<(), WalHeaderV1Error> {
    match status {
        NORMFS_WAL_HEADER_OK => Ok(()),
        NORMFS_WAL_HEADER_ERR_TRUNCATED => Err(WalHeaderV1Error::Truncated),
        NORMFS_WAL_HEADER_ERR_OVERFLOW => Err(WalHeaderV1Error::Overflow),
        NORMFS_WAL_HEADER_ERR_NON_CANONICAL => Err(WalHeaderV1Error::NonCanonical),
        NORMFS_WAL_HEADER_ERR_NO_SPACE => Err(WalHeaderV1Error::NoSpace),
        NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION => {
            Err(WalHeaderV1Error::UnsupportedVersion(version))
        }
        NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE => Err(WalHeaderV1Error::InvalidDataSizeBytes(
            header.data_size_bytes,
        )),
        NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE => {
            Err(WalHeaderV1Error::InvalidIdSizeBytes(header.id_size_bytes))
        }
        other => Err(WalHeaderV1Error::UnknownStatus(other)),
    }
}

/// Reads the version of a header without consuming it.
///
/// A V0 header starts with a `u64` little-endian zero, so byte zero tells the
/// two encodings apart before either decoder runs.
pub fn peek_version(data: &[u8]) -> Result<u64, WalHeaderV1Error> {
    let result = unsafe { normfs_wal_header_peek_version(data.as_ptr(), data.len()) };
    map_status(
        result.status,
        &CWalHeaderV1 {
            data_size_bytes: 0,
            id_size_bytes: 0,
            num_entries_before: 0,
        },
        result.version,
    )?;
    Ok(result.version)
}

async fn read_varint_u64<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(u64, usize), WalHeaderV1Error> {
    let mut buf = [0u8; varint::MAX_VARINT64_LEN];
    let mut len = 0usize;

    while len < varint::MAX_VARINT64_LEN {
        reader
            .read_exact(&mut buf[len..len + 1])
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;
        len += 1;
        if buf[len - 1] < 0x80 {
            break;
        }
    }

    let decoded = varint::decode_u64(&buf[..len])?;
    Ok((decoded.value, decoded.consumed))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeaderV1 {
    pub data_size_bytes: u64,
    pub id_size_bytes: u64,
    pub num_entries_before: u64,
}

impl WalHeaderV1 {
    pub fn new(
        data_size_bytes: u64,
        id_size_bytes: u64,
        num_entries_before: u64,
    ) -> Result<Self, WalHeaderV1Error> {
        if !UintN::is_valid_data_size(data_size_bytes) {
            return Err(WalHeaderV1Error::InvalidDataSizeBytes(data_size_bytes));
        }
        if !UintN::is_valid_data_size(id_size_bytes) {
            return Err(WalHeaderV1Error::InvalidIdSizeBytes(id_size_bytes));
        }

        Ok(Self {
            data_size_bytes,
            id_size_bytes,
            num_entries_before,
        })
    }

    /// Re-encodes a V0 header as V1. Fails when `num_entries_before` does not
    /// fit in a u64, which V1 headers are bounded to.
    pub fn from_v0(header: &WalHeader) -> Result<Self, WalHeaderV1Error> {
        let num_entries_before = header
            .num_entries_before
            .to_u64()
            .map_err(|_| WalHeaderV1Error::ValueTooLarge)?;

        Self::new(
            header.data_size_bytes,
            header.id_size_bytes,
            num_entries_before,
        )
    }

    fn as_c(&self) -> CWalHeaderV1 {
        CWalHeaderV1 {
            data_size_bytes: self.data_size_bytes,
            id_size_bytes: self.id_size_bytes,
            num_entries_before: self.num_entries_before,
        }
    }

    pub fn size(&self) -> usize {
        let header = self.as_c();
        unsafe { normfs_wal_header_v1_size(&header) }
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> Result<usize, WalHeaderV1Error> {
        let header = self.as_c();
        let mut buf = [0u8; WAL_HEADER_V1_MAX_SIZE];

        let result = unsafe { normfs_wal_header_v1_encode(&header, buf.as_mut_ptr(), buf.len()) };
        map_status(result.status, &header, WAL_HEADER_V1_VERSION)?;

        dest.extend_from_slice(&buf[..result.written]);
        Ok(result.written)
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), WalHeaderV1Error> {
        let result = unsafe { normfs_wal_header_v1_decode(data.as_ptr(), data.len()) };
        let version = peek_version(data).unwrap_or(WAL_HEADER_V1_VERSION);
        map_status(result.status, &result.header, version)?;

        Ok((
            Self {
                data_size_bytes: result.header.data_size_bytes,
                id_size_bytes: result.header.id_size_bytes,
                num_entries_before: result.header.num_entries_before,
            },
            result.consumed,
        ))
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, usize), WalHeaderV1Error> {
        let (version, version_len) = read_varint_u64(reader).await?;
        if version != WAL_HEADER_V1_VERSION {
            return Err(WalHeaderV1Error::UnsupportedVersion(version));
        }

        let (data_size_bytes, data_size_len) = read_varint_u64(reader).await?;
        let (id_size_bytes, id_size_len) = read_varint_u64(reader).await?;
        let (num_entries_before, num_entries_len) = read_varint_u64(reader).await?;

        let header = Self::new(data_size_bytes, id_size_bytes, num_entries_before)?;

        Ok((
            header,
            version_len + data_size_len + id_size_len + num_entries_len,
        ))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AnyWalHeaderError {
    V0(WalHeaderError),
    V1(WalHeaderV1Error),
}

impl std::fmt::Display for AnyWalHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyWalHeaderError::V0(e) => write!(f, "{}", e),
            AnyWalHeaderError::V1(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for AnyWalHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            AnyWalHeaderError::V0(e) => Some(e),
            AnyWalHeaderError::V1(e) => Some(e),
        }
    }
}

impl From<WalHeaderError> for AnyWalHeaderError {
    fn from(e: WalHeaderError) -> Self {
        AnyWalHeaderError::V0(e)
    }
}

impl From<WalHeaderV1Error> for AnyWalHeaderError {
    fn from(e: WalHeaderV1Error) -> Self {
        AnyWalHeaderError::V1(e)
    }
}

/// A WAL header of either on-disk version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnyWalHeader {
    V0(WalHeader),
    V1(WalHeaderV1),
}

impl AnyWalHeader {
    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), AnyWalHeaderError> {
        if peek_version(data)? == WAL_HEADER_V0_VERSION {
            let (header, bytes_read) = WalHeader::from_bytes(data)?;
            return Ok((AnyWalHeader::V0(header), bytes_read));
        }

        let (header, bytes_read) = WalHeaderV1::from_bytes(data)?;
        Ok((AnyWalHeader::V1(header), bytes_read))
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, usize), AnyWalHeaderError> {
        let mut version_byte = [0u8; 1];
        reader
            .read_exact(&mut version_byte)
            .await
            .map_err(|_| WalHeaderV1Error::Truncated)?;

        let mut reader = Cursor::new(version_byte).chain(reader);

        if version_byte[0] == 0 {
            let (header, bytes_read) = WalHeader::from_reader(&mut reader).await?;
            return Ok((AnyWalHeader::V0(header), bytes_read));
        }

        let (header, bytes_read) = WalHeaderV1::from_reader(&mut reader).await?;
        Ok((AnyWalHeader::V1(header), bytes_read))
    }

    pub fn version(&self) -> u64 {
        match self {
            AnyWalHeader::V0(_) => WAL_HEADER_V0_VERSION,
            AnyWalHeader::V1(_) => WAL_HEADER_V1_VERSION,
        }
    }

    pub fn data_size_bytes(&self) -> u64 {
        match self {
            AnyWalHeader::V0(header) => header.data_size_bytes,
            AnyWalHeader::V1(header) => header.data_size_bytes,
        }
    }

    pub fn id_size_bytes(&self) -> u64 {
        match self {
            AnyWalHeader::V0(header) => header.id_size_bytes,
            AnyWalHeader::V1(header) => header.id_size_bytes,
        }
    }

    pub fn num_entries_before(&self) -> UintN {
        match self {
            AnyWalHeader::V0(header) => header.num_entries_before.clone(),
            AnyWalHeader::V1(header) => UintN::from(header.num_entries_before),
        }
    }
}
