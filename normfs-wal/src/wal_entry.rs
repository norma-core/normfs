use super::wal_header::WalHeader;
use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use uintn::{Error as UintNError, UintN};
use xxhash_rust::xxh64;

pub const WAL_ENTRY_HEADER_FIXED_OVERHEAD: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub enum WalEntryError {
    SliceTooShort,
    UintN(UintNError),
    UnsupportedVersion(u64),
}

impl std::fmt::Display for WalEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalEntryError::SliceTooShort => write!(f, "Slice too short for WAL entry"),
            WalEntryError::UintN(e) => write!(f, "UintN error: {}", e),
            WalEntryError::UnsupportedVersion(version) => {
                write!(f, "Unsupported WAL entry version: {}", version)
            }
        }
    }
}

impl std::error::Error for WalEntryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalEntryError::UintN(e) => Some(e),
            _ => None,
        }
    }
}

impl From<UintNError> for WalEntryError {
    fn from(e: UintNError) -> Self {
        WalEntryError::UintN(e)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct WalEntryHeader {
    pub entry_id: UintN,
    pub record_size: UintN,
    pub xxhash: u64,
}

impl WalEntryHeader {
    pub fn new(entry_id: UintN, record_data: &[u8]) -> Self {
        let record_size = UintN::from(record_data.len() as u64);
        let xxhash = xxh64::xxh64(record_data, 0);

        Self {
            entry_id,
            record_size,
            xxhash,
        }
    }

    pub fn size(&self, wal_header: &WalHeader) -> usize {
        WAL_ENTRY_HEADER_FIXED_OVERHEAD
            + wal_header.id_size_bytes as usize
            + wal_header.data_size_bytes as usize
    }

    pub fn write_to_bytes(
        &self,
        dest: &mut BytesMut,
        wal_header: &WalHeader,
    ) -> Result<(), UintNError> {
        dest.put_u64_le(0); // version

        self.entry_id
            .write_value_to_buffer_with_size(dest, wal_header.id_size_bytes as usize)?;
        self.record_size
            .write_value_to_buffer_with_size(dest, wal_header.data_size_bytes as usize)?;

        dest.put_u64_le(self.xxhash);

        Ok(())
    }

    pub fn from_bytes(data: &[u8], wal_header: &WalHeader) -> Result<Self, WalEntryError> {
        let id_size = wal_header.id_size_bytes as usize;
        let data_size = wal_header.data_size_bytes as usize;
        let expected_size = WAL_ENTRY_HEADER_FIXED_OVERHEAD + id_size + data_size;

        if data.len() < expected_size {
            return Err(WalEntryError::SliceTooShort);
        }

        let version = u64::from_le_bytes(data[0..8].try_into().unwrap());
        if version != 0 {
            return Err(WalEntryError::UnsupportedVersion(version));
        }

        let entry_id_slice = &data[8..8 + id_size];
        let entry_id = UintN::read_value_from_slice(entry_id_slice, id_size)?;

        let record_size_slice = &data[8 + id_size..8 + id_size + data_size];
        let record_size = UintN::read_value_from_slice(record_size_slice, data_size)?;

        let xxhash_slice = &data[8 + id_size + data_size..expected_size];
        let xxhash = u64::from_le_bytes(xxhash_slice.try_into().unwrap());

        Ok(Self {
            entry_id,
            record_size,
            xxhash,
        })
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
        wal_header: &WalHeader,
    ) -> Result<Self, WalEntryError> {
        let id_size = wal_header.id_size_bytes as usize;
        let data_size = wal_header.data_size_bytes as usize;
        let expected_size = WAL_ENTRY_HEADER_FIXED_OVERHEAD + id_size + data_size;

        let mut buf = vec![0; expected_size];
        reader
            .read_exact(&mut buf)
            .await
            .map_err(|_| WalEntryError::SliceTooShort)?;

        Self::from_bytes(&buf, wal_header)
    }
}
