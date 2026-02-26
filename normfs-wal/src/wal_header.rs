use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};
use uintn::{Error as UintNError, UintN};

pub const WAL_HEADER_FIXED_SIZE: usize = 24;

#[derive(Debug, PartialEq, Eq)]
pub enum WalHeaderError {
    SliceTooShort,
    InvalidDataSizeBytes(u64),
    InvalidIdSizeBytes(u64),
    UintN(UintNError),
    UnsupportedVersion(u64),
}

impl std::fmt::Display for WalHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalHeaderError::SliceTooShort => write!(f, "Slice too short for WAL header"),
            WalHeaderError::InvalidDataSizeBytes(size) => {
                write!(f, "Invalid data size bytes: {}", size)
            }
            WalHeaderError::InvalidIdSizeBytes(size) => {
                write!(f, "Invalid ID size bytes: {}", size)
            }
            WalHeaderError::UintN(e) => write!(f, "UintN error: {}", e),
            WalHeaderError::UnsupportedVersion(version) => {
                write!(f, "Unsupported WAL version: {}", version)
            }
        }
    }
}

impl std::error::Error for WalHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            WalHeaderError::UintN(e) => Some(e),
            _ => None,
        }
    }
}

impl From<UintNError> for WalHeaderError {
    fn from(e: UintNError) -> Self {
        WalHeaderError::UintN(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalHeader {
    pub data_size_bytes: u64,
    pub id_size_bytes: u64,
    pub num_entries_before: UintN,
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            data_size_bytes: 8,
            id_size_bytes: 4,
            num_entries_before: UintN::zero(),
        }
    }
}

impl WalHeader {
    pub fn resize(&self, entry_id: &UintN, data_size: usize) -> Self {
        let required_id_size = entry_id.size_in_bytes() as u64;
        let required_data_size = UintN::from(data_size as u64).size_in_bytes() as u64;

        let mut new_header = self.clone();
        new_header.id_size_bytes = self.id_size_bytes.max(required_id_size);
        new_header.data_size_bytes = self.data_size_bytes.max(required_data_size);
        new_header
    }

    pub fn can_hold_entry(&self, entry_id: &UintN, data_size: usize) -> bool {
        entry_id.can_fit(self.id_size_bytes)
            && UintN::from(data_size as u64).can_fit(self.data_size_bytes)
    }

    pub fn new(
        data_size_bytes: u64,
        id_size_bytes: u64,
        num_entries_before: UintN,
    ) -> Result<Self, WalHeaderError> {
        if !UintN::is_valid_data_size(data_size_bytes) {
            return Err(WalHeaderError::InvalidDataSizeBytes(data_size_bytes));
        }
        if !UintN::is_valid_data_size(id_size_bytes) {
            return Err(WalHeaderError::InvalidIdSizeBytes(id_size_bytes));
        }

        Ok(Self {
            data_size_bytes,
            id_size_bytes,
            num_entries_before,
        })
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), WalHeaderError> {
        if data.len() < WAL_HEADER_FIXED_SIZE {
            return Err(WalHeaderError::SliceTooShort);
        }

        let version = u64::from_le_bytes(data[0..8].try_into().unwrap());
        if version != 0 {
            return Err(WalHeaderError::UnsupportedVersion(version));
        }

        let data_size_bytes = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let id_size_bytes = u64::from_le_bytes(data[16..24].try_into().unwrap());

        let (num_entries_before, bytes_read) =
            UintN::read_from_slice(&data[WAL_HEADER_FIXED_SIZE..])?;

        Ok((
            Self {
                data_size_bytes,
                id_size_bytes,
                num_entries_before,
            },
            WAL_HEADER_FIXED_SIZE + bytes_read,
        ))
    }

    pub async fn from_reader<R: AsyncRead + Unpin>(
        reader: &mut R,
    ) -> Result<(Self, usize), WalHeaderError> {
        let mut fixed_header_buf = [0u8; WAL_HEADER_FIXED_SIZE];
        reader
            .read_exact(&mut fixed_header_buf)
            .await
            .map_err(|_| WalHeaderError::SliceTooShort)?;

        let version = u64::from_le_bytes(fixed_header_buf[0..8].try_into().unwrap());
        let data_size_bytes = u64::from_le_bytes(fixed_header_buf[8..16].try_into().unwrap());
        let id_size_bytes = u64::from_le_bytes(fixed_header_buf[16..24].try_into().unwrap());

        if version != 0 {
            return Err(WalHeaderError::UnsupportedVersion(version));
        }

        let mut header = Self {
            data_size_bytes,
            id_size_bytes,
            num_entries_before: UintN::from(0u8), // Placeholder
        };

        let mut len_buf = [0u8; 8];
        reader
            .read_exact(&mut len_buf)
            .await
            .map_err(|_| WalHeaderError::SliceTooShort)?;
        let len = u64::from_le_bytes(len_buf) as usize;

        if len > 10 * 1024 * 1024 {
            // 10mb entry guard
            return Err(WalHeaderError::SliceTooShort);
        }

        let mut value_buf = vec![0u8; len];
        if len > 0 {
            reader
                .read_exact(&mut value_buf)
                .await
                .map_err(|_| WalHeaderError::SliceTooShort)?;
        }

        let num_entries_before = UintN::read_value_from_slice(&value_buf, len)?;
        header.num_entries_before = num_entries_before;

        Ok((header, WAL_HEADER_FIXED_SIZE + 8 + len))
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> usize {
        dest.put_u64_le(0); // version
        dest.put_u64_le(self.data_size_bytes);
        dest.put_u64_le(self.id_size_bytes);
        self.num_entries_before.write_to_buffer(dest);

        self.size()
    }

    pub fn size(&self) -> usize {
        WAL_HEADER_FIXED_SIZE + 8 + self.num_entries_before.size_in_bytes()
    }
}
