use bytes::{BufMut, BytesMut};
pub use normfs_types::{CompressionType, EncryptionType};
use uintn::{Error as UintNError, UintN};

#[derive(Debug, PartialEq, Eq)]
pub enum StoreHeaderError {
    SliceTooShort,
    UintN(UintNError),
    UnsupportedVersion(u64),
    UnsupportedCompression(u64),
    UnsupportedEncryption(u64),
    UnsupportedSignature(u64),
}

impl std::fmt::Display for StoreHeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreHeaderError::SliceTooShort => write!(f, "Slice too short for store header"),
            StoreHeaderError::UintN(e) => write!(f, "UintN error: {}", e),
            StoreHeaderError::UnsupportedVersion(version) => {
                write!(f, "Unsupported store version: {}", version)
            }
            StoreHeaderError::UnsupportedCompression(c) => {
                write!(f, "Unsupported compression type: {}", c)
            }
            StoreHeaderError::UnsupportedEncryption(e) => {
                write!(f, "Unsupported encryption type: {}", e)
            }
            StoreHeaderError::UnsupportedSignature(s) => {
                write!(f, "Unsupported signature type: {}", s)
            }
        }
    }
}

impl std::error::Error for StoreHeaderError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreHeaderError::UintN(e) => Some(e),
            _ => None,
        }
    }
}

impl From<UintNError> for StoreHeaderError {
    fn from(e: UintNError) -> Self {
        StoreHeaderError::UintN(e)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum StoreHeaderVersion {
    // configurable encryption and compression
    V0 = 0,
}

impl TryFrom<u64> for StoreHeaderVersion {
    type Error = StoreHeaderError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(StoreHeaderVersion::V0),
            _ => Err(StoreHeaderError::UnsupportedVersion(value)),
        }
    }
}

impl From<StoreHeaderVersion> for u64 {
    fn from(v: StoreHeaderVersion) -> Self {
        v as u64
    }
}

pub fn compression_type_from_u64(value: u64) -> Result<CompressionType, StoreHeaderError> {
    CompressionType::try_from(value).map_err(|_| StoreHeaderError::UnsupportedCompression(value))
}

pub fn encryption_type_from_u64(value: u64) -> Result<EncryptionType, StoreHeaderError> {
    EncryptionType::try_from(value).map_err(|_| StoreHeaderError::UnsupportedEncryption(value))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum SignatureType {
    Ed25519 = 0,
}

impl TryFrom<u64> for SignatureType {
    type Error = StoreHeaderError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SignatureType::Ed25519),
            _ => Err(StoreHeaderError::UnsupportedSignature(value)),
        }
    }
}

impl From<SignatureType> for u64 {
    fn from(v: SignatureType) -> Self {
        v as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum FileAuthVersion {
    V0 = 0,
}

impl TryFrom<u64> for FileAuthVersion {
    type Error = StoreHeaderError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FileAuthVersion::V0),
            _ => Err(StoreHeaderError::UnsupportedVersion(value)),
        }
    }
}

impl From<FileAuthVersion> for u64 {
    fn from(v: FileAuthVersion) -> Self {
        v as u64
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FileAuthentication {
    pub version: FileAuthVersion,
    pub header_signature_type: SignatureType,
    pub header_signature: [u8; 64],
    pub content_signature_type: SignatureType,
    pub content_signature: [u8; 64],
}

impl FileAuthentication {
    pub fn new(header_signature: [u8; 64], content_signature: [u8; 64]) -> Self {
        Self {
            version: FileAuthVersion::V0,
            header_signature_type: SignatureType::Ed25519,
            header_signature,
            content_signature_type: SignatureType::Ed25519,
            content_signature,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), StoreHeaderError> {
        if data.len() < 152 {
            return Err(StoreHeaderError::SliceTooShort);
        }

        let version_val = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let version = FileAuthVersion::try_from(version_val)?;

        let header_signature_type_val = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let header_signature_type = SignatureType::try_from(header_signature_type_val)?;

        let mut header_signature = [0u8; 64];
        header_signature.copy_from_slice(&data[16..80]);

        let content_signature_type_val = u64::from_le_bytes(data[80..88].try_into().unwrap());
        let content_signature_type = SignatureType::try_from(content_signature_type_val)?;

        let mut content_signature = [0u8; 64];
        content_signature.copy_from_slice(&data[88..152]);

        Ok((
            Self {
                version,
                header_signature_type,
                header_signature,
                content_signature_type,
                content_signature,
            },
            152,
        ))
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> usize {
        dest.put_u64_le(self.version.into());
        dest.put_u64_le(self.header_signature_type.into());
        dest.extend_from_slice(&self.header_signature);
        dest.put_u64_le(self.content_signature_type.into());
        dest.extend_from_slice(&self.content_signature);
        152
    }

    pub fn size(&self) -> usize {
        152 // 8 (version) + 8 (header_sig_type) + 64 (header_sig) + 8 (content_sig_type) + 64 (content_sig)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StoreHeader {
    pub version: StoreHeaderVersion,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    pub num_entries_before: UintN,
    pub num_entries: UintN,
}

impl StoreHeader {
    pub fn new(
        compression: CompressionType,
        encryption: EncryptionType,
        num_entries_before: UintN,
        num_entries: UintN,
    ) -> Self {
        Self {
            version: StoreHeaderVersion::V0,
            compression,
            encryption,
            num_entries_before,
            num_entries,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<(Self, usize), StoreHeaderError> {
        if data.len() < 24 {
            return Err(StoreHeaderError::SliceTooShort);
        }

        let version_val = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let version = StoreHeaderVersion::try_from(version_val)?;

        let mut offset = 8;

        let compression_val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let compression = compression_type_from_u64(compression_val)?;
        offset += 8;

        let encryption_val = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let encryption = encryption_type_from_u64(encryption_val)?;
        offset += 8;

        let (num_entries_before, bytes_read_before) = UintN::read_from_slice(&data[offset..])?;
        offset += bytes_read_before;

        let (num_entries, bytes_read_after) = UintN::read_from_slice(&data[offset..])?;
        offset += bytes_read_after;

        Ok((
            Self {
                version,
                compression,
                encryption,
                num_entries_before,
                num_entries,
            },
            offset,
        ))
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> usize {
        dest.put_u64_le(self.version.into());
        dest.put_u64_le(self.compression.into());
        dest.put_u64_le(self.encryption.into());

        self.num_entries_before.write_to_buffer(dest);
        self.num_entries.write_to_buffer(dest);

        self.size()
    }

    pub fn size(&self) -> usize {
        let mut size = 8; // version
        size += 8; // compression
        size += 8; // encryption
        size += 8 + self.num_entries_before.size_in_bytes();
        size += 8 + self.num_entries.size_in_bytes();
        size
    }

    pub fn is_encrypted(&self) -> bool {
        self.encryption != EncryptionType::None
    }

    pub fn is_compressed(&self) -> bool {
        self.compression != CompressionType::None
    }
}
