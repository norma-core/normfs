use dashu_int::UBig;
use std::fmt;
use std::path::PathBuf;
mod containers;
pub mod paths;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod paths_test;

pub use containers::*;

#[derive(Debug, Clone)]
pub enum UintN {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    Big(UBig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UintNType {
    U8,
    U16,
    U32,
    U64,
    U128,
    Big,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    ValueTooLarge,
    DecrementUnderflow,
    SliceTooShort,
    InvalidUintNSize,
    InvalidHexString,
    Io(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ValueTooLarge => write!(f, "value too large for target type"),
            Error::DecrementUnderflow => write!(f, "decrement underflow"),
            Error::SliceTooShort => write!(f, "slice is too short"),
            Error::InvalidUintNSize => write!(f, "invalid uintn size"),
            Error::InvalidHexString => write!(f, "invalid hex string"),
            Error::Io(s) => write!(f, "io error: {}", s),
        }
    }
}

impl std::error::Error for Error {}

impl PartialEq for UintN {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Same types
            (UintN::U8(a), UintN::U8(b)) => a == b,
            (UintN::U16(a), UintN::U16(b)) => a == b,
            (UintN::U32(a), UintN::U32(b)) => a == b,
            (UintN::U64(a), UintN::U64(b)) => a == b,
            (UintN::U128(a), UintN::U128(b)) => a == b,
            (UintN::Big(a), UintN::Big(b)) => a == b,

            // Cross-type comparisons - compare actual values
            (UintN::U8(a), UintN::U16(b)) => *a as u16 == *b,
            (UintN::U8(a), UintN::U32(b)) => *a as u32 == *b,
            (UintN::U8(a), UintN::U64(b)) => *a as u64 == *b,
            (UintN::U8(a), UintN::U128(b)) => *a as u128 == *b,

            (UintN::U16(a), UintN::U8(b)) => *a == *b as u16,
            (UintN::U16(a), UintN::U32(b)) => *a as u32 == *b,
            (UintN::U16(a), UintN::U64(b)) => *a as u64 == *b,
            (UintN::U16(a), UintN::U128(b)) => *a as u128 == *b,

            (UintN::U32(a), UintN::U8(b)) => *a == *b as u32,
            (UintN::U32(a), UintN::U16(b)) => *a == *b as u32,
            (UintN::U32(a), UintN::U64(b)) => *a as u64 == *b,
            (UintN::U32(a), UintN::U128(b)) => *a as u128 == *b,

            (UintN::U64(a), UintN::U8(b)) => *a == *b as u64,
            (UintN::U64(a), UintN::U16(b)) => *a == *b as u64,
            (UintN::U64(a), UintN::U32(b)) => *a == *b as u64,
            (UintN::U64(a), UintN::U128(b)) => *a as u128 == *b,

            (UintN::U128(a), UintN::U8(b)) => *a == *b as u128,
            (UintN::U128(a), UintN::U16(b)) => *a == *b as u128,
            (UintN::U128(a), UintN::U32(b)) => *a == *b as u128,
            (UintN::U128(a), UintN::U64(b)) => *a == *b as u128,

            // Big comparisons
            (UintN::Big(_), _) | (_, UintN::Big(_)) => {
                if let (UintN::Big(a), UintN::Big(b)) = (self, other) {
                    a == b
                } else {
                    match other.to_u128() {
                        Ok(b_val) => match self.to_u128() {
                            Ok(a_val) => a_val == b_val,
                            Err(_) => false,
                        },
                        Err(_) => false,
                    }
                }
            }
        }
    }
}

impl Eq for UintN {}

impl Default for UintN {
    fn default() -> Self {
        UintN::U8(0)
    }
}

impl UintN {
    pub fn zero() -> Self {
        UintN::U8(0)
    }

    pub fn one() -> Self {
        UintN::U8(1)
    }

    pub fn from_hex_digits(hex_string: &str) -> Result<Self, Error> {
        // hex::decode requires even-length strings, so pad if needed
        let padded = if hex_string.len() % 2 == 1 {
            format!("0{}", hex_string)
        } else {
            hex_string.to_string()
        };

        if let Ok(bytes) = hex::decode(&padded) {
            let integer = UBig::from_be_bytes(&bytes);
            Ok(UintN::from(integer))
        } else {
            Err(Error::InvalidHexString)
        }
    }

    pub fn get_type(&self) -> UintNType {
        match self {
            UintN::U8(_) => UintNType::U8,
            UintN::U16(_) => UintNType::U16,
            UintN::U32(_) => UintNType::U32,
            UintN::U64(_) => UintNType::U64,
            UintN::U128(_) => UintNType::U128,
            UintN::Big(_) => UintNType::Big,
        }
    }

    pub fn is_zero(&self) -> bool {
        match self {
            UintN::U8(v) => *v == 0,
            UintN::U16(v) => *v == 0,
            UintN::U32(v) => *v == 0,
            UintN::U64(v) => *v == 0,
            UintN::U128(v) => *v == 0,
            UintN::Big(v) => v == &UBig::ZERO,
        }
    }

    pub fn to_u64(&self) -> Result<u64, Error> {
        match self {
            UintN::U8(v) => Ok(*v as u64),
            UintN::U16(v) => Ok(*v as u64),
            UintN::U32(v) => Ok(*v as u64),
            UintN::U64(v) => Ok(*v),
            UintN::U128(v) => {
                if *v <= u64::MAX as u128 {
                    Ok(*v as u64)
                } else {
                    Err(Error::ValueTooLarge)
                }
            }
            UintN::Big(v) => v.try_into().map_err(|_| Error::ValueTooLarge),
        }
    }

    pub fn to_u128(&self) -> Result<u128, Error> {
        match self {
            UintN::U8(v) => Ok(*v as u128),
            UintN::U16(v) => Ok(*v as u128),
            UintN::U32(v) => Ok(*v as u128),
            UintN::U64(v) => Ok(*v as u128),
            UintN::U128(v) => Ok(*v),
            UintN::Big(v) => v.try_into().map_err(|_| Error::ValueTooLarge),
        }
    }

    pub fn to_ubig(&self) -> UBig {
        match self {
            UintN::U8(v) => UBig::from(*v),
            UintN::U16(v) => UBig::from(*v),
            UintN::U32(v) => UBig::from(*v),
            UintN::U64(v) => UBig::from(*v),
            UintN::U128(v) => UBig::from(*v),
            UintN::Big(v) => v.clone(),
        }
    }

    #[inline]
    pub fn increment(&self) -> Self {
        match self {
            UintN::U8(v) => {
                if *v == u8::MAX {
                    UintN::U16(256)
                } else {
                    UintN::U8(v + 1)
                }
            }
            UintN::U16(v) => {
                if *v == u16::MAX {
                    UintN::U32(65536)
                } else {
                    UintN::U16(v + 1)
                }
            }
            UintN::U32(v) => {
                if *v == u32::MAX {
                    UintN::U64(4294967296)
                } else {
                    UintN::U32(v + 1)
                }
            }
            UintN::U64(v) => {
                if *v == u64::MAX {
                    UintN::U128(18446744073709551616)
                } else {
                    UintN::U64(v + 1)
                }
            }
            UintN::U128(v) => {
                if *v == u128::MAX {
                    UintN::Big(UBig::from(*v) + 1u32)
                } else {
                    UintN::U128(v + 1)
                }
            }
            UintN::Big(v) => UintN::Big(v + 1u32),
        }
    }

    pub fn decrement(&self) -> Result<Self, Error> {
        if self.is_zero() {
            return Err(Error::DecrementUnderflow);
        }

        match self {
            UintN::U8(v) => Ok(UintN::from(*v - 1)),
            UintN::U16(v) => Ok(UintN::from(*v - 1)),
            UintN::U32(v) => Ok(UintN::from(*v - 1)),
            UintN::U64(v) => Ok(UintN::from(*v - 1)),
            UintN::U128(v) => Ok(UintN::from(*v - 1)),
            UintN::Big(v) => Ok(UintN::from(v - 1u32)),
        }
    }

    pub fn add(&self, other: &Self) -> Self {
        if other.is_zero() {
            return self.clone();
        }

        match (self, other) {
            (UintN::U8(a), UintN::U8(b)) => {
                if let Some(result) = a.checked_add(*b) {
                    UintN::U8(result)
                } else {
                    UintN::U16(*a as u16 + *b as u16)
                }
            }
            (UintN::U8(a), UintN::U16(b)) | (UintN::U16(b), UintN::U8(a)) => {
                let a = *a as u16;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U32(a as u32 + *b as u32)
                }
            }
            (UintN::U8(a), UintN::U32(b)) | (UintN::U32(b), UintN::U8(a)) => {
                let a = *a as u32;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U64(a as u64 + *b as u64)
                }
            }
            (UintN::U8(a), UintN::U64(b)) | (UintN::U64(b), UintN::U8(a)) => {
                let a = *a as u64;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U128(a as u128 + *b as u128)
                }
            }
            (UintN::U8(a), UintN::U128(b)) | (UintN::U128(b), UintN::U8(a)) => {
                let a = *a as u128;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::Big(UBig::from(a) + UBig::from(*b))
                }
            }
            (UintN::U16(a), UintN::U16(b)) => {
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U32(*a as u32 + *b as u32)
                }
            }
            (UintN::U16(a), UintN::U32(b)) | (UintN::U32(b), UintN::U16(a)) => {
                let a = *a as u32;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U64(a as u64 + *b as u64)
                }
            }
            (UintN::U16(a), UintN::U64(b)) | (UintN::U64(b), UintN::U16(a)) => {
                let a = *a as u64;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U128(a as u128 + *b as u128)
                }
            }
            (UintN::U16(a), UintN::U128(b)) | (UintN::U128(b), UintN::U16(a)) => {
                let a = *a as u128;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::Big(UBig::from(a) + UBig::from(*b))
                }
            }
            (UintN::U32(a), UintN::U32(b)) => {
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U64(*a as u64 + *b as u64)
                }
            }
            (UintN::U32(a), UintN::U64(b)) | (UintN::U64(b), UintN::U32(a)) => {
                let a = *a as u64;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U128(a as u128 + *b as u128)
                }
            }
            (UintN::U32(a), UintN::U128(b)) | (UintN::U128(b), UintN::U32(a)) => {
                let a = *a as u128;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::Big(UBig::from(a) + UBig::from(*b))
                }
            }
            (UintN::U64(a), UintN::U64(b)) => {
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::U128(*a as u128 + *b as u128)
                }
            }
            (UintN::U64(a), UintN::U128(b)) | (UintN::U128(b), UintN::U64(a)) => {
                let a = *a as u128;
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::Big(UBig::from(a) + UBig::from(*b))
                }
            }
            (UintN::U128(a), UintN::U128(b)) => {
                if let Some(result) = a.checked_add(*b) {
                    UintN::from(result)
                } else {
                    UintN::Big(UBig::from(*a) + UBig::from(*b))
                }
            }
            (UintN::Big(_), _) | (_, UintN::Big(_)) => {
                let a_big = self.to_ubig();
                let b_big = other.to_ubig();
                UintN::Big(a_big + b_big)
            }
        }
    }

    pub fn sub(&self, other: &Self) -> Result<Self, Error> {
        if other.is_zero() {
            return Ok(self.clone());
        }

        match (self, other) {
            (UintN::U8(a), UintN::U8(b)) => {
                if a >= b {
                    Ok(UintN::from(a - b))
                } else {
                    Err(Error::DecrementUnderflow)
                }
            }
            (UintN::U16(a), UintN::U8(b)) => Ok(UintN::from(a - *b as u16)),
            (UintN::U16(a), UintN::U16(b)) => {
                if a >= b {
                    Ok(UintN::from(a - b))
                } else {
                    Err(Error::DecrementUnderflow)
                }
            }
            (UintN::U32(a), UintN::U8(b)) => Ok(UintN::from(a - *b as u32)),
            (UintN::U32(a), UintN::U16(b)) => Ok(UintN::from(a - *b as u32)),
            (UintN::U32(a), UintN::U32(b)) => {
                if a >= b {
                    Ok(UintN::from(a - b))
                } else {
                    Err(Error::DecrementUnderflow)
                }
            }
            (UintN::U64(a), UintN::U8(b)) => Ok(UintN::from(a - *b as u64)),
            (UintN::U64(a), UintN::U16(b)) => Ok(UintN::from(a - *b as u64)),
            (UintN::U64(a), UintN::U32(b)) => Ok(UintN::from(a - *b as u64)),
            (UintN::U64(a), UintN::U64(b)) => {
                if a >= b {
                    Ok(UintN::from(a - b))
                } else {
                    Err(Error::DecrementUnderflow)
                }
            }
            (UintN::U128(a), UintN::U8(b)) => Ok(UintN::from(a - *b as u128)),
            (UintN::U128(a), UintN::U16(b)) => Ok(UintN::from(a - *b as u128)),
            (UintN::U128(a), UintN::U32(b)) => Ok(UintN::from(a - *b as u128)),
            (UintN::U128(a), UintN::U64(b)) => Ok(UintN::from(a - *b as u128)),
            (UintN::U128(a), UintN::U128(b)) => {
                if a >= b {
                    Ok(UintN::from(a - b))
                } else {
                    Err(Error::DecrementUnderflow)
                }
            }
            _ => {
                if self < other {
                    Err(Error::DecrementUnderflow)
                } else {
                    let a_big = self.to_ubig();
                    let b_big = other.to_ubig();
                    Ok(UintN::from(a_big - b_big))
                }
            }
        }
    }

    pub fn shrink_to_fit(&self) -> Self {
        match self {
            UintN::U8(_) => self.clone(),
            _ => UintN::from(self.to_ubig()),
        }
    }

    /// Calculate the middle value between two UintN values
    /// Returns (self + other) / 2
    pub fn middle(&self, other: &Self) -> Self {
        // For same types, we can do the calculation directly
        match (self, other) {
            (UintN::U8(a), UintN::U8(b)) => UintN::U8(((*a as u16 + *b as u16) / 2) as u8),
            (UintN::U16(a), UintN::U16(b)) => UintN::U16(((*a as u32 + *b as u32) / 2) as u16),
            (UintN::U32(a), UintN::U32(b)) => UintN::U32(((*a as u64 + *b as u64) / 2) as u32),
            (UintN::U64(a), UintN::U64(b)) => UintN::U64(((*a as u128 + *b as u128) / 2) as u64),
            (UintN::U128(a), UintN::U128(b)) => {
                // Use big integer to avoid overflow
                let a_big = UBig::from(*a);
                let b_big = UBig::from(*b);
                let sum = a_big + b_big;
                let half = sum / 2u32;
                if let Ok(val) = TryInto::<u128>::try_into(half.clone()) {
                    UintN::U128(val)
                } else {
                    UintN::Big(half)
                }
            }
            _ => {
                // For different types or Big integers, convert to Integer
                let a_big = self.to_ubig();
                let b_big = other.to_ubig();
                let sum = a_big + b_big;
                let half = sum / 2u32;
                UintN::from(half)
            }
        }
    }

    pub fn is_valid_data_size(size: u64) -> bool {
        matches!(size, 1 | 2 | 4 | 8 | 16)
    }

    pub fn read_from_slice(data: &[u8]) -> Result<(Self, usize), Error> {
        if data.len() < 8 {
            return Err(Error::SliceTooShort);
        }

        let len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut bytes_read = 8;

        if data.len() < bytes_read + len {
            return Err(Error::SliceTooShort);
        }

        let value_slice = &data[bytes_read..bytes_read + len];
        bytes_read += len;

        let value = Self::read_value_from_slice(value_slice, len)?;

        Ok((value, bytes_read))
    }

    pub fn read_value_from_slice(data: &[u8], size: usize) -> Result<Self, Error> {
        if data.len() < size {
            return Err(Error::SliceTooShort);
        }

        match size {
            0 => Ok(UintN::U8(0)),
            1 => Ok(UintN::U8(data[0])),
            2 => Ok(UintN::from(u16::from_le_bytes(
                data.try_into().map_err(|_| Error::SliceTooShort)?,
            ))),
            4 => Ok(UintN::from(u32::from_le_bytes(
                data.try_into().map_err(|_| Error::SliceTooShort)?,
            ))),
            8 => Ok(UintN::from(u64::from_le_bytes(
                data.try_into().map_err(|_| Error::SliceTooShort)?,
            ))),
            16 => Ok(UintN::from(u128::from_le_bytes(
                data.try_into().map_err(|_| Error::SliceTooShort)?,
            ))),
            _ => {
                // rug::Integer::from_digits expects little-endian bytes.
                Ok(UintN::Big(UBig::from_le_bytes(data)))
            }
        }
    }

    pub fn size_in_bytes(&self) -> usize {
        match self {
            UintN::U8(_) => 1,
            UintN::U16(_) => 2,
            UintN::U32(_) => 4,
            UintN::U64(_) => 8,
            UintN::U128(_) => 16,
            UintN::Big(v) => {
                let bytes = v.to_be_bytes();
                bytes.len()
            }
        }
    }

    pub fn value_to_bytes(&self) -> bytes::Bytes {
        let mut buffer = bytes::BytesMut::new();
        self.write_value_to_buffer(&mut buffer);
        buffer.freeze()
    }

    pub fn write_value_to_buffer(&self, buffer: &mut bytes::BytesMut) -> usize {
        let initial_len = buffer.len();
        match self {
            UintN::U8(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            UintN::U16(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            UintN::U32(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            UintN::U64(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            UintN::U128(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            UintN::Big(v) => {
                let bytes = v.to_le_bytes();
                buffer.extend_from_slice(&bytes);
            }
        }
        buffer.len() - initial_len
    }

    pub fn write_to_buffer(&self, buffer: &mut bytes::BytesMut) -> usize {
        let size = self.size_in_bytes();
        buffer.extend_from_slice(&(size as u64).to_le_bytes());
        8 + self.write_value_to_buffer(buffer)
    }

    pub fn write_value_to_buffer_with_size(
        &self,
        buffer: &mut bytes::BytesMut,
        size: usize,
    ) -> Result<usize, Error> {
        use bytes::BufMut;

        let value_bytes_len = self.size_in_bytes();
        if value_bytes_len > size {
            let max_val = match size {
                1 => UBig::from(u8::MAX),
                2 => UBig::from(u16::MAX),
                4 => UBig::from(u32::MAX),
                8 => UBig::from(u64::MAX),
                16 => UBig::from(u128::MAX),
                _ => return Err(Error::InvalidUintNSize),
            };
            if self.to_ubig() > max_val {
                return Err(Error::ValueTooLarge);
            }
        }

        let written_len = self.write_value_to_buffer(buffer);
        let padding = size.saturating_sub(written_len);
        if padding > 0 {
            buffer.put_bytes(0, padding);
        }

        Ok(size)
    }

    pub fn to_file_path(&self, base_folder: &str, extension: &str) -> PathBuf {
        let shrunk_value = self.shrink_to_fit();

        // Convert to hex string directly from the number
        let hex_string = match shrunk_value {
            UintN::U8(v) => format!("{:x}", v),
            UintN::U16(v) => format!("{:x}", v),
            UintN::U32(v) => format!("{:x}", v),
            UintN::U64(v) => format!("{:x}", v),
            UintN::U128(v) => format!("{:x}", v),
            UintN::Big(v) => format!("{:x}", v),
        };

        // Pad to multiple of 3 characters for consistent 3-char chunking
        // This ensures lexicographic order matches numeric order
        let target_len = hex_string.len().div_ceil(3) * 3;
        let padded_hex = format!("{:0>width$}", hex_string, width = target_len);

        let mut current_path = PathBuf::from(base_folder);

        // Split into 3-character chunks
        let mut remaining = padded_hex.as_str();

        while remaining.len() > 3 {
            current_path.push(&remaining[0..3]);
            remaining = &remaining[3..];
        }

        // Last chunk (always 3 chars due to padding) becomes the filename
        let mut file_name = remaining.to_string();
        if !extension.is_empty() {
            file_name.push('.');
            file_name.push_str(extension);
        }

        current_path.join(file_name)
    }

    pub fn can_fit(&self, num_bytes: u64) -> bool {
        self.size_in_bytes() as u64 <= num_bytes
    }

    pub fn in_step(&self, from: &Self, step: usize) -> bool {
        if step == 1 {
            return true;
        }
        if self < from {
            return false;
        }
        let diff = self.sub(from).unwrap_or_else(|_| UintN::zero());
        let step_uint = UintN::from(step as u64);
        (diff % step_uint).is_zero()
    }

    /// Add step to this value, returning a new UintN
    /// More efficient than checking in_step for every entry when iterating
    pub fn step_by(&self, step: usize) -> Self {
        self.add(&UintN::from(step as u64))
    }
}

impl From<u8> for UintN {
    fn from(value: u8) -> Self {
        UintN::U8(value)
    }
}

impl From<u16> for UintN {
    fn from(value: u16) -> Self {
        if value <= u8::MAX as u16 {
            UintN::U8(value as u8)
        } else {
            UintN::U16(value)
        }
    }
}

impl From<u32> for UintN {
    fn from(value: u32) -> Self {
        if value <= u8::MAX as u32 {
            UintN::U8(value as u8)
        } else if value <= u16::MAX as u32 {
            UintN::U16(value as u16)
        } else {
            UintN::U32(value)
        }
    }
}

impl From<u64> for UintN {
    fn from(value: u64) -> Self {
        if value <= u8::MAX as u64 {
            UintN::U8(value as u8)
        } else if value <= u16::MAX as u64 {
            UintN::U16(value as u16)
        } else if value <= u32::MAX as u64 {
            UintN::U32(value as u32)
        } else {
            UintN::U64(value)
        }
    }
}

impl From<u128> for UintN {
    fn from(value: u128) -> Self {
        if value <= u8::MAX as u128 {
            UintN::U8(value as u8)
        } else if value <= u16::MAX as u128 {
            UintN::U16(value as u16)
        } else if value <= u32::MAX as u128 {
            UintN::U32(value as u32)
        } else if value <= u64::MAX as u128 {
            UintN::U64(value as u64)
        } else {
            UintN::U128(value)
        }
    }
}

impl From<UBig> for UintN {
    fn from(value: UBig) -> Self {
        if let Ok(v) = TryInto::<u128>::try_into(value.clone()) {
            UintN::from(v)
        } else {
            UintN::Big(value)
        }
    }
}

impl fmt::Display for UintN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UintN::U8(v) => write!(f, "{}", v),
            UintN::U16(v) => write!(f, "{}", v),
            UintN::U32(v) => write!(f, "{}", v),
            UintN::U64(v) => write!(f, "{}", v),
            UintN::U128(v) => write!(f, "{}", v),
            UintN::Big(v) => write!(f, "{}", v),
        }
    }
}

impl PartialOrd for UintN {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::ops::Rem<UintN> for UintN {
    type Output = UintN;

    fn rem(self, rhs: UintN) -> Self::Output {
        (&self).rem(&rhs)
    }
}

impl<'b> std::ops::Rem<&'b UintN> for &UintN {
    type Output = UintN;

    fn rem(self, rhs: &'b UintN) -> Self::Output {
        if rhs.is_zero() {
            panic!("attempt to calculate the remainder with a divisor of zero");
        }

        match (self, rhs) {
            (UintN::U8(a), UintN::U8(b)) => UintN::from(*a % *b),
            (UintN::U16(a), UintN::U16(b)) => UintN::from(*a % *b),
            (UintN::U16(a), UintN::U8(b)) => UintN::from(*a % (*b as u16)),
            (UintN::U32(a), UintN::U32(b)) => UintN::from(*a % *b),
            (UintN::U32(a), UintN::U16(b)) => UintN::from(*a % (*b as u32)),
            (UintN::U32(a), UintN::U8(b)) => UintN::from(*a % (*b as u32)),
            (UintN::U64(a), UintN::U64(b)) => UintN::from(*a % *b),
            (UintN::U64(a), UintN::U32(b)) => UintN::from(*a % (*b as u64)),
            (UintN::U64(a), UintN::U16(b)) => UintN::from(*a % (*b as u64)),
            (UintN::U64(a), UintN::U8(b)) => UintN::from(*a % (*b as u64)),
            (UintN::U128(a), UintN::U128(b)) => UintN::from(*a % *b),
            (UintN::U128(a), UintN::U64(b)) => UintN::from(*a % (*b as u128)),
            (UintN::U128(a), UintN::U32(b)) => UintN::from(*a % (*b as u128)),
            (UintN::U128(a), UintN::U16(b)) => UintN::from(*a % (*b as u128)),
            (UintN::U128(a), UintN::U8(b)) => UintN::from(*a % (*b as u128)),
            (UintN::Big(a), UintN::Big(b)) => UintN::from(a % b),
            _ => {
                let a = self.to_ubig();
                let b = rhs.to_ubig();
                UintN::from(a % b)
            }
        }
    }
}

impl Ord for UintN {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (UintN::U8(a), UintN::U8(b)) => a.cmp(b),
            (UintN::U8(a), UintN::U16(b)) => (*a as u16).cmp(b),
            (UintN::U8(a), UintN::U32(b)) => (*a as u32).cmp(b),
            (UintN::U8(a), UintN::U64(b)) => (*a as u64).cmp(b),
            (UintN::U8(a), UintN::U128(b)) => (*a as u128).cmp(b),

            (UintN::U16(a), UintN::U8(b)) => a.cmp(&(*b as u16)),
            (UintN::U16(a), UintN::U16(b)) => a.cmp(b),
            (UintN::U16(a), UintN::U32(b)) => (*a as u32).cmp(b),
            (UintN::U16(a), UintN::U64(b)) => (*a as u64).cmp(b),
            (UintN::U16(a), UintN::U128(b)) => (*a as u128).cmp(b),

            (UintN::U32(a), UintN::U8(b)) => a.cmp(&(*b as u32)),
            (UintN::U32(a), UintN::U16(b)) => a.cmp(&(*b as u32)),
            (UintN::U32(a), UintN::U32(b)) => a.cmp(b),
            (UintN::U32(a), UintN::U64(b)) => (*a as u64).cmp(b),
            (UintN::U32(a), UintN::U128(b)) => (*a as u128).cmp(b),

            (UintN::U64(a), UintN::U8(b)) => a.cmp(&(*b as u64)),
            (UintN::U64(a), UintN::U16(b)) => a.cmp(&(*b as u64)),
            (UintN::U64(a), UintN::U32(b)) => a.cmp(&(*b as u64)),
            (UintN::U64(a), UintN::U64(b)) => a.cmp(b),
            (UintN::U64(a), UintN::U128(b)) => (*a as u128).cmp(b),

            (UintN::U128(a), UintN::U8(b)) => a.cmp(&(*b as u128)),
            (UintN::U128(a), UintN::U16(b)) => a.cmp(&(*b as u128)),
            (UintN::U128(a), UintN::U32(b)) => a.cmp(&(*b as u128)),
            (UintN::U128(a), UintN::U64(b)) => a.cmp(&(*b as u128)),
            (UintN::U128(a), UintN::U128(b)) => a.cmp(b),

            (UintN::Big(_), _) | (_, UintN::Big(_)) => {
                let self_big = self.to_ubig();
                let other_big = other.to_ubig();
                self_big.cmp(&other_big)
            }
        }
    }
}
