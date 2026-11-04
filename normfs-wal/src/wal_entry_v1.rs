use bytes::{BufMut, BytesMut};
use std::os::raw::c_int;

pub const WAL_ENTRY_V1_CRC_SIZE: usize = 4;
pub const WAL_ENTRY_V1_MIN_SIZE: usize = 5;
pub const WAL_ENTRY_V1_MAX_OVERHEAD: usize = 9;

const NORMFS_WAL_ENTRY_OK: c_int = 0;
const NORMFS_WAL_ENTRY_ERR_TRUNCATED: c_int = 1;
const NORMFS_WAL_ENTRY_ERR_OVERFLOW: c_int = 2;
const NORMFS_WAL_ENTRY_ERR_NON_CANONICAL: c_int = 3;
const NORMFS_WAL_ENTRY_ERR_NO_SPACE: c_int = 4;
const NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH: c_int = 5;
const NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW: c_int = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEntryV1Error {
    Truncated,
    Overflow,
    NonCanonical,
    NoSpace,
    CrcMismatch,
    IdOverflow,
    RecordTooLarge(usize),
    UnknownStatus(c_int),
}

impl std::fmt::Display for WalEntryV1Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalEntryV1Error::Truncated => write!(f, "Truncated V1 WAL entry"),
            WalEntryV1Error::Overflow => write!(f, "V1 WAL entry record size exceeds u32"),
            WalEntryV1Error::NonCanonical => write!(f, "Non-canonical V1 WAL entry record size"),
            WalEntryV1Error::NoSpace => write!(f, "Buffer too small for V1 WAL entry"),
            WalEntryV1Error::CrcMismatch => write!(f, "V1 WAL entry failed its CRC32C check"),
            WalEntryV1Error::IdOverflow => write!(f, "Derived entry ID exceeds u64"),
            WalEntryV1Error::RecordTooLarge(size) => {
                write!(f, "Record of {} bytes exceeds the u32 V1 entry limit", size)
            }
            WalEntryV1Error::UnknownStatus(status) => {
                write!(f, "Unknown V1 WAL entry status: {}", status)
            }
        }
    }
}

impl std::error::Error for WalEntryV1Error {}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

#[repr(C)]
struct CDecodeResult {
    record_offset: usize,
    record_size: usize,
    consumed: usize,
    crc: u32,
    status: c_int,
}

#[repr(C)]
struct CIdResult {
    entry_id: u64,
    status: c_int,
}

// Field order matches struct normfs_wal_entry_iter_result: the 8-byte fields
// first, then the two 4-byte fields, so both sides agree with no padding.
#[repr(C)]
struct CIterResult {
    record_offset: usize,
    record_size: usize,
    consumed: usize,
    entry_id: u64,
    crc: u32,
    status: c_int,
}

// Matches struct normfs_wal_scan_count_result.
#[repr(C)]
#[derive(Default)]
struct CScanCount {
    end: usize,
    count: u64,
}
const _: () = assert!(std::mem::size_of::<CScanCount>() == 16);

unsafe extern "C" {
    fn normfs_wal_entry_v1_size(record_size: u32) -> usize;

    fn normfs_wal_scan_count_v1(
        buf: *const u8,
        len: usize,
        start: usize,
        num_entries_before: u64,
        res: *mut CScanCount,
    );

    fn normfs_wal_entry_v1_encode(
        record: *const u8,
        record_size: u32,
        out: *mut u8,
        out_len: usize,
    ) -> CEncodeResult;

    fn normfs_wal_entry_v1_decode(buf: *const u8, len: usize) -> CDecodeResult;

    fn normfs_wal_entry_v1_id(num_entries_before: u64, index: u64) -> CIdResult;

    fn normfs_wal_entry_v1_iter_next(
        buf: *const u8,
        len: usize,
        num_entries_before: u64,
        index: u64,
    ) -> CIterResult;

    fn normfs_crc32c(crc: u32, data: *const u8, len: usize) -> u32;
}

fn map_status(status: c_int) -> Result<(), WalEntryV1Error> {
    match status {
        NORMFS_WAL_ENTRY_OK => Ok(()),
        NORMFS_WAL_ENTRY_ERR_TRUNCATED => Err(WalEntryV1Error::Truncated),
        NORMFS_WAL_ENTRY_ERR_OVERFLOW => Err(WalEntryV1Error::Overflow),
        NORMFS_WAL_ENTRY_ERR_NON_CANONICAL => Err(WalEntryV1Error::NonCanonical),
        NORMFS_WAL_ENTRY_ERR_NO_SPACE => Err(WalEntryV1Error::NoSpace),
        NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH => Err(WalEntryV1Error::CrcMismatch),
        NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW => Err(WalEntryV1Error::IdOverflow),
        other => Err(WalEntryV1Error::UnknownStatus(other)),
    }
}

/// CRC32C (Castagnoli) over `data`, continuing from `seed`.
///
/// Seed with 0 for a standalone checksum; feeding one result in as the next
/// seed checksums the concatenation.
pub fn crc32c(seed: u32, data: &[u8]) -> u32 {
    unsafe { normfs_crc32c(seed, data.as_ptr(), data.len()) }
}

/// Entry id for the `index`-th entry of a file whose header records
/// `num_entries_before`. V1 entries do not store their id.
pub fn derive_entry_id(num_entries_before: u64, index: u64) -> Result<u64, WalEntryV1Error> {
    let result = unsafe { normfs_wal_entry_v1_id(num_entries_before, index) };
    map_status(result.status)?;
    Ok(result.entry_id)
}

/// How many whole entries `content` holds from `start`, up to the first that
/// is cut short, corrupt or would overflow its id.
pub fn scan_whole_entries(content: &[u8], start: usize, num_entries_before: u64) -> u64 {
    assert!(start <= content.len());
    let mut res = CScanCount::default();
    // SAFETY: content is live for the call and start is inside it.
    unsafe {
        normfs_wal_scan_count_v1(
            content.as_ptr(),
            content.len(),
            start,
            num_entries_before,
            &mut res,
        )
    };
    res.count
}

/// Encoded size of a V1 entry holding a record of `record_size` bytes.
pub fn encoded_len(record_size: u32) -> usize {
    unsafe { normfs_wal_entry_v1_size(record_size) }
}

/// A V1 WAL entry: `[record_size varint32][record][crc32c u32 LE]`.
///
/// Decoding borrows the record out of the source buffer rather than copying it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalEntryV1<'a> {
    pub record: &'a [u8],
}

impl<'a> WalEntryV1<'a> {
    pub fn new(record: &'a [u8]) -> Self {
        Self { record }
    }

    fn record_size(&self) -> Result<u32, WalEntryV1Error> {
        u32::try_from(self.record.len())
            .map_err(|_| WalEntryV1Error::RecordTooLarge(self.record.len()))
    }

    pub fn size(&self) -> Result<usize, WalEntryV1Error> {
        Ok(encoded_len(self.record_size()?))
    }

    pub fn write_to_bytes(&self, dest: &mut BytesMut) -> Result<usize, WalEntryV1Error> {
        let record_size = self.record_size()?;
        let total = encoded_len(record_size);

        dest.reserve(total);

        // The C encoder writes exactly `total` bytes when it reports OK, which
        // its Frama-C contract pins down, and `reserve` has guaranteed at least
        // that much contiguous spare capacity. `record` cannot alias `dest`
        // because holding both a shared and an exclusive borrow is not
        // expressible in safe Rust.
        let result = unsafe {
            let out = dest.chunk_mut();
            normfs_wal_entry_v1_encode(
                self.record.as_ptr(),
                record_size,
                out.as_mut_ptr(),
                out.len(),
            )
        };
        map_status(result.status)?;
        unsafe { dest.advance_mut(result.written) };

        Ok(result.written)
    }

    pub fn from_bytes(data: &'a [u8]) -> Result<(Self, usize), WalEntryV1Error> {
        let result = unsafe { normfs_wal_entry_v1_decode(data.as_ptr(), data.len()) };
        map_status(result.status)?;

        let end = result.record_offset + result.record_size;
        Ok((
            Self {
                record: &data[result.record_offset..end],
            },
            result.consumed,
        ))
    }

    /// Decode the entry at the front of `data` and derive its id in one step.
    ///
    /// `num_entries_before` comes from the file header and `index` is the
    /// entry's 0-based position in the file, so the returned id is
    /// `num_entries_before + index`. Returns the borrowed entry, its id, and
    /// how many bytes it consumed — advance the cursor by that before the next
    /// call. Any error stops the iteration (see `normfs_wal_entry_v1_iter_next`).
    pub fn iter_next(
        data: &'a [u8],
        num_entries_before: u64,
        index: u64,
    ) -> Result<(Self, u64, usize), WalEntryV1Error> {
        let result = unsafe {
            normfs_wal_entry_v1_iter_next(data.as_ptr(), data.len(), num_entries_before, index)
        };
        map_status(result.status)?;

        let end = result.record_offset + result.record_size;
        Ok((
            Self {
                record: &data[result.record_offset..end],
            },
            result.entry_id,
            result.consumed,
        ))
    }
}
