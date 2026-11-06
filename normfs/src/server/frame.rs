use std::os::raw::c_int;

// Mirrors c/include/normfs/frame.h.
pub(crate) const FRAME_HEADER_SIZE: usize = 8;
pub(crate) const FRAME_MAX_PAYLOAD: u64 = 5 * 1024 * 1024 * 1024;

const NORMFS_FRAME_OK: c_int = 0;
const NORMFS_FRAME_ERR_BAD_LENGTH: c_int = 1;
const NORMFS_FRAME_ERR_TOO_LARGE: c_int = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameError {
    BadLength,
    TooLarge(u64),
    UnknownStatus(c_int),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::BadLength => {
                write!(f, "Frame header buffer is not {} bytes", FRAME_HEADER_SIZE)
            }
            FrameError::TooLarge(size) => write!(f, "Frame payload too large: {}", size),
            FrameError::UnknownStatus(status) => write!(f, "Unknown frame status: {}", status),
        }
    }
}

impl std::error::Error for FrameError {}

#[repr(C)]
struct CEncodeResult {
    written: usize,
    status: c_int,
}

unsafe extern "C" {
    fn normfs_frame_header_encode(payload_len: u64, out: *mut u8, out_len: usize) -> CEncodeResult;
}

fn map_status(status: c_int, payload_len: u64) -> Result<(), FrameError> {
    match status {
        NORMFS_FRAME_OK => Ok(()),
        NORMFS_FRAME_ERR_BAD_LENGTH => Err(FrameError::BadLength),
        NORMFS_FRAME_ERR_TOO_LARGE => Err(FrameError::TooLarge(payload_len)),
        other => Err(FrameError::UnknownStatus(other)),
    }
}

pub(crate) fn encode_header(payload_len: u64) -> Result<[u8; FRAME_HEADER_SIZE], FrameError> {
    let mut out = [0u8; FRAME_HEADER_SIZE];
    // SAFETY: out is FRAME_HEADER_SIZE writable bytes and the C writes at most out_len of them.
    let r = unsafe { normfs_frame_header_encode(payload_len, out.as_mut_ptr(), out.len()) };
    map_status(r.status, payload_len)?;
    Ok(out)
}
