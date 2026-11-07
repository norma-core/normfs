use std::os::raw::c_int;
use std::ptr;

use crate::proto::{Id, ServerResponse};

const NORMFS_RESPONSE_OK: c_int = 0;
const NORMFS_RESPONSE_ERR_BAD_LENGTH: c_int = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResponseError {
    TooLarge(usize),
    BadLength,
    UnknownStatus(c_int),
}

impl std::fmt::Display for ResponseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseError::TooLarge(size) => {
                write!(
                    f,
                    "Server response of {} bytes exceeds the frame limit",
                    size
                )
            }
            ResponseError::BadLength => write!(f, "Server response buffer has the wrong length"),
            ResponseError::UnknownStatus(status) => {
                write!(f, "Unknown server response status: {}", status)
            }
        }
    }
}

impl std::error::Error for ResponseError {}

// The structs below mirror the generated ones in c/gremlin/normfs.pb.h. Enum
// fields are C enums, 4 bytes; a negative proto enum goes as its 32-bit pattern.

#[repr(C)]
#[derive(Clone, Copy)]
struct CBytes {
    data: *const u8,
    len: usize,
}

impl CBytes {
    fn of(b: &[u8]) -> Self {
        CBytes {
            data: b.as_ptr(),
            len: b.len(),
        }
    }
}

#[repr(C)]
struct CId {
    raw: CBytes,
    size: usize,
}

#[repr(C)]
struct CSetupResponse {
    version: u64,
    instance_id_bytes: CBytes,
    instance_id: CBytes,
    size: usize,
}

#[repr(C)]
struct CPingRequest {
    sequence: u64,
    client_timestamp_ns: u64,
    size: usize,
}

#[repr(C)]
struct CPingResponse {
    local_stamp_ns: u64,
    monotonic_stamp_ns: u64,
    request: *const CPingRequest,
    size: usize,
}

#[repr(C)]
struct CWriteResponse {
    write_id: u64,
    result: u32,
    ids: *const *const CId,
    ids_count: usize,
    size: usize,
}

#[repr(C)]
struct CReadResponse {
    read_id: u64,
    result: u32,
    id: *const CId,
    data: CBytes,
    data_source: u32,
    encoding: u32,
    size: usize,
}

#[repr(C)]
struct CServerResponse {
    setup: *const CSetupResponse,
    ping: *const CPingResponse,
    write: *const CWriteResponse,
    read: *const CReadResponse,
    size: usize,
}

const _: () = assert!(std::mem::size_of::<CId>() == 24);
const _: () = assert!(std::mem::size_of::<CSetupResponse>() == 48);
const _: () = assert!(std::mem::size_of::<CPingRequest>() == 24);
const _: () = assert!(std::mem::size_of::<CPingResponse>() == 32);
const _: () = assert!(std::mem::size_of::<CWriteResponse>() == 40);
const _: () = assert!(std::mem::size_of::<CReadResponse>() == 56);
const _: () = assert!(std::mem::size_of::<CServerResponse>() == 40);

unsafe extern "C" {
    fn normfs_response_size(m: *const CServerResponse) -> usize;
    fn normfs_response_encode(m: *const CServerResponse, buf: *mut u8, len: usize) -> c_int;
}

fn c_id(id: &Id) -> CId {
    CId {
        raw: CBytes::of(&id.raw),
        size: 0,
    }
}

fn c_opt<T>(v: &mut Option<T>) -> *const T {
    v.as_mut()
        .map_or(ptr::null(), |v| ptr::from_mut(v).cast_const())
}

/// Encodes resp with the gremlin writer after headroom zero bytes left for the caller,
/// refusing before any allocation a response larger than max.
pub(crate) fn encode_response(
    resp: &ServerResponse,
    headroom: usize,
    max: u64,
) -> Result<Vec<u8>, ResponseError> {
    let mut setup = resp.setup.as_ref().map(|s| CSetupResponse {
        version: s.version,
        instance_id_bytes: CBytes::of(&s.instance_id_bytes),
        instance_id: CBytes::of(s.instance_id.as_bytes()),
        size: 0,
    });
    let mut ping_request = resp
        .ping
        .as_ref()
        .and_then(|p| p.request.as_ref())
        .map(|r| CPingRequest {
            sequence: r.sequence,
            client_timestamp_ns: r.client_timestamp_ns,
            size: 0,
        });
    let mut ping = resp.ping.as_ref().map(|p| CPingResponse {
        local_stamp_ns: p.local_stamp_ns,
        monotonic_stamp_ns: p.monotonic_stamp_ns,
        request: c_opt(&mut ping_request),
        size: 0,
    });
    let mut write_ids: Vec<CId> = resp
        .write
        .as_ref()
        .map_or_else(Vec::new, |w| w.ids.iter().map(c_id).collect());
    let write_id_ptrs: Vec<*const CId> = write_ids
        .iter_mut()
        .map(|id| ptr::from_mut(id).cast_const())
        .collect();
    let mut write = resp.write.as_ref().map(|w| CWriteResponse {
        write_id: w.write_id,
        result: w.result as u32,
        ids: write_id_ptrs.as_ptr(),
        ids_count: write_id_ptrs.len(),
        size: 0,
    });
    let mut read_id = resp.read.as_ref().and_then(|r| r.id.as_ref()).map(c_id);
    let mut read = resp.read.as_ref().map(|r| CReadResponse {
        read_id: r.read_id,
        result: r.result as u32,
        id: c_opt(&mut read_id),
        data: CBytes::of(&r.data),
        data_source: r.data_source as u32,
        encoding: r.encoding as u32,
        size: 0,
    });
    let mut m = CServerResponse {
        setup: c_opt(&mut setup),
        ping: c_opt(&mut ping),
        write: c_opt(&mut write),
        read: c_opt(&mut read),
        size: 0,
    };
    let m = ptr::from_mut(&mut m).cast_const();

    // SAFETY: m and everything it points to live until the end of this function, the pointers
    // come from mutable borrows so the C may write the _size caches, and the byte ranges are
    // borrowed from resp. The C also requires the encoded size to fit in i64, which holds for
    // any response a frame could carry.
    let size = unsafe { normfs_response_size(m) };
    if size as u64 > max {
        return Err(ResponseError::TooLarge(size));
    }
    // Zeroed rather than set_len: the C contract bounds what is written, not that every
    // byte is.
    let mut out = vec![0u8; headroom + size];
    // SAFETY: as above, and nothing m reaches changed since normfs_response_size, as the C
    // requires; out[headroom ..] is size bytes, the length the C is told.
    let status = unsafe { normfs_response_encode(m, out.as_mut_ptr().add(headroom), size) };
    match status {
        NORMFS_RESPONSE_OK => Ok(out),
        NORMFS_RESPONSE_ERR_BAD_LENGTH => Err(ResponseError::BadLength),
        other => Err(ResponseError::UnknownStatus(other)),
    }
}
