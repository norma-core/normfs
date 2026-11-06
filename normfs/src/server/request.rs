use bytes::Bytes;
use std::os::raw::c_int;

use crate::proto::{
    ClientRequest, Id, Offset, PingRequest, ReadRequest, SetupRequest, WriteRequest,
};

const NORMFS_REQUEST_OK: c_int = 0;
const NORMFS_REQUEST_ERR_TRUNCATED: c_int = 1;
const NORMFS_REQUEST_ERR_OVERFLOW: c_int = 2;
const NORMFS_REQUEST_ERR_BAD_WIRE_TYPE: c_int = 3;
const NORMFS_REQUEST_ERR_BAD_FIELD_NUM: c_int = 4;
const NORMFS_REQUEST_ERR_NO_SPACE: c_int = 5;
const NORMFS_REQUEST_ERR_REPEATED: c_int = 6;

const PACKETS_CAP: usize = 64;
const ENCODINGS_CAP: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestError {
    Truncated,
    Overflow,
    BadWireType,
    BadFieldNum,
    NoSpace,
    Repeated,
    InvalidUtf8,
    SpanOutOfBounds { off: usize, len: usize },
    UnknownStatus(c_int),
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestError::Truncated => write!(f, "Truncated client request"),
            RequestError::Overflow => write!(f, "Client request varint exceeds u64"),
            RequestError::BadWireType => write!(f, "Invalid wire type in client request"),
            RequestError::BadFieldNum => write!(f, "Invalid field number in client request"),
            RequestError::NoSpace => write!(f, "Client request outgrew its decode buffers"),
            RequestError::Repeated => {
                write!(f, "Client request gives a setup, ping, write or read twice")
            }
            RequestError::InvalidUtf8 => write!(f, "Queue id is not valid UTF-8"),
            RequestError::SpanOutOfBounds { off, len } => {
                write!(f, "Client request field out of bounds: {}+{}", off, len)
            }
            RequestError::UnknownStatus(status) => {
                write!(f, "Unknown client request status: {}", status)
            }
        }
    }
}

impl std::error::Error for RequestError {}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CSpan {
    off: usize,
    len: usize,
}

// Mirrors struct normfs_request in c/include/normfs/request.h.
#[repr(C)]
#[derive(Default)]
struct CRequest {
    setup_version: u64,
    ping_sequence: u64,
    ping_client_timestamp_ns: u64,
    write_id: u64,
    write_queue_id: CSpan,
    write_packets: usize,
    read_id: u64,
    read_queue_id: CSpan,
    read_step: u64,
    read_encodings: usize,
    read_offset_id_raw: CSpan,
    read_offset_type: i64,
    read_limit: u64,
    has_setup: c_int,
    has_ping: c_int,
    has_write: c_int,
    has_read: c_int,
    has_read_offset: c_int,
    has_read_offset_id: c_int,
}

const _: () = assert!(std::mem::size_of::<CSpan>() == 16);
const _: () = assert!(std::mem::size_of::<CRequest>() == 152);

unsafe extern "C" {
    fn normfs_request_decode(
        buf: *const u8,
        len: usize,
        req: *mut CRequest,
        packets: *mut CSpan,
        packets_cap: usize,
        encodings: *mut i32,
        encodings_cap: usize,
    ) -> c_int;
}

fn map_status(status: c_int) -> Result<(), RequestError> {
    match status {
        NORMFS_REQUEST_OK => Ok(()),
        NORMFS_REQUEST_ERR_TRUNCATED => Err(RequestError::Truncated),
        NORMFS_REQUEST_ERR_OVERFLOW => Err(RequestError::Overflow),
        NORMFS_REQUEST_ERR_BAD_WIRE_TYPE => Err(RequestError::BadWireType),
        NORMFS_REQUEST_ERR_BAD_FIELD_NUM => Err(RequestError::BadFieldNum),
        NORMFS_REQUEST_ERR_NO_SPACE => Err(RequestError::NoSpace),
        NORMFS_REQUEST_ERR_REPEATED => Err(RequestError::Repeated),
        other => Err(RequestError::UnknownStatus(other)),
    }
}

fn slice(buf: &[u8], span: CSpan) -> Result<&[u8], RequestError> {
    span.off
        .checked_add(span.len)
        .and_then(|end| buf.get(span.off..end))
        .ok_or(RequestError::SpanOutOfBounds {
            off: span.off,
            len: span.len,
        })
}

fn string(buf: &[u8], span: CSpan) -> Result<String, RequestError> {
    String::from_utf8(slice(buf, span)?.to_vec()).map_err(|_| RequestError::InvalidUtf8)
}

/// Decodes a ClientRequest with the gremlin reader; bytes are copied out of buf, as prost does.
pub(crate) fn decode_request(buf: &[u8]) -> Result<ClientRequest, RequestError> {
    let mut req = CRequest::default();
    let mut packets = [CSpan::default(); PACKETS_CAP];
    let mut encodings = [0i32; ENCODINGS_CAP];
    let mut status = decode(buf, &mut req, &mut packets, &mut encodings);
    if status != NORMFS_REQUEST_ERR_NO_SPACE {
        map_status(status)?;
        return build(buf, &req, &packets, &encodings);
    }

    let mut packets = vec![CSpan::default(); req.write_packets.max(PACKETS_CAP)];
    let mut encodings = vec![0i32; req.read_encodings.max(ENCODINGS_CAP)];
    status = decode(buf, &mut req, &mut packets, &mut encodings);
    map_status(status)?;
    build(buf, &req, &packets, &encodings)
}

fn decode(buf: &[u8], req: &mut CRequest, packets: &mut [CSpan], encodings: &mut [i32]) -> c_int {
    // SAFETY: buf, req, packets and encodings are live and disjoint, and the C writes at most
    // packets.len() spans and encodings.len() values.
    unsafe {
        normfs_request_decode(
            buf.as_ptr(),
            buf.len(),
            req,
            packets.as_mut_ptr(),
            packets.len(),
            encodings.as_mut_ptr(),
            encodings.len(),
        )
    }
}

fn build(
    buf: &[u8],
    req: &CRequest,
    packets: &[CSpan],
    encodings: &[i32],
) -> Result<ClientRequest, RequestError> {
    let packets = packets
        .get(..req.write_packets)
        .ok_or(RequestError::NoSpace)?;
    let encodings = encodings
        .get(..req.read_encodings)
        .ok_or(RequestError::NoSpace)?;

    let setup = (req.has_setup != 0).then_some(SetupRequest {
        version: req.setup_version,
    });
    let ping = (req.has_ping != 0).then_some(PingRequest {
        sequence: req.ping_sequence,
        client_timestamp_ns: req.ping_client_timestamp_ns,
    });
    let write = if req.has_write != 0 {
        Some(WriteRequest {
            write_id: req.write_id,
            queue_id: string(buf, req.write_queue_id)?,
            packets: packets
                .iter()
                .map(|&span| slice(buf, span).map(Bytes::copy_from_slice))
                .collect::<Result<_, _>>()?,
        })
    } else {
        None
    };
    let read = if req.has_read != 0 {
        let offset = if req.has_read_offset != 0 {
            let id = if req.has_read_offset_id != 0 {
                Some(Id {
                    raw: Bytes::copy_from_slice(slice(buf, req.read_offset_id_raw)?),
                })
            } else {
                None
            };
            Some(Offset {
                id,
                r#type: req.read_offset_type as i32,
            })
        } else {
            None
        };
        Some(ReadRequest {
            read_id: req.read_id,
            queue_id: string(buf, req.read_queue_id)?,
            step: req.read_step,
            accept_encodings: encodings.to_vec(),
            offset,
            limit: req.read_limit,
        })
    } else {
        None
    };

    Ok(ClientRequest {
        setup,
        ping,
        write,
        read,
    })
}
