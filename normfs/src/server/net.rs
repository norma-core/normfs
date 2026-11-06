use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::c_int;
use tokio::io::unix::AsyncFd;

use super::frame::FRAME_HEADER_SIZE;

const NORMFS_NET_OK: c_int = 0;
const NORMFS_NET_WOULD_BLOCK: c_int = 1;
const NORMFS_NET_EOF: c_int = 2;
const NORMFS_NET_ERR_IO: c_int = 3;
const NORMFS_NET_ERR_TOO_LARGE: c_int = 4;
const NORMFS_NET_ERR_BAD_LENGTH: c_int = 5;
const NORMFS_NET_YIELD: c_int = 6;

#[derive(Debug)]
pub(crate) enum NetError {
    Eof,
    TooLarge(u64),
    BadLength,
    Io(io::Error),
    UnknownStatus(c_int),
}

impl std::fmt::Display for NetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetError::Eof => write!(f, "Connection closed by peer"),
            NetError::TooLarge(size) => write!(f, "Frame payload too large: {}", size),
            NetError::BadLength => write!(f, "Read out of step with the frame"),
            NetError::Io(e) => write!(f, "{}", e),
            NetError::UnknownStatus(status) => write!(f, "Unknown socket status: {}", status),
        }
    }
}

impl std::error::Error for NetError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NetError::Io(e) => Some(e),
            _ => None,
        }
    }
}

#[repr(C)]
struct CResult {
    value: u64,
    os_error: c_int,
    status: c_int,
}

#[repr(C)]
struct CReader {
    header: [u8; FRAME_HEADER_SIZE],
    header_have: usize,
    payload_len: u64,
    payload_have: u64,
    in_payload: c_int,
}

#[repr(C)]
#[derive(Default)]
struct CAddr {
    ip: [u8; 16],
    scope_id: u32,
    port: u16,
    family: u8,
}

#[repr(C)]
#[derive(Default)]
struct CAccepted {
    peer: CAddr,
    fd: c_int,
    nodelay_error: c_int,
    unsent_error: c_int,
}

// The C fills these with memset of its own sizeof.
const _: () = assert!(std::mem::size_of::<CAddr>() == 24 && std::mem::align_of::<CAddr>() == 4);
const _: () = assert!(std::mem::size_of::<CAccepted>() == 36);

// test_net.c checks the C side of these.
const _: () = assert!(std::mem::size_of::<CResult>() == 16);
const _: () = assert!(std::mem::size_of::<CReader>() == 40);

unsafe extern "C" {
    fn normfs_net_reader_init(r: *mut CReader);
    fn normfs_net_read_header(r: *mut CReader, fd: c_int) -> CResult;
    fn normfs_net_read_payload(r: *mut CReader, fd: c_int, buf: *mut u8, len: usize) -> CResult;
    fn normfs_net_write(fd: c_int, buf: *const u8, len: usize, off: *mut usize) -> CResult;
    fn normfs_net_sys_listen(addr: *const CAddr, os_error: *mut c_int) -> c_int;
    fn normfs_net_sys_local_addr(fd: c_int, addr: *mut CAddr, os_error: *mut c_int) -> c_int;
    fn normfs_net_sys_accept(fd: c_int, out: *mut CAccepted, os_error: *mut c_int) -> c_int;
    fn normfs_net_sys_shutdown_write(fd: c_int);
    fn normfs_net_sys_close(fd: c_int);
}

fn to_c_addr(addr: SocketAddr) -> CAddr {
    let mut c = CAddr {
        port: addr.port(),
        ..Default::default()
    };
    match addr {
        SocketAddr::V4(a) => {
            c.family = 4;
            c.ip[..4].copy_from_slice(&a.ip().octets());
        }
        SocketAddr::V6(a) => {
            c.family = 6;
            c.ip.copy_from_slice(&a.ip().octets());
            c.scope_id = a.scope_id();
        }
    }
    c
}

fn from_c_addr(c: &CAddr) -> SocketAddr {
    if c.family == 4 {
        let ip = Ipv4Addr::new(c.ip[0], c.ip[1], c.ip[2], c.ip[3]);
        SocketAddr::V4(SocketAddrV4::new(ip, c.port))
    } else {
        SocketAddr::V6(SocketAddrV6::new(
            Ipv6Addr::from(c.ip),
            c.port,
            0,
            c.scope_id,
        ))
    }
}

fn os_error(os_error: c_int) -> io::Error {
    io::Error::from_raw_os_error(os_error)
}

/// A non-blocking socket descriptor, closed on drop.
pub(crate) struct Socket(RawFd);

impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        // SAFETY: self.0 is a descriptor this Socket owns and nothing uses it after drop.
        unsafe { normfs_net_sys_close(self.0) };
    }
}

pub(crate) struct Accepted {
    pub(crate) conn: Conn,
    pub(crate) peer: SocketAddr,
    pub(crate) nodelay_error: Option<io::Error>,
    pub(crate) unsent_error: Option<io::Error>,
}

pub(crate) struct Listener {
    fd: AsyncFd<Socket>,
}

impl Listener {
    pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
        let c = to_c_addr(addr);
        let mut err = 0;
        // SAFETY: c and err are live for the call.
        let fd = unsafe { normfs_net_sys_listen(&c, &mut err) };
        if fd < 0 {
            return Err(os_error(err));
        }
        Ok(Listener {
            fd: AsyncFd::new(Socket(fd))?,
        })
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        let mut c = CAddr::default();
        let mut err = 0;
        // SAFETY: c and err are live for the call, and the descriptor is open.
        if unsafe { normfs_net_sys_local_addr(self.fd.as_raw_fd(), &mut c, &mut err) } < 0 {
            return Err(os_error(err));
        }
        Ok(from_c_addr(&c))
    }

    pub(crate) async fn accept(&self) -> io::Result<Accepted> {
        loop {
            let mut guard = self.fd.readable().await?;
            // Spent before the accept, as tokio's own does, so a cancelled future leaves the
            // connection queued and a full backlog still yields.
            tokio::task::consume_budget().await;
            let mut out = CAccepted::default();
            let mut err = 0;
            // SAFETY: out and err are live for the call, and the descriptor is open.
            let rc = unsafe { normfs_net_sys_accept(self.fd.as_raw_fd(), &mut out, &mut err) };
            if rc < 0 {
                return Err(os_error(err));
            }
            if rc == 0 {
                guard.clear_ready();
                continue;
            }
            // Only failed connections this time; more may be queued, so readiness stays.
            if rc == 2 {
                continue;
            }
            let socket = Socket(out.fd);
            let opt = |e: c_int| (e != 0).then(|| os_error(e));
            return Ok(Accepted {
                conn: Conn(AsyncFd::new(socket)?),
                peer: from_c_addr(&out.peer),
                nodelay_error: opt(out.nodelay_error),
                unsent_error: opt(out.unsent_error),
            });
        }
    }
}

/// An accepted connection, shared by its read and write loops.
pub(crate) struct Conn(AsyncFd<Socket>);

impl Conn {
    pub(crate) fn shutdown_write(&self) {
        // SAFETY: the descriptor is open for as long as self lives.
        unsafe { normfs_net_sys_shutdown_write(self.0.as_raw_fd()) };
    }

    /// Writes all of buf, waiting for the socket to drain when it is full.
    pub(crate) async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        let mut off = 0usize;
        loop {
            // As tokio's own I/O does, so a socket that never blocks still lets the worker
            // yield; spent before the call so a timeout cannot drop what the call moved.
            tokio::task::consume_budget().await;
            // SAFETY: buf is live and off stays within it; the C writes no memory but off.
            let r =
                unsafe { normfs_net_write(self.0.as_raw_fd(), buf.as_ptr(), buf.len(), &mut off) };
            match r.status {
                NORMFS_NET_OK => return Ok(()),
                NORMFS_NET_WOULD_BLOCK => self.0.writable().await?.clear_ready(),
                NORMFS_NET_YIELD => {}
                NORMFS_NET_ERR_IO => return Err(os_error(r.os_error)),
                other => return Err(io::Error::other(NetError::UnknownStatus(other))),
            }
        }
    }
}

enum Step {
    Done(u64),
    WouldBlock,
    Yield,
}

/// The read side of a connection: one frame at a time, a header then its payload.
pub(crate) struct FrameReader(CReader);

impl FrameReader {
    pub(crate) fn new() -> Self {
        let mut r = std::mem::MaybeUninit::<CReader>::uninit();
        // SAFETY: normfs_net_reader_init writes every field of the reader it is given.
        unsafe {
            normfs_net_reader_init(r.as_mut_ptr());
            FrameReader(r.assume_init())
        }
    }

    fn status(r: CResult) -> Result<Step, NetError> {
        match r.status {
            NORMFS_NET_OK => Ok(Step::Done(r.value)),
            NORMFS_NET_WOULD_BLOCK => Ok(Step::WouldBlock),
            NORMFS_NET_YIELD => Ok(Step::Yield),
            NORMFS_NET_EOF => Err(NetError::Eof),
            NORMFS_NET_ERR_IO => Err(NetError::Io(os_error(r.os_error))),
            NORMFS_NET_ERR_TOO_LARGE => Err(NetError::TooLarge(r.value)),
            NORMFS_NET_ERR_BAD_LENGTH => Err(NetError::BadLength),
            other => Err(NetError::UnknownStatus(other)),
        }
    }

    /// The payload length of the next frame; zero is a keepalive.
    pub(crate) async fn read_header(&mut self, conn: &Conn) -> Result<u64, NetError> {
        loop {
            tokio::task::consume_budget().await;
            // SAFETY: self.0 is a live reader and the descriptor is open.
            let r = unsafe { normfs_net_read_header(&mut self.0, conn.0.as_raw_fd()) };
            match Self::status(r)? {
                Step::Done(len) => return Ok(len),
                Step::WouldBlock => conn.0.readable().await.map_err(NetError::Io)?.clear_ready(),
                Step::Yield => {}
            }
        }
    }

    pub(crate) async fn read_payload(
        &mut self,
        conn: &Conn,
        buf: &mut [u8],
    ) -> Result<(), NetError> {
        loop {
            tokio::task::consume_budget().await;
            // SAFETY: self.0 is a live reader, buf is buf.len() writable bytes and the C
            // writes no more than that.
            let r = unsafe {
                normfs_net_read_payload(
                    &mut self.0,
                    conn.0.as_raw_fd(),
                    buf.as_mut_ptr(),
                    buf.len(),
                )
            };
            match Self::status(r)? {
                Step::Done(_) => return Ok(()),
                Step::WouldBlock => conn.0.readable().await.map_err(NetError::Io)?.clear_ready(),
                Step::Yield => {}
            }
        }
    }
}
