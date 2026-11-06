use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::raw::c_int;
use tokio::io::unix::AsyncFd;
use tokio::net::TcpStream;

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

unsafe extern "C" {
    fn normfs_net_sys_listen(addr: *const CAddr, os_error: *mut c_int) -> c_int;
    fn normfs_net_sys_local_addr(fd: c_int, addr: *mut CAddr, os_error: *mut c_int) -> c_int;
    fn normfs_net_sys_accept(fd: c_int, out: *mut CAccepted, os_error: *mut c_int) -> c_int;
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
struct Socket(RawFd);

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
    pub(crate) stream: TcpStream,
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
            // SAFETY: out.fd is a connected, non-blocking socket that nothing else owns.
            let std_stream = unsafe { std::net::TcpStream::from_raw_fd(out.fd) };
            let opt = |e: c_int| (e != 0).then(|| os_error(e));
            return Ok(Accepted {
                stream: TcpStream::from_std(std_stream)?,
                peer: from_c_addr(&out.peer),
                nodelay_error: opt(out.nodelay_error),
                unsent_error: opt(out.unsent_error),
            });
        }
    }
}
