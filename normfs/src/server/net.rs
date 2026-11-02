use std::io;
use std::net::SocketAddr;
use std::os::raw::c_int;

// On Windows tokio owns the sockets; the C still reads and writes them.
#[cfg(unix)]
#[path = "net_unix.rs"]
mod imp;
#[cfg(windows)]
#[path = "net_windows.rs"]
mod imp;

pub(crate) use imp::{Conn, Listener};

/// What the C takes as `normfs_net_fd`.
#[cfg(unix)]
pub(crate) type SysFd = c_int;
#[cfg(windows)]
pub(crate) type SysFd = usize;

pub(crate) fn os_error(os_error: c_int) -> io::Error {
    io::Error::from_raw_os_error(os_error)
}

pub(crate) struct Accepted {
    pub(crate) conn: Conn,
    pub(crate) peer: SocketAddr,
    pub(crate) nodelay_error: Option<io::Error>,
    pub(crate) unsent_error: Option<io::Error>,
}
