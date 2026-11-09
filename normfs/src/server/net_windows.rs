use std::io;
use std::net::SocketAddr;
use std::os::windows::io::AsRawSocket;
use tokio::io::Interest;
use tokio::net::{TcpListener, TcpStream};

use super::{Accepted, SysFd};

/// NORMFS_NET_SYS_ACCEPT_TRIES of the Unix accept.
const ACCEPT_TRIES: u32 = 64;

unsafe extern "C" {
    fn normfs_net_sys_shutdown_write(fd: SysFd);
}

/// tokio has no readiness for a bare socket on Windows, so it owns the sockets.
pub(crate) struct Listener(TcpListener);

impl Listener {
    pub(crate) fn bind(addr: SocketAddr) -> io::Result<Self> {
        let std = std::net::TcpListener::bind(addr)?;
        std.set_nonblocking(true)?;
        Ok(Listener(TcpListener::from_std(std)?))
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }

    pub(crate) async fn accept(&self) -> io::Result<Accepted> {
        let mut failed = 0;
        let (stream, peer) = loop {
            match self.0.accept().await {
                // Reset or aborted while queued; accept took it off. Readiness stays set, so
                // this yields and gives up after as many as the Unix accept takes in a row.
                Err(e)
                    if failed < ACCEPT_TRIES
                        && matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                        ) =>
                {
                    failed += 1;
                    tokio::task::consume_budget().await;
                }
                other => break other?,
            }
        };
        // Windows has no TCP_NOTSENT_LOWAT; like macOS, it runs without the unsent limit.
        let nodelay_error = stream.set_nodelay(true).err();
        Ok(Accepted {
            conn: Conn(stream),
            peer,
            nodelay_error,
            unsent_error: None,
        })
    }
}

/// An accepted connection.
pub(crate) struct Conn(TcpStream);

/// Readiness tokio reported for one interest.
pub(crate) struct Ready<'a> {
    stream: &'a TcpStream,
    interest: Interest,
}

impl Ready<'_> {
    /// An I/O closure under try_io that would block clears the readiness and rearms the poll.
    pub(crate) fn clear_ready(&mut self) {
        let _ = self.stream.try_io(self.interest, || {
            Err::<(), _>(io::ErrorKind::WouldBlock.into())
        });
    }
}

impl Conn {
    pub(crate) fn raw(&self) -> SysFd {
        self.0.as_raw_socket() as SysFd
    }

    pub(crate) async fn readable(&self) -> io::Result<Ready<'_>> {
        self.0.readable().await?;
        Ok(Ready {
            stream: &self.0,
            interest: Interest::READABLE,
        })
    }

    pub(crate) async fn writable(&self) -> io::Result<Ready<'_>> {
        self.0.writable().await?;
        Ok(Ready {
            stream: &self.0,
            interest: Interest::WRITABLE,
        })
    }

    pub(crate) fn shutdown_write(&self) {
        // SAFETY: the socket is open for as long as self lives.
        unsafe { normfs_net_sys_shutdown_write(self.raw()) };
    }
}
