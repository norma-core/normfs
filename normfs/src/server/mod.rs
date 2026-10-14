mod client_handler;
pub mod command_processor;
#[cfg(test)]
mod command_processor_test;

#[cfg(feature = "websocket")]
pub mod websocket;

use log::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::NormFS;
use client_handler::ClientHandler;
pub use command_processor::{CommandProcessor, ResponseSender};

pub struct Server {
    normfs: Arc<NormFS>,
    listener: TcpListener,
}

impl Server {
    pub async fn new(addr: SocketAddr, normfs: Arc<NormFS>) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        info!("NormFS server listening on {}", addr);

        Ok(Server { normfs, listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            info!("Accepted connection from {}", addr);
            // A response is one write, but Nagle would still hold one that
            // follows an unacknowledged response until the client's delayed ACK.
            if let Err(e) = stream.set_nodelay(true) {
                warn!(
                    "Failed to set TCP_NODELAY (client_addr: {}, error: {})",
                    addr, e
                );
            }

            #[cfg(target_os = "linux")]
            if let Err(e) = limit_unsent(&stream) {
                warn!(
                    "Failed to set TCP_NOTSENT_LOWAT (client_addr: {}, error: {})",
                    addr, e
                );
            }

            let handler = ClientHandler::new(self.normfs.clone(), addr);
            tokio::spawn(async move {
                handler.handle_connection(stream).await;
            });
        }
    }
}

/// The kernel would otherwise queue megabytes per socket on a slow link, and a
/// pong or write ack would wait behind all of it.
#[cfg(target_os = "linux")]
const UNSENT_LIMIT: libc::c_int = 128 * 1024;

#[cfg(target_os = "linux")]
fn limit_unsent(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor belongs to `stream` for the call, and the value
    // is a c_int whose size is passed with it.
    let rc = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NOTSENT_LOWAT,
            &UNSENT_LIMIT as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
