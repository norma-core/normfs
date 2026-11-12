mod client_handler;
pub mod command_processor;
#[cfg(test)]
mod command_processor_test;
mod frame;
#[cfg(test)]
mod frame_test;
mod net;
#[cfg(test)]
mod net_test;
mod request;
#[cfg(test)]
mod request_test;
mod response;
#[cfg(test)]
mod response_test;

#[cfg(feature = "websocket")]
pub mod websocket;

use log::{error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::NormFS;
use client_handler::ClientHandler;
pub use command_processor::{CommandProcessor, ResponseSender};
use net::Listener;

const ACCEPT_RETRY: Duration = Duration::from_millis(100);
const STARVED_LOG: Duration = Duration::from_secs(60);

pub struct Server {
    normfs: Arc<NormFS>,
    listener: Listener,
}

impl Server {
    pub async fn new(addr: SocketAddr, normfs: Arc<NormFS>) -> Result<Self, std::io::Error> {
        let listener = Listener::bind(addr)?;
        info!("NormFS server listening on {}", addr);

        Ok(Server { normfs, listener })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.listener.local_addr()
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        let mut starved: Option<Instant> = None;
        loop {
            let accepted = match self.listener.accept().await {
                Ok(accepted) => {
                    if starved.take().is_some() {
                        info!("Accepting connections again");
                    }
                    accepted
                }
                // These clear up as connections close; any other error means the listener broke,
                // or a connection it cannot take off the queue would stall it.
                Err(e) if out_of_resources(&e) => {
                    if starved.is_none_or(|t| t.elapsed() >= STARVED_LOG) {
                        error!("Failed to accept connection (error: {})", e);
                        starved = Some(Instant::now());
                    }
                    tokio::time::sleep(ACCEPT_RETRY).await;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let addr = accepted.peer;
            info!("Accepted connection from {}", addr);
            if let Some(e) = accepted.nodelay_error {
                warn!(
                    "Failed to set TCP_NODELAY (client_addr: {}, error: {})",
                    addr, e
                );
            }
            if let Some(e) = accepted.unsent_error {
                warn!(
                    "Failed to set TCP_NOTSENT_LOWAT (client_addr: {}, error: {})",
                    addr, e
                );
            }

            let handler = ClientHandler::new(self.normfs.clone(), addr);
            let conn = accepted.conn;
            tokio::spawn(async move {
                handler.handle_connection(conn).await;
            });
        }
    }
}

#[cfg(unix)]
fn out_of_resources(e: &std::io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM | libc::ENOSPC)
    )
}

/// WSAEMFILE, WSAENOBUFS and ERROR_NOT_ENOUGH_MEMORY.
#[cfg(windows)]
fn out_of_resources(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(10024 | 10055 | 8))
}
