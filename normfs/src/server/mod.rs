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

#[cfg(feature = "websocket")]
pub mod websocket;

use log::{info, warn};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::NormFS;
use client_handler::ClientHandler;
pub use command_processor::{CommandProcessor, ResponseSender};
use net::Listener;

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
        loop {
            let accepted = self.listener.accept().await?;
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
            let stream = accepted.stream;
            tokio::spawn(async move {
                handler.handle_connection(stream).await;
            });
        }
    }
}
