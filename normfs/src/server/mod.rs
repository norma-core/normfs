mod client_handler;
pub mod command_processor;

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

            let handler = ClientHandler::new(self.normfs.clone(), addr);
            tokio::spawn(async move {
                handler.handle_connection(stream).await;
            });
        }
    }
}
