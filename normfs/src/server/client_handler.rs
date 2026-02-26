use log::{debug, error, info};
use prost::Message;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use super::command_processor::{CommandProcessor, ResponseSender};
use crate::proto::{ClientRequest, ServerResponse};
use crate::NormFS;

const MAX_MESSAGE_SIZE: u64 = 5 * 1024 * 1024 * 1024; // 5GB
const REQUEST_CHANNEL_BUFFER: usize = 10;
const RESPONSE_CHANNEL_BUFFER: usize = 10;
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// TCP-specific implementation of ResponseSender
pub struct TcpResponseSender {
    client_addr: SocketAddr,
    response_tx: mpsc::Sender<ServerResponse>,
}

impl TcpResponseSender {
    fn new(client_addr: SocketAddr, response_tx: mpsc::Sender<ServerResponse>) -> Self {
        TcpResponseSender {
            client_addr,
            response_tx,
        }
    }
}

impl ResponseSender for TcpResponseSender {
    fn send_response(
        &self,
        response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move { self.response_tx.send(response).await.is_ok() })
    }

    fn client_id(&self) -> String {
        self.client_addr.to_string()
    }
}

pub struct ClientHandler {
    normfs: Arc<NormFS>,
    addr: SocketAddr,
}

impl ClientHandler {
    pub fn new(normfs: Arc<NormFS>, addr: SocketAddr) -> Self {
        ClientHandler { normfs, addr }
    }

    pub async fn handle_connection(self, conn: TcpStream) {
        debug!("Starting handler (client_addr: {})", self.addr);

        let (incoming_requests_tx, mut incoming_requests) = mpsc::channel(REQUEST_CHANNEL_BUFFER);
        let (outgoing_responses, outgoing_responses_rx) = mpsc::channel(RESPONSE_CHANNEL_BUFFER);
        let (done_tx, mut done) = mpsc::channel::<()>(1);

        let (read_half, write_half) = conn.into_split();

        // Spawn read loop
        let incoming_tx = incoming_requests_tx.clone();
        let done_tx_read = done_tx.clone();
        let addr = self.addr;
        let read_handle = tokio::spawn(async move {
            Self::read_loop(read_half, incoming_tx, done_tx_read, addr).await;
        });

        // Spawn write loop
        let done_tx_write = done_tx.clone();
        let addr = self.addr;
        let write_handle = tokio::spawn(async move {
            Self::write_loop(write_half, outgoing_responses_rx, done_tx_write, addr).await;
        });

        // Process loop runs in current task
        self.process_loop(&mut incoming_requests, &outgoing_responses, &mut done)
            .await;

        // Wait for other loops to finish
        let _ = read_handle.await;
        let _ = write_handle.await;

        info!("Connection closed (client_addr: {})", self.addr);
    }

    async fn read_loop(
        mut read_half: tokio::net::tcp::OwnedReadHalf,
        incoming_tx: mpsc::Sender<ClientRequest>,
        done_tx: mpsc::Sender<()>,
        addr: SocketAddr,
    ) {
        debug!("Read loop started (client_addr: {})", addr);

        loop {
            // Read message size (8 bytes)
            let mut size_bytes = [0u8; 8];
            match tokio::time::timeout(IDLE_TIMEOUT, read_half.read_exact(&mut size_bytes)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!("Client disconnected (EOF) (client_addr: {})", addr);
                    break;
                }
                Ok(Err(e)) => {
                    error!(
                        "Error reading message size (client_addr: {}, error: {})",
                        addr, e
                    );
                    break;
                }
                Err(_) => {
                    info!("Client idle timeout (client_addr: {})", addr);
                    break;
                }
            }

            let size = u64::from_le_bytes(size_bytes);

            if size > MAX_MESSAGE_SIZE {
                error!(
                    "Message size exceeds maximum (client_addr: {}, message_size: {}, max_size: {})",
                    addr, size, MAX_MESSAGE_SIZE
                );
                break;
            }

            if size == 0 {
                continue;
            }

            // Read message payload
            let mut payload = vec![0u8; size as usize];
            match tokio::time::timeout(IDLE_TIMEOUT, read_half.read_exact(&mut payload)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    debug!(
                        "Client disconnected (EOF in payload) (client_addr: {})",
                        addr
                    );
                    break;
                }
                Ok(Err(e)) => {
                    error!(
                        "Error reading message payload (client_addr: {}, error: {})",
                        addr, e
                    );
                    break;
                }
                Err(_) => {
                    info!(
                        "Client idle timeout waiting for payload (client_addr: {})",
                        addr
                    );
                    break;
                }
            }

            // Parse protobuf message
            match ClientRequest::decode(&payload[..]) {
                Ok(request) => {
                    if incoming_tx.send(request).await.is_err() {
                        debug!(
                            "Process loop has stopped, exiting read loop (client_addr: {})",
                            addr
                        );
                        break;
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to decode client request (client_addr: {}, error: {}, payload_size: {})",
                        addr, e, payload.len()
                    );
                    break;
                }
            }
        }

        let _ = done_tx.send(()).await;
        debug!("Read loop ended (client_addr: {})", addr);
    }

    async fn write_loop(
        mut write_half: tokio::net::tcp::OwnedWriteHalf,
        mut outgoing_rx: mpsc::Receiver<ServerResponse>,
        done_tx: mpsc::Sender<()>,
        addr: SocketAddr,
    ) {
        debug!("Write loop started (client_addr: {})", addr);

        while let Some(response) = outgoing_rx.recv().await {
            let payload = response.encode_to_vec();
            let size = payload.len() as u64;
            let size_bytes = size.to_le_bytes();

            // Write size
            if let Err(e) = write_half.write_all(&size_bytes).await {
                error!(
                    "Error writing response size (client_addr: {}, error: {})",
                    addr, e
                );
                break;
            }

            // Write payload
            if size > 0 {
                if let Err(e) = write_half.write_all(&payload).await {
                    error!(
                        "Error writing response payload (client_addr: {}, error: {})",
                        addr, e
                    );
                    break;
                }
            }
        }

        let _ = done_tx.send(()).await;
        debug!("Write loop ended (client_addr: {})", addr);
    }

    async fn process_loop(
        &self,
        incoming_requests: &mut mpsc::Receiver<ClientRequest>,
        outgoing_responses: &mpsc::Sender<ServerResponse>,
        done: &mut mpsc::Receiver<()>,
    ) {
        debug!("Process loop started (client_addr: {})", self.addr);

        // Create command processor and response sender
        let command_processor = CommandProcessor::new(self.normfs.clone());
        let response_sender = Arc::new(TcpResponseSender::new(
            self.addr,
            outgoing_responses.clone(),
        ));

        loop {
            tokio::select! {
                Some(request) = incoming_requests.recv() => {
                    command_processor.handle_request(request, response_sender.clone()).await;
                }
                Some(_) = done.recv() => {
                    debug!("Process loop shutting down due to done signal (client_addr: {})", self.addr);
                    break;
                }
            }
        }

        debug!("Process loop ended (client_addr: {})", self.addr);
    }
}
