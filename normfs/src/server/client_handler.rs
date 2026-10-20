use log::{debug, error, info};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::command_processor::{CommandProcessor, ResponseSender};
use super::frame::{self, FRAME_HEADER_SIZE, FRAME_MAX_PAYLOAD};
use super::net::{Conn, FrameReader, NetError};
use super::request;
use super::response;
use crate::proto::{ClientRequest, ServerResponse};
use crate::NormFS;

const REQUEST_CHANNEL_BUFFER: usize = 10;
const RESPONSE_CHANNEL_BUFFER: usize = 10;
const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// TCP-specific implementation of ResponseSender
pub struct TcpResponseSender {
    client_addr: SocketAddr,
    replies: mpsc::Sender<ServerResponse>,
    reads: mpsc::Sender<ServerResponse>,
}

impl TcpResponseSender {
    fn new(
        client_addr: SocketAddr,
        replies: mpsc::Sender<ServerResponse>,
        reads: mpsc::Sender<ServerResponse>,
    ) -> Self {
        TcpResponseSender {
            client_addr,
            replies,
            reads,
        }
    }
}

impl ResponseSender for TcpResponseSender {
    fn send_response(
        &self,
        response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        // Read responses keep one queue so a read's end never overtakes its
        // entries; pongs and write acks go ahead of them.
        let tx = if response.read.is_some() {
            &self.reads
        } else {
            &self.replies
        };
        Box::pin(async move { tx.send(response).await.is_ok() })
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

    pub async fn handle_connection(self, conn: Conn) {
        debug!("Starting handler (client_addr: {})", self.addr);

        let (incoming_requests_tx, mut incoming_requests) = mpsc::channel(REQUEST_CHANNEL_BUFFER);
        let (replies, replies_rx) = mpsc::channel(RESPONSE_CHANNEL_BUFFER);
        let (reads, reads_rx) = mpsc::channel(RESPONSE_CHANNEL_BUFFER);
        let (done_tx, mut done) = mpsc::channel::<()>(1);

        let conn = Arc::new(conn);
        let read_conn = conn.clone();

        // Spawn read loop
        let incoming_tx = incoming_requests_tx.clone();
        let done_tx_read = done_tx.clone();
        let addr = self.addr;
        let read_handle = tokio::spawn(async move {
            Self::read_loop(read_conn, incoming_tx, done_tx_read, addr).await;
        });

        // Spawn write loop
        let done_tx_write = done_tx.clone();
        let addr = self.addr;
        let write_handle = tokio::spawn(async move {
            Self::write_loop(conn, replies_rx, reads_rx, done_tx_write, addr).await;
        });

        // Process loop runs in current task
        let sender = Arc::new(TcpResponseSender::new(self.addr, replies, reads));
        self.process_loop(&mut incoming_requests, sender, &mut done)
            .await;

        // Wait for other loops to finish
        let _ = read_handle.await;
        let _ = write_handle.await;

        info!("Connection closed (client_addr: {})", self.addr);
    }

    async fn read_loop(
        conn: Arc<Conn>,
        incoming_tx: mpsc::Sender<ClientRequest>,
        done_tx: mpsc::Sender<()>,
        addr: SocketAddr,
    ) {
        debug!("Read loop started (client_addr: {})", addr);

        let mut reader = FrameReader::new();
        loop {
            let size = match tokio::time::timeout(IDLE_TIMEOUT, reader.read_header(&conn)).await {
                Ok(Ok(size)) => size,
                Ok(Err(NetError::Eof)) => {
                    debug!("Client disconnected (EOF) (client_addr: {})", addr);
                    break;
                }
                Ok(Err(NetError::TooLarge(size))) => {
                    error!(
                        "Message size exceeds maximum (client_addr: {}, message_size: {}, max_size: {})",
                        addr, size, FRAME_MAX_PAYLOAD
                    );
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
            };

            if size == 0 {
                continue;
            }

            let mut payload = vec![0u8; size as usize];
            match tokio::time::timeout(IDLE_TIMEOUT, reader.read_payload(&conn, &mut payload)).await
            {
                Ok(Ok(())) => {}
                Ok(Err(NetError::Eof)) => {
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
            match request::decode_request(&payload) {
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
        conn: Arc<Conn>,
        mut replies: mpsc::Receiver<ServerResponse>,
        mut reads: mpsc::Receiver<ServerResponse>,
        done_tx: mpsc::Sender<()>,
        addr: SocketAddr,
    ) {
        debug!("Write loop started (client_addr: {})", addr);

        loop {
            let response = tokio::select! {
                biased;
                Some(response) = replies.recv() => response,
                Some(response) = reads.recv() => response,
                else => break,
            };
            let mut message =
                match response::encode_response(&response, FRAME_HEADER_SIZE, FRAME_MAX_PAYLOAD) {
                    Ok(message) => message,
                    Err(e) => {
                        error!(
                            "Cannot encode response (client_addr: {}, error: {})",
                            addr, e
                        );
                        break;
                    }
                };
            let size = message.len() - FRAME_HEADER_SIZE;
            match frame::encode_header(size as u64) {
                Ok(header) => message[..FRAME_HEADER_SIZE].copy_from_slice(&header),
                Err(e) => {
                    error!(
                        "Cannot frame response (client_addr: {}, error: {})",
                        addr, e
                    );
                    break;
                }
            }
            // Let go of the payload, and any page it pins, before a slow write.
            drop(response);

            if let Err(e) = conn.write_all(&message).await {
                error!(
                    "Error writing response (client_addr: {}, error: {})",
                    addr, e
                );
                break;
            }
        }

        // The read loop still holds the socket, so nothing else would end the stream.
        conn.shutdown_write();
        let _ = done_tx.send(()).await;
        debug!("Write loop ended (client_addr: {})", addr);
    }

    async fn process_loop(
        &self,
        incoming_requests: &mut mpsc::Receiver<ClientRequest>,
        response_sender: Arc<TcpResponseSender>,
        done: &mut mpsc::Receiver<()>,
    ) {
        debug!("Process loop started (client_addr: {})", self.addr);

        let command_processor = CommandProcessor::new(self.normfs.clone());

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
