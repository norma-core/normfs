//! WebSocket handler for NormFS
//!
//! This module provides WebSocket support for NormFS, allowing clients to connect
//! via WebSocket and send/receive NormFS protocol messages.

use log::{debug, error, warn};
use prost::Message;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::command_processor::{CommandProcessor, ResponseSender};
use crate::{
    proto::{ClientRequest, ServerResponse},
    NormFS,
};

use fastwebsockets::{FragmentCollector, Frame, OpCode};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;

/// WebSocket implementation of ResponseSender for normfs
pub struct WebSocketResponseSender {
    client_id: String,
    response_tx: mpsc::UnboundedSender<ServerResponse>,
}

impl WebSocketResponseSender {
    pub fn new(client_id: String, response_tx: mpsc::UnboundedSender<ServerResponse>) -> Self {
        WebSocketResponseSender {
            client_id,
            response_tx,
        }
    }
}

impl ResponseSender for WebSocketResponseSender {
    fn send_response(
        &self,
        response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async move { self.response_tx.send(response).is_ok() })
    }

    fn client_id(&self) -> String {
        self.client_id.clone()
    }
}

/// Handle a WebSocket connection for NormFS
///
/// This function processes a WebSocket upgrade and handles NormFS protocol messages.
/// It creates a bidirectional channel for responses and handles sending and receiving
/// messages in a single task using tokio::select.
pub async fn handle_websocket(
    ws: fastwebsockets::WebSocket<TokioIo<Upgraded>>,
    normfs: Arc<NormFS>,
    client_id: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut ws = FragmentCollector::new(ws);

    // Create channel for normfs responses
    let (normfs_response_tx, mut normfs_response_rx) = mpsc::unbounded_channel::<ServerResponse>();

    // Create command processor
    let command_processor = CommandProcessor::new(normfs.clone());
    let response_sender = Arc::new(WebSocketResponseSender::new(
        client_id.clone(),
        normfs_response_tx.clone(),
    ));

    // Handle incoming and outgoing messages
    loop {
        tokio::select! {
            // Handle outgoing normfs responses
            Some(response) = normfs_response_rx.recv() => {
                let encoded = response.encode_to_vec();
                let frame = Frame::binary(encoded.into());
                if let Err(e) = ws.write_frame(frame).await {
                    error!("Error writing normfs response: {:?}", e);
                    break;
                }
            }

            // Handle incoming messages
            result = ws.read_frame() => {
                let frame = match result {
                    Ok(frame) => frame,
                    Err(e) => {
                        debug!("WebSocket read error: {:?}", e);
                        break;
                    }
                };

                match frame.opcode {
                    OpCode::Binary => {
                        let data = frame.payload;

                        // Try to decode as ClientRequest
                        match ClientRequest::decode(data.as_ref()) {
                            Ok(request) => {
                                debug!("Received NormFS ClientRequest");
                                command_processor.handle_request(request, response_sender.clone()).await;
                            }
                            Err(e) => {
                                warn!("Failed to decode ClientRequest: {:?}", e);
                            }
                        }
                    }
                    OpCode::Close => {
                        debug!("WebSocket close received");
                        break;
                    }
                    OpCode::Ping => {
                        if let Err(e) = ws.write_frame(Frame::pong(frame.payload)).await {
                            error!("Error writing pong: {:?}", e);
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
