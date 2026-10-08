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

use fastwebsockets::{FragmentCollectorRead, Frame, OpCode};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;

/// How many responses may be queued for one WebSocket client.
///
/// Bounded, and bounded at the same depth as the TCP path. A response holds its
/// payload, and a payload read from memory holds a pin on the page it was read
/// from until it is encoded onto the wire. Unbounded, a client that stops
/// reading accumulates pinned pages until the queue it is reading has no memory
/// left to append into — the read side deciding how much of the write side's
/// memory it may hold, which is what back-pressure exists to prevent.
const RESPONSE_CHANNEL_BUFFER: usize = 10;

/// Pong and close replies the reader owes the peer.
const CONTROL_CHANNEL_BUFFER: usize = 4;

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// WebSocket implementation of ResponseSender for normfs
pub struct WebSocketResponseSender {
    client_id: String,
    response_tx: mpsc::Sender<ServerResponse>,
}

impl WebSocketResponseSender {
    pub fn new(client_id: String, response_tx: mpsc::Sender<ServerResponse>) -> Self {
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
        Box::pin(async move { self.response_tx.send(response).await.is_ok() })
    }

    fn client_id(&self) -> String {
        self.client_id.clone()
    }
}

/// Handle a WebSocket connection for NormFS
///
/// Frames are read in a task of their own: `read_frame` is not cancel-safe, and
/// a `select!` that drops it mid-frame loses the header bytes it has consumed.
pub async fn handle_websocket(
    ws: fastwebsockets::WebSocket<TokioIo<Upgraded>>,
    normfs: Arc<NormFS>,
    client_id: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (rx, mut tx) = ws.split(tokio::io::split);
    let mut rx = FragmentCollectorRead::new(rx);

    let (normfs_response_tx, mut normfs_response_rx) =
        mpsc::channel::<ServerResponse>(RESPONSE_CHANNEL_BUFFER);
    let (control_tx, mut control_rx) = mpsc::channel::<Frame<'static>>(CONTROL_CHANNEL_BUFFER);

    let command_processor = CommandProcessor::new(normfs.clone());
    let response_sender = Arc::new(WebSocketResponseSender::new(
        client_id.clone(),
        normfs_response_tx,
    ));

    // Dropping this future, e.g. when the connection's task is cancelled,
    // must not leave the reader running.
    let _reader = AbortOnDrop(tokio::spawn(async move {
        let mut send_control = |frame: Frame<'static>| {
            let control_tx = control_tx.clone();
            async move {
                control_tx
                    .send(frame)
                    .await
                    .map_err(|_| "websocket writer has stopped")
            }
        };
        loop {
            let frame = match rx.read_frame(&mut send_control).await {
                Ok(frame) => frame,
                Err(e) => {
                    debug!("WebSocket read error: {:?}", e);
                    break;
                }
            };

            match frame.opcode {
                OpCode::Binary => match ClientRequest::decode(frame.payload.as_ref()) {
                    Ok(request) => {
                        debug!("Received NormFS ClientRequest");
                        command_processor
                            .handle_request(request, response_sender.clone())
                            .await;
                    }
                    Err(e) => {
                        warn!("Failed to decode ClientRequest: {:?}", e);
                    }
                },
                OpCode::Close => {
                    debug!("WebSocket close received");
                    break;
                }
                _ => {}
            }
        }
    }));

    // The control channel closes once the reader is done and its replies are written.
    loop {
        tokio::select! {
            Some(response) = normfs_response_rx.recv() => {
                let encoded = response.encode_to_vec();
                if let Err(e) = tx.write_frame(Frame::binary(encoded.into())).await {
                    error!("Error writing normfs response: {:?}", e);
                    break;
                }
            }
            control = control_rx.recv() => {
                let Some(frame) = control else { break };
                let close = frame.opcode == OpCode::Close;
                if let Err(e) = tx.write_frame(frame).await {
                    error!("Error writing control frame: {:?}", e);
                    break;
                }
                if close {
                    break;
                }
            }
        }
    }

    Ok(())
}
