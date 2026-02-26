use bytes::{Bytes, BytesMut};
use log::{debug, error, warn};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;
use uintn::UintN;

use crate::proto::OffsetType;
use crate::proto::{
    read_response, write_response, ClientRequest, Id, PingRequest, PingResponse, ReadRequest,
    ReadResponse, ServerResponse, SetupRequest, SetupResponse, WriteRequest, WriteResponse,
};
use crate::{DataSource, Error, NormFS};
use normfs_types::ReadPosition;

fn get_local_stamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(target_os = "linux")]
fn get_monotonic_stamp_ns() -> u64 {
    use libc::{clock_gettime, timespec, CLOCK_BOOTTIME};
    unsafe {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        if clock_gettime(CLOCK_BOOTTIME, &mut ts as *mut _) != 0 {
            return 0;
        }

        let nanos = (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64);

        nanos
    }
}

#[cfg(target_os = "macos")]
fn get_monotonic_stamp_ns() -> u64 {
    use libc::{clock_gettime, timespec, CLOCK_MONOTONIC_RAW};
    unsafe {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        if clock_gettime(CLOCK_MONOTONIC_RAW, &mut ts as *mut _) != 0 {
            return 0;
        }

        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }
}

#[cfg(target_os = "freebsd")]
fn get_monotonic_stamp_ns() -> u64 {
    use libc::{clock_gettime, timespec, CLOCK_UPTIME};
    unsafe {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };

        if clock_gettime(CLOCK_UPTIME, &mut ts as *mut _) != 0 {
            return 0;
        }

        (ts.tv_sec as u64)
            .saturating_mul(1_000_000_000)
            .saturating_add(ts.tv_nsec as u64)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
fn get_monotonic_stamp_ns() -> u64 {
    // Fallback for unsupported platforms
    get_local_stamp_ns()
}

/// Convert DataSource to protobuf enum value
fn data_source_to_proto(source: DataSource) -> i32 {
    use read_response::DataSource as ProtoDataSource;
    match source {
        DataSource::None => ProtoDataSource::DsNone as i32,
        DataSource::Cloud => ProtoDataSource::DsCloud as i32,
        DataSource::DiskStore => ProtoDataSource::DsDiskStore as i32,
        DataSource::DiskWal => ProtoDataSource::DsDiskWal as i32,
        DataSource::Memory => ProtoDataSource::DsMemory as i32,
    }
}

/// Trait for sending responses back to the client
/// This abstracts the transport layer (TCP, WebSocket, etc.)
pub trait ResponseSender: Send + Sync {
    fn send_response(
        &self,
        response: ServerResponse,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
    fn client_id(&self) -> String;
}

/// Command processor that handles NormFS requests
/// This is transport-agnostic and can be used with TCP, WebSocket, etc.
#[derive(Clone)]
pub struct CommandProcessor {
    normfs: Arc<NormFS>,
}

impl CommandProcessor {
    pub fn new(normfs: Arc<NormFS>) -> Self {
        CommandProcessor { normfs }
    }

    /// Process a single client request
    pub async fn handle_request<T: ResponseSender + 'static>(
        &self,
        request: ClientRequest,
        sender: Arc<T>,
    ) {
        if let Some(setup) = request.setup {
            let processor = self.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                processor.handle_setup(setup, &sender).await;
            });
        }

        if let Some(ping) = request.ping {
            let processor = self.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                processor.handle_ping(ping, &sender).await;
            });
        }

        if let Some(write) = request.write {
            let processor = self.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                processor.handle_write(write, &sender).await;
            });
        }

        if let Some(read) = request.read {
            let processor = self.clone();
            let sender = sender.clone();
            tokio::spawn(async move {
                processor.handle_read(read, &sender).await;
            });
        }
    }

    async fn handle_setup<T: ResponseSender>(&self, request: SetupRequest, sender: &Arc<T>) {
        debug!(
            "Handling SetupRequest (client_id: {}, version: {})",
            sender.client_id(),
            request.version
        );

        let response = ServerResponse {
            setup: Some(SetupResponse {
                version: request.version,
                instance_id_bytes: self.normfs.get_instance_id_bytes(),
                instance_id: self.normfs.get_instance_id().to_string(),
            }),
            ..Default::default()
        };

        let _ = sender.send_response(response).await;
    }

    async fn handle_ping<T: ResponseSender>(&self, request: PingRequest, sender: &Arc<T>) {
        debug!(
            "Handling PingRequest (client_id: {}, sequence: {})",
            sender.client_id(),
            request.sequence
        );

        let response = ServerResponse {
            ping: Some(PingResponse {
                local_stamp_ns: get_local_stamp_ns(),
                monotonic_stamp_ns: get_monotonic_stamp_ns(),
                request: Some(request),
            }),
            ..Default::default()
        };

        let _ = sender.send_response(response).await;
    }

    async fn handle_write<T: ResponseSender>(&self, request: WriteRequest, sender: &Arc<T>) {
        debug!(
            "Handling WriteRequest (client_id: {}, write_id: {}, queue_id: {}, num_packets: {})",
            sender.client_id(),
            request.write_id,
            request.queue_id,
            request.packets.len()
        );

        let write_id = request.write_id;

        if request.packets.is_empty() {
            let response = ServerResponse {
                write: Some(WriteResponse {
                    write_id,
                    result: write_response::Result::WrDone as i32,
                    ids: vec![],
                }),
                ..Default::default()
            };
            let _ = sender.send_response(response).await;
            return;
        }

        let packets: Vec<Bytes> = request.packets.into_iter().collect();

        let queue_id = self.normfs.resolve(request.queue_id.as_str());

        if let Err(e) = self.normfs.ensure_queue_exists_for_write(&queue_id).await {
            error!(
                "Failed to ensure queue exists (client_id: {}, write_id: {}, queue_id: {}, error: {:?})",
                sender.client_id(), write_id, queue_id, e
            );
            let response = ServerResponse {
                write: Some(WriteResponse {
                    write_id,
                    result: write_response::Result::WrServerError as i32,
                    ids: vec![],
                }),
                ..Default::default()
            };
            let _ = sender.send_response(response).await;
            return;
        }

        let result = if packets.len() == 1 {
            self.normfs
                .enqueue(&queue_id, packets[0].clone())
                .map(|id| vec![id])
        } else {
            self.normfs.enqueue_batch(&queue_id, packets)
        };

        match result {
            Ok(ids) => {
                let id_messages: Vec<Id> = ids
                    .into_iter()
                    .map(|id| {
                        let mut buffer = BytesMut::new();
                        id.write_value_to_buffer(&mut buffer);
                        Id {
                            raw: buffer.freeze(),
                        }
                    })
                    .collect();

                let response = ServerResponse {
                    write: Some(WriteResponse {
                        write_id,
                        result: write_response::Result::WrDone as i32,
                        ids: id_messages,
                    }),
                    ..Default::default()
                };
                let _ = sender.send_response(response).await;
            }
            Err(e) => {
                error!(
                    "Failed to write to queue (client_id: {}, queue_id: {}, write_id: {}, error: {:?})",
                    sender.client_id(), request.queue_id, write_id, e
                );

                let response = ServerResponse {
                    write: Some(WriteResponse {
                        write_id,
                        result: write_response::Result::WrServerError as i32,
                        ids: vec![],
                    }),
                    ..Default::default()
                };
                let _ = sender.send_response(response).await;
            }
        }
    }

    async fn handle_read<T: ResponseSender>(&self, request: ReadRequest, sender: &Arc<T>) {
        let read_id = request.read_id;
        let queue_id = self.normfs.resolve(request.queue_id.as_str());

        if let Err(e) = self.normfs.ensure_queue_exists_for_read(&queue_id).await {
            error!(
                "Failed to ensure queue exists (client_id: {}, read_id: {}, queue_id: {}, error: {:?})",
                sender.client_id(), read_id, queue_id, e
            );
            let response = ServerResponse {
                read: Some(ReadResponse {
                    read_id,
                    result: read_response::Result::RrServerError as i32,
                    ..Default::default()
                }),
                ..Default::default()
            };
            let _ = sender.send_response(response).await;
            return;
        }

        let offset_id = if let Some(offset) = &request.offset {
            if let Some(id) = &offset.id {
                if !id.raw.is_empty() {
                    match UintN::read_value_from_slice(&id.raw, id.raw.len()) {
                        Ok(id) => id,
                        Err(e) => {
                            error!(
                                "Failed to parse offset id (client_id: {}, read_id: {}, error: {:?})",
                                sender.client_id(), read_id, e
                            );
                            let response = ServerResponse {
                                read: Some(ReadResponse {
                                    read_id,
                                    result: read_response::Result::RrNotFound as i32,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            };
                            let _ = sender.send_response(response).await;
                            return;
                        }
                    }
                } else {
                    UintN::zero()
                }
            } else {
                UintN::zero()
            }
        } else {
            UintN::zero()
        };

        let offset_type = request
            .offset
            .as_ref()
            .and_then(|o| OffsetType::try_from(o.r#type).ok())
            .unwrap_or(OffsetType::OtAbsolute);

        let position = match offset_type {
            OffsetType::OtAbsolute => ReadPosition::Absolute(offset_id.clone()),
            OffsetType::OtShiftFromTail => ReadPosition::ShiftFromTail(offset_id.clone()),
        };

        let limit = request.limit;
        let step = if request.step == 0 { 1 } else { request.step };

        debug!(
            "Handling ReadRequest (client_id: {}, read_id: {}, queue_id: {}, offset_id: {}, position: {:?}, limit: {}, step: {})",
            sender.client_id(), read_id, queue_id, offset_id, position, limit, step
        );

        let start_response = ServerResponse {
            read: Some(ReadResponse {
                read_id,
                result: read_response::Result::RrStart as i32,
                ..Default::default()
            }),
            ..Default::default()
        };

        if !sender.send_response(start_response).await {
            warn!(
                "Client disconnected before RR_START could be sent (client_id: {}, read_id: {})",
                sender.client_id(),
                read_id
            );
            return;
        }

        let (tx, mut rx) = mpsc::channel(2048);

        let normfs = self.normfs.clone();
        let queue_id_clone = queue_id.clone();
        let read_handle = tokio::spawn(async move {
            normfs
                .reader_fsm
                .read(queue_id_clone, position, limit, step, tx)
                .await
        });
        while let Some(entry) = rx.recv().await {
            let mut buffer = BytesMut::new();
            entry.id.write_value_to_buffer(&mut buffer);

            let entry_response = ServerResponse {
                read: Some(ReadResponse {
                    read_id,
                    result: read_response::Result::RrEntry as i32,
                    id: Some(Id {
                        raw: buffer.freeze(),
                    }),
                    data: entry.data,
                    data_source: data_source_to_proto(entry.source),
                }),
                ..Default::default()
            };

            if !sender.send_response(entry_response).await {
                warn!(
                    "Client disconnected while sending RR_ENTRY (client_id: {}, read_id: {})",
                    sender.client_id(),
                    read_id
                );
                return;
            }
        }

        match read_handle.await {
            Ok(Ok(subscribed)) => {
                if !subscribed {
                    // Send RR_END response only if not subscribed
                    let end_response = ServerResponse {
                        read: Some(ReadResponse {
                            read_id,
                            result: read_response::Result::RrEnd as i32,
                            ..Default::default()
                        }),
                        ..Default::default()
                    };
                    let _ = sender.send_response(end_response).await;
                } else {
                    // Subscribed - entries will continue to be sent indefinitely, no END message
                    debug!(
                        "Read transitioned to subscription mode (client_id: {}, read_id: {})",
                        sender.client_id(),
                        read_id
                    );
                }
            }
            Ok(Err(err)) => {
                match err {
                    Error::QueueNotFound => {
                        debug!(
                            "Queue not found (client_id: {}, read_id: {}, queue_id: {})",
                            sender.client_id(),
                            read_id,
                            queue_id
                        );
                        let error_response = ServerResponse {
                            read: Some(ReadResponse {
                                read_id,
                                result: read_response::Result::RrQueueNotFound as i32,
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        let _ = sender.send_response(error_response).await;
                    }
                    Error::NotFound => {
                        debug!(
                            "Entry not found (client_id: {}, read_id: {}, queue_id: {})",
                            sender.client_id(),
                            read_id,
                            queue_id
                        );
                        let error_response = ServerResponse {
                            read: Some(ReadResponse {
                                read_id,
                                result: read_response::Result::RrNotFound as i32,
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        let _ = sender.send_response(error_response).await;
                    }
                    Error::ClientDisconnected => {
                        debug!(
                            "Client disconnected during read (client_id: {}, read_id: {})",
                            sender.client_id(),
                            read_id
                        );
                        // No need to send response, client is already gone
                    }
                    Error::QueueEmpty
                    | Error::Wal(_)
                    | Error::Store(_)
                    | Error::Cloud(_)
                    | Error::Io(_) => {
                        error!(
                            "Read stream failed (client_id: {}, read_id: {}, queue_id: {}, error: {:?})",
                            sender.client_id(), read_id, queue_id, err
                        );
                        let error_response = ServerResponse {
                            read: Some(ReadResponse {
                                read_id,
                                result: read_response::Result::RrServerError as i32,
                                ..Default::default()
                            }),
                            ..Default::default()
                        };
                        let _ = sender.send_response(error_response).await;
                    }
                }
            }
            Err(e) => {
                error!(
                    "Read task panicked (client_id: {}, read_id: {}, error: {:?})",
                    sender.client_id(),
                    read_id,
                    e
                );
                let error_response = ServerResponse {
                    read: Some(ReadResponse {
                        read_id,
                        result: read_response::Result::RrServerError as i32,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let _ = sender.send_response(error_response).await;
            }
        }
    }
}
