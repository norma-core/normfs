use bytes::{Bytes, BytesMut};
use normfs_types::events::{
    self, EventSink, EvictionBlock, FileFacts, SystemEvent, SystemEvents, UploadFailure,
};
use normfs_types::{CompressionType, EncryptionType, QueueId};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use uintn::UintN;

use crate::proto::system as pb;
use crate::proto::Id;
use crate::{Persist, Placer, PoolKind, QueueConfig};

/// Where NormFS records its own work, relative to the instance.
pub const SYSTEM_QUEUE: &str = "normfs/system";

/// Events waiting for the writer. Past this they are dropped and counted, so
/// the memory the system queue takes is fixed whatever the write path does.
const BACKLOG: usize = 1024;

/// Error text from a client library can be arbitrarily long.
const MAX_MESSAGE: usize = 512;

/// Lets the writer finish what is queued on close, but not wait out a stall.
const DRAIN_ON_CLOSE: Duration = Duration::from_secs(5);

/// The system queue keeps its files on the local disk whatever the rules say,
/// and sends them to the bucket only from there. A cloud-only system queue
/// would make start-up wait for the bucket, and would lose its own record of
/// an outage during the outage. An instance that keeps nothing on disk keeps
/// this queue in memory too.
pub(crate) fn queue_config(default: &QueueConfig, cloud: bool) -> QueueConfig {
    let persist = if default.persist.is_memory() {
        Persist::MEMORY
    } else {
        Persist {
            wal: false,
            store: true,
            cloud,
        }
    };
    QueueConfig {
        pool: PoolKind::Passive,
        persist,
        ..*default
    }
}

pub(crate) struct Stamped {
    unix_ns: u64,
    event: SystemEvent,
}

/// The emitting side, handed to every component as an [`EventSink`].
pub(crate) struct SystemQueue {
    queue: QueueId,
    started_unix_ns: u64,
    tx: mpsc::Sender<Stamped>,
    dropped: AtomicU64,
}

impl SystemQueue {
    pub(crate) fn new(queue: QueueId) -> (Arc<Self>, mpsc::Receiver<Stamped>) {
        let (tx, rx) = mpsc::channel(BACKLOG);
        let queue = Arc::new(Self {
            queue,
            started_unix_ns: unix_ns(),
            tx,
            dropped: AtomicU64::new(0),
        });
        (queue, rx)
    }

    pub(crate) fn queue(&self) -> &QueueId {
        &self.queue
    }

    pub(crate) fn sink(self: &Arc<Self>) -> EventSink {
        self.clone()
    }

    fn count_dropped(&self, n: u64) {
        self.dropped.fetch_add(n, Ordering::Relaxed);
    }

    /// Takes the drop count into the record. A record that is not written
    /// gives it back, plus one for itself.
    fn encode(&self, stamped: Stamped) -> (Bytes, u64) {
        let dropped_before = self.dropped.swap(0, Ordering::Relaxed);
        let event = pb::Event {
            unix_ns: stamped.unix_ns,
            started_unix_ns: self.started_unix_ns,
            dropped_before,
            kind: Some(kind(stamped.event)),
        };
        (Bytes::from(event.encode_to_vec()), dropped_before)
    }
}

impl SystemEvents for SystemQueue {
    fn emit(&self, event: SystemEvent) {
        // Recording the system queue's own files would feed back into it.
        if event.queue() == &self.queue {
            return;
        }
        let stamped = Stamped {
            unix_ns: unix_ns(),
            event,
        };
        if self.tx.try_send(stamped).is_err() {
            self.count_dropped(1);
        }
    }
}

/// Moves events from the backlog into the queue, one at a time.
pub(crate) struct Writer {
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
    stop: Arc<Notify>,
}

impl Writer {
    pub(crate) fn idle() -> Self {
        Self {
            task: std::sync::Mutex::new(None),
            stop: Arc::new(Notify::new()),
        }
    }

    /// Once the system queue is started: until then there is nowhere to put
    /// an event, and the backlog holds them.
    pub(crate) fn run(
        &self,
        system: Arc<SystemQueue>,
        mut rx: mpsc::Receiver<Stamped>,
        max_record: usize,
        placer: Placer,
    ) {
        let stopped = self.stop.clone();
        let task = tokio::spawn(async move {
            let write = async |stamped: Stamped| {
                let (record, dropped_before) = system.encode(stamped);
                if record.len() > max_record
                    || placer.append(system.queue(), record).await.is_none()
                {
                    system.count_dropped(dropped_before + 1);
                }
            };
            loop {
                tokio::select! {
                    biased;
                    _ = stopped.notified() => {
                        while let Ok(stamped) = rx.try_recv() {
                            write(stamped).await;
                        }
                        return;
                    }
                    stamped = rx.recv() => match stamped {
                        Some(stamped) => write(stamped).await,
                        None => return,
                    },
                }
            }
        });
        *self.task.lock().unwrap() = Some(task);
    }

    pub(crate) async fn stop(&self) {
        let Some(mut task) = self.task.lock().unwrap().take() else {
            return;
        };
        self.stop.notify_one();
        if tokio::time::timeout(DRAIN_ON_CLOSE, &mut task)
            .await
            .is_err()
        {
            log::warn!(target: "normfs",
                "system queue writer did not drain within {DRAIN_ON_CLOSE:?}; the rest is dropped");
            task.abort();
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(task) = self.task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos().min(u64::MAX as u128) as u64)
}

fn id(value: &UintN) -> Id {
    let mut raw = BytesMut::new();
    value.write_value_to_buffer(&mut raw);
    Id { raw: raw.freeze() }
}

fn millis(d: Duration) -> u64 {
    d.as_millis().min(u64::MAX as u128) as u64
}

fn truncated(mut message: String) -> String {
    if message.len() > MAX_MESSAGE {
        let mut end = MAX_MESSAGE;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    message
}

fn compression(c: CompressionType) -> pb::Compression {
    match c {
        CompressionType::None => pb::Compression::CNone,
        CompressionType::Gzip => pb::Compression::CGzip,
        CompressionType::Xz => pb::Compression::CXz,
        CompressionType::Zstd => pb::Compression::CZstd,
    }
}

fn encryption(e: EncryptionType) -> pb::Encryption {
    match e {
        EncryptionType::None => pb::Encryption::ENone,
        EncryptionType::Aes => pb::Encryption::EAes,
    }
}

fn file(f: FileFacts) -> pb::File {
    pb::File {
        queue: f.queue.to_string(),
        file_id: Some(id(&f.file_id)),
        first_id: Some(id(&f.first_id)),
        num_entries: f.num_entries.to_u64().unwrap_or(u64::MAX),
        file_bytes: f.file_bytes,
        raw_bytes: f.raw_bytes.unwrap_or(0),
        compression: compression(f.compression) as i32,
        encryption: encryption(f.encryption) as i32,
        content_signature: Bytes::copy_from_slice(&f.content_signature),
    }
}

fn kind(event: SystemEvent) -> pb::event::Kind {
    use pb::event::Kind;
    match event {
        SystemEvent::FileStored(f) => Kind::FileStored(pb::FileStored {
            file: Some(file(f)),
        }),
        SystemEvent::FileLanded {
            file: f,
            key,
            took,
            landed_through,
        } => Kind::FileLanded(pb::FileLanded {
            file: Some(file(f)),
            key,
            duration_ms: millis(took),
            landed_through: Some(id(&landed_through)),
        }),
        SystemEvent::UploadFailed {
            queue,
            file_id,
            failure,
            message,
        } => {
            use pb::upload_failed::Reason;
            let mut out = pb::UploadFailed {
                queue: queue.to_string(),
                file_id: Some(id(&file_id)),
                message: truncated(message),
                ..Default::default()
            };
            let reason = match failure {
                UploadFailure::Network => Reason::RNetwork,
                UploadFailure::Status(status) => {
                    out.http_status = u32::from(status);
                    Reason::RHttpStatus
                }
                UploadFailure::Missing => Reason::RMissing,
                UploadFailure::SizeMismatch { local, remote } => {
                    out.local_bytes = local;
                    out.remote_bytes = remote;
                    Reason::RSizeMismatch
                }
                UploadFailure::LocalRead => Reason::RLocalRead,
            };
            out.reason = reason as i32;
            Kind::UploadFailed(out)
        }
        SystemEvent::FileEvicted {
            queue,
            kind,
            file_id,
            file_bytes,
            queue_bytes,
            in_cloud,
        } => Kind::FileEvicted(pb::FileEvicted {
            queue: queue.to_string(),
            kind: match kind {
                events::FileKind::Wal => pb::FileKind::FkWal,
                events::FileKind::Store => pb::FileKind::FkStore,
            } as i32,
            file_id: Some(id(&file_id)),
            file_bytes,
            queue_bytes,
            in_cloud,
        }),
        SystemEvent::EvictionBlocked {
            queue,
            reason,
            held_at,
            queue_bytes,
            limit_bytes,
        } => {
            use pb::eviction_blocked::Reason;
            Kind::EvictionBlocked(pb::EvictionBlocked {
                queue: queue.to_string(),
                reason: match reason {
                    EvictionBlock::NotOffloaded => Reason::RNotOffloaded,
                    EvictionBlock::DeleteFailed => Reason::RDeleteFailed,
                    EvictionBlock::NothingFound => Reason::RNothingFound,
                } as i32,
                held_at: held_at.as_ref().map(id),
                queue_bytes,
                limit_bytes,
            })
        }
        SystemEvent::PoolStalled {
            queue,
            waits,
            stalled_for,
            resumed,
        } => Kind::PoolStalled(pb::PoolStalled {
            queue: queue.to_string(),
            waits,
            stalled_for_ms: millis(stalled_for),
            resumed,
        }),
        SystemEvent::QueueStarted {
            queue,
            readonly,
            wal,
            store,
            cloud,
            last_id,
        } => Kind::QueueStarted(pb::QueueStarted {
            queue: queue.to_string(),
            readonly,
            wal,
            store,
            cloud,
            last_id: last_id.as_ref().map(id),
        }),
        SystemEvent::QueueClosed { queue, last_id } => Kind::QueueClosed(pb::QueueClosed {
            queue: queue.to_string(),
            last_id: last_id.as_ref().map(id),
        }),
    }
}

#[cfg(test)]
#[path = "system_test.rs"]
mod system_test;
