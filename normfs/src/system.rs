use bytes::{Bytes, BytesMut};
use normfs_time::Stamp;
use normfs_types::events::{
    self, EventSink, EvictionBlock, FileFacts, SystemEvent, SystemEvents, UploadFailure,
};
use normfs_types::{CompressionType, EncryptionType, QueueId};
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    stamp: Stamp,
    event: SystemEvent,
}

/// The emitting side, handed to every component as an [`EventSink`].
pub(crate) struct SystemQueue {
    queue: QueueId,
    tx: mpsc::Sender<Stamped>,
    dropped: AtomicU64,
}

impl SystemQueue {
    pub(crate) fn new(queue: QueueId) -> (Arc<Self>, mpsc::Receiver<Stamped>) {
        let (tx, rx) = mpsc::channel(BACKLOG);
        let queue = Arc::new(Self {
            queue,
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
            stamp: Some(stamp(stamped.stamp)),
            dropped_before,
            ..event(stamped.event)
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
            stamp: Stamp::now(),
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

fn stamp(s: Stamp) -> crate::proto::Stamp {
    crate::proto::Stamp {
        monotonic_stamp_ns: s.monotonic_stamp_ns,
        local_stamp_ns: s.local_stamp_ns,
        app_start_id: s.app_start_id,
    }
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

fn event(event: SystemEvent) -> pb::Event {
    use pb::EventType as T;
    let mut out = pb::Event {
        queue: event.queue().to_string(),
        ..Default::default()
    };
    let kind = match event {
        SystemEvent::FileStored(f) => {
            out.file = Some(file(f));
            T::EtFileOnDisk
        }
        SystemEvent::FileLanded {
            file: f,
            key,
            took,
            landed_through,
        } => {
            out.file = Some(file(f));
            out.in_cloud = Some(pb::InCloud {
                key,
                upload_ms: millis(took),
                in_cloud_through: Some(id(&landed_through)),
            });
            T::EtFileInCloud
        }
        SystemEvent::UploadFailed {
            file_id,
            failure,
            message,
            ..
        } => {
            let mut failed = pb::UploadFailed {
                file_id: Some(id(&file_id)),
                error: truncated(message),
                ..Default::default()
            };
            let reason = match failure {
                UploadFailure::Network => pb::UploadFailure::UfNetwork,
                UploadFailure::Status(status) => {
                    failed.http_status = u32::from(status);
                    pb::UploadFailure::UfHttpStatus
                }
                UploadFailure::Missing => pb::UploadFailure::UfMissing,
                UploadFailure::SizeMismatch { local, remote } => {
                    failed.local_bytes = local;
                    failed.remote_bytes = remote;
                    pb::UploadFailure::UfSizeMismatch
                }
                UploadFailure::LocalRead => pb::UploadFailure::UfLocalRead,
            };
            failed.reason = reason as i32;
            out.upload_failed = Some(failed);
            T::EtCloudUploadFailed
        }
        SystemEvent::FileEvicted {
            kind,
            file_id,
            file_bytes,
            queue_bytes,
            in_cloud,
            ..
        } => {
            out.disk_file_deleted = Some(pb::DiskFileDeleted {
                kind: match kind {
                    events::FileKind::Wal => pb::FileKind::FkWal,
                    events::FileKind::Store => pb::FileKind::FkStore,
                } as i32,
                file_id: Some(id(&file_id)),
                bytes: file_bytes,
                in_cloud,
                queue_bytes,
            });
            T::EtDiskFileDeleted
        }
        SystemEvent::EvictionBlocked {
            reason,
            held_at,
            queue_bytes,
            limit_bytes,
            ..
        } => {
            out.disk_cleanup_blocked = Some(pb::DiskCleanupBlocked {
                reason: match reason {
                    EvictionBlock::NotOffloaded => pb::CleanupBlock::CbNotInCloud,
                    EvictionBlock::DeleteFailed => pb::CleanupBlock::CbDeleteFailed,
                    EvictionBlock::NothingFound => pb::CleanupBlock::CbNothingToFind,
                } as i32,
                held_at: held_at.as_ref().map(id),
                queue_bytes,
                limit_bytes,
            });
            T::EtDiskCleanupBlocked
        }
        SystemEvent::PoolStalled {
            waits,
            stalled_for,
            resumed,
            ..
        } => {
            out.memory_wait = Some(pb::MemoryWait {
                waits,
                waiting_ms: millis(stalled_for),
                resumed,
            });
            T::EtQueueWaitingForMemory
        }
        SystemEvent::QueueStarted {
            readonly,
            wal,
            store,
            cloud,
            last_id,
            ..
        } => {
            out.queue_state = Some(pb::QueueState {
                last_id: last_id.as_ref().map(id),
                readonly,
                wal,
                store,
                cloud,
            });
            T::EtQueueOpened
        }
        SystemEvent::QueueClosed { last_id, .. } => {
            out.queue_state = Some(pb::QueueState {
                last_id: last_id.as_ref().map(id),
                ..Default::default()
            });
            T::EtQueueClosed
        }
    };
    out.r#type = kind as i32;
    out
}

#[cfg(test)]
#[path = "system_test.rs"]
mod system_test;
