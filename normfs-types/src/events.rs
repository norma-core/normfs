use std::sync::Arc;
use std::time::Duration;
use uintn::UintN;

use crate::{CompressionType, EncryptionType, QueueId};

/// What NormFS records about its own work in the system queue.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemEvent {
    /// A store file reached the local store directory.
    FileStored(FileFacts),
    /// A store file is in the bucket and its size was read back.
    FileLanded {
        file: FileFacts,
        key: String,
        took: Duration,
        /// The highest file id below which the queue has nothing left to
        /// upload: the bound the disk monitor deletes local files up to.
        landed_through: UintN,
    },
    UploadFailed {
        queue: QueueId,
        file_id: UintN,
        failure: UploadFailure,
        message: String,
    },
    FileEvicted {
        queue: QueueId,
        kind: FileKind,
        file_id: UintN,
        file_bytes: u64,
        queue_bytes: u64,
        in_cloud: bool,
    },
    /// Over its limit, the queue holds files it may not delete yet.
    EvictionBlocked {
        queue: QueueId,
        reason: EvictionBlock,
        held_at: Option<UintN>,
        queue_bytes: u64,
        limit_bytes: u64,
    },
    /// Appenders are waiting for a page, or have stopped waiting.
    PoolStalled {
        queue: QueueId,
        waits: u64,
        stalled_for: Duration,
        resumed: bool,
    },
    QueueStarted {
        queue: QueueId,
        readonly: bool,
        wal: bool,
        store: bool,
        cloud: bool,
        last_id: Option<UintN>,
    },
    QueueClosed {
        queue: QueueId,
        last_id: Option<UintN>,
    },
}

impl SystemEvent {
    pub fn queue(&self) -> &QueueId {
        match self {
            SystemEvent::FileStored(file) | SystemEvent::FileLanded { file, .. } => &file.queue,
            SystemEvent::UploadFailed { queue, .. }
            | SystemEvent::FileEvicted { queue, .. }
            | SystemEvent::EvictionBlocked { queue, .. }
            | SystemEvent::PoolStalled { queue, .. }
            | SystemEvent::QueueStarted { queue, .. }
            | SystemEvent::QueueClosed { queue, .. } => queue,
        }
    }
}

/// A store file as its header and signature block describe it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileFacts {
    pub queue: QueueId,
    pub file_id: UintN,
    pub first_id: UintN,
    pub num_entries: UintN,
    pub file_bytes: u64,
    /// The WAL bytes before compression; unknown for a file read back from disk.
    pub raw_bytes: Option<u64>,
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    /// Ed25519 over the file body, as in its signature block.
    pub content_signature: [u8; 64],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadFailure {
    /// The request did not complete.
    Network,
    /// The bucket answered with something other than 200.
    Status(u16),
    /// The object was missing after the PUT.
    Missing,
    SizeMismatch {
        local: u64,
        remote: u64,
    },
    /// The local file could not be read.
    LocalRead,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Wal,
    Store,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionBlock {
    /// The next file has not been uploaded yet.
    NotOffloaded,
    /// The next file could not be removed.
    DeleteFailed,
    /// No file was found to delete.
    NothingFound,
}

/// Takes events from anywhere in the write path. `emit` never waits.
pub trait SystemEvents: Send + Sync {
    fn emit(&self, event: SystemEvent);
}

pub type EventSink = Arc<dyn SystemEvents>;

struct Discard;

impl SystemEvents for Discard {
    fn emit(&self, _event: SystemEvent) {}
}

/// A sink for components running without a system queue.
pub fn discard() -> EventSink {
    Arc::new(Discard)
}
