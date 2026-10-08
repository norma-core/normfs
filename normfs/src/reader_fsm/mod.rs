mod cold;
mod fsm;

#[cfg(test)]
mod cold_test;

pub use fsm::ReaderFSM;
pub use normfs_types::{DataSource, QueueId, ReadEntry};

use crate::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uintn::UintN;

/// Where a file's entries are to be had.
#[derive(Debug)]
pub(crate) enum Fetched {
    /// Decoded from the store or the cloud.
    Decoded(bytes::Bytes, DataSource),
    /// In a WAL file, read from disk as the entries are sent.
    Wal,
    Missing,
    /// A prefetch found every load slot taken.
    Busy,
}

/// Aborted on drop, so a read that ends before the next file stops loading it.
#[derive(Debug)]
pub(crate) struct Prefetch(JoinHandle<Result<Fetched, Error>>);

impl Drop for Prefetch {
    fn drop(&mut self) {
        self.0.abort();
    }
}

type PrefetchHandle = Option<Prefetch>;

/// Common context for read operations
#[derive(Debug)]
pub struct ReadContext {
    pub queue: QueueId,
    pub file_id: UintN,
    pub next_id: UintN, // The entry ID we're looking for in this file
    pub step: u64,
    pub last_id: Option<UintN>,
    pub sender: mpsc::Sender<ReadEntry>,
}

impl ReadContext {
    pub fn new(
        queue: QueueId,
        file_id: UintN,
        next_id: UintN,
        step: u64,
        last_id: Option<UintN>,
        sender: mpsc::Sender<ReadEntry>,
    ) -> Self {
        Self {
            queue,
            file_id,
            next_id,
            step,
            last_id,
            sender,
        }
    }

    pub fn with_file_id(mut self, file_id: UintN) -> Self {
        self.file_id = file_id;
        self
    }

    pub fn with_next_id(mut self, next_id: UintN) -> Self {
        self.next_id = next_id;
        self
    }
}

/// Reader FSM states for managing read operations across different storage backends
#[derive(Debug)]
pub enum ReaderState {
    /// Lookup entry ID with positive offset from beginning
    LookupPositive {
        queue: QueueId,
        offset: UintN,
        limit: u64,
        sender: mpsc::Sender<ReadEntry>,
    },

    /// Lookup entry ID with negative offset from end
    LookupNegative {
        queue: QueueId,
        offset: UintN,
        limit: u64,
        sender: mpsc::Sender<ReadEntry>,
    },

    /// Look up which file contains the requested start_id
    LookupFile {
        queue: QueueId,
        start_id: UintN,
        end_id: Option<UintN>,
        step: u64,
        sender: mpsc::Sender<ReadEntry>,
    },

    /// Intermediate state for debugging - transitions to ReadStore
    ReadFile { ctx: ReadContext },

    /// Try to read from Store first
    ReadStore { ctx: ReadContext },

    /// Try to read from WAL (after Store failed)
    ReadWal { ctx: ReadContext },

    /// Try to read from S3 (after Store and WAL failed)
    ReadS3 { ctx: ReadContext },

    /// Parse WAL bytes and send entries
    ParseWalBytes {
        ctx: ReadContext,
        wal_bytes: bytes::Bytes,
        data_source: DataSource,
        prefetch_handle: PrefetchHandle,
    },

    /// Move to next file in sequence
    ReadNextFile {
        current_file: UintN,
        ctx: ReadContext, // ctx.next_id is the entry we're looking for in next file
        prefetch_handle: PrefetchHandle,
    },

    /// Subscribed to queue (ongoing, never ends normally)
    Subscribed,

    /// Read operation completed successfully
    Completed,

    /// Read operation failed
    Failed(Error),
}

impl ReaderState {
    /// Returns true if this is a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ReaderState::Completed | ReaderState::Failed(_) | ReaderState::Subscribed
        )
    }
}
