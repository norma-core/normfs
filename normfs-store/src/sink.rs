use normfs_types::QueueId;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use uintn::UintN;

use crate::DiskUsage;
use crate::ranges::RangeStore;
use crate::store_file::{self, SealedFile};

/// Where a sealed file goes.
///
/// `Ok` from `land` means the bytes are safe where they were sent: the caller
/// marks them durable and hands their pages back right after. So an
/// implementation returns only after its own fsync or upload verification,
/// never on a queued write.
pub trait SealedFileSink: Send + Sync {
    fn land<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// The local store directory, landed exactly as the WAL migration lands a file.
pub struct LocalStoreSink {
    root: PathBuf,
    range_store: Arc<RangeStore>,
    disk_usage: Arc<DiskUsage>,
    store_done_tx: mpsc::UnboundedSender<(QueueId, UintN)>,
    fsync: bool,
}

impl LocalStoreSink {
    pub(crate) fn new(
        root: PathBuf,
        range_store: Arc<RangeStore>,
        disk_usage: Arc<DiskUsage>,
        store_done_tx: mpsc::UnboundedSender<(QueueId, UintN)>,
        fsync: bool,
    ) -> Self {
        Self {
            root,
            range_store,
            disk_usage,
            store_done_tx,
            fsync,
        }
    }
}

impl SealedFileSink for LocalStoreSink {
    fn land<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            store_file::land_local(
                &self.root,
                queue,
                file_id,
                file,
                self.fsync,
                &self.disk_usage,
            )
            .await?;
            if let Some(last) = file.last_entry_id() {
                self.range_store
                    .record_range(queue, file_id, &file.entries_before, &last)
                    .await
                    .map_err(io::Error::other)?;
            }
            // A receiver that has gone away is the instance shutting down; the
            // file is on disk either way.
            let _ = self.store_done_tx.send((queue.clone(), file_id.clone()));
            Ok(())
        })
    }
}
