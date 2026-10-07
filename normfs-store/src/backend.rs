use normfs_fs::{Fs, PublishSpec};
use normfs_types::QueueId;
use std::path::Path;
use std::sync::Arc;

use crate::DiskUsage;

pub use normfs_wal::PackSlot;
pub use normfs_wal::backend::{
    AppendTarget, Appended, Backend, BackendError, BackendFuture, Body, End, FileRead, Fill,
    Layout, Local, Publisher, Reader,
};

impl Publisher for DiskUsage {
    fn publish<'a>(
        &'a self,
        fs: &'a Fs,
        queue: &'a QueueId,
        spec: PublishSpec,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move { Ok(DiskUsage::publish(self, fs, queue, spec).await?) })
    }
}

/// The local store directory: puts synced when `fsync` says so and counted
/// against the queue's disk usage.
pub fn local_store(fs: Fs, root: impl AsRef<Path>, fsync: bool, usage: Arc<DiskUsage>) -> Local {
    Local::store(fs, root, fsync, usage)
}
