use normfs_store::{SealedFile, SealedFileSink};
use normfs_types::QueueId;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use uintn::UintN;

use crate::downloader::CloudDownloader;
use crate::offloader::put_verified;

/// Where a cloud-direct queue records what has landed, so a restart knows
/// its last id and last file without listing the bucket.
///
/// `mark_landed` returns only once the record is durable: a restart that read
/// a stale one would start the next file at an id the bucket already holds.
pub trait LandedIndex: Send + Sync {
    fn mark_landed(
        &self,
        queue: &QueueId,
        last_entry_id: &UintN,
        file_id: &UintN,
    ) -> io::Result<()>;
}

/// The bucket, directly: a sealed page becomes one object and no local disk
/// is touched. Every attempt is a fresh PUT of the same key, so the caller
/// may retry freely.
pub struct CloudSink {
    downloader: Arc<CloudDownloader>,
    index: Arc<dyn LandedIndex>,
}

impl CloudSink {
    pub fn new(downloader: Arc<CloudDownloader>, index: Arc<dyn LandedIndex>) -> Self {
        Self { downloader, index }
    }
}

impl SealedFileSink for CloudSink {
    fn land<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let key = self.downloader.key(queue, file_id);
            put_verified(self.downloader.client(), &key, &file.to_bytes())
                .await
                .map_err(io::Error::other)?;
            if let Some(last) = file.last_entry_id() {
                // Reads consult this before they range-GET the object.
                self.downloader
                    .record_range(queue, file_id, &file.entries_before, &last)
                    .await
                    .map_err(io::Error::other)?;
                self.index.mark_landed(queue, &last, file_id)?;
            }
            Ok(())
        })
    }
}
