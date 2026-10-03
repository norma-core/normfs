use normfs_store::{SealedFile, SealedFileSink};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, SystemEvent};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio::time::Instant;
use uintn::UintN;

use crate::downloader::CloudDownloader;
use crate::offloader::put_verified;

/// Where a cloud-direct queue records what has landed, so a restart knows
/// its last id and last file without listing the bucket.
///
/// `mark_landed` returns only once the record is durable: a restart that read
/// a stale one would start the next file at an id the bucket already holds.
pub trait LandedIndex: Send + Sync {
    fn mark_landed<'a>(
        &'a self,
        queue: &'a QueueId,
        last_entry_id: &'a UintN,
        file_id: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// The bucket, directly: a sealed page becomes one object and no local disk
/// is touched. Every attempt is a fresh PUT of the same key, so the caller
/// may retry freely.
pub struct CloudSink {
    downloader: Arc<CloudDownloader>,
    index: Arc<dyn LandedIndex>,
    events: EventSink,
}

impl CloudSink {
    pub fn new(
        downloader: Arc<CloudDownloader>,
        index: Arc<dyn LandedIndex>,
        events: EventSink,
    ) -> Self {
        Self {
            downloader,
            index,
            events,
        }
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
            let started = Instant::now();
            if let Err(e) = put_verified(self.downloader.client(), &key, &file.to_bytes()).await {
                self.events.emit(SystemEvent::UploadFailed {
                    queue: queue.clone(),
                    file_id: file_id.clone(),
                    failure: e.failure(),
                    message: e.to_string(),
                });
                return Err(io::Error::other(e));
            }
            let took = started.elapsed();
            if let Some(last) = file.last_entry_id() {
                // Reads consult this before they range-GET the object.
                self.downloader
                    .record_range(queue, file_id, &file.entries_before, &last)
                    .await
                    .map_err(io::Error::other)?;
                self.index.mark_landed(queue, &last, file_id).await?;
            }
            // The writer lands a queue's files one at a time and in order.
            match file.facts(queue, file_id) {
                Ok(facts) => self.events.emit(SystemEvent::FileLanded {
                    file: facts,
                    key,
                    took,
                    landed_through: file_id.clone(),
                }),
                Err(e) => log::warn!(
                    "queue {queue}: file {file_id} landed but its blocks do not parse: {e}"
                ),
            }
            Ok(())
        })
    }
}
