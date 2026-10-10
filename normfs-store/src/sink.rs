use normfs_types::events::{EventSink, SystemEvent};
use normfs_types::{DataSource, QueueId};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Instant;
use uintn::UintN;

use crate::backend::{Backend, Body};
use crate::layer::Layer;
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

    /// The id the next file is sealed under, given the one the writer would
    /// use. An error is retried like a failed landing.
    fn file_id<'a>(
        &'a self,
        _queue: &'a QueueId,
        planned: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = io::Result<UintN>> + Send + 'a>> {
        Box::pin(std::future::ready(Ok(planned.clone())))
    }
}

/// Where a queue whose files are kept nowhere local records what it sent to
/// the bucket. The reserve is what keeps a restart off ids already used; the
/// landed file is a hint, since the bucket is asked for file ids either way.
pub trait LandedIndex: Send + Sync {
    fn mark_landed<'a>(
        &'a self,
        queue: &'a QueueId,
        last_entry_id: &'a UintN,
        file_id: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    /// Records, before the upload starts, that ids up to `last_entry_id` may
    /// be in the bucket: a restart that cannot list it starts after them.
    fn reserve<'a>(
        &'a self,
        queue: &'a QueueId,
        last_entry_id: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;
}

/// What follows once a file is safe in the layer it landed in.
pub enum AfterLanding {
    /// A local layer: announced, so the file can move on to the next layer
    /// and be counted against the queue's disk limit.
    Announce(mpsc::UnboundedSender<(QueueId, UintN)>),
    /// No local copy will list it on restart, so the landing is recorded.
    Record(Arc<dyn LandedIndex>),
}

/// The first of a queue's file layers: a sealed file lands there, and from a
/// local layer it is moved on to the next by the offloader.
pub struct LayerSink {
    layer: Arc<Layer>,
    put: Arc<dyn Backend>,
    after: AfterLanding,
    events: EventSink,
}

impl LayerSink {
    pub fn new(layer: Arc<Layer>, after: AfterLanding, events: EventSink) -> Self {
        Self {
            put: layer.backend().clone(),
            layer,
            after,
            events,
        }
    }

    /// The layer's files written through another backend over the same place,
    /// as for a queue whose fsync setting differs from the layer's.
    pub fn with_put(mut self, put: Arc<dyn Backend>) -> Self {
        self.put = put;
        self
    }
}

impl SealedFileSink for LayerSink {
    fn land<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            if let (AfterLanding::Record(index), Some(last)) = (&self.after, file.last_entry_id()) {
                index.reserve(queue, &last).await?;
            }
            let source = self.put.source();
            let started = Instant::now();
            if let Err(e) = self.put.put(queue, file_id, Body::Runs(file.runs())).await {
                if source == DataSource::Cloud {
                    self.events.emit(SystemEvent::UploadFailed {
                        queue: queue.clone(),
                        file_id: file_id.clone(),
                        failure: e.failure(),
                        message: e.to_string(),
                    });
                }
                return Err(e.into());
            }
            let took = started.elapsed();
            if let Some(last) = file.last_entry_id() {
                // Reads consult this before they read the file's header.
                self.layer
                    .record_range(queue, file_id, &file.entries_before, &last);
                if let AfterLanding::Record(index) = &self.after {
                    index.mark_landed(queue, &last, file_id).await?;
                }
            }
            match source {
                DataSource::Cloud => match file.facts(queue, file_id) {
                    // The writer lands a queue's files one at a time and in order.
                    Ok(facts) => self.events.emit(SystemEvent::FileLanded {
                        file: facts,
                        key: self.put.key(queue, file_id),
                        took,
                        landed_through: file_id.clone(),
                    }),
                    Err(e) => log::warn!(target: "normfs-store",
                        "queue {}: file {file_id} landed but its blocks do not parse: {e}", queue.short()),
                },
                _ => store_file::report_stored(self.events.as_ref(), queue, file_id, file),
            }
            // A receiver that has gone away is the instance shutting down; the
            // file is where it was sent either way.
            if let AfterLanding::Announce(tx) = &self.after {
                let _ = tx.send((queue.clone(), file_id.clone()));
            }
            Ok(())
        })
    }
}
