use normfs_store::offloader::QueueOffloader;
use normfs_store::Layer;
use normfs_types::events::EventSink;
use normfs_types::QueueId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uintn::UintN;

/// One offloader per queue that keeps local files and a cloud copy, started
/// with the queue: moving files to the bucket does not wait for a disk limit.
pub(crate) struct Offloaders {
    from: Arc<Layer>,
    to: Arc<Layer>,
    events: EventSink,
    queues: RwLock<HashMap<QueueId, Arc<QueueOffloader>>>,
}

impl Offloaders {
    pub(crate) fn new(from: Arc<Layer>, to: Arc<Layer>, events: EventSink) -> Self {
        Self {
            from,
            to,
            events,
            queues: RwLock::new(HashMap::new()),
        }
    }

    /// A queue started again keeps its offloader and the bound it reached.
    pub(crate) async fn start(&self, queue: &QueueId) -> Arc<QueueOffloader> {
        if let Some(offloader) = self.queues.read().await.get(queue) {
            return offloader.clone();
        }
        let mut queues = self.queues.write().await;
        if let Some(offloader) = queues.get(queue) {
            return offloader.clone();
        }
        let offloader = Arc::new(
            QueueOffloader::new(
                self.from.clone(),
                self.to.clone(),
                queue.clone(),
                self.events.clone(),
            )
            .await,
        );
        queues.insert(queue.clone(), offloader.clone());
        offloader
    }

    pub(crate) async fn file_landed(&self, queue: &QueueId, file_id: UintN) {
        let Some(offloader) = self.queues.read().await.get(queue).cloned() else {
            return;
        };
        if let Err(e) = offloader.enqueue_file(file_id.clone()).await {
            log::error!(target: "normfs",
                "Failed to enqueue file for offload: queue={}, file_id={:?}, error={}",
                queue, file_id, e);
        }
    }
}
