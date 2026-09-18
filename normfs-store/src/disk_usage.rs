use normfs_types::QueueId;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

#[derive(Default)]
pub struct DiskUsage {
    queues: Mutex<HashMap<QueueId, Arc<AsyncMutex<u64>>>>,
}

impl DiskUsage {
    pub fn queue(&self, queue: &QueueId) -> Arc<AsyncMutex<u64>> {
        self.queues
            .lock()
            .unwrap()
            .entry(queue.clone())
            .or_default()
            .clone()
    }

    pub async fn publish(
        &self,
        queue: &QueueId,
        temp_path: &Path,
        store_path: &Path,
        size: u64,
    ) -> io::Result<()> {
        let mut tracked = self.queue(queue).lock_owned().await;
        let temp_path = temp_path.to_path_buf();
        let store_path = store_path.to_path_buf();
        tokio::task::spawn_blocking(move || {
            let old_size = match std::fs::symlink_metadata(&store_path) {
                Ok(metadata) if metadata.is_file() => metadata.len(),
                Ok(_) => 0,
                Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
                Err(e) => return Err(e),
            };
            std::fs::rename(temp_path, store_path)?;
            *tracked = tracked.saturating_sub(old_size).saturating_add(size);
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }
}
