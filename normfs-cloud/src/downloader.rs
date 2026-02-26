use std::sync::Arc;

use normfs_types::QueueId;
use uintn::UintN;

use crate::cache::RangeCache;
use crate::client::S3Client;
use crate::errors::CloudError;
use crate::paths;

pub struct CloudDownloader {
    client: Arc<S3Client>,
    base_prefix: String,
    range_cache: Arc<RangeCache>,
}

impl CloudDownloader {
    pub fn new(client: Arc<S3Client>, prefix: &str) -> Self {
        // Normalize the base prefix once during construction
        let base_prefix = if prefix.is_empty() {
            String::new()
        } else if prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{}/", prefix)
        };

        Self {
            client,
            base_prefix,
            range_cache: Arc::new(RangeCache::new()),
        }
    }

    pub async fn find_min_id(&self, queue: &QueueId) -> Result<Option<UintN>, CloudError> {
        let full_prefix = queue.to_cloud_queue_path(&self.base_prefix);

        match paths::find_min_id(&self.client, &full_prefix, "store").await {
            Ok(id) => Ok(Some(id)),
            Err(CloudError::NoFilesFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn get_file_range(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, CloudError> {
        self.range_cache
            .get_file_range(&self.client, &self.base_prefix, queue, file_id)
            .await
    }

    pub async fn record_range(
        &self,
        queue: &QueueId,
        file_id: &UintN,
        first_id: &UintN,
        last_id: &UintN,
    ) -> Result<(), CloudError> {
        self.range_cache
            .record_range(queue, file_id, first_id, last_id)
            .await
    }

    pub async fn get_queue_start(&self, queue: &QueueId) -> Result<Option<UintN>, CloudError> {
        log::debug!("Getting queue start for queue: {}", queue);

        let first_file_id = match self.find_min_id(queue).await? {
            Some(id) => id,
            None => {
                log::debug!("No files found for queue: {}", queue);
                return Ok(None);
            }
        };

        log::debug!("First file ID for queue {}: {:?}", queue, first_file_id);

        match self.get_file_range(queue, &first_file_id).await {
            Ok(Some(range)) => {
                log::debug!("Queue {} starts at entry ID: {:?}", queue, range.0);
                Ok(Some(range.0))
            }
            Ok(None) => {
                log::debug!("No range found for first file in queue {}", queue);
                Ok(None)
            }
            Err(e) => {
                log::error!(
                    "Error getting range for first file in queue {}: {:?}",
                    queue,
                    e
                );
                Err(e)
            }
        }
    }

    /// Get the raw store file bytes from S3.
    /// Returns None if the file doesn't exist (404).
    pub async fn get_store_bytes(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<bytes::Bytes>, CloudError> {
        log::info!(
            "Getting store bytes from S3 for queue: {}, file_id: {:?}",
            queue,
            file_id
        );

        let key = queue.to_cloud_key(&self.base_prefix, file_id);

        match self.client.get_object(&key).await {
            Ok(Some(store_bytes)) => {
                log::debug!(
                    "Downloaded {} bytes from S3 for queue {} file {:?}",
                    store_bytes.len(),
                    queue,
                    file_id
                );
                Ok(Some(store_bytes))
            }
            Ok(None) => {
                log::info!(
                    "Store file not found in S3 for queue {} file {:?}: {}",
                    queue,
                    file_id,
                    key
                );
                Ok(None)
            }
            Err(e) => {
                log::error!(
                    "Error downloading store file from S3 for queue {} file {:?}: {:?}",
                    queue,
                    file_id,
                    e
                );
                Err(e)
            }
        }
    }
}
