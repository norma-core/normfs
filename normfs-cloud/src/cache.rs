use crate::client::S3Client;
use crate::errors::CloudError;
use normfs_store::header::StoreHeader;
use normfs_types::QueueId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uintn::UintN;

pub struct RangeCache {
    ranges: RwLock<HashMap<String, (UintN, UintN)>>,
}

impl RangeCache {
    pub fn new() -> Self {
        Self {
            ranges: RwLock::new(HashMap::new()),
        }
    }

    fn key(queue_id: &QueueId, file_id: &UintN) -> String {
        format!("{}-{}", queue_id, file_id)
    }

    async fn read_range_from_s3(
        client: &Arc<S3Client>,
        base_prefix: &str,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, CloudError> {
        // Construct the S3 key for the file
        let key = queue_id.to_cloud_key(base_prefix, file_id);

        log::debug!(
            "Reading range from S3 for queue: {}, file_id: {:?}, key: {}",
            queue_id,
            file_id,
            key
        );

        // Read just the header portion of the file (first 128 bytes should be enough)
        log::debug!(
            "Attempting to read header from S3 key: {} (0-127 bytes)",
            key
        );
        let header_bytes = match client.get_object_range(&key, 0, Some(127)).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => {
                log::debug!(
                    "S3 object not found for queue: {}, file_id: {:?}, key: {}",
                    queue_id,
                    file_id,
                    key
                );
                return Ok(None);
            }
            Err(e) => {
                log::error!(
                    "Error reading S3 object for queue: {}, file_id: {:?}: {:?}",
                    queue_id,
                    file_id,
                    e
                );
                return Err(e);
            }
        };

        // Parse the header to extract the range information

        log::debug!(
            "Read {} bytes from S3 for header parsing (queue: {}, file_id: {:?})",
            header_bytes.len(),
            queue_id,
            file_id
        );

        let (header, _) = match StoreHeader::from_bytes(&header_bytes) {
            Ok(result) => result,
            Err(e) => {
                log::error!(
                    "Error parsing store header for queue: {}, file_id: {:?}: {:?}",
                    queue_id,
                    file_id,
                    e
                );
                return Err(CloudError::StoreHeader(e.into()));
            }
        };

        log::debug!(
            "Read header for queue: {}, file_id: {:?}, entries_before: {:?}, num_entries: {:?}",
            queue_id,
            file_id,
            header.num_entries_before,
            header.num_entries
        );

        if header.num_entries.is_zero() {
            log::debug!(
                "Empty file (zero entries) for queue: {}, file_id: {:?}",
                queue_id,
                file_id
            );
            return Ok(None);
        }

        let first_id = header.num_entries_before;
        let num_entries_minus_one = header
            .num_entries
            .sub(&UintN::one())
            .map_err(CloudError::UintN)?;
        let last_id = first_id.add(&num_entries_minus_one);

        let range = (first_id.clone(), last_id.clone());

        log::debug!(
            "Calculated range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
            queue_id,
            file_id,
            first_id,
            last_id
        );

        Ok(Some(range))
    }

    pub async fn get_file_range(
        &self,
        client: &Arc<S3Client>,
        base_prefix: &str,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, CloudError> {
        let key = Self::key(queue_id, file_id);

        log::debug!(
            "Getting range for queue: {}, file_id: {:?}, cache key: {}",
            queue_id,
            file_id,
            key
        );

        // Check cache first
        {
            let ranges = self.ranges.read().await;
            if let Some(range) = ranges.get(&key) {
                log::debug!(
                    "Found cached range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
                    queue_id,
                    file_id,
                    range.0,
                    range.1
                );
                return Ok(Some(range.clone()));
            }
        }

        log::debug!(
            "Range not in cache, reading from S3 for queue: {}, file_id: {:?}",
            queue_id,
            file_id
        );

        // Read from S3 if not in cache
        let range = Self::read_range_from_s3(client, base_prefix, queue_id, file_id).await?;

        // Cache the result if we found a range
        if let Some(range_val) = &range {
            let mut ranges = self.ranges.write().await;
            ranges.insert(key.clone(), range_val.clone());
            log::debug!(
                "Cached range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
                queue_id,
                file_id,
                range_val.0,
                range_val.1
            );
        }

        Ok(range)
    }

    pub async fn record_range(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        first_id: &UintN,
        last_id: &UintN,
    ) -> Result<(), CloudError> {
        let key = Self::key(queue_id, file_id);

        log::debug!(
            "Recording range for queue: {}, file_id: {:?}, range: {:?} to {:?}, cache key: {}",
            queue_id,
            file_id,
            first_id,
            last_id,
            key
        );

        let mut ranges = self.ranges.write().await;
        ranges.insert(key, (first_id.clone(), last_id.clone()));

        log::debug!(
            "Successfully recorded range for queue: {}, file_id: {:?}",
            queue_id,
            file_id
        );

        Ok(())
    }
}
