use std::{path::PathBuf, sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_fs::{Fs, Scan, ScanResult};
use normfs_types::QueueId;
use tokio::sync::{RwLock, mpsc};
use uintn::UintN;

use crate::client::S3Client;

const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum OffloadError {
    LocalFileError(String),
    RemoteError(String),
}

impl std::fmt::Display for OffloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffloadError::LocalFileError(msg) => write!(f, "Local file error: {}", msg),
            OffloadError::RemoteError(msg) => write!(f, "Remote error: {}", msg),
        }
    }
}

impl std::error::Error for OffloadError {}

/// Puts `data` at `key` and reads its size back. S3 and its lookalikes are
/// read-after-write consistent for a new key, so the HEAD is the verification
/// and nothing has to wait for it.
pub async fn put_verified(client: &S3Client, key: &str, data: &[u8]) -> Result<(), OffloadError> {
    let status_code = client
        .put_object(key, data)
        .await
        .map_err(|e| OffloadError::RemoteError(format!("S3 put_object failed: {}", e)))?;
    if status_code != 200 {
        return Err(OffloadError::RemoteError(format!(
            "Failed to upload to S3, response code: {}",
            status_code
        )));
    }

    let s3_size = client
        .head_object(key)
        .await
        .map_err(|e| OffloadError::RemoteError(format!("S3 head_object failed: {}", e)))?
        .ok_or_else(|| OffloadError::RemoteError(format!("Not found after upload: {}", key)))?;
    if s3_size != data.len() as u64 {
        return Err(OffloadError::RemoteError(format!(
            "Size mismatch after upload: local={}, s3={}",
            data.len(),
            s3_size
        )));
    }
    Ok(())
}

impl From<std::io::Error> for OffloadError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied => {
                OffloadError::LocalFileError(err.to_string())
            }
            _ => OffloadError::RemoteError(err.to_string()),
        }
    }
}

#[derive(Debug)]
pub struct QueueOffloader {
    offload_sender: mpsc::Sender<UintN>,
    latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
}

impl QueueOffloader {
    pub async fn new(
        fs: Fs,
        queue_id: QueueId,
        root_path: PathBuf,
        client: Arc<S3Client>,
        prefix: &str,
    ) -> Self {
        let queue_path = queue_id.to_store_dir(&root_path);
        let (offload_sender, offload_receiver) = mpsc::channel::<UintN>(1000);
        let latest_offloaded_id = Arc::new(RwLock::new(None));

        let worker_queue_id = queue_id.clone();
        let worker_queue_path = queue_path.clone();
        let worker_latest_id = latest_offloaded_id.clone();

        let prefix = prefix.to_string();

        let worker_fs = fs.clone();
        tokio::spawn(async move {
            Self::offload_worker(
                worker_fs,
                worker_queue_id,
                worker_queue_path,
                client,
                prefix,
                offload_receiver,
                worker_latest_id,
            )
            .await;
        });

        let init_sender = offload_sender.clone();

        let offloader = Self {
            offload_sender,
            latest_offloaded_id,
        };

        // Detach the initialization to avoid blocking
        let init_queue_path = queue_path;
        let init_queue_id = queue_id.clone();
        tokio::spawn(async move {
            log::trace!(
                "Starting background initialization of upload queue for queue_id: {}",
                init_queue_id
            );
            if let Err(e) = Self::initialize_upload_queue_static(
                &fs,
                &init_queue_id,
                &init_queue_path,
                init_sender,
            )
            .await
            {
                error!("Failed to initialize upload queue: {}", e);
            }
        });

        offloader
    }

    async fn initialize_upload_queue_static(
        fs: &Fs,
        queue_id: &QueueId,
        queue_path: &PathBuf,
        offload_sender: mpsc::Sender<UintN>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log::trace!("Initializing upload queue for queue_id: {}", queue_id);

        let min_id = match fs.scan_ids(queue_path, "store", Scan::Min).await? {
            ScanResult::One(id) => id,
            _ => {
                warn!("No store files found in queue {:?}", queue_path);
                return Ok(());
            }
        };

        let max_id = match fs.scan_ids(queue_path, "store", Scan::Max).await? {
            ScanResult::One(id) => id,
            _ => return Ok(()),
        };

        info!("Found store files from {:?} to {:?}", min_id, max_id);

        let mut current_id = min_id;
        let mut enqueued_count = 0;

        loop {
            let local_path = current_id.to_file_path(&queue_path.to_string_lossy(), "store");
            if fs.stat(&local_path).await?.is_some() {
                if let Err(e) = offload_sender.send(current_id.clone()).await {
                    error!("Failed to enqueue file {:?}: {}", current_id, e);
                } else {
                    enqueued_count += 1;
                }
            }

            if current_id == max_id {
                break;
            }

            current_id = current_id.increment();
        }

        info!("Enqueued {} files for upload", enqueued_count);
        Ok(())
    }

    pub async fn enqueue_file(&self, file_id: UintN) -> Result<(), Box<dyn std::error::Error>> {
        self.offload_sender
            .send(file_id)
            .await
            .map_err(|e| format!("Failed to send file ID to upload queue: {}", e).into())
    }

    pub async fn get_latest_offloaded_id(&self) -> Option<UintN> {
        self.latest_offloaded_id.read().await.clone()
    }

    async fn offload_worker(
        fs: Fs,
        queue_id: QueueId,
        queue_path: PathBuf,
        client: Arc<S3Client>,
        prefix: String,
        mut receiver: mpsc::Receiver<UintN>,
        latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
    ) {
        info!("Starting offload worker for queue_id: {}", queue_id);

        while let Some(file_id) = receiver.recv().await {
            let temp_offloader = QueueOffloaderWorker {
                fs: fs.clone(),
                queue_id: queue_id.clone(),
                queue_path: queue_path.clone(),
                client: client.clone(),
                prefix: prefix.clone(),
            };

            loop {
                match temp_offloader.is_file_offloaded(&file_id).await {
                    Ok(true) => {
                        let mut latest = latest_offloaded_id.write().await;
                        match &*latest {
                            None => *latest = Some(file_id.clone()),
                            Some(current) => {
                                if file_id > *current {
                                    *latest = Some(file_id.clone());
                                }
                            }
                        }

                        break;
                    }
                    Ok(false) => {}
                    Err(OffloadError::LocalFileError(e)) => {
                        error!("File {:?} does not exist locally, skipping: {}", file_id, e);
                        break;
                    }
                    Err(OffloadError::RemoteError(e)) => {
                        error!(
                            "Failed to check if file {:?} is offloaded: {}, retrying in 1 second",
                            file_id, e
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                }

                match temp_offloader.upload_file(&file_id).await {
                    Ok(()) => {
                        info!("Successfully uploaded file {:?}", file_id);

                        let mut latest = latest_offloaded_id.write().await;
                        match &*latest {
                            None => *latest = Some(file_id.clone()),
                            Some(current) => {
                                if file_id > *current {
                                    *latest = Some(file_id.clone());
                                }
                            }
                        }

                        break;
                    }
                    Err(OffloadError::LocalFileError(e)) => {
                        error!("Cannot read file {:?} locally, skipping: {}", file_id, e);
                        break;
                    }
                    Err(OffloadError::RemoteError(e)) => {
                        error!(
                            "Failed to upload file {:?}: {}, retrying in 1 second",
                            file_id, e
                        );
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                }
            }
        }

        info!("Offload worker stopped for queue_id: {}", queue_id);
    }
}

struct QueueOffloaderWorker {
    fs: Fs,
    queue_id: QueueId,
    queue_path: PathBuf,
    client: Arc<S3Client>,
    prefix: String,
}

impl QueueOffloaderWorker {
    async fn is_file_offloaded(&self, file_id: &UintN) -> Result<bool, OffloadError> {
        let local_path = file_id.to_file_path(&self.queue_path.to_string_lossy(), "store");

        let local_size = match self.fs.stat(&local_path).await {
            Ok(Some(metadata)) => metadata.len(),
            Ok(None) => {
                return Err(OffloadError::LocalFileError(format!(
                    "Local file does not exist: {:?}",
                    local_path
                )));
            }
            Err(e) => return Err(OffloadError::from(std::io::Error::from(e))),
        };

        let s3_key = self.queue_id.to_cloud_key(&self.prefix, file_id);

        match self.client.head_object(&s3_key).await {
            Ok(Some(s3_size)) => Ok(local_size == s3_size),
            Ok(None) => Ok(false),
            Err(e) => Err(OffloadError::RemoteError(format!(
                "Error checking S3 object {}: {}",
                s3_key, e
            ))),
        }
    }

    async fn upload_file(&self, file_id: &UintN) -> Result<(), OffloadError> {
        let local_path = file_id.to_file_path(&self.queue_path.to_string_lossy(), "store");
        let s3_key = self.queue_id.to_cloud_key(&self.prefix, file_id);

        info!("Uploading file {:?} to S3 key: {}", file_id, s3_key);

        let file_data = self
            .fs
            .read_whole(&local_path)
            .await
            .map_err(|e| OffloadError::from(std::io::Error::from(e)))?;

        put_verified(&self.client, &s3_key, &file_data).await?;

        Ok(())
    }
}
