use std::{path::PathBuf, sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_fs::{Fs, Scan, ScanResult};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, FileFacts, SystemEvent, UploadFailure};
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;
use uintn::UintN;

use crate::client::S3Client;
use crate::errors::CloudError;

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

#[derive(Debug)]
pub enum PutError {
    Request(CloudError),
    Status(u16),
    /// HEAD found nothing right after the PUT.
    Missing,
    SizeMismatch {
        local: u64,
        remote: u64,
    },
}

impl PutError {
    pub fn failure(&self) -> UploadFailure {
        match self {
            PutError::Request(CloudError::InvalidStatusCode(code)) | PutError::Status(code) => {
                UploadFailure::Status(*code)
            }
            PutError::Request(_) => UploadFailure::Network,
            PutError::Missing => UploadFailure::Missing,
            PutError::SizeMismatch { local, remote } => UploadFailure::SizeMismatch {
                local: *local,
                remote: *remote,
            },
        }
    }
}

impl std::fmt::Display for PutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PutError::Request(e) => write!(f, "S3 request failed: {}", e),
            PutError::Status(code) => write!(f, "Failed to upload to S3, response code: {}", code),
            PutError::Missing => write!(f, "Not found after upload"),
            PutError::SizeMismatch { local, remote } => write!(
                f,
                "Size mismatch after upload: local={}, s3={}",
                local, remote
            ),
        }
    }
}

impl std::error::Error for PutError {}

impl From<PutError> for OffloadError {
    fn from(e: PutError) -> Self {
        OffloadError::RemoteError(e.to_string())
    }
}

/// Puts `data` at `key` and reads its size back. S3 and its lookalikes are
/// read-after-write consistent for a new key, so the HEAD is the verification
/// and nothing has to wait for it.
pub async fn put_verified(client: &S3Client, key: &str, data: &[u8]) -> Result<(), PutError> {
    let status_code = client
        .put_object(key, data)
        .await
        .map_err(PutError::Request)?;
    if status_code != 200 {
        return Err(PutError::Status(status_code));
    }

    let s3_size = client
        .head_object(key)
        .await
        .map_err(PutError::Request)?
        .ok_or(PutError::Missing)?;
    if s3_size != data.len() as u64 {
        return Err(PutError::SizeMismatch {
            local: data.len() as u64,
            remote: s3_size,
        });
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
        events: EventSink,
    ) -> Self {
        let queue_path = queue_id.to_store_dir(&root_path);
        let (offload_sender, offload_receiver) = mpsc::channel::<UintN>(1000);
        let latest_offloaded_id = Arc::new(RwLock::new(None));

        let worker = QueueOffloaderWorker {
            fs: fs.clone(),
            queue_id: queue_id.clone(),
            queue_path: queue_path.clone(),
            client,
            prefix: prefix.to_string(),
            events,
        };
        let worker_latest_id = latest_offloaded_id.clone();
        tokio::spawn(async move {
            Self::offload_worker(worker, offload_receiver, worker_latest_id).await;
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
        worker: QueueOffloaderWorker,
        mut receiver: mpsc::Receiver<UintN>,
        latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
    ) {
        info!("Starting offload worker for queue_id: {}", worker.queue_id);

        while let Some(file_id) = receiver.recv().await {
            let mut attempt: u32 = 0;
            loop {
                match worker.is_file_offloaded(&file_id).await {
                    Ok(true) => {
                        advance(&latest_offloaded_id, &file_id).await;
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

                attempt = attempt.saturating_add(1);
                match worker.upload_file(&file_id, attempt).await {
                    Ok(uploaded) => {
                        info!("Successfully uploaded file {:?}", file_id);
                        let landed_through = advance(&latest_offloaded_id, &file_id).await;
                        if let Some(file) = uploaded.facts {
                            worker.events.emit(SystemEvent::FileLanded {
                                file,
                                key: uploaded.key,
                                took: uploaded.took,
                                landed_through,
                            });
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

        info!("Offload worker stopped for queue_id: {}", worker.queue_id);
    }
}

/// Raises the offloaded bound to `file_id` and returns the bound.
async fn advance(latest: &RwLock<Option<UintN>>, file_id: &UintN) -> UintN {
    let mut latest = latest.write().await;
    match &*latest {
        Some(current) if current >= file_id => current.clone(),
        _ => {
            *latest = Some(file_id.clone());
            file_id.clone()
        }
    }
}

struct Uploaded {
    /// `None` when the file's own blocks do not parse; it is uploaded anyway.
    facts: Option<FileFacts>,
    key: String,
    took: Duration,
}

struct QueueOffloaderWorker {
    fs: Fs,
    queue_id: QueueId,
    queue_path: PathBuf,
    client: Arc<S3Client>,
    prefix: String,
    events: EventSink,
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

    /// Failures are reported on attempts 1, 2, 4, 8...: the worker retries
    /// every second for as long as the bucket is unreachable.
    async fn upload_file(&self, file_id: &UintN, attempt: u32) -> Result<Uploaded, OffloadError> {
        let report = attempt.is_power_of_two();
        let local_path = file_id.to_file_path(&self.queue_path.to_string_lossy(), "store");
        let s3_key = self.queue_id.to_cloud_key(&self.prefix, file_id);

        info!("Uploading file {:?} to S3 key: {}", file_id, s3_key);

        let file_data = match self.fs.read_whole(&local_path).await {
            Ok(data) => data,
            Err(e) => {
                let e = OffloadError::from(std::io::Error::from(e));
                if report {
                    self.report_failure(file_id, UploadFailure::LocalRead, &e);
                }
                return Err(e);
            }
        };

        let started = Instant::now();
        if let Err(e) = put_verified(&self.client, &s3_key, &file_data).await {
            if report {
                self.report_failure(file_id, e.failure(), &e);
            }
            return Err(e.into());
        }
        let took = started.elapsed();

        let facts = normfs_store::store_file::facts(&self.queue_id, file_id, &file_data)
            .inspect_err(|e| {
                warn!(
                    "Uploaded file {:?} of {} but its blocks do not parse: {}",
                    file_id, self.queue_id, e
                )
            })
            .ok();
        Ok(Uploaded {
            facts,
            key: s3_key,
            took,
        })
    }

    fn report_failure(&self, file_id: &UintN, failure: UploadFailure, e: &dyn std::fmt::Display) {
        self.events.emit(SystemEvent::UploadFailed {
            queue: self.queue_id.clone(),
            file_id: file_id.clone(),
            failure,
            message: e.to_string(),
        });
    }
}
