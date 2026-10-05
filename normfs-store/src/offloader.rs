use std::{sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, FileFacts, SystemEvent, UploadFailure};
use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;
use uintn::UintN;

use crate::backend::BackendError;
use crate::layer::Layer;
use crate::store_file::{self, HEAD_LEN};

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

impl From<BackendError> for OffloadError {
    fn from(e: BackendError) -> Self {
        OffloadError::from(std::io::Error::from(e))
    }
}

/// Moves a queue's files from one of its layers to the next: today the local
/// store to the bucket. A file counts as moved once the next layer holds it
/// at the same size; the bound of moved files is what eviction may delete up
/// to.
#[derive(Debug)]
pub struct QueueOffloader {
    offload_sender: mpsc::Sender<UintN>,
    latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
}

impl QueueOffloader {
    pub async fn new(
        from: Arc<Layer>,
        to: Arc<Layer>,
        queue_id: QueueId,
        events: EventSink,
    ) -> Self {
        let (offload_sender, offload_receiver) = mpsc::channel::<UintN>(1000);
        let latest_offloaded_id = Arc::new(RwLock::new(None));

        let worker = QueueOffloaderWorker {
            from: from.clone(),
            to,
            queue_id: queue_id.clone(),
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
        tokio::spawn(async move {
            log::trace!(
                "Starting background initialization of upload queue for queue_id: {}",
                queue_id
            );
            if let Err(e) = Self::initialize_upload_queue(&from, &queue_id, init_sender).await {
                error!("Failed to initialize upload queue: {}", e);
            }
        });

        offloader
    }

    async fn initialize_upload_queue(
        from: &Layer,
        queue_id: &QueueId,
        offload_sender: mpsc::Sender<UintN>,
    ) -> Result<(), BackendError> {
        log::trace!("Initializing upload queue for queue_id: {}", queue_id);

        let Some(min_id) = from.first_file_id(queue_id).await? else {
            warn!("No store files found in queue {}", queue_id);
            return Ok(());
        };
        let Some(max_id) = from.last_file_id(queue_id).await? else {
            return Ok(());
        };

        info!("Found store files from {:?} to {:?}", min_id, max_id);

        let mut current_id = min_id;
        let mut enqueued_count = 0;

        loop {
            if from.backend().size(queue_id, &current_id).await?.is_some() {
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
            let mut first_put = None;
            loop {
                match worker.is_file_offloaded(&file_id).await {
                    Ok(true) => {
                        let landed_through = advance(&latest_offloaded_id, &file_id).await;
                        // A put this worker saw fail can still have landed.
                        if let Some(started) = first_put {
                            worker
                                .report_landed(&file_id, started, landed_through)
                                .await;
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

                attempt = attempt.saturating_add(1);
                first_put.get_or_insert_with(Instant::now);
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
    from: Arc<Layer>,
    to: Arc<Layer>,
    queue_id: QueueId,
    events: EventSink,
}

impl QueueOffloaderWorker {
    async fn is_file_offloaded(&self, file_id: &UintN) -> Result<bool, OffloadError> {
        let local_size = match self.from.backend().size(&self.queue_id, file_id).await {
            Ok(Some(size)) => size,
            Ok(None) => {
                return Err(OffloadError::LocalFileError(format!(
                    "Local file does not exist: {}",
                    self.from.backend().key(&self.queue_id, file_id)
                )));
            }
            Err(e) => return Err(OffloadError::from(e)),
        };

        match self.to.backend().size(&self.queue_id, file_id).await {
            Ok(Some(remote_size)) => Ok(local_size == remote_size),
            Ok(None) => Ok(false),
            Err(e) => Err(OffloadError::RemoteError(format!(
                "Error checking {}: {}",
                self.to.backend().key(&self.queue_id, file_id),
                e
            ))),
        }
    }

    /// Failures are reported on attempts 1, 2, 4, 8...: the worker retries
    /// every second for as long as the next layer is unreachable.
    async fn upload_file(&self, file_id: &UintN, attempt: u32) -> Result<Uploaded, OffloadError> {
        let report = attempt.is_power_of_two();
        let key = self.to.backend().key(&self.queue_id, file_id);

        info!("Uploading file {:?} to {}", file_id, key);

        let (body, head) = match self.read(file_id).await {
            Ok(read) => read,
            Err(e) => {
                let e = OffloadError::from(e);
                if report {
                    self.report_failure(file_id, UploadFailure::LocalRead, &e);
                }
                return Err(e);
            }
        };
        let len = body.len();

        let started = Instant::now();
        if let Err(e) = self.to.backend().put(&self.queue_id, file_id, body).await {
            if report {
                self.report_failure(file_id, e.failure(), &e);
            }
            return Err(OffloadError::RemoteError(e.to_string()));
        }
        let took = started.elapsed();

        let facts = store_file::facts_of_head(&self.queue_id, file_id, &head, len)
            .inspect_err(|e| {
                warn!(
                    "Uploaded file {:?} of {} but its blocks do not parse: {}",
                    file_id, self.queue_id, e
                )
            })
            .ok();
        Ok(Uploaded { facts, key, took })
    }

    /// The file to send, and its first bytes for the facts.
    async fn read(&self, file_id: &UintN) -> Result<(crate::Body, bytes::Bytes), BackendError> {
        let not_found = || BackendError::Io(std::io::ErrorKind::NotFound.into());
        let from = self.from.backend();
        let head = from
            .get_range(&self.queue_id, file_id, 0, HEAD_LEN as u64)
            .await?
            .ok_or_else(not_found)?;
        let body = from
            .body(&self.queue_id, file_id)
            .await?
            .ok_or_else(not_found)?;
        Ok((body, head))
    }

    async fn report_landed(&self, file_id: &UintN, started: Instant, landed_through: UintN) {
        let from = self.from.backend();
        let head = from
            .get_range(&self.queue_id, file_id, 0, HEAD_LEN as u64)
            .await;
        let len = from.size(&self.queue_id, file_id).await;
        let (Ok(Some(head)), Ok(Some(len))) = (head, len) else {
            warn!(
                "File {:?} of {} landed but cannot be read back for its facts",
                file_id, self.queue_id
            );
            return;
        };
        match store_file::facts_of_head(&self.queue_id, file_id, &head, len) {
            Ok(file) => self.events.emit(SystemEvent::FileLanded {
                file,
                key: self.to.backend().key(&self.queue_id, file_id),
                took: started.elapsed(),
                landed_through,
            }),
            Err(e) => warn!(
                "Uploaded file {:?} of {} but its blocks do not parse: {}",
                file_id, self.queue_id, e
            ),
        }
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
