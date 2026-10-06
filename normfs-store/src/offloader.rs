use std::{sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, FileFacts, SystemEvent, UploadFailure};
use tokio::sync::{RwLock, watch};
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
    /// The first and the last file the worker has been told of. Telling it
    /// never waits, so a queue whose next layer is down holds up no other.
    landed: watch::Sender<Option<(UintN, UintN)>>,
    latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
}

impl QueueOffloader {
    pub async fn new(
        from: Arc<Layer>,
        to: Arc<Layer>,
        queue_id: QueueId,
        events: EventSink,
    ) -> Self {
        let (landed, landed_rx) = watch::channel(None);
        let latest_offloaded_id = Arc::new(RwLock::new(None));

        let worker = QueueOffloaderWorker {
            from,
            to,
            queue_id,
            events,
        };
        let worker_latest_id = latest_offloaded_id.clone();
        tokio::spawn(async move {
            Self::offload_worker(worker, landed_rx, worker_latest_id).await;
        });

        Self {
            landed,
            latest_offloaded_id,
        }
    }

    /// The files already in the layer when the worker starts.
    async fn existing(from: &Layer, queue_id: &QueueId) -> Option<(UintN, UintN)> {
        let found = async {
            let Some(min_id) = from.first_file_id(queue_id).await? else {
                return Ok(None);
            };
            let max_id = from.last_file_id(queue_id).await?.unwrap_or(min_id.clone());
            Ok::<_, BackendError>(Some((min_id, max_id)))
        };
        match found.await {
            Ok(Some((min_id, max_id))) => {
                info!("Found store files from {:?} to {:?}", min_id, max_id);
                Some((min_id, max_id))
            }
            Ok(None) => {
                warn!("No store files found in queue {}", queue_id);
                None
            }
            Err(e) => {
                error!("Failed to scan store files of queue {}: {}", queue_id, e);
                None
            }
        }
    }

    /// `file_id` is in the first layer, after every lower id the queue landed.
    pub fn file_landed(&self, file_id: UintN) {
        self.landed
            .send_modify(|range| widen(range, &file_id, &file_id));
    }

    pub async fn get_latest_offloaded_id(&self) -> Option<UintN> {
        self.latest_offloaded_id.read().await.clone()
    }

    async fn offload_worker(
        worker: QueueOffloaderWorker,
        mut landed: watch::Receiver<Option<(UintN, UintN)>>,
        latest_offloaded_id: Arc<RwLock<Option<UintN>>>,
    ) {
        info!("Starting offload worker for queue_id: {}", worker.queue_id);

        let existing = Self::existing(&worker.from, &worker.queue_id).await;
        let mut next: Option<UintN> = None;

        loop {
            let mut range = existing.clone();
            if let Some((first, last)) = landed.borrow_and_update().clone() {
                widen(&mut range, &first, &last);
            }
            let Some((first, last)) = range else {
                if landed.changed().await.is_err() {
                    break;
                }
                continue;
            };
            let file_id = match &next {
                Some(id) if id <= &last => id.clone(),
                Some(_) => {
                    if landed.changed().await.is_err() {
                        break;
                    }
                    continue;
                }
                None => first,
            };
            next = Some(file_id.increment());
            match worker.from.backend().size(&worker.queue_id, &file_id).await {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(e) => {
                    error!(
                        "Failed to check local file {:?}: {}, retrying in 1 second",
                        file_id, e
                    );
                    next = Some(file_id);
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
            }
            let mut attempt: u32 = 0;
            let mut first_put: Option<Instant> = None;
            loop {
                match worker.is_file_offloaded(&file_id).await {
                    Ok(true) => {
                        // A put this worker saw fail can still have landed; its facts
                        // are read first so whoever sees the bound finds the record.
                        let facts = match first_put {
                            Some(_) => worker.local_facts(&file_id).await,
                            None => None,
                        };
                        let landed_through = advance(&latest_offloaded_id, &file_id).await;
                        if let (Some(file), Some(started)) = (facts, first_put) {
                            worker.events.emit(SystemEvent::FileLanded {
                                file,
                                key: worker.to.backend().key(&worker.queue_id, &file_id),
                                took: started.elapsed(),
                                landed_through,
                            });
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

fn widen(range: &mut Option<(UintN, UintN)>, first: &UintN, last: &UintN) {
    match range {
        Some((lo, hi)) => {
            if first < lo {
                *lo = first.clone();
            }
            if last > hi {
                *hi = last.clone();
            }
        }
        None => *range = Some((first.clone(), last.clone())),
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
        let mut body = self
            .from
            .backend()
            .body(&self.queue_id, file_id)
            .await?
            .ok_or_else(|| BackendError::Io(std::io::ErrorKind::NotFound.into()))?;
        // The head comes from the open that is sent, so a file is opened once.
        let head = match &mut body {
            crate::Body::Stream { file, len } => {
                use tokio::io::AsyncReadExt;
                let mut head = vec![0u8; (*len as usize).min(HEAD_LEN)];
                file.read_exact(&mut head).await?;
                file.seek(std::io::SeekFrom::Start(0)).await?;
                bytes::Bytes::from(head)
            }
            crate::Body::Runs(runs) => {
                let mut head = Vec::with_capacity(HEAD_LEN);
                for run in runs.iter() {
                    let take = (HEAD_LEN - head.len()).min(run.len());
                    head.extend_from_slice(&run[..take]);
                    if head.len() == HEAD_LEN {
                        break;
                    }
                }
                bytes::Bytes::from(head)
            }
        };
        Ok((body, head))
    }

    async fn local_facts(&self, file_id: &UintN) -> Option<FileFacts> {
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
            return None;
        };
        store_file::facts_of_head(&self.queue_id, file_id, &head, len)
            .inspect_err(|e| {
                warn!(
                    "Uploaded file {:?} of {} but its blocks do not parse: {}",
                    file_id, self.queue_id, e
                )
            })
            .ok()
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
