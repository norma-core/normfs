use std::{sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, FileFacts, SystemEvent, UploadFailure};
use std::collections::BTreeSet;
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use uintn::UintN;

use crate::backend::{Backend, BackendError, End};
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
/// at the same size. The bound handed to eviction is contiguous: every file
/// up to it is in the next layer, whatever order the files landed in.
#[derive(Debug)]
pub struct QueueOffloader {
    shared: Arc<Shared>,
}

#[derive(Debug, Default)]
struct Shared {
    /// Files landed and not yet moved. Telling the worker never waits, so a
    /// queue whose next layer is down holds up no other.
    pending: Mutex<BTreeSet<UintN>>,
    wake: Notify,
    moved_through: Mutex<Option<UintN>>,
}

/// Rechecks a gap below moved files while nothing else wakes the worker, so a
/// WAL file that goes away without becoming a store file does not hold the
/// bound for good.
const GAP_RECHECK: Duration = Duration::from_secs(5);

impl QueueOffloader {
    /// `wal` is where the files of `from` come from when they are migrated,
    /// if anywhere: an id still there may yet land in `from`.
    pub async fn new(
        from: Arc<Layer>,
        to: Arc<Layer>,
        wal: Option<Arc<dyn Backend>>,
        queue_id: QueueId,
        events: EventSink,
    ) -> Self {
        let shared = Arc::new(Shared::default());
        let worker = QueueOffloaderWorker {
            from,
            to,
            wal,
            queue_id,
            events,
        };
        let worker_shared = shared.clone();
        tokio::spawn(async move {
            Self::offload_worker(worker, worker_shared).await;
        });
        Self { shared }
    }

    /// `file_id` is in the first layer. Files may land in any order.
    pub fn file_landed(&self, file_id: UintN) {
        self.shared.pending.lock().unwrap().insert(file_id);
        self.shared.wake.notify_one();
    }

    pub async fn get_latest_offloaded_id(&self) -> Option<UintN> {
        self.shared.moved_through.lock().unwrap().clone()
    }

    async fn offload_worker(worker: QueueOffloaderWorker, shared: Arc<Shared>) {
        info!("Starting offload worker for queue_id: {}", worker.queue_id);
        worker.queue_existing(&shared).await;
        let mut moved = Moved::default();

        loop {
            let next = shared.pending.lock().unwrap().pop_first();
            let Some(file_id) = next else {
                if moved.has_gap() {
                    let _ = tokio::time::timeout(GAP_RECHECK, shared.wake.notified()).await;
                    moved.advance(&worker, &shared).await;
                } else {
                    shared.wake.notified().await;
                }
                continue;
            };
            let landed = match worker.move_file(&file_id).await {
                Outcome::Moved(landed) => {
                    moved.mark(file_id);
                    landed
                }
                Outcome::Gone => None,
            };
            let through = moved.advance(&worker, &shared).await;
            if let (Some(mut landed), Some(through)) = (landed, through) {
                if let SystemEvent::FileLanded { landed_through, .. } = landed.as_mut() {
                    *landed_through = through;
                }
                worker.events.emit(*landed);
            }
        }
    }
}

/// Files known to be in the next layer above the contiguous bound.
#[derive(Default)]
struct Moved {
    /// The lowest id not yet known to be in the next layer; `None` until the
    /// first move fixes where the queue's files start.
    next: Option<UintN>,
    above: BTreeSet<UintN>,
}

impl Moved {
    fn mark(&mut self, file_id: UintN) {
        if self.next.as_ref().is_none_or(|next| &file_id >= next) {
            self.above.insert(file_id);
        }
    }

    fn has_gap(&self) -> bool {
        !self.above.is_empty()
    }

    /// Raises the bound over moved files and over ids that can no longer
    /// land, and publishes it.
    async fn advance(&mut self, worker: &QueueOffloaderWorker, shared: &Shared) -> Option<UintN> {
        if self.next.is_none() {
            match worker.first_id(shared, &self.above).await {
                Ok(first) => self.next = first,
                Err(e) => warn!(
                    "queue {}: cannot find where its files start ({}); the offloaded \
                     bound waits",
                    worker.queue_id, e
                ),
            }
        }
        if let Some(next) = self.next.as_mut() {
            loop {
                if self.above.remove(next) {
                    *next = next.increment();
                    continue;
                }
                if self.above.range(next.clone()..).next().is_none() {
                    break;
                }
                if worker.may_land(shared, next).await {
                    break;
                }
                *next = next.increment();
            }
        }
        let through = self
            .next
            .as_ref()
            .filter(|next| !next.is_zero())
            .and_then(|next| next.sub(&UintN::one()).ok());
        *shared.moved_through.lock().unwrap() = through.clone();
        through
    }
}

enum Outcome {
    /// In the next layer, with the record to emit when this worker put it.
    Moved(Option<Box<SystemEvent>>),
    /// No longer in the first layer.
    Gone,
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
    wal: Option<Arc<dyn Backend>>,
    queue_id: QueueId,
    events: EventSink,
}

impl QueueOffloaderWorker {
    /// The files already in the layer when the worker starts.
    /// A failed scan is retried: files it would miss stay below the bound and
    /// would hold it for good.
    async fn queue_existing(&self, shared: &Shared) {
        let from = self.from.backend();
        let found = loop {
            match self.scan_existing().await {
                Ok(found) => break found,
                Err(e) => {
                    error!(
                        "Failed to scan store files of queue {}: {}, retrying in 1 second",
                        self.queue_id, e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        };
        let Some((mut id, last)) = found else {
            return;
        };
        info!("Found store files from {:?} to {:?}", id, last);
        loop {
            // A file whose size cannot be read is still tried: skipping it
            // would hold the bound below it for good.
            if !matches!(from.size(&self.queue_id, &id).await, Ok(None)) {
                shared.pending.lock().unwrap().insert(id.clone());
            }
            if id >= last {
                break;
            }
            id = id.increment();
        }
    }

    async fn scan_existing(&self) -> Result<Option<(UintN, UintN)>, BackendError> {
        let from = self.from.backend();
        let Some(first) = from.find(&self.queue_id, End::Min).await? else {
            return Ok(None);
        };
        let last = from
            .find(&self.queue_id, End::Max)
            .await?
            .unwrap_or(first.clone());
        Ok(Some((first, last)))
    }

    /// Where the queue's files start: nothing below the lowest file still in
    /// the WAL, in the first layer, waiting or moved can land any more. The
    /// WAL is looked at first because a migrated file reaches the first layer
    /// before its WAL file is deleted. A failed look decides nothing.
    async fn first_id(
        &self,
        shared: &Shared,
        moved: &BTreeSet<UintN>,
    ) -> Result<Option<UintN>, BackendError> {
        let mut first = moved.first().cloned();
        let mut lower = |id: Option<UintN>| {
            if let Some(id) = id
                && first.as_ref().is_none_or(|f| &id < f)
            {
                first = Some(id);
            }
        };
        if let Some(wal) = &self.wal {
            lower(wal.find(&self.queue_id, End::Min).await?);
        }
        lower(self.from.backend().find(&self.queue_id, End::Min).await?);
        lower(shared.pending.lock().unwrap().first().cloned());
        Ok(first)
    }

    /// Whether `file_id` may still reach the next layer through this worker.
    /// A file lands in the first layer before its WAL file is deleted and WAL
    /// files are made in id order, so an id below a moved one that is in
    /// neither place never will.
    /// A failed look counts as "may".
    async fn may_land(&self, shared: &Shared, file_id: &UintN) -> bool {
        if shared.pending.lock().unwrap().contains(file_id) {
            return true;
        }
        let mut places = vec![self.from.backend()];
        if let Some(wal) = &self.wal {
            places.insert(0, wal);
        }
        for place in places {
            match place.size(&self.queue_id, file_id).await {
                Ok(None) => {}
                Ok(Some(_)) => return true,
                Err(e) => {
                    warn!(
                        "queue {}: cannot tell whether file {:?} may still land ({}); \
                         the offloaded bound waits for it",
                        self.queue_id, file_id, e
                    );
                    return true;
                }
            }
        }
        false
    }

    async fn move_file(&self, file_id: &UintN) -> Outcome {
        let mut attempt: u32 = 0;
        let mut first_put: Option<Instant> = None;
        loop {
            match self.is_file_offloaded(file_id).await {
                Ok(true) => {
                    // A put this worker saw fail can still have landed.
                    let landed =
                        match first_put {
                            Some(started) => self.local_facts(file_id).await.map(|file| {
                                SystemEvent::FileLanded {
                                    file,
                                    key: self.to.backend().key(&self.queue_id, file_id),
                                    took: started.elapsed(),
                                    landed_through: file_id.clone(),
                                }
                            }),
                            None => None,
                        };
                    return Outcome::Moved(landed.map(Box::new));
                }
                Ok(false) => {}
                Err(OffloadError::LocalFileError(e)) => {
                    error!("File {:?} does not exist locally, skipping: {}", file_id, e);
                    return Outcome::Gone;
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
            match self.upload_file(file_id, attempt).await {
                Ok(uploaded) => {
                    info!("Successfully uploaded file {:?}", file_id);
                    let landed = uploaded.facts.map(|file| SystemEvent::FileLanded {
                        file,
                        key: uploaded.key,
                        took: uploaded.took,
                        landed_through: file_id.clone(),
                    });
                    return Outcome::Moved(landed.map(Box::new));
                }
                Err(OffloadError::LocalFileError(e)) => {
                    error!("Cannot read file {:?} locally, skipping: {}", file_id, e);
                    return Outcome::Gone;
                }
                Err(OffloadError::RemoteError(e)) => {
                    error!(
                        "Failed to upload file {:?}: {}, retrying in 1 second",
                        file_id, e
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    }

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
