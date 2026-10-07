use std::{sync::Arc, time::Duration};

use log::{error, info, warn};
use normfs_types::QueueId;
use normfs_types::events::{EventSink, FileFacts, SystemEvent, UploadFailure};
use normfs_wal::WAL_HEADER_V1_MIN_SIZE;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use uintn::UintN;

use crate::backend::{Backend, BackendError, End};
use crate::layer::Layer;
use crate::store_file::{self, HEAD_LEN};

const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum OffloadError {
    /// The file is no longer in the first layer.
    LocalFileError(String),
    /// The file is there but could not be read this time.
    LocalReadError(String),
    RemoteError(String),
}

impl OffloadError {
    fn local(e: BackendError) -> Self {
        let e = std::io::Error::from(e);
        if e.kind() == std::io::ErrorKind::NotFound {
            OffloadError::LocalFileError(e.to_string())
        } else {
            OffloadError::LocalReadError(e.to_string())
        }
    }
}

impl std::fmt::Display for OffloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffloadError::LocalFileError(msg) => write!(f, "Local file error: {}", msg),
            OffloadError::LocalReadError(msg) => write!(f, "Local read error: {}", msg),
            OffloadError::RemoteError(msg) => write!(f, "Remote error: {}", msg),
        }
    }
}

impl std::error::Error for OffloadError {}

/// Moves a queue's files from one of its layers to the next: today the local
/// store to the bucket. A file counts as moved once the next layer holds it
/// at the same size. The bound handed to eviction is contiguous: every file
/// up to it is in the next layer, whatever order the files landed in.
#[derive(Debug)]
pub struct QueueOffloader {
    shared: Arc<Shared>,
    worker: JoinHandle<()>,
}

impl Drop for QueueOffloader {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

#[derive(Debug, Default)]
struct Shared {
    /// Files landed and not yet moved. Telling the worker never waits, so a
    /// queue whose next layer is down holds up no other.
    pending: Mutex<BTreeSet<UintN>>,
    wake: Notify,
    moved_through: Mutex<Option<UintN>>,
}

/// Rechecks a gap below moved files while nothing else wakes the worker: a
/// WAL file removed without becoming a store file is never reported to it.
const GAP_RECHECK: Duration = Duration::from_secs(5);

/// How long one id may hold the bound below moved files before it is logged.
const HELD_WARN: Duration = Duration::from_secs(60);

/// Landed records kept while there is no bound to carry; later ones are dropped.
const MAX_HELD_RECORDS: usize = 1024;

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
        let worker = tokio::spawn(async move {
            Self::offload_worker(worker, worker_shared).await;
        });
        Self { shared, worker }
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
        // Records of files this worker put, held until there is a bound to carry.
        let mut landed = Vec::new();
        let mut dropped: u64 = 0;
        let mut later = BTreeMap::new();
        let mut retry_at = Instant::now();

        loop {
            if !later.is_empty() && Instant::now() >= retry_at {
                shared.pending.lock().unwrap().extend(later.keys().cloned());
            }
            let next = shared.pending.lock().unwrap().pop_first();
            let Some(file_id) = next else {
                if moved.has_gap() || !landed.is_empty() || !later.is_empty() {
                    let until = if later.is_empty() {
                        Instant::now() + GAP_RECHECK
                    } else {
                        retry_at
                    };
                    let _ = tokio::time::timeout_at(until, shared.wake.notified()).await;
                    let through = moved.advance(&worker, &shared).await;
                    worker.emit_landed(&mut landed, through);
                } else {
                    shared.wake.notified().await;
                }
                continue;
            };
            let tried = later.remove(&file_id).unwrap_or_default();
            match worker.move_file(&file_id, tried).await {
                Outcome::Moved(put) => {
                    moved.mark(file_id);
                    if let Some(put) = put {
                        if landed.len() < MAX_HELD_RECORDS {
                            landed.push(*put);
                        } else {
                            dropped += 1;
                            if dropped.is_power_of_two() {
                                warn!(
                                    "queue {}: {} landed records dropped while the offloaded \
                                     bound is unknown",
                                    worker.queue_id, dropped
                                );
                            }
                        }
                    }
                }
                Outcome::Later(tried) => {
                    later.insert(file_id, tried);
                    retry_at = Instant::now() + RETRY_DELAY;
                }
                Outcome::Gone => {}
            }
            let through = moved.advance(&worker, &shared).await;
            worker.emit_landed(&mut landed, through);
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
    held: Option<Held>,
    start_failures: u32,
}

struct Held {
    id: UintN,
    since: Instant,
    logged: bool,
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
                Err(e) => {
                    self.start_failures = self.start_failures.saturating_add(1);
                    if self.start_failures.is_power_of_two() {
                        warn!(
                            "queue {}: cannot find where its files start ({}, {} tries); \
                             the offloaded bound waits",
                            worker.queue_id, e, self.start_failures
                        );
                    }
                }
            }
        }
        if let Some(next) = self.next.as_mut() {
            let mut listed = Listed::NotYet;
            loop {
                if self.above.remove(next) {
                    *next = next.increment();
                    continue;
                }
                let Some(moved) = self.above.range(next.clone()..).next().cloned() else {
                    break;
                };
                if worker.may_land(shared, next).await {
                    break;
                }
                // Jumps to the next id anything is known of, so an empty gap
                // costs one listing rather than a look at every id in it.
                let after = next.increment();
                if let Listed::NotYet = listed {
                    listed = match worker.list_ids().await {
                        Ok(ids) => Listed::Ids(ids),
                        Err(BackendError::Unsupported(_)) => Listed::Unsupported,
                        Err(e) => {
                            warn!(
                                "queue {}: cannot list its files ({}); the offloaded bound \
                                 waits",
                                worker.queue_id, e
                            );
                            break;
                        }
                    };
                }
                let ids = match &listed {
                    Listed::Ids(ids) => ids,
                    _ => {
                        *next = after;
                        continue;
                    }
                };
                let waiting = shared
                    .pending
                    .lock()
                    .unwrap()
                    .range(after.clone()..)
                    .next()
                    .cloned();
                let known = ids.range(after..).next().cloned();
                *next = [waiting, known].into_iter().flatten().fold(moved, Ord::min);
            }
        }
        self.note_held(worker);
        let through = self
            .next
            .as_ref()
            .filter(|next| !next.is_zero())
            .and_then(|next| next.sub(&UintN::one()).ok());
        *shared.moved_through.lock().unwrap() = through.clone();
        through
    }

    fn note_held(&mut self, worker: &QueueOffloaderWorker) {
        let Some(next) = self.next.as_ref().filter(|_| !self.above.is_empty()) else {
            self.held = None;
            return;
        };
        let held = match self.held.take() {
            Some(held) if &held.id == next => held,
            _ => Held {
                id: next.clone(),
                since: Instant::now(),
                logged: false,
            },
        };
        let held = self.held.insert(held);
        if !held.logged && held.since.elapsed() >= HELD_WARN {
            held.logged = true;
            warn!(
                "queue {}: file {:?} has held the offloaded bound for over {:?} with later \
                 files moved; eviction stops below it until it moves or can no longer land",
                worker.queue_id, held.id, HELD_WARN
            );
        }
    }
}

enum Listed {
    NotYet,
    Ids(BTreeSet<UintN>),
    Unsupported,
}

enum Outcome {
    /// In the next layer, with the record to emit when this worker put it.
    Moved(Option<Box<SystemEvent>>),
    /// Still in the first layer but unreadable this time.
    Later(Tried),
    /// No longer in the first layer.
    Gone,
}

/// What a file set aside for a later try carries into it.
#[derive(Default)]
struct Tried {
    attempt: u32,
    first_put: Option<Instant>,
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
        if let (Some(first), Some(last)) = (found.first(), found.last()) {
            info!(
                "Found {} store files from {:?} to {:?}",
                found.len(),
                first,
                last
            );
        }
        shared.pending.lock().unwrap().extend(found);
    }

    async fn scan_existing(&self) -> Result<BTreeSet<UintN>, BackendError> {
        let from = self.from.backend();
        match from.list(&self.queue_id).await {
            Err(BackendError::Unsupported(_)) => {}
            listed => return listed.map(BTreeSet::from_iter),
        }
        let mut found = BTreeSet::new();
        let Some(mut id) = from.find(&self.queue_id, End::Min).await? else {
            return Ok(found);
        };
        let last = from
            .find(&self.queue_id, End::Max)
            .await?
            .unwrap_or(id.clone());
        loop {
            // A file whose size cannot be read is still tried: skipping it
            // would hold the bound below it for good.
            if !matches!(from.size(&self.queue_id, &id).await, Ok(None)) {
                found.insert(id.clone());
            }
            if id >= last {
                return Ok(found);
            }
            id = id.increment();
        }
    }

    /// Where the queue's files start: nothing below the lowest file still in
    /// the WAL, in the first layer, waiting or moved can land any more. The
    /// WAL is looked at first, as in [`Self::may_land`]; a failed look
    /// decides nothing.
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

    /// The files in the WAL and in the first layer, the WAL first as in
    /// [`Self::may_land`].
    async fn list_ids(&self) -> Result<BTreeSet<UintN>, BackendError> {
        let mut ids = BTreeSet::new();
        if let Some(wal) = &self.wal {
            ids.extend(wal.list(&self.queue_id).await?);
        }
        ids.extend(self.from.backend().list(&self.queue_id).await?);
        Ok(ids)
    }

    /// Whether `file_id` may still reach the next layer through this worker.
    /// A file lands in the first layer before its WAL file is deleted, so the
    /// WAL is looked at first; WAL files are made in id order, so an id below
    /// a moved one that is in neither place never will. A failed look counts
    /// as "may".
    async fn may_land(&self, shared: &Shared, file_id: &UintN) -> bool {
        if shared.pending.lock().unwrap().contains(file_id) {
            return true;
        }
        let mut places = vec![(self.from.backend(), 0)];
        if let Some(wal) = &self.wal {
            // Too short for a header, it holds no entry and never migrates.
            places.insert(0, (wal, WAL_HEADER_V1_MIN_SIZE as u64));
        }
        for (place, least) in places {
            match place.size(&self.queue_id, file_id).await {
                Ok(Some(len)) if len >= least => return true,
                Ok(_) => {}
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

    async fn move_file(&self, file_id: &UintN, tried: Tried) -> Outcome {
        let Tried {
            mut attempt,
            mut first_put,
        } = tried;
        loop {
            let failed = match self.is_file_offloaded(file_id).await {
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
                Ok(false) => {
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
                        Err(e) => e,
                    }
                }
                Err(e @ OffloadError::LocalReadError(_)) => {
                    attempt = attempt.saturating_add(1);
                    e
                }
                Err(e) => e,
            };
            match failed {
                OffloadError::LocalFileError(e) => {
                    error!("File {:?} does not exist locally, skipping: {}", file_id, e);
                    return Outcome::Gone;
                }
                OffloadError::LocalReadError(e) => {
                    if attempt.is_power_of_two() {
                        error!(
                            "Cannot read file {:?} locally, trying it later: {}",
                            file_id, e
                        );
                    }
                    return Outcome::Later(Tried { attempt, first_put });
                }
                OffloadError::RemoteError(e) => {
                    error!(
                        "Failed to move file {:?}: {}, retrying in 1 second",
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
            Err(e) => return Err(OffloadError::local(e)),
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
                let e = OffloadError::local(e);
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

    fn emit_landed(&self, landed: &mut Vec<SystemEvent>, through: Option<UintN>) {
        let Some(through) = through else {
            return;
        };
        for mut event in landed.drain(..) {
            if let SystemEvent::FileLanded { landed_through, .. } = &mut event {
                *landed_through = through.clone();
            }
            self.events.emit(event);
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
