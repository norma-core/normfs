use super::cold::{Cold, ColdFiles};
use super::{DataSource, Fetched, Prefetch, PrefetchHandle, ReadContext, ReadEntry, ReaderState};
use crate::{mem::MemStore, Error};
use normfs_store::{Layer, PersistStore, StoreError};
use normfs_types::{QueueId, ReadPosition};
use normfs_wal::{PausableRead, ReadRangeResult, WalError, WalStore};
use std::sync::Arc;
use tokio::sync::mpsc;
use uintn::UintN;

/// A file id that was never written answers "not there" however often it is
/// asked; a file being archived answers that only until the rename lands. The
/// wait is sized to that rename and to nothing longer.
const MIGRATING_FILE_RETRIES: u32 = 20;
const MIGRATING_FILE_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(25);

enum LoadError {
    Read(Error),
    Corrupt(StoreError),
}

/// Reader FSM that manages read operations across different storage backends
#[derive(Clone)]
pub struct ReaderFSM {
    pub(crate) wal: Arc<WalStore>,
    pub(crate) store: Arc<PersistStore>,
    pub(crate) mem: Arc<MemStore>,
    pub(crate) cloud: Option<Arc<Layer>>,
    queue_settings: Arc<crate::QueueSettings>,
    pointers: Arc<crate::memory_pointers::MemoryPointers>,
    cold: Arc<ColdFiles>,
}

impl ReaderFSM {
    pub fn new(
        wal: Arc<WalStore>,
        store: Arc<PersistStore>,
        mem: Arc<MemStore>,
        cloud: Option<Arc<Layer>>,
        queue_settings: Arc<crate::QueueSettings>,
        pointers: Arc<crate::memory_pointers::MemoryPointers>,
        cold_read_files: usize,
    ) -> Self {
        Self {
            wal,
            store,
            mem,
            cloud,
            queue_settings,
            pointers,
            cold: Arc::new(ColdFiles::new(cold_read_files)),
        }
    }

    /// The last file any cloud-direct life of this queue landed. It bounds a
    /// file walk alongside the last local file: a queue moved from store to
    /// cloud-direct has both, and the landed ones come after.
    fn cloud_last(&self, queue: &QueueId) -> Option<UintN> {
        self.pointers.last_landed(queue).map(|(_, file)| file)
    }

    /// A memory queue has no files, and a read must not go looking: its
    /// restart contract rests on the directory being absent.
    fn is_memory(&self, queue: &QueueId) -> bool {
        self.queue_settings
            .get_config(&queue.to_string())
            .persist
            .is_memory()
    }

    fn storage_miss(&self, queue: &QueueId) -> ReaderState {
        if self.mem.get_last_id(queue).is_none() {
            ReaderState::Failed(Error::QueueNotFound)
        } else {
            ReaderState::Failed(Error::NotFound)
        }
    }

    pub async fn read(
        &self,
        queue: QueueId,
        position: ReadPosition,
        limit: u64,
        step: u64,
        sender: mpsc::Sender<ReadEntry>,
    ) -> Result<bool, Error> {
        if sender.is_closed() {
            return Err(Error::ClientDisconnected);
        }

        let mut state = match position {
            ReadPosition::Absolute(offset) => ReaderState::LookupPositive {
                queue,
                offset,
                limit,
                sender,
            },
            ReadPosition::ShiftFromTail(offset) => ReaderState::LookupNegative {
                queue,
                offset,
                limit,
                sender,
            },
        };

        while !state.is_terminal() {
            state = self.transition(state, step).await?;
        }

        match state {
            ReaderState::Completed => Ok(false),
            ReaderState::Subscribed => Ok(true),
            ReaderState::Failed(err) => Err(err),
            _ => unreachable!(),
        }
    }

    /// Execute a state transition
    async fn transition(&self, state: ReaderState, step: u64) -> Result<ReaderState, Error> {
        match state {
            ReaderState::LookupPositive {
                queue,
                offset,
                limit,
                sender,
            } => {
                self.handle_lookup_positive(queue, offset, limit, step, sender)
                    .await
            }
            ReaderState::LookupNegative {
                queue,
                offset,
                limit,
                sender,
            } => {
                self.handle_lookup_negative(queue, offset, limit, step, sender)
                    .await
            }
            ReaderState::LookupFile {
                queue,
                start_id,
                end_id,
                step,
                sender,
            } => {
                self.handle_lookup_file(queue, start_id, end_id, step, sender)
                    .await
            }
            ReaderState::ReadFile { ctx } => self.handle_read_file(ctx).await,
            ReaderState::ReadStore { ctx } => self.handle_read_store(ctx).await,
            ReaderState::ReadWal { ctx } => self.handle_read_wal(ctx).await,
            ReaderState::ReadS3 { ctx } => self.handle_read_s3(ctx).await,
            ReaderState::ParseWalBytes {
                ctx,
                wal_bytes,
                data_source,
                prefetch_handle,
            } => {
                self.handle_parse_wal_bytes(ctx, wal_bytes, data_source, prefetch_handle)
                    .await
            }
            ReaderState::ReadNextFile {
                current_file,
                ctx,
                prefetch_handle,
            } => {
                self.handle_read_next_file(current_file, ctx, prefetch_handle)
                    .await
            }
            _ => unreachable!("Terminal states should not be passed to transition()"),
        }
    }

    /// Prefetch and prepare WAL bytes for the next file
    /// Checks file range first to see if it contains the entry we need
    /// Tries Store → WAL → S3 and returns ready-to-parse WAL bytes
    /// The file's bytes, waiting out a migration if that is what "not there"
    /// means.
    ///
    /// The backends are asked in turn -- store, then WAL -- and archiving
    /// renames the store copy into place before deleting the WAL one, so no
    /// instant exists in which a file is in neither. The two questions are
    /// asked at two instants, though, and a file that migrates between them
    /// answers no to both. Walking past that answer drops every record the
    /// file holds, silently: the walk never returns to those ids and the
    /// caller is told the read completed.
    async fn file_bytes_or_wait(&self, queue: &QueueId, file_id: &UintN) -> Fetched {
        for attempt in 1..=MIGRATING_FILE_RETRIES {
            tokio::time::sleep(MIGRATING_FILE_RETRY_DELAY).await;
            match self
                .clone()
                .prefetch_next_file(queue.clone(), file_id.clone(), true)
                .await
            {
                Ok(Fetched::Missing) | Ok(Fetched::Busy) => {}
                Ok(found) => {
                    log::info!(target: "normfs-reader-fsm",
                        "Queue '{}' - file {} was unreadable for {} attempt(s) and then \
                         appeared; it was migrating between backends, not missing",
                        queue.short(), file_id, attempt);
                    return found;
                }
                Err(e) => {
                    log::warn!(target: "normfs-reader-fsm",
                        "Queue '{}' - error re-reading file {}: {:?}", queue.short(), file_id, e);
                    return Fetched::Missing;
                }
            }
        }
        Fetched::Missing
    }

    /// A store or cloud file, decoded. Never `Busy` with `wait`.
    async fn decoded(
        &self,
        queue: &QueueId,
        file_id: &UintN,
        source: DataSource,
        wait: bool,
    ) -> Result<Cold, LoadError> {
        let cloud = match source {
            DataSource::Cloud => match &self.cloud {
                Some(cloud) => Some(cloud),
                None => return Ok(Cold::Missing),
            },
            _ => None,
        };
        let key = (queue.clone(), file_id.clone(), cloud.is_some());
        self.cold
            .get(key, wait, || async {
                let store_bytes = match cloud {
                    Some(cloud) => cloud
                        .get_store_bytes(queue, file_id)
                        .await
                        .map_err(|e| LoadError::Read(Error::Cloud(e.into())))?,
                    None => self
                        .store
                        .get_store_bytes(queue, file_id)
                        .await
                        .map_err(|e| LoadError::Read(Error::Store(e)))?,
                };
                let Some(store_bytes) = store_bytes else {
                    return Ok(None);
                };
                log::trace!(target: "normfs-reader-fsm",
                    "Read {} bytes of file {} from {:?}", store_bytes.len(), file_id, source);
                let verify = cloud.is_some_and(|cloud| cloud.verifies_bodies());
                self.store
                    .extract_wal_bytes(queue, file_id, store_bytes, verify)
                    .map(Some)
                    .map_err(LoadError::Corrupt)
            })
            .await
    }

    /// Without `wait` a file that would need a load slot none is free for
    /// comes back `Busy`, and the read loads it itself when it gets there.
    async fn prefetch_next_file(
        self,
        queue: QueueId,
        file_id: UintN,
        wait: bool,
    ) -> Result<Fetched, Error> {
        log::trace!(target: "normfs-reader-fsm",
            "Prefetching file: queue={}, file_id={}",
            queue.short(), file_id);

        if let Some(found) = self
            .prefetch_decoded(&queue, &file_id, DataSource::DiskStore, wait)
            .await?
        {
            return Ok(found);
        }

        match self.wal.get_entries_before(&queue, &file_id).await {
            Ok(_) | Err(WalError::WalEmpty(_)) => return Ok(Fetched::Wal),
            Err(WalError::WalNotFound) => {}
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "Prefetch: WAL error: queue={}, file_id={}, error={:?}",
                    queue.short(), file_id, e);
                return Err(Error::Wal(e));
            }
        }

        if let Some(found) = self
            .prefetch_decoded(&queue, &file_id, DataSource::Cloud, wait)
            .await?
        {
            return Ok(found);
        }

        log::debug!(target: "normfs-reader-fsm",
            "Prefetch: file not found in any backend: queue={}, file_id={}",
            queue.short(), file_id);
        Ok(Fetched::Missing)
    }

    async fn prefetch_decoded(
        &self,
        queue: &QueueId,
        file_id: &UintN,
        source: DataSource,
        wait: bool,
    ) -> Result<Option<Fetched>, Error> {
        match self.decoded(queue, file_id, source, wait).await {
            Ok(Cold::Found(wal_bytes)) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Prefetch successful from {:?}: queue={}, file_id={}, wal_bytes={}",
                    source, queue.short(), file_id, wal_bytes.len());
                Ok(Some(Fetched::Decoded(wal_bytes, source)))
            }
            Ok(Cold::Busy) => Ok(Some(Fetched::Busy)),
            Ok(Cold::Missing) => Ok(None),
            Err(LoadError::Corrupt(e)) => {
                log::warn!(target: "normfs-reader-fsm",
                    "Prefetch: corrupted {:?} file, treating as not found: queue={}, file_id={}, error={:?}",
                    source, queue.short(), file_id, e);
                Ok(Some(Fetched::Missing))
            }
            Err(LoadError::Read(e)) => {
                log::error!(target: "normfs-reader-fsm",
                    "Prefetch: {:?} error: queue={}, file_id={}, error={:?}",
                    source, queue.short(), file_id, e);
                Err(e)
            }
        }
    }

    /// The state that reads a file found by a retry or a prefetch, or skips it.
    fn read_found(ctx: ReadContext, found: Fetched) -> ReaderState {
        match found {
            Fetched::Decoded(wal_bytes, data_source) => ReaderState::ParseWalBytes {
                ctx,
                wal_bytes,
                data_source,
                prefetch_handle: None,
            },
            Fetched::Wal => ReaderState::ReadWal { ctx },
            Fetched::Missing | Fetched::Busy => {
                log::debug!(target: "normfs-reader-fsm",
                    "File not found in any backend, moving to next file: queue={}, file_id={}",
                    ctx.queue.short(), ctx.file_id);
                ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                }
            }
        }
    }

    async fn handle_lookup_positive(
        &self,
        queue: QueueId,
        offset: UintN,
        limit: u64,
        step: u64,
        sender: mpsc::Sender<ReadEntry>,
    ) -> Result<ReaderState, Error> {
        let start_id = offset;

        // Check if sender is still open
        if sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        // Calculate end_id: None means unlimited (subscribe mode), Some means limited read
        let end_id = if limit > 0 {
            Some(start_id.add(&UintN::from((limit - 1) * step)))
        } else if self.mem.is_closed(&queue) {
            // A follow on a closed queue is a bounded read: nothing arrives
            // past the last record. The bound comes from the closed record,
            // not the map, which close has already emptied.
            match self.mem.closed_last_id(&queue) {
                Some(last) if start_id <= last => Some(last),
                // Closed and nothing at or past the start: the reader already
                // has everything.
                _ => return Ok(ReaderState::Completed),
            }
        } else {
            None
        };

        // If we have an end_id (limited read), try to read from memory first
        if let Some(ref end_id_val) = end_id {
            let result = self
                .mem
                .read_full(
                    &queue,
                    start_id.clone(),
                    end_id_val.clone(),
                    step as usize,
                    &sender,
                )
                .await;

            if result.success {
                return Ok(ReaderState::Completed);
            }
        } else {
            // No end_id means subscribe mode - try to follow from memory
            let result = self
                .mem
                .follow_full(&queue, &start_id, start_id.clone(), step as usize, &sender)
                .await;

            if result.success && result.subscription_id.is_some() {
                return Ok(ReaderState::Subscribed);
            }
        }

        if self.is_memory(&queue) {
            return Ok(self.storage_miss(&queue));
        }

        // Not in memory or memory is empty, transition to file lookup
        Ok(ReaderState::LookupFile {
            queue,
            start_id,
            end_id,
            step,
            sender,
        })
    }

    async fn handle_lookup_negative(
        &self,
        queue: QueueId,
        offset: UintN,
        limit: u64,
        step: u64,
        sender: mpsc::Sender<ReadEntry>,
    ) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        // Check if we have a limit
        if limit > 0 {
            let result = self
                .mem
                .read_full_negative(&queue, offset.clone(), step as usize, limit, &sender)
                .await;

            if result.success {
                return Ok(ReaderState::Completed);
            }

            let start_id = result.start_id.ok_or(Error::QueueNotFound)?;
            let end_id = Some(start_id.add(&UintN::from((limit - 1) * step)));

            if self.is_memory(&queue) {
                return Ok(self.storage_miss(&queue));
            }

            Ok(ReaderState::LookupFile {
                queue,
                start_id,
                end_id,
                step,
                sender,
            })
        } else {
            if self.mem.is_closed(&queue) {
                // Same conversion as the positive lookup. Offset zero keeps
                // the live semantics (subscribe from the *next* record), and
                // a closed queue's next record does not exist.
                if offset == UintN::zero() {
                    return Ok(ReaderState::Completed);
                }

                return match self.mem.closed_last_id(&queue) {
                    Some(last) => {
                        let start_id = if offset > last {
                            UintN::zero()
                        } else {
                            last.sub(&offset).unwrap_or(UintN::zero())
                        };
                        Ok(ReaderState::LookupFile {
                            queue,
                            start_id,
                            end_id: Some(last),
                            step,
                            sender,
                        })
                    }
                    None => Ok(ReaderState::Completed),
                };
            }

            let result = self
                .mem
                .follow_full_negative(&queue, offset.clone(), step as usize, &sender)
                .await;

            if result.success {
                if let Some(_sub_id) = result.subscription_id {
                    return Ok(ReaderState::Subscribed);
                }
            }

            let start_id = result.start_id.ok_or(Error::QueueNotFound)?;

            if self.is_memory(&queue) {
                return Ok(self.storage_miss(&queue));
            }

            Ok(ReaderState::LookupFile {
                queue,
                start_id,
                end_id: None,
                step,
                sender,
            })
        }
    }

    async fn handle_lookup_file(
        &self,
        queue: QueueId,
        start_id: UintN,
        end_id: Option<UintN>,
        step: u64,
        sender: mpsc::Sender<ReadEntry>,
    ) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        if self.is_memory(&queue) {
            return Ok(self.storage_miss(&queue));
        }
        let store = &self.store;
        let wal = &self.wal;

        let file_id = match crate::lookup::find_file_with_s3(
            &queue,
            &start_id,
            store,
            wal,
            self.cloud.as_ref(),
            self.cloud_last(&queue),
        )
        .await
        {
            Ok(Some(id)) => id,
            Ok(None) => {
                // No file found for this ID - queue might not exist or ID is out of range
                return Ok(ReaderState::Failed(Error::QueueNotFound));
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "Lookup failed for queue '{}', id {}: {:?}",
                    queue.short(), start_id, e);
                let error = match e {
                    crate::lookup::LookupError::Cloud(e) => Error::Cloud(e),
                    _ => Error::NotFound,
                };
                return Ok(ReaderState::Failed(error));
            }
        };

        // Transition to ReadFile state - the storage backend will be determined during read
        Ok(ReaderState::ReadFile {
            ctx: ReadContext::new(queue, file_id, start_id, step, end_id, sender),
        })
    }

    async fn handle_read_file(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        // Intermediate debugging state - transition directly to ReadStore
        Ok(ReaderState::ReadStore { ctx })
    }

    async fn handle_read_store(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Attempting to read from Store: queue={}, file_id={}, next_id={}, last_id={:?}",
            ctx.queue.short(), ctx.file_id, ctx.next_id, ctx.last_id);

        match self
            .decoded(&ctx.queue, &ctx.file_id, DataSource::DiskStore, true)
            .await
        {
            Ok(Cold::Found(wal_bytes)) => Ok(ReaderState::ParseWalBytes {
                ctx,
                wal_bytes,
                data_source: DataSource::DiskStore,
                prefetch_handle: None,
            }),
            Ok(Cold::Missing) | Ok(Cold::Busy) => {
                log::trace!(target: "normfs-reader-fsm",
                    "File not found in Store, trying WAL: queue={}, file_id={}",
                    ctx.queue.short(), ctx.file_id);
                Ok(ReaderState::ReadWal { ctx })
            }
            Err(e) => Ok(self.load_failed(ctx, e, DataSource::DiskStore)),
        }
    }

    async fn handle_read_wal(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Attempting to read from WAL: queue={}, file_id={}, next_id={}, last_id={:?}",
            ctx.queue.short(), ctx.file_id, ctx.next_id, ctx.last_id);

        let wal = &self.wal;

        // Read straight from the file: only the entries sent are ever in memory.
        let result = match wal.get_entries_before(&ctx.queue, &ctx.file_id).await {
            Ok(before) => {
                self.warn_gap(&ctx, &before);
                wal.read_wal_range(
                    &ctx.queue,
                    &ctx.file_id,
                    &ctx.next_id,
                    &ctx.last_id,
                    ctx.step as usize,
                    &ctx.sender,
                    DataSource::DiskWal,
                )
                .await
            }
            Err(WalError::WalEmpty(_)) => Ok(ReadRangeResult::PartialRead {
                last_read_id: None,
                last_id_in_file: ctx.next_id.clone(),
            }),
            Err(e) => Err(e),
        };

        match result {
            Err(WalError::WalNotFound) => {
                log::trace!(target: "normfs-reader-fsm",
                    "File not found in WAL: queue={}, file_id={}",
                    ctx.queue.short(), ctx.file_id);
                // Not in WAL, try S3 if available, otherwise move to next file
                if self.cloud.is_some() {
                    log::trace!(target: "normfs-reader-fsm",
                        "Trying S3: queue={}, file_id={}",
                        ctx.queue.short(), ctx.file_id);
                    Ok(ReaderState::ReadS3 { ctx })
                } else {
                    let found = self.file_bytes_or_wait(&ctx.queue, &ctx.file_id).await;
                    Ok(Self::read_found(ctx, found))
                }
            }
            Err(WalError::IoError(e)) => {
                log::error!(target: "normfs-reader-fsm",
                    "WAL read error: queue={}, file_id={}, error={:?}",
                    ctx.queue.short(), ctx.file_id, e);
                Ok(ReaderState::Failed(Error::Wal(WalError::IoError(e))))
            }
            result => Ok(self.after_range(ctx, result, DataSource::DiskWal, None)),
        }
    }

    async fn handle_read_s3(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        if self.cloud.is_none() {
            log::error!(target: "normfs-reader-fsm",
                "S3 state reached but S3 downloader not configured: queue={}, file_id={}",
                ctx.queue.short(), ctx.file_id);
            return Ok(ReaderState::Failed(Error::NotFound));
        }

        log::info!(target: "normfs-reader-fsm",
            "Attempting to read from S3: queue={}, file_id={}, next_id={}, last_id={:?}",
            ctx.queue.short(), ctx.file_id, ctx.next_id, ctx.last_id);

        match self
            .decoded(&ctx.queue, &ctx.file_id, DataSource::Cloud, true)
            .await
        {
            Ok(Cold::Found(wal_bytes)) => Ok(ReaderState::ParseWalBytes {
                ctx,
                wal_bytes,
                data_source: DataSource::Cloud,
                prefetch_handle: None,
            }),
            Ok(Cold::Missing) | Ok(Cold::Busy) => {
                log::info!(target: "normfs-reader-fsm",
                    "File not found in S3 (404), moving to next file: queue={}, file_id={}",
                    ctx.queue.short(), ctx.file_id);
                // S3 file not found (404), move to next file
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                })
            }
            Err(e) => Ok(self.load_failed(ctx, e, DataSource::Cloud)),
        }
    }

    fn load_failed(&self, ctx: ReadContext, e: LoadError, data_source: DataSource) -> ReaderState {
        match e {
            LoadError::Read(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "{:?} read error: queue={}, file_id={}, error={:?}",
                    data_source, ctx.queue.short(), ctx.file_id, e);
                ReaderState::Failed(e)
            }
            LoadError::Corrupt(e) => {
                log::warn!(target: "normfs-reader-fsm",
                    "Failed to extract WAL bytes from corrupted store file, moving to next file: queue={}, file_id={}, error={:?}, source={:?}",
                    ctx.queue.short(), ctx.file_id, e, data_source);
                // Extraction errors (decryption/decompression failures) indicate data corruption
                // Cannot restore corrupted data, so skip to next file
                ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                }
            }
        }
    }

    /// Every entry at or above the id asked for is delivered, so a file
    /// that begins above it reads as a valid answer and the walk moves on.
    /// The records in between are never requested again and the caller
    /// sees a complete read; the gap is real either way, but it is not
    /// something to pass over in silence.
    fn warn_gap(&self, ctx: &ReadContext, num_entries_before: &UintN) {
        if num_entries_before > &ctx.next_id {
            log::error!(target: "normfs-reader-fsm",
                "Queue '{}' - file {} begins at {} while entry {} was still owed: ids \
                 {}..{} reach no file this read can see, and the entries above them are \
                 returned without them",
                ctx.queue.short(), ctx.file_id, num_entries_before, ctx.next_id,
                ctx.next_id, num_entries_before);
        }
    }

    /// Whether a read may still need entries past the file it is reading.
    async fn reads_past(&self, ctx: &ReadContext, data_source: DataSource) -> bool {
        let Some(last_id) = &ctx.last_id else {
            return true;
        };
        if &ctx.next_id >= last_id {
            return false;
        }
        let file_end = match (data_source, &self.cloud) {
            (DataSource::DiskStore, _) => {
                self.store.get_file_end(&ctx.queue, &ctx.file_id).await.ok()
            }
            (DataSource::Cloud, Some(cloud)) => cloud
                .get_file_range(&ctx.queue, &ctx.file_id)
                .await
                .ok()
                .map(|range| range.map(|(_, end)| end)),
            _ => None,
        };
        file_end.flatten().is_none_or(|end| last_id > &end)
    }

    async fn handle_parse_wal_bytes(
        &self,
        ctx: ReadContext,
        wal_bytes: bytes::Bytes,
        data_source: DataSource,
        _prefetch_handle: PrefetchHandle,
    ) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Parsing WAL bytes: queue={}, file_id={}, bytes={}, source={:?}",
            ctx.queue.short(), ctx.file_id, wal_bytes.len(), data_source);

        // Prefetch the next file only for a sequential read that goes past
        // this one; a prefetch that finds no free slot is skipped.
        let prefetch_handle = if ctx.step == 1 && self.reads_past(&ctx, data_source).await {
            let next_file_id = ctx.file_id.increment();
            Some(Prefetch(tokio::spawn({
                let fsm = self.clone();
                let queue = ctx.queue.clone();
                async move { fsm.prefetch_next_file(queue, next_file_id, false).await }
            })))
        } else {
            None
        };

        if let Ok(header) = normfs_wal::get_wal_header(&wal_bytes) {
            self.warn_gap(&ctx, &header.num_entries_before);
        }

        // A send never waits here: a stalled consumer would keep the file,
        // and its slot, for as long as it stalls.
        let result = normfs_wal::read_wal_bytes_range_pausing(
            &wal_bytes,
            ctx.resume.as_ref(),
            &ctx.next_id,
            &ctx.last_id,
            ctx.step as usize,
            &ctx.sender,
            data_source,
        )
        .await;
        let result = match result {
            Ok(PausableRead::Done(result)) => Ok(result),
            Ok(PausableRead::Paused(pause)) => {
                drop(wal_bytes);
                drop(prefetch_handle);
                return Ok(self.wait_for_room(ctx.paused(pause)).await);
            }
            Err(e) => Err(e),
        };
        Ok(self.after_range(ctx, result, data_source, prefetch_handle))
    }

    /// Waits for the consumer to take half its channel, then comes back to
    /// the file: from the recent file if it is still kept, else loaded again.
    async fn wait_for_room(&self, ctx: ReadContext) -> ReaderState {
        log::debug!(target: "normfs-reader-fsm",
            "Channel full, letting go of file {} of queue {} until it drains",
            ctx.file_id, ctx.queue.short());
        let room = (ctx.sender.max_capacity() / 2).max(1);
        match ctx.sender.reserve_many(room).await {
            Ok(permits) => drop(permits),
            Err(_) => return ReaderState::Failed(Error::ClientDisconnected),
        }
        ReaderState::ReadFile { ctx }
    }

    fn after_range(
        &self,
        ctx: ReadContext,
        result: Result<ReadRangeResult, WalError>,
        data_source: DataSource,
        prefetch_handle: PrefetchHandle,
    ) -> ReaderState {
        match result {
            Ok(ReadRangeResult::Complete) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Read completed from {:?}: queue={}, file_id={}",
                    data_source, ctx.queue.short(), ctx.file_id);
                ReaderState::Completed
            }
            Ok(ReadRangeResult::PartialRead {
                last_id_in_file,
                last_read_id,
            }) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Partial read from {:?}: queue={}, file_id={}, last_id_in_file={}",
                    data_source, ctx.queue.short(), ctx.file_id, last_id_in_file);

                // Calculate next_id for next file
                let new_next_id = if let Some(last_read) = last_read_id {
                    // We found and read an entry - advance to next step
                    last_read.add(&UintN::from(ctx.step))
                } else {
                    // No entries found in this file - keep looking for the same entry
                    ctx.next_id.clone()
                };

                // Move to next file
                ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx: ctx.with_next_id(new_next_id),
                    prefetch_handle,
                }
            }
            Ok(ReadRangeResult::ChannelClosed) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Channel closed during parse from {:?}: queue={}, file_id={}",
                    data_source, ctx.queue.short(), ctx.file_id);
                ReaderState::Failed(Error::ClientDisconnected)
            }
            Err(e) => {
                log::warn!(target: "normfs-reader-fsm",
                    "Corrupted WAL bytes cannot be restored, moving to next file: queue={}, file_id={}, error={:?}, source={:?}",
                    ctx.queue.short(), ctx.file_id, e, data_source);
                // Parse errors indicate corrupted WAL data
                // Cannot restore corrupted data, so skip to next file and continue reading
                ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle,
                }
            }
        }
    }

    async fn handle_read_next_file(
        &self,
        current_file: UintN,
        ctx: ReadContext,
        prefetch_handle: PrefetchHandle,
    ) -> Result<ReaderState, Error> {
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Checking if more files exist: queue={}, current_file={}, next_id={}, has_prefetch={}",
            ctx.queue.short(), current_file, ctx.next_id, prefetch_handle.is_some());

        // Check if next_id is beyond the queue's last entry
        // If so, we've read all available data - complete the read.
        // Only a bounded read is done at that point: a follow that has caught
        // up has delivered its backlog, not finished, and falls through to
        // the subscribe below -- which the now-empty backlog lets succeed.
        if ctx.last_id.is_some() {
            if let Some(Some(queue_last_id)) = self.mem.get_last_id(&ctx.queue) {
                if ctx.next_id > queue_last_id {
                    log::debug!(target: "normfs-reader-fsm",
                        "Reached end of queue: next_id={} > queue_last_id={}, completing read",
                        ctx.next_id, queue_last_id);
                    return Ok(ReaderState::Completed);
                }
            }
        } else if self.mem.is_closed(&ctx.queue) {
            // The queue closed mid-walk; the subscribe below can never be
            // answered, so the closed record's last id bounds the walk.
            match self.mem.closed_last_id(&ctx.queue) {
                Some(last) if ctx.next_id <= last => {}
                _ => return Ok(ReaderState::Completed),
            }
        }

        // Try to read remaining data from memory to determine if more files exist
        if let Some(ref end_id) = ctx.last_id {
            let result = self
                .mem
                .read_full(
                    &ctx.queue,
                    ctx.next_id.clone(),
                    end_id.clone(),
                    ctx.step as usize,
                    &ctx.sender,
                )
                .await;

            if result.success {
                log::debug!(target: "normfs-reader-fsm",
                    "Completed read from memory: queue={}",
                    ctx.queue.short());
                return Ok(ReaderState::Completed);
            }
        } else {
            let result = self
                .mem
                .follow_full(
                    &ctx.queue,
                    &ctx.next_id,
                    ctx.next_id.clone(),
                    ctx.step as usize,
                    &ctx.sender,
                )
                .await;

            if result.success {
                if let Some(_sub_id) = result.subscription_id {
                    log::debug!(target: "normfs-reader-fsm",
                        "Subscribed to memory: queue={}, subscription_id={}",
                        ctx.queue.short(), _sub_id);
                    return Ok(ReaderState::Subscribed);
                }
            }
        }

        // Data not in memory yet - check prefetch result
        let next_file_id = current_file.increment();

        // next_id never advances when no file holds it, so without this bound
        // the walk increments the file id forever.
        let wal_last_id = async {
            let wal = &self.wal;
            wal.get_last_file_id(&ctx.queue).await.ok().flatten()
        };
        let store_last_id = async {
            let store = &self.store;
            store.get_last_file_id(&ctx.queue).await.ok().flatten()
        };
        let (wal_last_id, store_last_id) = tokio::join!(wal_last_id, store_last_id);
        let last_file_id = [wal_last_id, store_last_id, self.cloud_last(&ctx.queue)]
            .into_iter()
            .flatten()
            .max();
        if last_file_id.is_none_or(|last| next_file_id > last) {
            log::debug!(target: "normfs-reader-fsm",
                "No file after {} for queue {}, completing read",
                current_file, ctx.queue.short());
            return Ok(ReaderState::Completed);
        }

        if let Some(mut handle) = prefetch_handle {
            log::trace!(target: "normfs-reader-fsm",
                "Checking prefetch result for file: queue={}, file_id={}",
                ctx.queue.short(), next_file_id);

            match (&mut handle.0).await {
                Ok(Ok(Fetched::Busy)) => {}
                Ok(Ok(found)) => {
                    let found = match found {
                        Fetched::Missing => {
                            self.file_bytes_or_wait(&ctx.queue, &next_file_id).await
                        }
                        found => found,
                    };
                    return Ok(Self::read_found(ctx.with_file_id(next_file_id), found));
                }
                Ok(Err(e)) => {
                    log::error!(target: "normfs-reader-fsm",
                        "Prefetch failed with error: queue={}, file_id={}, error={:?}",
                        ctx.queue.short(), next_file_id, e);
                    // Real error from prefetch (IO, network, etc.)
                    return Ok(ReaderState::Failed(e));
                }
                Err(e) => {
                    log::error!(target: "normfs-reader-fsm",
                        "Prefetch task panicked: queue={}, file_id={}, error={:?}",
                        ctx.queue.short(), next_file_id, e);
                    // Task panic
                    return Ok(ReaderState::Failed(Error::Io(std::io::Error::other(
                        format!("Prefetch task panicked: {}", e),
                    ))));
                }
            }
        }

        // No prefetch or prefetch not available - use normal backend cascade
        log::trace!(target: "normfs-reader-fsm",
            "No prefetch available, using normal cascade: queue={}, file_id={}",
            ctx.queue.short(), next_file_id);

        Ok(ReaderState::ReadFile {
            ctx: ctx.with_file_id(next_file_id),
        })
    }
}
