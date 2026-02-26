use super::{DataSource, PrefetchHandle, ReadContext, ReadEntry, ReaderState};
use crate::{mem::MemStore, Error};
use normfs_cloud::CloudDownloader;
use normfs_store::PersistStore;
use normfs_types::{QueueId, ReadPosition};
use normfs_wal::WalStore;
use std::sync::Arc;
use tokio::sync::mpsc;
use uintn::UintN;

/// Reader FSM that manages read operations across different storage backends
#[derive(Clone)]
pub struct ReaderFSM {
    pub(crate) wal: Arc<WalStore>,
    pub(crate) store: Arc<PersistStore>,
    pub(crate) mem: Arc<MemStore>,
    pub(crate) s3_downloader: Option<Arc<CloudDownloader>>,
}

impl ReaderFSM {
    pub fn new(
        wal: Arc<WalStore>,
        store: Arc<PersistStore>,
        mem: Arc<MemStore>,
        s3_downloader: Option<Arc<CloudDownloader>>,
    ) -> Self {
        Self {
            wal,
            store,
            mem,
            s3_downloader,
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
            ReaderState::ExtractWalBytes {
                ctx,
                store_bytes,
                data_source,
            } => {
                self.handle_extract_wal_bytes(ctx, store_bytes, data_source)
                    .await
            }
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
    async fn prefetch_next_file(
        self,
        queue: QueueId,
        file_id: UintN,
    ) -> Result<Option<(bytes::Bytes, DataSource)>, Error> {
        log::trace!(target: "normfs-reader-fsm",
            "Prefetching file: queue={}, file_id={}",
            queue, file_id);

        // Try Store first
        match self.store.get_store_bytes(&queue, &file_id).await {
            Ok(Some(store_bytes)) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Prefetch: got {} bytes from Store for file {}",
                    store_bytes.len(), file_id);
                // Extract WAL bytes (decrypt/decompress) - no verification for local files
                match self
                    .store
                    .extract_wal_bytes(&queue, &file_id, store_bytes, false)
                {
                    Ok(wal_bytes) => {
                        log::debug!(target: "normfs-reader-fsm",
                            "Prefetch successful from Store: queue={}, file_id={}, wal_bytes={}",
                            queue, file_id, wal_bytes.len());
                        return Ok(Some((wal_bytes, DataSource::DiskStore)));
                    }
                    Err(e) => {
                        log::warn!(target: "normfs-reader-fsm",
                            "Prefetch: corrupted store file, treating as not found: queue={}, file_id={}, error={:?}",
                            queue, file_id, e);
                        // Corruption - treat as missing file
                        return Ok(None);
                    }
                }
            }
            Ok(None) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Prefetch: file not in Store, trying WAL: queue={}, file_id={}",
                    queue, file_id);
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "Prefetch: Store error: queue={}, file_id={}, error={:?}",
                    queue, file_id, e);
                return Err(Error::Store(e));
            }
        }

        // Try WAL
        match self.wal.get_wal_bytes(&queue, &file_id).await {
            Ok(Some(wal_bytes)) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Prefetch successful from WAL: queue={}, file_id={}, wal_bytes={}",
                    queue, file_id, wal_bytes.len());
                return Ok(Some((wal_bytes, DataSource::DiskWal)));
            }
            Ok(None) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Prefetch: file not in WAL: queue={}, file_id={}",
                    queue, file_id);
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "Prefetch: WAL error: queue={}, file_id={}, error={:?}",
                    queue, file_id, e);
                return Err(Error::Wal(e));
            }
        }

        // Try S3 (if available)
        if let Some(s3) = &self.s3_downloader {
            match s3.get_store_bytes(&queue, &file_id).await {
                Ok(Some(store_bytes)) => {
                    log::info!(target: "normfs-reader-fsm",
                        "Prefetch: got {} bytes from S3 for file {}",
                        store_bytes.len(), file_id);
                    // Extract WAL bytes (decrypt/decompress) - verify signatures for S3 files
                    match self
                        .store
                        .extract_wal_bytes(&queue, &file_id, store_bytes, true)
                    {
                        Ok(wal_bytes) => {
                            log::debug!(target: "normfs-reader-fsm",
                                "Prefetch successful from S3: queue={}, file_id={}, wal_bytes={}",
                                queue, file_id, wal_bytes.len());
                            return Ok(Some((wal_bytes, DataSource::Cloud)));
                        }
                        Err(e) => {
                            log::warn!(target: "normfs-reader-fsm",
                                "Prefetch: corrupted S3 file, treating as not found: queue={}, file_id={}, error={:?}",
                                queue, file_id, e);
                            // Corruption - treat as missing file
                            return Ok(None);
                        }
                    }
                }
                Ok(None) => {
                    log::trace!(target: "normfs-reader-fsm",
                        "Prefetch: file not in S3 (404): queue={}, file_id={}",
                        queue, file_id);
                }
                Err(e) => {
                    log::error!(target: "normfs-reader-fsm",
                        "Prefetch: S3 error: queue={}, file_id={}, error={:?}",
                        queue, file_id, e);
                    return Err(Error::Cloud(e));
                }
            }
        }

        // File not found in any backend
        log::debug!(target: "normfs-reader-fsm",
            "Prefetch: file not found in any backend: queue={}, file_id={}",
            queue, file_id);
        Ok(None)
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

            Ok(ReaderState::LookupFile {
                queue,
                start_id,
                end_id,
                step,
                sender,
            })
        } else {
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

        // Look up which file contains start_id
        let file_id = match crate::lookup::find_file_with_s3(
            &queue,
            &start_id,
            &self.store,
            &self.wal,
            self.s3_downloader.as_ref(),
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
                    queue, start_id, e);
                // Convert lookup error to appropriate NormFS error
                return Ok(ReaderState::Failed(Error::NotFound));
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
            ctx.queue, ctx.file_id, ctx.next_id, ctx.last_id);

        // Get store file bytes
        match self.store.get_store_bytes(&ctx.queue, &ctx.file_id).await {
            Ok(Some(store_bytes)) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Read {} bytes from Store file: queue={}, file_id={}",
                    store_bytes.len(), ctx.queue, ctx.file_id);
                // Transition to ExtractWalBytes to decrypt/decompress
                Ok(ReaderState::ExtractWalBytes {
                    ctx,
                    store_bytes,
                    data_source: DataSource::DiskStore,
                })
            }
            Ok(None) => {
                log::trace!(target: "normfs-reader-fsm",
                    "File not found in Store, trying WAL: queue={}, file_id={}",
                    ctx.queue, ctx.file_id);
                // Not in store, try WAL
                Ok(ReaderState::ReadWal { ctx })
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "Store get_store_bytes error: queue={}, file_id={}, error={:?}",
                    ctx.queue, ctx.file_id, e);
                Ok(ReaderState::Failed(Error::Store(e)))
            }
        }
    }

    async fn handle_read_wal(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Attempting to read from WAL: queue={}, file_id={}, next_id={}, last_id={:?}",
            ctx.queue, ctx.file_id, ctx.next_id, ctx.last_id);

        // Get WAL bytes from WAL file
        match self.wal.get_wal_bytes(&ctx.queue, &ctx.file_id).await {
            Ok(Some(wal_bytes)) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Read {} bytes from WAL file: queue={}, file_id={}",
                    wal_bytes.len(), ctx.queue, ctx.file_id);
                // Transition to ParseWalBytes
                Ok(ReaderState::ParseWalBytes {
                    ctx,
                    wal_bytes,
                    data_source: DataSource::DiskWal,
                    prefetch_handle: None,
                })
            }
            Ok(None) => {
                log::trace!(target: "normfs-reader-fsm",
                    "File not found in WAL: queue={}, file_id={}",
                    ctx.queue, ctx.file_id);
                // Not in WAL, try S3 if available, otherwise move to next file
                if self.s3_downloader.is_some() {
                    log::trace!(target: "normfs-reader-fsm",
                        "Trying S3: queue={}, file_id={}",
                        ctx.queue, ctx.file_id);
                    Ok(ReaderState::ReadS3 { ctx })
                } else {
                    log::debug!(target: "normfs-reader-fsm",
                        "File not found and S3 not configured, moving to next file: queue={}, file_id={}",
                        ctx.queue, ctx.file_id);
                    // No S3 available, move to next file
                    Ok(ReaderState::ReadNextFile {
                        current_file: ctx.file_id.clone(),
                        ctx,
                        prefetch_handle: None,
                    })
                }
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "WAL get_wal_bytes error: queue={}, file_id={}, error={:?}",
                    ctx.queue, ctx.file_id, e);
                Ok(ReaderState::Failed(Error::Wal(e)))
            }
        }
    }

    async fn handle_read_s3(&self, ctx: ReadContext) -> Result<ReaderState, Error> {
        use normfs_cloud::errors::CloudError;

        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        let s3_downloader = match &self.s3_downloader {
            Some(s3) => s3,
            None => {
                log::error!(target: "normfs-reader-fsm",
                    "S3 state reached but S3 downloader not configured: queue={}, file_id={}",
                    ctx.queue, ctx.file_id);
                return Ok(ReaderState::Failed(Error::NotFound));
            }
        };

        log::info!(target: "normfs-reader-fsm",
            "Attempting to read from S3: queue={}, file_id={}, next_id={}, last_id={:?}",
            ctx.queue, ctx.file_id, ctx.next_id, ctx.last_id);

        // Get store bytes from S3
        match s3_downloader
            .get_store_bytes(&ctx.queue, &ctx.file_id)
            .await
        {
            Ok(Some(store_bytes)) => {
                log::info!(target: "normfs-reader-fsm",
                    "Downloaded {} bytes from S3: queue={}, file_id={}",
                    store_bytes.len(), ctx.queue, ctx.file_id);
                // Transition to ExtractWalBytes to decrypt/decompress
                Ok(ReaderState::ExtractWalBytes {
                    ctx,
                    store_bytes,
                    data_source: DataSource::Cloud,
                })
            }
            Ok(None) => {
                log::info!(target: "normfs-reader-fsm",
                    "File not found in S3 (404), moving to next file: queue={}, file_id={}",
                    ctx.queue, ctx.file_id);
                // S3 file not found (404), move to next file
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                })
            }
            Err(CloudError::NoFilesFound) => {
                log::info!(target: "normfs-reader-fsm",
                    "No files found in S3, moving to next file: queue={}, file_id={}",
                    ctx.queue, ctx.file_id);
                // No files found, move to next file
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                })
            }
            Err(e) => {
                log::error!(target: "normfs-reader-fsm",
                    "S3 download error (network/auth/etc): queue={}, file_id={}, error={:?}",
                    ctx.queue, ctx.file_id, e);
                // Network error, auth error, or other transient S3 error - fail the read
                Ok(ReaderState::Failed(Error::Cloud(e)))
            }
        }
    }

    async fn handle_extract_wal_bytes(
        &self,
        ctx: ReadContext,
        store_bytes: bytes::Bytes,
        data_source: DataSource,
    ) -> Result<ReaderState, Error> {
        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Extracting WAL bytes from store bytes: queue={}, file_id={}, bytes={}, source={:?}",
            ctx.queue, ctx.file_id, store_bytes.len(), data_source);

        // Extract WAL bytes from store bytes (decrypt/decompress)
        // Verify signatures for S3 files, skip verification for local files
        let verify_signatures = matches!(data_source, DataSource::Cloud);
        match self
            .store
            .extract_wal_bytes(&ctx.queue, &ctx.file_id, store_bytes, verify_signatures)
        {
            Ok(wal_bytes) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Extracted {} WAL bytes: queue={}, file_id={}",
                    wal_bytes.len(), ctx.queue, ctx.file_id);
                // Transition to ParseWalBytes
                Ok(ReaderState::ParseWalBytes {
                    ctx,
                    wal_bytes,
                    data_source,
                    prefetch_handle: None,
                })
            }
            Err(e) => {
                log::warn!(target: "normfs-reader-fsm",
                    "Failed to extract WAL bytes from corrupted store file, moving to next file: queue={}, file_id={}, error={:?}, source={:?}",
                    ctx.queue, ctx.file_id, e, data_source);
                // Extraction errors (decryption/decompression failures) indicate data corruption
                // Cannot restore corrupted data, so skip to next file
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle: None,
                })
            }
        }
    }

    async fn handle_parse_wal_bytes(
        &self,
        ctx: ReadContext,
        wal_bytes: bytes::Bytes,
        data_source: DataSource,
        _prefetch_handle: PrefetchHandle,
    ) -> Result<ReaderState, Error> {
        use normfs_wal::ReadRangeResult;

        // Check if sender is still open
        if ctx.sender.is_closed() {
            return Ok(ReaderState::Failed(Error::ClientDisconnected));
        }

        log::trace!(target: "normfs-reader-fsm",
            "Parsing WAL bytes: queue={}, file_id={}, bytes={}, source={:?}",
            ctx.queue, ctx.file_id, wal_bytes.len(), data_source);

        // Spawn prefetch for next file immediately
        // Only prefetch if:
        // - step == 1 (sequential reads)
        // - AND either no limit (subscription mode) OR we haven't reached the last entry yet
        let should_prefetch = ctx.step == 1
            && ctx.last_id.as_ref().is_none_or(|last_id| {
                // If we have a last_id, only prefetch if next_id < last_id
                // (meaning there are more entries to read after this file)
                &ctx.next_id < last_id
            });

        let prefetch_handle = if should_prefetch {
            let next_file_id = ctx.file_id.increment();
            Some(tokio::spawn({
                let fsm = self.clone();
                let queue = ctx.queue.clone();
                async move { fsm.prefetch_next_file(queue, next_file_id).await }
            }))
        } else {
            None
        };

        // Parse and send entries from WAL bytes
        match self
            .wal
            .read_wal_content_range(
                &wal_bytes,
                &ctx.next_id,
                &ctx.last_id,
                ctx.step as usize,
                &ctx.sender,
                data_source,
            )
            .await
        {
            Ok(ReadRangeResult::Complete) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Read completed from {:?}: queue={}, file_id={}",
                    data_source, ctx.queue, ctx.file_id);
                Ok(ReaderState::Completed)
            }
            Ok(ReadRangeResult::PartialRead {
                last_id_in_file,
                last_read_id,
            }) => {
                log::trace!(target: "normfs-reader-fsm",
                    "Partial read from {:?}: queue={}, file_id={}, last_id_in_file={}",
                    data_source, ctx.queue, ctx.file_id, last_id_in_file);

                // Calculate next_id for next file
                let new_next_id = if let Some(last_read) = last_read_id {
                    // We found and read an entry - advance to next step
                    last_read.add(&UintN::from(ctx.step))
                } else {
                    // No entries found in this file - keep looking for the same entry
                    ctx.next_id.clone()
                };

                // Move to next file
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx: ctx.with_next_id(new_next_id),
                    prefetch_handle,
                })
            }
            Ok(ReadRangeResult::ChannelClosed) => {
                log::debug!(target: "normfs-reader-fsm",
                    "Channel closed during parse from {:?}: queue={}, file_id={}",
                    data_source, ctx.queue, ctx.file_id);
                Ok(ReaderState::Failed(Error::ClientDisconnected))
            }
            Err(e) => {
                log::warn!(target: "normfs-reader-fsm",
                    "Corrupted WAL bytes cannot be restored, moving to next file: queue={}, file_id={}, error={:?}, source={:?}",
                    ctx.queue, ctx.file_id, e, data_source);
                // Parse errors indicate corrupted WAL data
                // Cannot restore corrupted data, so skip to next file and continue reading
                Ok(ReaderState::ReadNextFile {
                    current_file: ctx.file_id.clone(),
                    ctx,
                    prefetch_handle,
                })
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
            ctx.queue, current_file, ctx.next_id, prefetch_handle.is_some());

        // Check if next_id is beyond the queue's last entry
        // If so, we've read all available data - complete the read
        if let Some(Some(queue_last_id)) = self.mem.get_last_id(&ctx.queue) {
            if ctx.next_id > queue_last_id {
                log::debug!(target: "normfs-reader-fsm",
                    "Reached end of queue: next_id={} > queue_last_id={}, completing read",
                    ctx.next_id, queue_last_id);
                return Ok(ReaderState::Completed);
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
                    ctx.queue);
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
                        ctx.queue, _sub_id);
                    return Ok(ReaderState::Subscribed);
                }
            }
        }

        // Data not in memory yet - check prefetch result
        let next_file_id = current_file.increment();

        if let Some(handle) = prefetch_handle {
            log::trace!(target: "normfs-reader-fsm",
                "Checking prefetch result for file: queue={}, file_id={}",
                ctx.queue, next_file_id);

            match handle.await {
                Ok(Ok(Some((wal_bytes, source)))) => {
                    log::debug!(target: "normfs-reader-fsm",
                        "Prefetch succeeded! Using cached data: queue={}, file_id={}, source={:?}, bytes={}",
                        ctx.queue, next_file_id, source, wal_bytes.len());
                    // Prefetch succeeded! Go directly to ParseWalBytes with prefetched data
                    return Ok(ReaderState::ParseWalBytes {
                        ctx: ctx.with_file_id(next_file_id),
                        wal_bytes,
                        data_source: source,
                        prefetch_handle: None, // Will spawn new prefetch in ParseWalBytes
                    });
                }
                Ok(Ok(None)) => {
                    log::debug!(target: "normfs-reader-fsm",
                        "Prefetch: file not found in any backend (skipped file): queue={}, file_id={}",
                        ctx.queue, next_file_id);
                    // File not found - this is a skipped file, try next file
                    // Recurse to ReadNextFile for the next file (no prefetch handle)
                    return Ok(ReaderState::ReadNextFile {
                        current_file: next_file_id,
                        ctx,
                        prefetch_handle: None,
                    });
                }
                Ok(Err(e)) => {
                    log::error!(target: "normfs-reader-fsm",
                        "Prefetch failed with error: queue={}, file_id={}, error={:?}",
                        ctx.queue, next_file_id, e);
                    // Real error from prefetch (IO, network, etc.)
                    return Ok(ReaderState::Failed(e));
                }
                Err(e) => {
                    log::error!(target: "normfs-reader-fsm",
                        "Prefetch task panicked: queue={}, file_id={}, error={:?}",
                        ctx.queue, next_file_id, e);
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
            ctx.queue, next_file_id);

        Ok(ReaderState::ReadFile {
            ctx: ctx.with_file_id(next_file_id),
        })
    }
}
