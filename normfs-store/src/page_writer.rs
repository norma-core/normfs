use bytes::BytesMut;
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_wal::{FileRuns, PagePool, WAL_HEADER_V1_MAX_SIZE, WalHeader, WalHeaderV1};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use uintn::UintN;

use crate::header::{CompressionType, EncryptionType};
use crate::sink::SealedFileSink;
use crate::store_file::{self, SealedFile};

/// Attempts between complaints while a file will not land: about every five
/// seconds at the default delay.
const LAND_WARN_EVERY: u32 = 500;

const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct PageWriterSettings {
    pub compression: CompressionType,
    pub encryption: EncryptionType,
    /// First delay between attempts to land a file; doubles up to thirty
    /// seconds. Attempts are unbounded: the pool fills behind a file that will
    /// not land and appenders wait, which is back-pressure rather than loss.
    pub retry_delay: Duration,
    /// Attempts a close gives an outstanding file before reporting itself
    /// incomplete. The file keeps retrying after that.
    pub close_max_attempts: u32,
}

enum Request {
    Flush(oneshot::Sender<()>),
    Close,
}

/// One queue's page-per-file writer: each sealed page becomes one store file
/// through the sink, with no `.wal` and no timer in between.
///
/// A file is born when its page fills, on [`PageStoreWriter::flush`] and on
/// close. Nothing else moves bytes, so a crash loses at most the open page.
#[derive(Clone)]
pub struct PageStoreWriter {
    tx: mpsc::UnboundedSender<Request>,
    closing: watch::Sender<bool>,
    done: watch::Receiver<Option<bool>>,
}

impl PageStoreWriter {
    pub(crate) fn is_closing(&self) -> bool {
        *self.closing.borrow()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start(
        queue: &QueueId,
        file_id: &UintN,
        header: WalHeader,
        settings: PageWriterSettings,
        pool: Arc<PagePool>,
        sink: Arc<dyn SealedFileSink>,
        crypto: Arc<CryptoContext>,
        written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let (closing, close_requested) = watch::channel(false);
        let (close_done, done) = watch::channel(None);
        let flush = Arc::new(Notify::new());
        pool.set_flush_signal(flush.clone());
        // From here this task drains the pool, so an appender may wait for a
        // page: a landed file ends the wait.
        pool.set_drainer();
        pool.arm_page_files(WAL_HEADER_V1_MAX_SIZE as u64);

        let task = Task {
            queue: queue.clone(),
            file_id: file_id.clone(),
            header,
            next_epoch: 0,
            build_failed: false,
            settings,
            pool,
            sink,
            crypto,
            written_sender,
            closing: close_requested,
            done: close_done,
        };
        tokio::spawn(task.run(rx, flush));
        Self { tx, closing, done }
    }

    /// Lands everything accepted so far, the open page included. `false` if
    /// the writer is gone.
    pub async fn flush(&self) -> bool {
        let (reply, done) = oneshot::channel();
        if self.tx.send(Request::Flush(reply)).is_err() {
            return false;
        }
        done.await.is_ok()
    }

    /// As [`PageStoreWriter::flush`], then stops. `false` when an outstanding file
    /// did not land within the close budget; it keeps trying, and the pool
    /// reports the gap until it does.
    pub async fn close(mut self) -> bool {
        if !self.closing.send_replace(true) {
            let _ = self.tx.send(Request::Close);
        }
        self.done
            .wait_for(|done| done.is_some())
            .await
            .map(|done| done.unwrap_or(false))
            .unwrap_or(false)
    }
}

struct Task {
    queue: QueueId,
    file_id: UintN,
    header: WalHeader,
    next_epoch: u64,
    build_failed: bool,
    settings: PageWriterSettings,
    pool: Arc<PagePool>,
    sink: Arc<dyn SealedFileSink>,
    crypto: Arc<CryptoContext>,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    closing: watch::Receiver<bool>,
    done: watch::Sender<Option<bool>>,
}

/// A file built from the pool but not yet landed. The pool's cursors have
/// already moved past its bytes, so this is the only copy.
struct Built {
    sealed: SealedFile,
    header: WalHeader,
    first_entry_id: u64,
    last_entry_id: u64,
}

impl Task {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Request>, flush: Arc<Notify>) {
        loop {
            tokio::select! {
                _ = flush.notified() => self.catch_up().await,
                req = rx.recv() => match req {
                    Some(Request::Flush(reply)) => {
                        self.catch_up().await;
                        if let Some(runs) = self.seal() {
                            self.land(runs).await;
                        }
                        let _ = reply.send(());
                    }
                    Some(Request::Close) => {
                        self.catch_up().await;
                        if let Some(runs) = self.seal() {
                            self.land(runs).await;
                        }
                        // Reader pins can outlive the writer; they prevent
                        // page reuse, but do not undo a successful landing.
                        self.done.send_replace(Some(!self.build_failed));
                        self.pool.clear_drainer();
                        return;
                    }
                    None => {
                        self.pool.clear_drainer();
                        return;
                    }
                },
            }
        }
    }

    async fn catch_up(&mut self) {
        while self.next_epoch < self.pool.epoch() {
            let epoch = self.next_epoch;
            if let Some(runs) = self.pool.take_file(epoch) {
                self.land(runs).await;
            }
            self.next_epoch = epoch + 1;
        }
    }

    fn seal(&mut self) -> Option<FileRuns> {
        let (epoch, runs) = self.pool.seal_open_file()?;
        debug_assert_eq!(
            epoch, self.next_epoch,
            "sealed an epoch the writer had not reached"
        );
        self.next_epoch = epoch + 1;
        Some(runs)
    }

    async fn land(&mut self, runs: FileRuns) {
        if let Some(built) = self.build(runs).await {
            self.try_land(&built.sealed).await;
            self.finish(built);
        } else {
            self.build_failed = true;
        }
    }

    /// Compressing, encrypting and signing a page is CPU work that would
    /// otherwise sit on a runtime worker thread; three queues sealing at once
    /// on a four-core box starved everything else, including the appenders
    /// whose pages this is meant to free.
    async fn build(&self, runs: FileRuns) -> Option<Built> {
        let first = UintN::from(runs.first_entry_id);
        let last = UintN::from(runs.last_entry_id);
        let num_entries = UintN::from(runs.last_entry_id - runs.first_entry_id + 1);

        let mut header = self.header.resize(&last, self.pool.page_size());
        header.num_entries_before = first.clone();

        let mut wal_bytes = BytesMut::new();
        let written = WalHeaderV1::from_v0(&header)
            .and_then(|h| h.write_to_bytes(&mut wal_bytes))
            .map_err(|e| e.to_string());
        for (_, bytes) in &runs.runs {
            wal_bytes.extend_from_slice(bytes);
        }

        let sealed = match written {
            Ok(_) => {
                let (queue, file_id, crypto) = (
                    self.queue.clone(),
                    self.file_id.clone(),
                    self.crypto.clone(),
                );
                let (compression, encryption) =
                    (self.settings.compression, self.settings.encryption);
                let wal_bytes = wal_bytes.freeze();
                tokio::task::spawn_blocking(move || {
                    store_file::build(
                        &queue,
                        &file_id,
                        compression,
                        encryption,
                        first,
                        num_entries,
                        &wal_bytes,
                        &crypto,
                    )
                })
                .await
                .map_err(|e| e.to_string())
                .and_then(|r| r.map_err(|e| e.to_string()))
            }
            Err(e) => Err(e),
        };
        match sealed {
            Ok(sealed) => Some(Built {
                sealed,
                header,
                first_entry_id: runs.first_entry_id,
                last_entry_id: runs.last_entry_id,
            }),
            Err(e) => {
                // Deterministic, so a retry would fail the same way; the
                // records stay in memory, unreported as durable, until evicted.
                log::error!(target: "normfs-store",
                    "cannot build store file {} for queue {} (entries {}..={}): {e}; \
                     these records reach no file",
                    self.file_id, self.queue, runs.first_entry_id, runs.last_entry_id);
                None
            }
        }
    }

    async fn try_land(&self, sealed: &SealedFile) {
        let mut closing = self.closing.clone();
        let mut close_attempts = 0u32;
        let mut delay = self.settings.retry_delay;
        let mut attempt: u32 = 0;
        loop {
            match self.sink.land(&self.queue, &self.file_id, sealed).await {
                Ok(()) => return,
                Err(e) => {
                    attempt = attempt.saturating_add(1);
                    if *closing.borrow_and_update() {
                        if close_attempts == 0 {
                            delay = self.settings.retry_delay;
                        }
                        close_attempts = close_attempts.saturating_add(1);
                        if close_attempts >= self.settings.close_max_attempts {
                            // Retain this file and keep retrying after the
                            // caller learns that shutdown is incomplete.
                            self.done.send_replace(Some(false));
                        }
                    }
                    if attempt == 1 || attempt.is_multiple_of(LAND_WARN_EVERY) {
                        log::warn!(target: "normfs-store",
                            "store file {} for queue {} did not land (attempt {attempt}): {e}",
                            self.file_id, self.queue);
                    }
                    if *closing.borrow() || closing.has_changed().is_err() {
                        tokio::time::sleep(delay).await;
                    } else {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {},
                            _ = closing.changed() => {
                                delay = self.settings.retry_delay;
                            },
                        }
                    }
                    delay = (delay * 2).min(MAX_RETRY_DELAY);
                }
            }
        }
    }

    fn finish(&mut self, built: Built) {
        self.pool
            .mark_durable(built.last_entry_id.saturating_add(1));
        let _ = self
            .written_sender
            .send((self.queue.clone(), UintN::from(built.last_entry_id)));
        self.file_id = self.file_id.increment();
        self.header = built.header;
        self.header.num_entries_before = UintN::from(built.last_entry_id).increment();
        log::debug!(target: "normfs-store",
            "queue {}: entries {}..={} landed as store file {}",
            self.queue, built.first_entry_id, built.last_entry_id, self.file_id);
    }
}
