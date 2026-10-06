use bytes::BytesMut;
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_wal::{FileRuns, PagePool, WAL_HEADER_V1_MAX_SIZE, WalHeader, WalHeaderV1};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use uintn::UintN;

use crate::StoreError;
use crate::header::{CompressionType, EncryptionType};
use crate::pack::Packer;
use crate::sink::SealedFileSink;
use crate::store_file::SealedFile;

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
    Flush(oneshot::Sender<bool>),
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
        packer: Arc<Packer>,
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
            durable_floor: None,
            settings,
            pool,
            sink,
            packer,
            crypto,
            written_sender,
            closing: close_requested,
            done: close_done,
        };
        tokio::spawn(task.run(rx, flush));
        Self { tx, closing, done }
    }

    /// Lands everything accepted so far, the open page included.
    /// [`StoreError::FlushIncomplete`] once any file has failed to build, on
    /// this flush or an earlier one: those records are in no file.
    pub async fn flush(&self) -> Result<(), StoreError> {
        let stopped = || {
            StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "page writer stopped",
            ))
        };
        let (reply, done) = oneshot::channel();
        self.tx.send(Request::Flush(reply)).map_err(|_| stopped())?;
        match done.await {
            Ok(true) => Ok(()),
            Ok(false) => Err(StoreError::FlushIncomplete),
            Err(_) => Err(stopped()),
        }
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
    /// The first id of the first file that failed to build. Nothing from here
    /// on is reported durable, even once later files land: their pages would
    /// be recycled and a close would certify records that are in no file.
    durable_floor: Option<u64>,
    settings: PageWriterSettings,
    pool: Arc<PagePool>,
    sink: Arc<dyn SealedFileSink>,
    packer: Arc<Packer>,
    crypto: Arc<CryptoContext>,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    closing: watch::Receiver<bool>,
    done: watch::Sender<Option<bool>>,
}

/// A file built from the pool but not yet landed. It holds a pack slot; the
/// records stay on their pinned pages, so a failed attempt drops it and the
/// next one builds it again.
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
                        self.land_all().await;
                        let _ = reply.send(self.durable_floor.is_none());
                    }
                    Some(Request::Close) => {
                        self.land_all().await;
                        // Reader pins can outlive the writer; they prevent
                        // page reuse, but do not undo a successful landing.
                        self.done.send_replace(Some(self.durable_floor.is_none()));
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

    /// Everything accepted so far, the open page included.
    async fn land_all(&mut self) {
        self.catch_up().await;
        for runs in seal_through(&self.pool, &mut self.next_epoch) {
            self.land(runs).await;
        }
        // An empty seal says nothing about files closed after the first look.
        self.catch_up().await;
    }

    async fn land(&mut self, runs: FileRuns) {
        match self.try_land(&runs).await {
            Some(built) => self.finish(built),
            None => {
                self.durable_floor.get_or_insert(runs.first_entry_id);
            }
        }
    }

    /// Off the runtime: three queues sealing at once on a four-core box
    /// starved everything else, including the appenders whose pages this frees.
    async fn build(&self, runs: &FileRuns) -> Result<Built, String> {
        let first = UintN::from(runs.first_entry_id);
        let last = UintN::from(runs.last_entry_id);
        let num_entries = UintN::from(runs.last_entry_id - runs.first_entry_id + 1);

        let mut header = self.header.resize(&last, self.pool.page_size());
        header.num_entries_before = first.clone();

        let mut header_bytes = BytesMut::new();
        WalHeaderV1::from_v0(&header)
            .and_then(|h| h.write_to_bytes(&mut header_bytes))
            .map_err(|e| e.to_string())?;
        let len = header_bytes.len() + runs.len();
        if len > self.packer.input_cap() {
            return Err(format!(
                "{len} bytes exceed the {} a pack slot holds",
                self.packer.input_cap()
            ));
        }

        let mut slot = self.packer.take().await;
        let buf = slot.buf();
        buf[..header_bytes.len()].copy_from_slice(&header_bytes);
        self.pool.copy_file(runs, &mut buf[header_bytes.len()..len]);

        let (packer, queue, file_id, crypto) = (
            self.packer.clone(),
            self.queue.clone(),
            self.file_id.clone(),
            self.crypto.clone(),
        );
        let (compression, encryption) = (self.settings.compression, self.settings.encryption);
        let sealed = tokio::task::spawn_blocking(move || {
            packer.seal(
                slot,
                len,
                &queue,
                &file_id,
                compression,
                encryption,
                first,
                num_entries,
                &crypto,
            )
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;
        Ok(Built {
            sealed,
            header,
            first_entry_id: runs.first_entry_id,
            last_entry_id: runs.last_entry_id,
        })
    }

    /// Builds and lands `runs` until it is in, or `None` when it cannot be
    /// built: that is deterministic, so a retry would fail the same way, and
    /// the records stay in memory, held there by `durable_floor`.
    async fn try_land(&self, runs: &FileRuns) -> Option<Built> {
        let mut closing = self.closing.clone();
        let mut close_attempts = 0u32;
        let mut delay = self.settings.retry_delay;
        let mut attempt: u32 = 0;
        loop {
            let built = match self.build(runs).await {
                Ok(built) => built,
                Err(e) => {
                    log::error!(target: "normfs-store",
                        "cannot build store file {} for queue {} (entries {}..={}): {e}; \
                         these records reach no file",
                        self.file_id, self.queue, runs.first_entry_id, runs.last_entry_id);
                    return None;
                }
            };
            let landed = self
                .sink
                .land(&self.queue, &self.file_id, &built.sealed)
                .await;
            let e = match landed {
                Ok(()) => return Some(built),
                Err(e) => e,
            };
            // The slot goes back while this waits.
            drop(built);
            attempt = attempt.saturating_add(1);
            if *closing.borrow_and_update() {
                if close_attempts == 0 {
                    delay = self.settings.retry_delay;
                }
                close_attempts = close_attempts.saturating_add(1);
                if close_attempts >= self.settings.close_max_attempts {
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

    fn finish(&mut self, built: Built) {
        let through = built.last_entry_id.saturating_add(1);
        match self.durable_floor {
            Some(floor) => self.pool.mark_durable(floor.min(through)),
            None => {
                self.pool.mark_durable(through);
                let _ = self
                    .written_sender
                    .send((self.queue.clone(), UintN::from(built.last_entry_id)));
            }
        }
        self.file_id = self.file_id.increment();
        self.header = built.header;
        self.header.num_entries_before = UintN::from(built.last_entry_id).increment();
        log::debug!(target: "normfs-store",
            "queue {}: entries {}..={} landed as store file {}",
            self.queue, built.first_entry_id, built.last_entry_id, self.file_id);
    }
}

/// Seals the open file, preceded by any file closed since the writer last
/// looked. An append can open a page between `catch_up` reading the epoch and
/// the seal taking the lock; sealing alone would then skip the file it closed,
/// and the next landing would report that file's records durable.
pub(crate) fn seal_through(pool: &Arc<PagePool>, next_epoch: &mut u64) -> Vec<FileRuns> {
    let Some((sealed_epoch, sealed)) = pool.seal_open_file() else {
        return Vec::new();
    };
    let mut files = Vec::new();
    while *next_epoch < sealed_epoch {
        if let Some(runs) = pool.take_file(*next_epoch) {
            files.push(runs);
        }
        *next_epoch += 1;
    }
    files.push(sealed);
    *next_epoch = sealed_epoch + 1;
    files
}
