use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use normfs_fs::{AppendOutcome, Fs, Runs, TmpMode};
use normfs_types::QueueId;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::JoinHandle;
use uintn::UintN;

use crate::page_pool::PagePool;

#[derive(Debug, Clone)]
pub struct AckFileWriterSettings {
    pub max_buffer_size: usize,
    pub max_file_size: u64,
    pub write_interval: Duration,
    pub fsync: bool,
    pub max_retries: u32,
    pub retry_delay: Duration,
}

impl Default for AckFileWriterSettings {
    fn default() -> Self {
        Self {
            max_buffer_size: 128 * 1024 * 1024, // 128MB
            max_file_size: 128 * 1024 * 1024,   // 128MB
            write_interval: Duration::from_millis(20),
            fsync: true,
            max_retries: 1000,
            retry_delay: Duration::from_millis(10),
        }
    }
}

/// The file and the write-side view of it, under one lock.
///
/// `flushed_len` is the length known to be on the medium: the header plus
/// every batch the fs layer committed. It is the `at` every append starts
/// from, and the fs layer's APPEND plan promises that after a failed attempt
/// the file is cut back to it, so a torn write cannot leave a partial frame
/// for the retry to append after -- V1's positional ids turn any stray bytes
/// into every later entry answering under the wrong id.
#[derive(Debug)]
pub(crate) struct FileTail {
    pub(crate) file: Arc<File>,
    pub(crate) inode: u64,
    pub(crate) flushed_len: u64,
    pub(crate) needs_restore: bool,
}

/// One batch, committed only whole, at the file's known-good length.
///
/// Nothing is committed until the write and the sync have both succeeded: a
/// failed sync leaves the page cache in a state Linux does not promise to
/// sync later, so the only safe retry is to cut the file back and write the
/// bytes again. The APPEND plan does the cutting; an attempt it could not
/// cut back is followed by a RESTORE before the next one.
pub(crate) async fn commit(
    fs: &Fs,
    path: &Path,
    tail: &mut FileTail,
    runs: Runs,
    settings: &AckFileWriterSettings,
) -> bool {
    let total = runs.total();
    for attempt in 0..settings.max_retries {
        let at = tail.flushed_len;
        if tail.needs_restore {
            if let Err(e) = fs
                .restore_with_inode(tail.file.clone(), tail.inode, path, at)
                .await
            {
                log::error!(target: "normfs", "Cannot restore {} to {at}: {e}", path.display());
                if attempt + 1 < settings.max_retries {
                    tokio::time::sleep(settings.retry_delay).await;
                }
                continue;
            }
            tail.needs_restore = false;
        }
        tail.needs_restore = true;
        match fs
            .append_sync_with_inode(
                tail.file.clone(),
                tail.inode,
                path,
                at,
                runs.clone(),
                settings.fsync,
            )
            .await
        {
            Ok(AppendOutcome::Committed) => {
                tail.needs_restore = false;
                tail.flushed_len += total;
                return true;
            }
            Ok(AppendOutcome::Failed { err, restored }) => {
                log::error!(
                    target: "normfs",
                    "Failed to commit {} bytes to {} at {} (attempt {}/{}): {}",
                    total,
                    path.display(),
                    at,
                    attempt + 1,
                    settings.max_retries,
                    err
                );
                tail.needs_restore = !restored;
            }
            Err(e) => {
                log::error!(
                    target: "normfs",
                    "The fs layer could not run the commit to {} (attempt {}/{}): {}",
                    path.display(),
                    attempt + 1,
                    settings.max_retries,
                    e
                );
            }
        }
        if attempt + 1 < settings.max_retries {
            tokio::time::sleep(settings.retry_delay).await;
        }
    }
    false
}

#[derive(Debug)]
struct PendingAck {
    queue_id: QueueId,
    entry_id: UintN,
    buffered: bool,
}

#[derive(Debug)]
struct WriterState {
    buffer: BytesMut,
    /// One per entry handed to this writer, in the order they were handed
    /// over, with `buffered` saying whether its bytes went into `buffer` or
    /// stayed in the pool's pages. An ack is a durability watermark, so a
    /// flush may only drain the entries it has actually made durable -- and
    /// which flush that is depends on where the bytes were.
    acks: Vec<PendingAck>,
    current_size: u64,
}

pub struct AckFileWriter {
    settings: AckFileWriterSettings,
    state: Arc<Mutex<WriterState>>,
    writer_handle: Mutex<Option<JoinHandle<bool>>>,
    shutdown_tx: mpsc::Sender<()>,
    buffer_full_notify: Arc<Notify>,
    pooled: bool,
    /// The pool this writer drains, so `write_maybe_pooled` can tell it which
    /// entries it has taken responsibility for.
    pool: Option<Arc<PagePool>>,
    /// Shared with the writer task, so where the file validly ends is still
    /// answerable once that task has finished.
    tail: Arc<Mutex<FileTail>>,
}

/// Writes out everything the pool has appended since the last flush, then
/// reports what is now durable.
///
/// The bytes come straight from the pages the records were written into, so
/// there is no second copy of the data on its way to the file — which is the
/// point of the pool holding pages rather than the writer holding a buffer.
///
/// `mark_durable` is called **only** after both the write and the sync have
/// succeeded, and only for the ids that run actually covered. That ordering is
/// the whole guarantee: the watermark is what allows the C ring to hand a page
/// back to be overwritten, so advancing it early would let a record be lost
/// between being accepted and being on disk.
async fn flush_pool(
    fs: &Fs,
    path: &Path,
    file: &Arc<Mutex<FileTail>>,
    state: &Arc<Mutex<WriterState>>,
    ack_sender: &mpsc::UnboundedSender<(QueueId, UintN)>,
    pool: &Arc<PagePool>,
    epoch: u64,
    settings: &AckFileWriterSettings,
) -> bool {
    let pending = pool.take_pending(epoch);
    if pending.is_empty() {
        return true;
    }

    let total: u64 = pending.iter().map(|(_, b)| b.len() as u64).sum();
    let last = pending
        .last()
        .map(|(w, _)| w.last_entry_id)
        .expect("pending is non-empty");
    log::debug!(
        target: "normfs",
        "Writing {} run(s), {} bytes, to {}",
        pending.len(),
        total,
        path.display()
    );

    // Until the commit, take_pending hands the same runs out again by itself.
    let mut tail_guard = file.lock().await;
    let runs = Runs(pending.iter().map(|(_, b)| b.clone()).collect());
    let flushed = commit(fs, path, &mut tail_guard, runs, settings).await;
    if !flushed {
        log::error!(
            target: "normfs",
            "Every attempt to write entries ..={last} to {} failed; they stay pending for \
             the next flush",
            path.display()
        );
        return false;
    }
    for (write, _) in &pending {
        pool.commit_written(write);
    }
    drop(tail_guard);

    pool.mark_durable(last.saturating_add(1));

    // The acks belong here too. `write_maybe_pooled` records one per entry
    // whether or not the bytes were buffered, but only `flush_buffer` used to
    // drain them — and it returns early on an empty buffer, which on the pooled
    // path it always is. So without this the list grows without bound and the
    // memory store is never told anything reached disk.
    //
    // Only entries this flush actually made durable, and only the last of them:
    // the ack is a watermark, and the reader of this channel takes the highest
    // it has seen.
    let ack = {
        let mut state_guard = state.lock().await;
        let cut = state_guard
            .acks
            .iter()
            .position(|a| !a.entry_id.to_u64().is_ok_and(|v| v <= last))
            .unwrap_or(state_guard.acks.len());
        let mut done: Vec<PendingAck> = state_guard.acks.drain(0..cut).collect();
        done.pop()
    };
    // An ack is "durable through here" and its reader keeps the highest it has
    // seen, so this file's fsync must not speak for an earlier one whose tail
    // is still in a retry queue.
    let ack = ack.filter(|a| match pool.stranded_from() {
        Some(first) => a.entry_id.to_u64().is_ok_and(|id| id < first),
        None => true,
    });
    if let Some(ack) = ack
        && ack_sender.send((ack.queue_id, ack.entry_id)).is_err()
    {
        log::error!(
            target: "normfs",
            "Failed to report entries in {} durable: the ack channel is closed",
            path.display(),
        );
    }
    true
}

impl AckFileWriter {
    pub async fn new(
        fs: Fs,
        path: impl AsRef<Path>,
        settings: AckFileWriterSettings,
        ack_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
        header: Bytes,
        pool: Option<Arc<PagePool>>,
        epoch: u64,
    ) -> std::io::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            fs.mkdir_all(parent).await?;
        }

        // Only successful creation certifies the header; recovery must still
        // accept a torn header left by a crash before creation completed.
        let initial_size = header.len() as u64;
        let (file, inode) = fs
            .create_durable_with_inode(
                path.as_ref(),
                Runs(vec![header]),
                TmpMode::Trunc,
                settings.fsync,
            )
            .await?;
        let file = Arc::new(file);

        let path = path.as_ref().to_path_buf();

        let state = Arc::new(Mutex::new(WriterState {
            buffer: BytesMut::with_capacity(settings.max_buffer_size),
            acks: Vec::new(),
            current_size: initial_size,
        }));
        let buffer_full_notify = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let pooled = pool.is_some();
        // The pooled path never fills `buffer`, so the interval tick would
        // otherwise be the only thing that can start a flush.
        if let Some(p) = pool.as_ref() {
            p.set_flush_signal(buffer_full_notify.clone());
        }
        let tail = Arc::new(Mutex::new(FileTail {
            file,
            inode,
            flushed_len: initial_size,
            needs_restore: false,
        }));
        let writer_handle = tokio::spawn(writer_task(
            fs.clone(),
            path.clone(),
            tail.clone(),
            state.clone(),
            settings.clone(),
            shutdown_rx,
            buffer_full_notify.clone(),
            ack_sender,
            pool.clone(),
            epoch,
        ));

        Ok(Self {
            settings,
            state,
            writer_handle: Mutex::new(Some(writer_handle)),
            shutdown_tx,
            buffer_full_notify,
            pooled,
            pool,
            tail,
        })
    }

    pub(crate) async fn flushed_len(&self) -> u64 {
        self.tail.lock().await.flushed_len
    }

    pub async fn can_add(&self, size: usize) -> bool {
        let state = self.state.lock().await;
        state.current_size + (size as u64) <= self.settings.max_file_size
    }

    /// Records that an entry belongs to this file, buffering its bytes.
    pub async fn write(&self, queue_id: QueueId, entry_id: UintN, entry: Bytes) {
        self.write_maybe_pooled(queue_id, entry_id, entry, false)
            .await
    }

    /// `in_pool` says the record's bytes are the pool's to hand over, so they
    /// must not be buffered again here. With a pool that is every record: one
    /// no page can hold was refused before it was given an id.
    ///
    /// A pooled record is not counted into `current_size` either. That counter
    /// exists only to answer `can_add`, and `can_add` is not consulted on the
    /// pooled path: the pool decides rotation, because it is the only side that
    /// runs before the bytes enter a page. Keeping a second, unread counter in
    /// step with the pool's would be a source of drift and no source of truth,
    /// so there is exactly one fill accounting per path and they never meet.
    ///
    /// Taking responsibility is `PagePool::note_handed_over`, which the WAL
    /// writer calls once per batch after handing every entry here: the pool
    /// writes nothing above that mark, so a record still on its way cannot be
    /// overtaken by the pages behind it.
    pub async fn write_maybe_pooled(
        &self,
        queue_id: QueueId,
        entry_id: UintN,
        entry: Bytes,
        in_pool: bool,
    ) {
        let mut state = self.state.lock().await;

        let buffered = !(self.pooled && in_pool);
        if buffered {
            state.buffer.extend_from_slice(&entry);
            state.current_size += entry.len() as u64;
        }
        state.acks.push(PendingAck {
            queue_id,
            entry_id,
            buffered,
        });

        if state.buffer.len() >= self.settings.max_buffer_size {
            self.buffer_full_notify.notify_one();
        }
    }

    pub async fn close(&mut self) -> std::io::Result<()> {
        let _ = self.shutdown_tx.send(()).await;
        if let Some(handle) = self.writer_handle.lock().await.take() {
            let clean = handle.await.map_err(std::io::Error::other)?;
            if !clean {
                return Err(std::io::Error::other(
                    "the closing flush could not write everything owed to the file",
                ));
            }
        }
        Ok(())
    }
}

async fn writer_task(
    fs: Fs,
    path: PathBuf,
    file: Arc<Mutex<FileTail>>,
    state: Arc<Mutex<WriterState>>,
    settings: AckFileWriterSettings,
    mut shutdown_rx: mpsc::Receiver<()>,
    buffer_full_notify: Arc<Notify>,
    ack_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    pool: Option<Arc<PagePool>>,
    epoch: u64,
) -> bool {
    let mut interval = tokio::time::interval(settings.write_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    loop {
        tokio::select! {
            biased;
            _ = shutdown_rx.recv() => {
                // The task's return value: whether the closing flush wrote
                // everything owed. Mid-life flushes may fail and retry, but a
                // failure here has no next flush behind it, and `close()`
                // must not report a file complete that is missing its tail.
                return flush(&fs, &path, &file, &state, &ack_sender, &pool, epoch, &settings).await;
            }
            _ = buffer_full_notify.notified() => {
                let _ = flush(&fs, &path, &file, &state, &ack_sender, &pool, epoch, &settings).await;
            }
            // The timer is what a queue nobody writes to often depends on:
            // nothing else on this path can start a flush, so one record on an
            // otherwise silent queue reaches disk a write_interval later rather
            // than when the buffer or the pool fills. It is therefore left
            // free-running rather than armed by a write -- and kept cheap
            // instead, by asking whether there is anything to do before taking
            // the pool lock and walking every page.
            _ = interval.tick() => {
                if has_pending(&state, &pool).await {
                    let _ = flush(&fs, &path, &file, &state, &ack_sender, &pool, epoch, &settings).await;
                }
            }
        }
    }
}

/// Whether either store owes the file bytes, or the channel an ack. All
/// cheap: two lengths and a counter.
async fn has_pending(state: &Arc<Mutex<WriterState>>, pool: &Option<Arc<PagePool>>) -> bool {
    {
        let state = state.lock().await;
        // A leading buffered ack counts as pending on its own: its bytes may
        // already be durable, and only a flush's empty-buffer pass will send
        // it. A pooled ack at the head does not count -- no flush can move it
        // until the pool does, and the pool check below says whether it will.
        if !state.buffer.is_empty() || buffered_run_len(&state) > 0 {
            return true;
        }
    }
    pool.as_ref().is_some_and(|p| p.has_pending())
}

/// Sends the flush to whichever holds the unwritten bytes: the pool if this
/// writer was given one, the entry buffer otherwise. False when something
/// owed to the file is still unwritten.
async fn flush(
    fs: &Fs,
    path: &Path,
    file: &Arc<Mutex<FileTail>>,
    state: &Arc<Mutex<WriterState>>,
    ack_sender: &mpsc::UnboundedSender<(QueueId, UintN)>,
    pool: &Option<Arc<PagePool>>,
    epoch: u64,
    settings: &AckFileWriterSettings,
) -> bool {
    // Buffer first, then pages. With a pool the buffer is always empty --
    // everything the pool accepted comes out of `take_pending`, in one id
    // order -- so this is the unpooled path only, and the two never mix within
    // a file.
    //
    // The early return is not tidiness. `flush_buffer` returns its bytes to the
    // head of the buffer when every attempt failed, so they are still owed to
    // the file; going on to write pages would put later records in front of
    // them, and V1's positional ids would hand every payload after that point
    // out under the wrong id.
    if !flush_buffer(fs, path, file, state, ack_sender, settings).await {
        return false;
    }
    if let Some(pool) = pool {
        return flush_pool(fs, path, file, state, ack_sender, pool, epoch, settings).await;
    }
    true
}

/// False when the buffer still owes the file bytes, so nothing may be written
/// after them.
async fn flush_buffer(
    fs: &Fs,
    path: &Path,
    file: &Arc<Mutex<FileTail>>,
    state: &Arc<Mutex<WriterState>>,
    ack_sender: &mpsc::UnboundedSender<(QueueId, UintN)>,
    settings: &AckFileWriterSettings,
) -> bool {
    let (data_to_write, ack_cut) = {
        let mut state_guard = state.lock().await;
        if state_guard.buffer.is_empty() {
            // Nothing owed. Any buffered entry still at the head of the list
            // was written and synced by an earlier flush -- a failed one puts
            // its bytes back, so an empty buffer means every buffered byte
            // reached the file -- and reporting it here is what stops it
            // waiting for a flush that has no work of its own to do.
            let cut = buffered_run_len(&state_guard);
            let ack = take_buffered_ack(&mut state_guard, cut);
            drop(state_guard);
            send_ack(ack, ack_sender);
            return true;
        }
        // Under the same lock as the split, so the cut names exactly the
        // entries whose bytes are in `data_to_write`. Anything buffered during
        // the file I/O below waits for the flush that writes its bytes.
        let cut = buffered_run_len(&state_guard);
        (state_guard.buffer.split().freeze(), cut)
    };

    if data_to_write.is_empty() {
        return true;
    }

    log::debug!(
        target: "normfs",
        "Writing to file {}, block size: {}",
        path.display(),
        data_to_write.len()
    );

    let mut tail_guard = file.lock().await;
    let write_successful = commit(
        fs,
        path,
        &mut tail_guard,
        Runs(vec![data_to_write.clone()]),
        settings,
    )
    .await;
    drop(tail_guard);

    if write_successful {
        let mut state_guard = state.lock().await;
        let ack = take_buffered_ack(&mut state_guard, ack_cut);
        drop(state_guard);
        send_ack(ack, ack_sender);
    } else {
        log::error!(target: "normfs", "All write attempts failed. Returning data to buffer to prevent loss.");
        // Only the bytes come back. The acks were never taken: they are drained
        // once the bytes they describe are on disk and not before, so a failed
        // attempt has nothing to undo.
        let mut state_guard = state.lock().await;
        let mut new_buffer =
            BytesMut::with_capacity(data_to_write.len() + state_guard.buffer.len());
        new_buffer.extend_from_slice(&data_to_write);
        new_buffer.extend_from_slice(&state_guard.buffer);
        state_guard.buffer = new_buffer;
    }

    write_successful
}

/// How many acks at the head of the list describe buffered bytes. Only valid
/// under the state lock; callers capture it beside the `split()` rather than
/// recomputing it after the file I/O.
fn buffered_run_len(state: &WriterState) -> usize {
    state
        .acks
        .iter()
        .position(|a| !a.buffered)
        .unwrap_or(state.acks.len())
}

/// Takes the first `cut` acks and returns the highest of them to report.
///
/// An ack is a watermark, so it may only ever name bytes the file already
/// has. `cut` was captured at split time, which keeps out entries buffered
/// during the flush's file I/O, and the run stops at the first pooled entry,
/// which keeps out records still sitting in their pages.
fn take_buffered_ack(state: &mut WriterState, cut: usize) -> Option<PendingAck> {
    let mut done: Vec<PendingAck> = state.acks.drain(0..cut).collect();
    done.pop()
}

fn send_ack(ack: Option<PendingAck>, ack_sender: &mpsc::UnboundedSender<(QueueId, UintN)>) {
    if let Some(ack) = ack
        && let Err(e) = ack_sender.send((ack.queue_id, ack.entry_id))
    {
        log::debug!(target: "normfs", "FATAL: Failed to send ack for written data: {}. The application integrity is compromised.", e);
    }
}

impl Drop for AckFileWriter {
    fn drop(&mut self) {
        // Ask for the shutdown flush and let the task run it. Aborting here
        // killed it before it could be polled once after the request, so the
        // flush never ran -- and on the pooled path everything not yet written
        // is in the pages, not in a buffer.
        let _ = self.shutdown_tx.try_send(());
        if let Ok(mut guard) = self.writer_handle.try_lock() {
            let _ = guard.take();
        }
    }
}
