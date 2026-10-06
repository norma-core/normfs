use std::path::{Path, PathBuf};
use std::time::Duration;

use std::sync::Arc;

use crate::ack_file_writer::{AckFileWriter, AckFileWriterSettings};
use crate::page_pool::{PagePool, Placement, RotateHint};
use crate::wal_entry_v1::{self, WalEntryV1, WalEntryV1Error};
use crate::wal_header::WalHeader;
use crate::wal_header_v1::WalHeaderV1;
use crate::writer_buffer::OrderedBuffer;
use crate::{WalError, WalFile, WalSettings};
use bytes::{Bytes, BytesMut};
use normfs_types::QueueId;
use tokio::sync::{mpsc, oneshot};
use uintn::UintN;

enum WriteRequest {
    Enqueue(UintN, Bytes, Placement),
    EnqueueBatch(Vec<(UintN, Bytes, Placement)>),
    Close(oneshot::Sender<bool>),
}

#[derive(Clone)]
pub struct WalWriter {
    write_chan: mpsc::UnboundedSender<WriteRequest>,
    /// Set by `close()` before its request is sent: the request cannot reach
    /// a task that is inside the rotation retry loop, but this can.
    closing: Arc<std::sync::atomic::AtomicBool>,
}

struct WriterState {
    fs: normfs_fs::Fs,
    queue_path: PathBuf,
    queue_id: QueueId,
    file_id: UintN,
    header: WalHeader,
    settings: WalSettings,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    wal_complete_sender: mpsc::UnboundedSender<WalFile>,
    file_writer: AckFileWriter,
    has_written: bool,
    // 0-based index of the next entry within the current file. V1 does not
    // store the entry id; the reader derives it as num_entries_before + index,
    // so this must count exactly the entries written to this file and reset on
    // rotation. Unused by the V0 path.
    entry_index: u64,
    buffer: OrderedBuffer,
    // Handed to every file writer this queue opens, including after a
    // rotation, so the bytes always come from the pages the records were
    // appended into rather than from a copy.
    pool: Option<Arc<PagePool>>,
    // Which file the writer believes it is on, counted from zero. The enqueue
    // side counts the same thing independently; comparing them turns a silent
    // disagreement — which writes a file whose entry ids do not line up with
    // its header — into something greppable.
    file_epoch: u64,
    // Set by `WalWriter::close`; the rotation retry loop checks it, because
    // that loop runs on the only task that could dequeue the close request.
    closing: Arc<std::sync::atomic::AtomicBool>,
    // A rotation stood down for a close, so this writer has no file. Entries
    // that still arrive are dropped loudly.
    rotation_abandoned: bool,
    // Spawned the first time a file is left owing records; outlives this task,
    // so a close that reports itself incomplete can be followed by one that
    // succeeds.
    drainer: Option<mpsc::UnboundedSender<crate::drainer::DrainRequest>>,
}

impl WalWriter {
    pub async fn new(
        fs: normfs_fs::Fs,
        queue: &QueueId,
        root: &Path,
        file_id: &UintN,
        header: WalHeader,
        settings: WalSettings,
        written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
        wal_complete_sender: mpsc::UnboundedSender<WalFile>,
        last_entry_id: Option<UintN>,
        pool: Option<Arc<PagePool>>,
    ) -> Result<Self, WalError> {
        log::info!(
            "WAL writer: creating new writer for queue '{}', file: {}, last_entry_id: {:?}",
            queue,
            file_id,
            last_entry_id
        );

        let (tx, mut rx) = mpsc::unbounded_channel();

        let queue_fs_path = queue.to_wal_dir(root);
        fs.mkdir_all(&queue_fs_path).await?;

        // From here a file writer is draining these pages, so an appender may
        // wait for one to be freed: a flush will end the wait. And from here
        // the enqueue side owns the rotation decision, because it is the only
        // side that runs before a record's bytes enter a page.
        //
        // The header charge is the widest a V1 header can be, not this one's
        // encoded length: `WalHeader::resize` can widen the id and data size
        // fields on rotation, and the enqueue side cannot predict the next
        // header without duplicating that. Over-charging rotates a few bytes
        // early, which is the safe direction against a cap.
        if let Some(p) = pool.as_ref() {
            p.set_drainer();
            p.arm_file_fill(
                settings.max_file_size as u64,
                crate::wal_header_v1::WAL_HEADER_V1_MAX_SIZE as u64,
            );
        }

        let file_writer = new_file_writer(
            fs.clone(),
            &queue_fs_path,
            file_id,
            &header,
            &settings,
            written_sender.clone(),
            pool.clone(),
            0,
        )
        .await?;

        let closing = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut state = WriterState {
            fs,
            queue_path: queue_fs_path,
            queue_id: queue.clone(),
            file_id: file_id.clone(),
            header,
            settings,
            written_sender,
            wal_complete_sender,
            file_writer,
            has_written: false,
            entry_index: 0,
            buffer: OrderedBuffer::new(last_entry_id, queue.clone()),
            pool,
            file_epoch: 0,
            closing: Arc::clone(&closing),
            rotation_abandoned: false,
            drainer: None,
        };

        let queue_log_str = queue.to_string();
        tokio::spawn(async move {
            log::debug!(
                "WAL writer: starting writer task for queue '{}'",
                queue_log_str
            );

            while let Some(req) = rx.recv().await {
                match req {
                    WriteRequest::Enqueue(entry_id, data, placement) => {
                        if let Err(e) = state.write(entry_id, data, placement).await {
                            log::error!(
                                "WAL writer: error writing entry for queue '{}': {}",
                                state.queue_id,
                                e
                            );
                        }
                    }
                    WriteRequest::EnqueueBatch(entries) => {
                        if let Err(e) = state.write_batch(entries).await {
                            log::error!(
                                "WAL writer: error writing batch for queue '{}': {}",
                                state.queue_id,
                                e
                            );
                        }
                    }
                    WriteRequest::Close(responder) => {
                        log::info!("WAL writer: closing writer for queue '{}'", state.queue_id);
                        // Entries parked out of order never reached the file
                        // writer, and the closing flush is bounded by what did.
                        // They are lost; the least a close can do is say so.
                        if !state.buffer.pending.is_empty() {
                            let ids: Vec<String> = state
                                .buffer
                                .pending
                                .iter()
                                .map(|(id, _, _)| id.to_string())
                                .collect();
                            log::error!(
                                "WAL writer: closing queue '{}' with {} entrie(s) still \
                                 waiting for order ({}); they reach no file",
                                state.queue_id,
                                ids.len(),
                                ids.join(", ")
                            );
                        }
                        let flushed = match state.file_writer.close().await {
                            Ok(()) => true,
                            Err(e) => {
                                log::error!(
                                    "WAL writer: error closing file writer for queue '{}': {}",
                                    state.queue_id,
                                    e
                                );
                                false
                            }
                        };
                        // A clean close is this file's only chance to
                        // migrate: no writer will ever rotate it out. A file
                        // that lost entries stays in the WAL for recovery.
                        let closed_ok = flushed && state.buffer.pending.is_empty();
                        // The last file has no rotation behind it to notice what
                        // its closing flush left owed. Not awaited: this close
                        // reports itself incomplete, and a later one succeeds.
                        let owed = state.hand_off_stranded().await;
                        // Never a header-only file: the store worker would
                        // parse nothing and compute a last id from it.
                        if !owed && closed_ok && state.has_written {
                            let _ = state.wal_complete_sender.send(WalFile {
                                queue_id: state.queue_id.clone(),
                                file_id: state.file_id.clone(),
                                encryption_type: state.settings.encryption_type,
                                compression_type: state.settings.compression_type,
                            });
                        }
                        // After the closing flush, so it releases whatever is
                        // still waiting.
                        if let Some(pool) = state.pool.as_ref() {
                            pool.clear_drainer();
                        }
                        let _ = responder.send(closed_ok);
                        break;
                    }
                }
            }

            log::debug!(
                "WAL writer: writer task ended for queue '{}'",
                state.queue_id
            );
        });

        Ok(Self {
            write_chan: tx,
            closing,
        })
    }

    pub fn enqueue(
        &self,
        entry_id: UintN,
        data: Bytes,
        placement: Placement,
    ) -> Result<(), WalError> {
        log::trace!("WAL writer: enqueuing entry {} for write", entry_id);

        // A pooled record's bytes reach the file from its page, so they do not
        // ride the channel: only `placement.record_len` does. Otherwise every
        // record in flight is held twice until the writer task gets to it.
        let data = if placement.in_pool {
            Bytes::new()
        } else {
            data
        };
        self.write_chan
            .send(WriteRequest::Enqueue(entry_id, data, placement))
            .map_err(|_| WalError::SendError)
    }

    pub fn enqueue_batch(
        &self,
        mut entries: Vec<(UintN, Bytes, Placement)>,
    ) -> Result<(), WalError> {
        log::trace!(
            "WAL writer: enqueuing batch of {} entries for write",
            entries.len()
        );

        for (_, data, placement) in entries.iter_mut() {
            if placement.in_pool {
                *data = Bytes::new();
            }
        }
        self.write_chan
            .send(WriteRequest::EnqueueBatch(entries))
            .map_err(|_| WalError::SendError)
    }

    pub async fn close(&self) -> Result<(), WalError> {
        self.closing
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.write_chan
            .send(WriteRequest::Close(tx))
            .map_err(|_| WalError::SendError)?;
        match rx.await {
            Ok(true) => Ok(()),
            Ok(false) => Err(WalError::CloseIncomplete),
            Err(_) => Err(WalError::SendError),
        }
    }
}

impl WriterState {
    async fn write(
        &mut self,
        entry_id: UintN,
        data: Bytes,
        placement: Placement,
    ) -> Result<(), WalError> {
        log::debug!(
            "WAL writer: writing entry {} to queue '{}', data size: {} bytes",
            entry_id,
            self.queue_id,
            data.len()
        );

        if self.rotation_abandoned {
            log::error!(
                "WAL writer: queue '{}' entry {}: dropped -- a close abandoned a rotation, \
                 so this writer has no file to give it",
                self.queue_id,
                entry_id
            );
            return Err(WalError::IoError(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "the writer's rotation was abandoned by a close",
            )));
        }

        let entries = if self.buffer.can_write(&entry_id) {
            // direct write
            log::debug!(
                "WAL writer: entry {} is in order for queue '{}', proceeding to write",
                entry_id,
                self.queue_id
            );
            vec![(entry_id.clone(), data, placement)]
        } else {
            // buffer and get ready entries
            log::debug!(
                "WAL writer: entry {} is out of order for queue '{}', buffering",
                entry_id,
                self.queue_id
            );
            self.buffer
                .wait_for_order((entry_id.clone(), data, placement))
        };

        let mut handed_through: Option<u64> = None;
        // Held rather than returned, so the handover mark below runs whatever
        // happens in the loop. An entry the file writer has already been given
        // that the pool is never told about is a page the pool will not hand
        // over -- `take_pending` stops at the handover bound -- so it never
        // drains, never becomes reusable, and the queue stops on a full pool.
        // Failing to write one record must not cost the records behind it.
        let mut outcome: Result<(), WalError> = Ok(());
        if entries.is_empty() {
            log::debug!(
                "WAL writer: no entries ready to write for queue '{}'",
                self.queue_id
            );
            return Ok(());
        }

        for (entry_id, data, placement) in entries {
            // A pooled record arrives without its payload; the placement
            // carries the length instead.
            let data_len = if placement.in_pool {
                placement.record_len
            } else {
                data.len()
            };

            // Rotation is decided on the encoded length, not the record length:
            // the varint prefix and CRC also have to fit. A record wider than a
            // u32 has no frame at all, so it is an error rather than a rotation.
            let record_size = match u32::try_from(data_len) {
                Ok(size) => size,
                Err(_) => {
                    outcome = Err(WalError::WalEntryV1Error(WalEntryV1Error::RecordTooLarge(
                        data_len,
                    )));
                    break;
                }
            };

            // With a pool the decision was already taken, at enqueue time and
            // before these bytes entered a page — so the writer carries it out
            // rather than making it again. Re-deciding here from `can_add`
            // would decide on accounting the pooled path does not maintain,
            // and, worse, decide it after the record was already placed.
            let must_rotate = match placement.rotate {
                RotateHint::WriterDecides => {
                    self.has_written
                        && !self
                            .file_writer
                            .can_add(wal_entry_v1::encoded_len(record_size))
                            .await
                }
                RotateHint::Before => true,
                RotateHint::None => false,
            };

            self.check_epoch(&placement, &entry_id);

            if must_rotate {
                log::debug!(
                    "WAL writer: need to rotate file for queue '{}', entry {}, data size: {}",
                    self.queue_id,
                    entry_id,
                    data_len
                );
                // The mark below the loop is too late for a rotation inside
                // it: `wait_for_order` releases several entries at once, and
                // the ones already handed to this file would still sit behind
                // the pool's old bound, so the closing flush would cut them
                // off and report the file complete without them.
                if let (Some(pool), Some(id)) = (self.pool.as_ref(), handed_through) {
                    pool.note_handed_over(id);
                }
                if !self.rotate(entry_id.clone(), data_len).await {
                    self.rotation_abandoned = true;
                    outcome = Err(WalError::IoError(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "the writer's rotation was abandoned by a close",
                    )));
                    break;
                }
            } else {
                log::debug!(
                    "WAL writer: current file {} for queue '{}' can hold entry {}, data size: {}",
                    self.file_id,
                    self.queue_id,
                    entry_id,
                    data_len
                );
            }

            // [record_size varint32][record][crc32c u32 LE] — the id is not
            // stored; the reader derives it from num_entries_before + index.
            //
            // A record in a page is already framed by the same codec, in the
            // bytes the flush takes, and `write_maybe_pooled` drops this one
            // unread.
            let pooled = self.pool.is_some() && placement.in_pool;
            let entry_buf = if pooled {
                Bytes::new()
            } else {
                let mut entry_buf = BytesMut::new();
                if let Err(e) = WalEntryV1::new(&data).write_to_bytes(&mut entry_buf) {
                    outcome = Err(e.into());
                    break;
                }
                entry_buf.freeze()
            };

            log::debug!(
                "WAL writer: writing entry {} to file {} for queue '{}', total size: {} bytes",
                entry_id,
                self.file_id,
                self.queue_id,
                entry_buf.len()
            );

            // Ids are positional. Guard that the caller's id is exactly
            // num_entries_before + index — the equality the reader depends on.
            #[cfg(debug_assertions)]
            if let (Ok(eid), Ok(base)) =
                (entry_id.to_u64(), self.header.num_entries_before.to_u64())
            {
                debug_assert_eq!(
                    eid,
                    base + self.entry_index,
                    "entry id must equal num_entries_before + index"
                );
            }

            self.file_writer
                .write_maybe_pooled(
                    self.queue_id.clone(),
                    entry_id.clone(),
                    entry_buf,
                    placement.in_pool,
                )
                .await;

            if let Ok(id) = entry_id.to_u64() {
                handed_through = Some(handed_through.map_or(id, |h: u64| h.max(id)));
            }

            // Update last written entry ID
            self.has_written = true;
            self.entry_index += 1;

            log::debug!(
                "WAL writer: successfully wrote entry {} to queue '{}'",
                entry_id,
                self.queue_id
            );
        }

        // One mark for the whole batch, not one per record: the pool lock is
        // the producer's hot lock, and `note_handed_over` only ever needs the
        // highest id. A flush landing mid-batch sees the previous mark and
        // writes less, which is the safe direction.
        if let (Some(pool), Some(id)) = (self.pool.as_ref(), handed_through) {
            pool.note_handed_over(id);
        }

        outcome
    }

    /// Hands the file this writer is leaving to the retrier if it owes
    /// anything. The answer is also "may this file be reported complete": a
    /// file missing records must not reach the store worker, which archives it
    /// and deletes it from the WAL.
    ///
    /// The evidence is the pool's, not `close()`'s verdict, which only reports
    /// a flush that ran and gave up. A file can be left owing records without
    /// that -- see the handover mark before the rotation above.
    async fn hand_off_stranded(&mut self) -> bool {
        let Some(pool) = self.pool.as_ref() else {
            return false;
        };
        let Some(stranded) = pool.take_stranded(self.file_epoch) else {
            return false;
        };

        log::error!(
            "WAL writer: queue '{}': file {} is leaving entries {}..={} unwritten; they are \
             held for retry rather than reported complete, and this queue cannot certify \
             them durable until the retry lands",
            self.queue_id,
            self.file_id,
            stranded.first_entry_id,
            stranded.last_entry_id
        );

        let file = Box::new(crate::drainer::StrandedFile {
            queue_id: self.queue_id.clone(),
            file_id: self.file_id.clone(),
            epoch: self.file_epoch,
            path: self
                .file_id
                .to_file_path(self.queue_path.to_str().unwrap(), "wal"),
            valid_len: self.file_writer.flushed_len().await,
            wal_file: WalFile {
                queue_id: self.queue_id.clone(),
                file_id: self.file_id.clone(),
                encryption_type: self.settings.encryption_type,
                compression_type: self.settings.compression_type,
            },
            fsync: self.settings.enable_fsync,
            stranded,
        });

        let drainer = self.drainer.get_or_insert_with(|| {
            crate::drainer::spawn(
                self.fs.clone(),
                Arc::clone(pool),
                self.wal_complete_sender.clone(),
                self.written_sender.clone(),
            )
        });
        if drainer
            .send(crate::drainer::DrainRequest::Retry(file))
            .is_err()
        {
            log::error!(
                "WAL writer: queue '{}': the retrier for file {} is gone; its records reach \
                 no file",
                self.queue_id,
                self.file_id
            );
        }
        true
    }

    /// Checks the writer's idea of which file it is on against the enqueue
    /// side's.
    ///
    /// Purely a tripwire: the two sides count files independently, and if they
    /// ever disagree the writer produces a file whose entry ids do not line up
    /// with its `num_entries_before` — which the reader turns into silently
    /// wrong data rather than an error. Cheap to check, so check it.
    fn check_epoch(&self, placement: &Placement, entry_id: &UintN) {
        let expected = match placement.rotate {
            RotateHint::WriterDecides => return,
            RotateHint::Before => self.file_epoch + 1,
            RotateHint::None => self.file_epoch,
        };
        if placement.epoch != expected {
            // The hint is still obeyed. Falling back to `can_add` would be
            // worse, not better: on the pooled path `current_size` is not
            // maintained, so it would answer "yes, there is room" forever and
            // every later boundary would be wrong too, rather than just this
            // one.
            log::error!(
                "WAL writer: queue '{}' entry {}: the enqueue side charged this record to file \
                 {} but the writer is on file {} (expected {}). The two sides have diverged, so \
                 this file's entry ids may not line up with its header.",
                self.queue_id,
                entry_id,
                placement.epoch,
                self.file_epoch,
                expected,
            );
            debug_assert_eq!(
                placement.epoch, expected,
                "enqueue-side and writer-side file counters diverged"
            );
        }
    }

    async fn write_batch(
        &mut self,
        entries: Vec<(UintN, Bytes, Placement)>,
    ) -> Result<(), WalError> {
        log::debug!(
            "WAL writer: writing batch of {} entries to queue '{}'",
            entries.len(),
            self.queue_id
        );

        // Every entry is attempted, and the first failure is what the batch
        // reports. Stopping at it would leave the entries behind it in their
        // pages with nothing ever handing them over, so one unwritable record
        // would take the rest of the batch -- and the queue -- with it.
        let mut outcome = Ok(());
        for (entry_id, data, placement) in entries {
            if let Err(e) = self.write(entry_id, data, placement).await {
                log::error!(
                    "WAL writer: error writing an entry of a batch for queue '{}': {}",
                    self.queue_id,
                    e
                );
                if outcome.is_ok() {
                    outcome = Err(e);
                }
            }
        }

        log::debug!(
            "WAL writer: completed batch write to queue '{}'",
            self.queue_id
        );

        outcome
    }

    /// Rotates onto the next file. Does not fail, and must not.
    ///
    /// The enqueue side decided this rotation before the record's bytes entered
    /// a page: it has already advanced its own file counter and stamped the
    /// pages with it. So the two sides are only in step again once the writer
    /// has arrived at the same file, and there is no consistent state to roll
    /// back *to* -- the pages carry the new epoch whatever the writer does, and
    /// the enqueue side has run on ahead over an unbounded channel.
    ///
    /// Leaving them apart is the worst of the options and used to be what
    /// happened: `take_pending` skips pages stamped above the writer's epoch,
    /// so every one of them would be skipped for ever. They never reach the
    /// file, never become durable, never become reusable, and the queue stops
    /// on a full pool that nothing can drain.
    ///
    /// So this converges forward instead, and waits as long as it has to. A
    /// queue held up here is back-pressure of the same kind a full pool is:
    /// appenders wait, and nothing is discarded or written out of order.
    ///
    /// A close is the one thing that interrupts the wait: its request sits in
    /// the channel this task is not reading, so the retry loop checks the
    /// flag `close()` sets instead. False means the rotation stood down and
    /// the writer has no file; the caller must stop handing it entries.
    async fn rotate(&mut self, next_entry_id: UintN, next_data_size: usize) -> bool {
        log::info!(
            "WAL writer: rotating file for queue '{}', current file: {}, next entry: {}",
            self.queue_id,
            self.file_id,
            next_entry_id
        );

        // A failed close is the old file's final flush not completing, and its
        // records are `take_pending`'s "already closed" case, which says so.
        // The rotation still has to happen: stopping here would leave the two
        // sides on different files for ever, which loses this file's records
        // *and* every later file's.
        let closed_ok = match self.file_writer.close().await {
            Ok(()) => true,
            Err(e) => {
                log::error!(
                    "WAL writer: queue '{}': closing file {} failed ({}); its unflushed records \
                     reach no file. Rotating anyway -- staying on a file the enqueue side has \
                     already moved past would stall the queue for good.",
                    self.queue_id,
                    self.file_id,
                    e
                );
                false
            }
        };
        // Withheld while anything is owed; the retrier sends it once the tail
        // lands. Only this file's waits -- archiving is what frees WAL space,
        // and a disk that just failed a write is the worst place to stop.
        let owed = self.hand_off_stranded().await;
        if !owed && closed_ok {
            let _ = self.wal_complete_sender.send(WalFile {
                queue_id: self.queue_id.clone(),
                file_id: self.file_id.clone(),
                encryption_type: self.settings.encryption_type,
                compression_type: self.settings.compression_type,
            });
        } else if !owed {
            log::error!(
                "WAL writer: queue '{}': file {} is not reported complete; it stays in the \
                 WAL for recovery and reads",
                self.queue_id,
                self.file_id
            );
        }

        let old_file_id = self.file_id.clone();
        self.file_id = self.file_id.increment();
        self.header = self.header.resize(&next_entry_id, next_data_size);
        self.header.num_entries_before = next_entry_id.clone();

        log::debug!(
            "WAL writer: creating new file {} for queue '{}', entries before: {}, id size bytes: {}, data size bytes: {}",
            self.file_id,
            self.queue_id,
            self.header.num_entries_before,
            self.header.id_size_bytes,
            self.header.data_size_bytes
        );

        self.has_written = false;
        self.entry_index = 0;
        self.file_epoch += 1;

        // After `close()`, and carrying the new epoch. The old file's final
        // flush ran inside `close()` and asked the pool for its own epoch's
        // pages, so it could not take this file's records however far ahead the
        // enqueue side had run — which is the whole of trap 3.
        //
        // Retried rather than reported: the counters above have already moved,
        // so returning here would leave the writer on a file it has no writer
        // for. Opening a file is the kind of failure that passes -- a full
        // disk, a descriptor limit -- so waiting for it to pass is a real
        // strategy rather than a hope.
        let mut attempt: u32 = 0;
        loop {
            match new_file_writer(
                self.fs.clone(),
                &self.queue_path,
                &self.file_id,
                &self.header,
                &self.settings,
                self.written_sender.clone(),
                self.pool.clone(),
                self.file_epoch,
            )
            .await
            {
                Ok(writer) => {
                    self.file_writer = writer;
                    break;
                }
                Err(e) => {
                    if self.closing.load(std::sync::atomic::Ordering::Relaxed) {
                        log::error!(
                            "WAL writer: queue '{}': abandoning the rotation to file {} ({}); \
                             a close is waiting on this task. Records not yet written stay \
                             unwritten.",
                            self.queue_id,
                            self.file_id,
                            e
                        );
                        return false;
                    }
                    if attempt % ROTATE_WARN_EVERY == 0 {
                        log::error!(
                            "WAL writer: queue '{}': opening file {} failed ({}); retrying. \
                             Nothing is written or lost while this queue waits, but it is \
                             not making progress either.",
                            self.queue_id,
                            self.file_id,
                            e
                        );
                    }
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(ROTATE_RETRY_DELAY).await;
                }
            }
        }

        log::info!(
            "WAL writer: successfully rotated from file {} to {} for queue '{}'",
            old_file_id,
            self.file_id,
            self.queue_id
        );
        true
    }
}

/// How long to wait between attempts to open the file a rotation needs.
const ROTATE_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Attempts between complaints, so a queue stuck here says so about every five
/// seconds rather than once or continuously.
const ROTATE_WARN_EVERY: u32 = 500;

async fn new_file_writer(
    fs: normfs_fs::Fs,
    queue_path: &Path,
    file_id: &UintN,
    header: &WalHeader,
    settings: &WalSettings,
    written_sender: mpsc::UnboundedSender<(QueueId, UintN)>,
    pool: Option<Arc<PagePool>>,
    epoch: u64,
) -> Result<AckFileWriter, WalError> {
    let file_path = file_id.to_file_path(queue_path.to_str().unwrap(), "wal");

    // A V1 header over V1 entries, so the file is self-consistent. Readers
    // dispatch on the version word, so a queue may still hold older V0 files
    // and keep reading each correctly.
    let mut header_buf = BytesMut::new();
    WalHeaderV1::from_v0(header)?.write_to_bytes(&mut header_buf)?;

    let writer = AckFileWriter::new(
        fs,
        file_path,
        AckFileWriterSettings {
            max_buffer_size: settings.write_buffer_size,
            max_file_size: settings.max_file_size as u64,
            write_interval: settings.write_interval,
            fsync: settings.enable_fsync,
            max_retries: settings.flush_max_retries,
            retry_delay: settings.flush_retry_delay,
        },
        written_sender,
        header_buf.freeze(),
        pool,
        epoch,
    )
    .await?;

    Ok(writer)
}
