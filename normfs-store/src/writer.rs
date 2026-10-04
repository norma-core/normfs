use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_types::events::EventSink;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, broadcast, mpsc};
use uintn::UintN;

use crate::pack::Packer;
use crate::ranges::RangeStore;
use crate::store_file::{self, SealedFile};
use crate::{DiskUsage, WalFile};
use normfs_wal::{PackSlot, WalStore};

/// A WAL file read into a pack slot, or the reason it was not.
enum Read {
    Slot(PackSlot, usize),
    TooLarge(usize),
}

pub struct StoreWriteWorker {
    root_dir: PathBuf,
    wal_store: Arc<WalStore>,
    range_store: Arc<RangeStore>,
    disk_usage: Arc<DiskUsage>,
    crypto_ctx: Arc<CryptoContext>,
    packer: Option<Arc<Packer>>,
    events: EventSink,
    shutting_down: Arc<AtomicBool>,
}

impl StoreWriteWorker {
    pub fn new(
        root_dir: PathBuf,
        crypto_ctx: Arc<CryptoContext>,
        wal_store: Arc<WalStore>,
        range_store: Arc<RangeStore>,
        disk_usage: Arc<DiskUsage>,
        packer: Option<Arc<Packer>>,
        events: EventSink,
    ) -> Self {
        Self {
            root_dir,
            wal_store,
            range_store,
            disk_usage,
            crypto_ctx,
            packer,
            events,
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn run(
        &self,
        rx: Arc<Mutex<mpsc::UnboundedReceiver<WalFile>>>,
        mut shutdown_rx: broadcast::Receiver<()>,
        store_done_tx: mpsc::UnboundedSender<(QueueId, UintN)>,
    ) {
        log::debug!(target: "normfs-store", "StoreWriteWorker started");

        loop {
            tokio::select! {
                biased;
                _ = shutdown_rx.recv() => {
                    log::debug!(target: "normfs-store", "StoreWriteWorker received shutdown signal");
                    self.shutting_down.store(true, Ordering::Relaxed);
                    break;
                },
                entry_opt = async { rx.lock().await.recv().await } => {
                    if let Some(entry) = entry_opt {
                        log::debug!(
                            target: "normfs-store",
                            "StoreWriteWorker received entry for queue: {}, file_id: {:?}",
                            entry.queue_id,
                            entry.file_id
                        );
                        self.process_entry(&entry, &store_done_tx).await;
                    } else {
                        log::debug!(target: "normfs-store", "StoreWriteWorker channel closed");
                        self.shutting_down.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        }

        log::debug!(target: "normfs-store", "StoreWriteWorker stopped");
    }

    async fn process_entry(
        &self,
        wal_file: &WalFile,
        store_done_tx: &mpsc::UnboundedSender<(QueueId, UintN)>,
    ) {
        let queue_id = &wal_file.queue_id;
        let file_id = &wal_file.file_id;
        log::debug!(target: "normfs-store",
            "Processing entry for queue: {}, file_id: {:?}", queue_id, file_id);

        let sealed = match &self.packer {
            Some(packer) => self.seal_in_slot(wal_file, packer).await,
            None => self.seal_read_whole(wal_file).await,
        };
        let sealed = match sealed {
            Ok(sealed) => sealed,
            Err(e) => {
                if !self.shutting_down.load(Ordering::Relaxed) {
                    log::error!(target: "normfs-store",
                        "Error sealing store file for queue: {}, file_id: {:?}: {}",
                        queue_id, file_id, e);
                }
                return;
            }
        };

        if let Err(e) = store_file::land_local(
            self.wal_store.fs(),
            &self.root_dir,
            queue_id,
            file_id,
            &sealed,
            true,
            &self.disk_usage,
        )
        .await
        {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Error writing store file for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e
                );
            }
            return;
        }
        store_file::report_stored(self.events.as_ref(), queue_id, file_id, &sealed);

        if let Err(e) = store_done_tx.send((queue_id.clone(), file_id.clone()))
            && !self.shutting_down.load(Ordering::Relaxed)
        {
            log::error!(target: "normfs-store",
                "Failed to send store completion notification for queue: {}, file_id: {:?}: {:?}",
                queue_id, file_id, e);
        }

        let entries_before = sealed.entries_before.clone();
        let last_id = entries_before.add(&sealed.num_entries).sub(&UintN::one());

        if last_id.is_err() {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Error calculating last entry id for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, last_id.err());
            }
            return;
        }
        let last_id = last_id.unwrap();

        log::debug!(target: "normfs-store",
            "Entry range for queue: {}, file_id: {:?}: {:?} to {:?}",
            queue_id, file_id, entries_before, last_id);

        if let Err(e) = self
            .range_store
            .record_range(queue_id, file_id, &entries_before, &last_id)
            .await
        {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Error recording range for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e);
            }
        } else {
            log::debug!(target: "normfs-store",
                "Recorded range for queue: {}, file_id: {:?}", queue_id, file_id);
        }

        if let Err(e) = self.wal_store.delete_wal_file(queue_id, file_id).await {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Error deleting WAL file for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e);
            }
        } else {
            log::info!(target: "normfs-store",
                "Successfully processed and deleted WAL file for queue: {}, file_id: {:?}, entries: {:?} to {:?}",
                queue_id, file_id, entries_before, last_id);
        }
    }

    /// Reads the file straight into a pack slot and seals it there. A file
    /// larger than a slot, left by a bigger `max_file_size`, is read whole.
    async fn seal_in_slot(
        &self,
        wal_file: &WalFile,
        packer: &Arc<Packer>,
    ) -> Result<SealedFile, String> {
        let (queue_id, file_id) = (&wal_file.queue_id, &wal_file.file_id);
        let slot = packer.take().await;
        let path = self.wal_store.wal_file_path(queue_id, file_id);
        let cap = packer.input_cap();
        let read = self
            .wal_store
            .fs()
            .run_blocking(move || {
                use std::os::unix::fs::FileExt;
                let file = std::fs::File::open(&path)?;
                let len = file.metadata()?.len() as usize;
                if len > cap {
                    return Ok(Read::TooLarge(len));
                }
                let mut slot = slot;
                file.read_exact_at(&mut slot.buf()[..len], 0)?;
                Ok(Read::Slot(slot, len))
            })
            .await
            .map_err(|e| format!("reading WAL file: {:?}", std::io::Error::from(e)))?;
        let (mut slot, len) = match read {
            Read::Slot(slot, len) => (slot, len),
            Read::TooLarge(len) => {
                log::warn!(target: "normfs-store",
                    "WAL file {file_id} of queue {queue_id} is {len} bytes, more than the \
                     {cap} a pack slot holds; reading it whole");
                return self.seal_read_whole(wal_file).await;
            }
        };
        let (entries_before, num_entries) = normfs_wal::count_entries(&slot.buf()[..len], file_id)
            .map_err(|e| format!("reading WAL file: {e:?}"))?;

        let (packer, queue_id, file_id, crypto) = (
            packer.clone(),
            queue_id.clone(),
            file_id.clone(),
            self.crypto_ctx.clone(),
        );
        let (compression, encryption) = (wal_file.compression_type, wal_file.encryption_type);
        tokio::task::spawn_blocking(move || {
            packer.seal(
                slot,
                len,
                &queue_id,
                &file_id,
                compression,
                encryption,
                entries_before,
                num_entries,
                &crypto,
            )
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }

    async fn seal_read_whole(&self, wal_file: &WalFile) -> Result<SealedFile, String> {
        let (queue_id, file_id) = (&wal_file.queue_id, &wal_file.file_id);
        let wal_data = self
            .wal_store
            .get_wal_file_content(queue_id, file_id)
            .await
            .map_err(|e| format!("reading WAL file: {e:?}"))?;

        // Off the runtime for the same reason as the page writer: a file's
        // worth of zstd and AES on a worker thread stalls every other task.
        let (queue_id, file_id, crypto) =
            (queue_id.clone(), file_id.clone(), self.crypto_ctx.clone());
        let (compression, encryption) = (wal_file.compression_type, wal_file.encryption_type);
        tokio::task::spawn_blocking(move || {
            store_file::build(
                &queue_id,
                &file_id,
                compression,
                encryption,
                wal_data.entries_before,
                wal_data.num_entries,
                &wal_data.content,
                &crypto,
            )
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())
    }
}
