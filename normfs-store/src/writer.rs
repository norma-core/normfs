use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, broadcast, mpsc};
use uintn::UintN;

use crate::WalFile;
use crate::ranges::RangeStore;
use crate::store_file;
use normfs_wal::WalStore;

pub struct StoreWriteWorker {
    root_dir: PathBuf,
    wal_store: Arc<WalStore>,
    range_store: Arc<RangeStore>,
    crypto_ctx: Arc<CryptoContext>,
    shutting_down: Arc<AtomicBool>,
}

impl StoreWriteWorker {
    pub fn new(
        root_dir: PathBuf,
        crypto_ctx: Arc<CryptoContext>,
        wal_store: Arc<WalStore>,
        range_store: Arc<RangeStore>,
    ) -> Self {
        Self {
            root_dir,
            wal_store,
            range_store,
            crypto_ctx,
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

        let wal_data = match self.wal_store.get_wal_file_content(queue_id, file_id).await {
            Ok(data) => {
                log::debug!(target: "normfs-store",
                    "Read WAL file for queue: {}, file_id: {:?}, entries_before: {:?}, num_entries: {:?}",
                    queue_id, file_id, data.entries_before, data.num_entries);
                data
            }
            Err(e) => {
                if !self.shutting_down.load(Ordering::Relaxed) {
                    log::error!(target: "normfs-store",
                        "Error reading WAL file for queue: {}, file_id: {:?}: {:?}",
                        queue_id, file_id, e);
                }
                return;
            }
        };

        // Off the runtime for the same reason as the page writer: a file's
        // worth of zstd and AES on a worker thread stalls every other task.
        let build = {
            let (queue_id, file_id, crypto) =
                (queue_id.clone(), file_id.clone(), self.crypto_ctx.clone());
            let (compression, encryption) = (wal_file.compression_type, wal_file.encryption_type);
            let (before, num, content) = (
                wal_data.entries_before.clone(),
                wal_data.num_entries.clone(),
                wal_data.content.clone(),
            );
            tokio::task::spawn_blocking(move || {
                store_file::build(
                    &queue_id,
                    &file_id,
                    compression,
                    encryption,
                    before,
                    num,
                    &content,
                    &crypto,
                )
            })
            .await
            .map_err(std::io::Error::other)
            .and_then(|r| r)
        };
        let sealed = match build {
            Ok(sealed) => sealed,
            Err(e) => {
                if !self.shutting_down.load(Ordering::Relaxed) {
                    log::error!(target: "normfs-store",
                        "Error sealing store file for queue: {}, file_id: {:?}: {:?}",
                        queue_id, file_id, e);
                }
                return;
            }
        };

        if let Err(e) =
            store_file::land_local(&self.root_dir, queue_id, file_id, &sealed, true).await
        {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Error writing store file for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e
                );
            }
            return;
        }

        if let Err(e) = store_done_tx.send((queue_id.clone(), file_id.clone()))
            && !self.shutting_down.load(Ordering::Relaxed)
        {
            log::error!(target: "normfs-store",
                "Failed to send store completion notification for queue: {}, file_id: {:?}: {:?}",
                queue_id, file_id, e);
        }

        let last_id = wal_data
            .entries_before
            .add(&wal_data.num_entries)
            .sub(&UintN::one());

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
            queue_id, file_id, wal_data.entries_before, last_id);

        if let Err(e) = self
            .range_store
            .record_range(queue_id, file_id, &wal_data.entries_before, &last_id)
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
                queue_id, file_id, wal_data.entries_before, last_id);
        }
    }
}
