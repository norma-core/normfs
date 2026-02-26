use bytes::{Bytes, BytesMut};
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, broadcast, mpsc};
use uintn::UintN;
use uuid::Uuid;

use crate::WalFile;
use crate::header::{CompressionType, EncryptionType, FileAuthentication, StoreHeader};
use crate::ranges::RangeStore;
use normfs_wal::{WalContent, WalStore};

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

        let data_to_write = match self.maybe_compress_and_encrypt(&wal_data.content, wal_file) {
            Ok(data) => {
                log::debug!(target: "normfs-store",
                        "Data to write for queue: {}, file_id: {:?}, size: {} bytes",
                        queue_id, file_id, data.len());
                data
            }
            Err(e) => {
                if !self.shutting_down.load(Ordering::Relaxed) {
                    log::error!(target: "normfs-store",
                        "Error compressing/encrypting data for queue: {}, file_id: {:?}: {:?}",
                        queue_id, file_id, e
                    );
                }
                return;
            }
        };

        if let Err(e) = self
            .write_store_file(wal_file, &wal_data, &data_to_write, store_done_tx)
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

        log::debug!(target: "normfs-store",
            "Successfully wrote store file for queue: {}, file_id: {:?}",
            queue_id, file_id);

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

    fn maybe_compress_and_encrypt(
        &self,
        data: &Bytes,
        wal_file: &WalFile,
    ) -> Result<Bytes, std::io::Error> {
        let mut data_to_process = data.clone();

        if wal_file.compression_type != CompressionType::None {
            log::debug!(target: "normfs-store",
                "Compressing data with {:?} for queue: {}, original size: {} bytes",
                wal_file.compression_type, wal_file.queue_id, data.len());

            let compressed_data = match wal_file.compression_type {
                CompressionType::Zstd => crate::compression::zstd_compress(data.as_ref())?,
                CompressionType::None => data.to_vec(),
                _ => {
                    return Err(std::io::Error::other(format!(
                        "Unsupported compression type: {:?}",
                        wal_file.compression_type
                    )));
                }
            };

            log::debug!(target: "normfs-store",
                "Compressed data with {:?} for queue: {}, compressed size: {} bytes, ratio: {:.2}%",
                wal_file.compression_type, wal_file.queue_id, compressed_data.len(),
                (compressed_data.len() as f64 / data.len() as f64) * 100.0);
            data_to_process = Bytes::from(compressed_data);
        }

        if wal_file.encryption_type != EncryptionType::None {
            let (nonce, ciphertext) = self
                .crypto_ctx
                .encrypt(&wal_file.queue_id, &wal_file.file_id, &data_to_process)
                .map_err(|e| std::io::Error::other(e.to_string()))?;

            let mut result = BytesMut::with_capacity(nonce.len() + ciphertext.len());
            result.extend_from_slice(&nonce);
            result.extend_from_slice(&ciphertext);

            log::debug!(target: "normfs-store",
                "Encrypted data for queue: {}, final size: {} bytes",
                wal_file.queue_id, result.len());

            data_to_process = result.freeze();
        }

        Ok(data_to_process)
    }

    async fn write_store_file(
        &self,
        wal_file: &WalFile,
        wal_data: &WalContent,
        data: &Bytes,
        store_done_tx: &mpsc::UnboundedSender<(QueueId, UintN)>,
    ) -> Result<(), std::io::Error> {
        let compression = wal_file.compression_type;
        let encryption = wal_file.encryption_type;

        let header = StoreHeader::new(
            compression,
            encryption,
            wal_data.entries_before.clone(),
            wal_data.num_entries.clone(),
        );

        let mut header_bytes = BytesMut::new();
        header.write_to_bytes(&mut header_bytes);

        let header_signature = self.crypto_ctx.sign(&header_bytes);
        let content_signature = self.crypto_ctx.sign(data);

        let file_auth =
            FileAuthentication::new(header_signature.to_bytes(), content_signature.to_bytes());

        let store_file_path = wal_file
            .queue_id
            .to_store_path(&self.root_dir, &wal_file.file_id);

        log::debug!(target: "normfs-store",
            "Writing store file for queue: {}, file_id: {:?}, path: {:?}",
            wal_file.queue_id, wal_file.file_id, store_file_path);

        if let Some(parent) = store_file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        let tmp_dir = self.root_dir.join("tmp");
        fs::create_dir_all(&tmp_dir).await?;

        let temp_file_path = tmp_dir.join(format!("{}.tmp", Uuid::new_v4()));

        log::debug!(target: "normfs-store",
            "Writing to temp file: {:?}", temp_file_path);

        // Write: FileAuthentication + StoreHeader + encrypted/compressed data
        let mut file = fs::File::create(&temp_file_path).await?;
        let mut auth_bytes = BytesMut::new();
        file_auth.write_to_bytes(&mut auth_bytes);
        file.write_all(&auth_bytes).await?;
        file.write_all(&header_bytes).await?;
        file.write_all(data).await?;
        file.sync_all().await?;

        fs::rename(&temp_file_path, &store_file_path).await?;

        log::debug!(target: "normfs-store",
            "Successfully renamed temp file to store file for queue: {}, file_id: {:?}",
            wal_file.queue_id, wal_file.file_id);

        if let Err(e) = store_done_tx.send((wal_file.queue_id.clone(), wal_file.file_id.clone())) {
            if !self.shutting_down.load(Ordering::Relaxed) {
                log::error!(target: "normfs-store",
                    "Failed to send store completion notification for queue: {}, file_id: {:?}: {:?}",
                    wal_file.queue_id, wal_file.file_id, e);
            }
        } else {
            log::debug!(target: "normfs-store",
                "Sent store completion notification for queue: {}, file_id: {:?}",
                wal_file.queue_id, wal_file.file_id);
        }

        Ok(())
    }
}
