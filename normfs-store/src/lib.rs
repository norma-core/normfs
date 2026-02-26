use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use normfs_wal::{WalError, WalFile, WalHeaderError, WalStore};
use std::path::PathBuf;
use std::{path::Path, sync::Arc};
use tokio::fs;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use uintn::paths;
use uintn::{Error as UintNError, UintN};

use crate::ranges::RangeStoreError;

mod compression;
pub mod header;
pub mod parser;
mod ranges;
mod writer;

#[cfg(test)]
mod header_test;

#[cfg(test)]
mod signature_test;

#[derive(Debug)]
pub enum StoreError {
    Io(std::io::Error),
    Header(header::StoreHeaderError),
    Decrypt,
    Wal(WalError),
    UintN(UintNError),
    Range(RangeStoreError),
    Path(paths::PathError),
    WalHeaderError(WalHeaderError),
    FileNotFound,
    SignatureVerificationFailed,
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "IO error: {}", e),
            StoreError::Header(e) => write!(f, "Store header error: {}", e),
            StoreError::Decrypt => write!(f, "Decryption error"),
            StoreError::Wal(e) => write!(f, "WAL error: {}", e),
            StoreError::UintN(e) => write!(f, "UintN error: {}", e),
            StoreError::Range(e) => write!(f, "Range store error: {}", e),
            StoreError::Path(e) => write!(f, "Path error: {}", e),
            StoreError::WalHeaderError(e) => write!(f, "WAL header error: {}", e),
            StoreError::FileNotFound => write!(f, "File not found"),
            StoreError::SignatureVerificationFailed => write!(f, "Signature verification failed"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io(e) => Some(e),
            StoreError::Header(e) => Some(e),
            StoreError::Wal(e) => Some(e),
            StoreError::UintN(e) => Some(e),
            StoreError::Range(e) => Some(e),
            StoreError::Path(e) => Some(e),
            StoreError::WalHeaderError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl From<header::StoreHeaderError> for StoreError {
    fn from(e: header::StoreHeaderError) -> Self {
        StoreError::Header(e)
    }
}

impl From<WalError> for StoreError {
    fn from(e: WalError) -> Self {
        StoreError::Wal(e)
    }
}

impl From<UintNError> for StoreError {
    fn from(e: UintNError) -> Self {
        StoreError::UintN(e)
    }
}

impl From<RangeStoreError> for StoreError {
    fn from(e: RangeStoreError) -> Self {
        StoreError::Range(e)
    }
}

impl From<paths::PathError> for StoreError {
    fn from(e: paths::PathError) -> Self {
        StoreError::Path(e)
    }
}

impl From<WalHeaderError> for StoreError {
    fn from(e: WalHeaderError) -> Self {
        StoreError::WalHeaderError(e)
    }
}

#[derive(Debug, Clone)]
pub struct StoreWriteConfig {
    pub num_workers: usize,
    pub verify_signatures: bool,
}

impl Default for StoreWriteConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            verify_signatures: false,
        }
    }
}

pub struct PersistStore {
    root: PathBuf,
    range_store: Arc<ranges::RangeStore>,
    config: StoreWriteConfig,
    crypto_ctx: Arc<CryptoContext>,
    wal_store: Arc<WalStore>,

    writer_handles: Mutex<Option<Vec<JoinHandle<()>>>>,
    shutdown_tx: Mutex<Option<broadcast::Sender<()>>>,
    store_done_tx: Option<mpsc::UnboundedSender<(QueueId, UintN)>>,
}

impl PersistStore {
    pub fn new(
        root: impl AsRef<Path>,
        config: StoreWriteConfig,
        crypto_ctx: Arc<CryptoContext>,
        wal_store: Arc<WalStore>,
    ) -> Self {
        let root_path = root.as_ref().to_path_buf();

        let tmp_dir = root_path.join("tmp");
        std::fs::create_dir_all(&tmp_dir).unwrap_or_else(|e| {
            log::warn!(target: "normfs-store", "Failed to create tmp directory: {:?}", e);
        });

        Self {
            root: root_path.clone(),
            range_store: Arc::new(ranges::RangeStore::new(
                root_path,
                crypto_ctx.clone(),
                config.verify_signatures,
            )),
            writer_handles: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            store_done_tx: None,
            config,
            crypto_ctx,
            wal_store,
        }
    }

    pub async fn start_writers(
        &mut self,
        wal_done_chan: mpsc::UnboundedReceiver<WalFile>,
    ) -> mpsc::UnboundedReceiver<(QueueId, UintN)> {
        log::debug!(target: "normfs-store", "Starting {} writer workers", self.config.num_workers);

        let mut handles = Vec::with_capacity(self.config.num_workers);
        let (shutdown_tx, _) = broadcast::channel(1);

        let wal_done_chan = Arc::new(Mutex::new(wal_done_chan));

        let (store_done_tx, store_done_rx) = mpsc::unbounded_channel();
        self.store_done_tx = Some(store_done_tx.clone());

        let worker = Arc::new(writer::StoreWriteWorker::new(
            self.root.clone(),
            self.crypto_ctx.clone(),
            self.wal_store.clone(),
            self.range_store.clone(),
        ));

        for worker_id in 0..self.config.num_workers {
            let worker = worker.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let wal_done_chan = wal_done_chan.clone();
            let store_done_tx = store_done_tx.clone();
            let handle = tokio::spawn(async move {
                log::debug!(target: "normfs-store", "Worker {} started", worker_id);
                worker.run(wal_done_chan, shutdown_rx, store_done_tx).await;
                log::debug!(target: "normfs-store", "Worker {} stopped", worker_id);
            });
            handles.push(handle);
        }

        *self.writer_handles.lock().await = Some(handles);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);

        log::info!(target: "normfs-store", "All {} writer workers started successfully", self.config.num_workers);

        store_done_rx
    }

    pub async fn close(&self) {
        log::debug!(target: "normfs-store", "Closing PersistStore, shutting down workers");

        if let Some(shutdown_tx) = self.shutdown_tx.lock().await.take() {
            let _ = shutdown_tx.send(());
            log::debug!(target: "normfs-store", "Shutdown signal sent to all workers");
        }

        if let Some(handles) = self.writer_handles.lock().await.take() {
            for (idx, handle) in handles.into_iter().enumerate() {
                let _ = handle.await;
                log::debug!(target: "normfs-store", "Worker {} joined", idx);
            }
        }

        log::info!(target: "normfs-store", "PersistStore closed successfully");
    }

    pub async fn get_queue_start(&self, queue: &QueueId) -> Result<Option<UintN>, StoreError> {
        let first_file_id = self.get_first_file_id(queue).await?;
        if let Some(first_file_id) = first_file_id {
            let range = self.get_file_range(queue, &first_file_id).await?;
            Ok(range.map(|r| r.0))
        } else {
            Ok(None)
        }
    }

    /// Get the latest Store file ID for a queue.
    pub async fn find_last_file_id(&self, queue: &QueueId) -> Result<UintN, StoreError> {
        let queue_path = queue.to_store_dir(&self.root);
        tokio::fs::create_dir_all(&queue_path).await?;

        paths::find_max_id(&queue_path, "store").map_err(StoreError::Path)
    }

    pub async fn get_file_range(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, StoreError> {
        log::debug!(target: "normfs-store", "Getting file range for queue: {}, file_id: {:?}", queue, file_id);

        let result = self
            .range_store
            .get_range(queue, file_id)
            .await
            .map_err(StoreError::from)?;

        if let Some((start, end)) = &result {
            log::debug!(target: "normfs-store", "File range for queue {} file {:?}: start={:?}, end={:?}",
                queue, file_id, start, end);
        } else {
            log::debug!(target: "normfs-store", "No range found for queue {} file {:?}", queue, file_id);
        }

        Ok(result)
    }

    /// Get the last entry ID in a specific Store file.
    /// Returns None if the file has no entries or doesn't exist.
    pub async fn get_file_end(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<UintN>, StoreError> {
        let range = self.get_file_range(queue, file_id).await?;
        Ok(range.map(|(_, last)| last))
    }

    /// Get the header from a specific Store file by decrypting/decompressing its contents.
    /// Returns None if the file doesn't exist or can't be read.
    pub async fn get_file_header(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<normfs_wal::WalHeader>, StoreError> {
        log::info!(target: "normfs-store",
            "Getting header for queue '{}', file {}",
            queue, file_id
        );

        let queue_fs_path = queue.to_store_dir(&self.root);
        let file_path = file_id.to_file_path(queue_fs_path.to_str().unwrap(), "store");

        if !file_path.exists() {
            log::info!(target: "normfs-store",
                "Store file not found for queue '{}', file {}",
                queue, file_id
            );
            return Ok(None);
        }

        let store_file_bytes = match fs::read(&file_path).await {
            Ok(bytes) => {
                if bytes.len() < 10 {
                    // File is too small to have a valid header
                    log::info!(target: "normfs-store",
                        "Store file too small for queue '{}', file {}",
                        queue, file_id
                    );
                    return Ok(None);
                }
                bytes
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::info!(target: "normfs-store",
                    "Store file not found for queue '{}', file {}",
                    queue, file_id
                );
                return Ok(None);
            }
            Err(e) => return Err(StoreError::Io(e)),
        };

        match parser::extract_wal_header(
            &store_file_bytes,
            &self.crypto_ctx,
            queue,
            file_id,
            self.config.verify_signatures,
        ) {
            Ok(wal_header) => {
                log::info!(target: "normfs-store",
                    "Store file {} for queue '{}' - data_size={}, id_size={}, entries_before={}",
                    file_id, queue,
                    wal_header.data_size_bytes,
                    wal_header.id_size_bytes,
                    wal_header.num_entries_before
                );
                Ok(Some(wal_header))
            }
            Err(e) => {
                log::warn!(target: "normfs-store",
                    "Failed to extract WAL header from Store file {} for queue '{}': {}",
                    file_id, queue, e
                );
                // Return None instead of error - file might be corrupted
                Ok(None)
            }
        }
    }

    pub async fn get_first_file_id(&self, queue: &QueueId) -> Result<Option<UintN>, StoreError> {
        log::debug!(target: "normfs-store", "Getting first file ID for queue: {}", queue);

        let queue_fs_path = queue.to_store_dir(&self.root);
        tokio::fs::create_dir_all(&queue_fs_path).await?;

        match paths::find_min_id(&queue_fs_path, "store") {
            Ok(id) => {
                log::debug!(target: "normfs-store", "First file ID for queue {}: {:?}", queue, id);
                Ok(Some(id))
            }
            Err(paths::PathError::NoFilesFound) => {
                log::debug!(target: "normfs-store", "No store files found for queue: {}", queue);
                Ok(None)
            }
            Err(e) => {
                log::error!(target: "normfs-store", "Error finding first file ID for queue {}: {:?}", queue, e);
                Err(StoreError::Path(e))
            }
        }
    }

    pub async fn get_last_file_id(&self, queue: &QueueId) -> Result<Option<UintN>, StoreError> {
        log::debug!(target: "normfs-store", "Getting last file ID for queue: {}", queue);

        let queue_fs_path = queue.to_store_dir(&self.root);
        tokio::fs::create_dir_all(&queue_fs_path).await?;

        let last_file = match paths::find_max_id(&queue_fs_path, "store") {
            Err(paths::PathError::NoFilesFound) => {
                log::debug!(target: "normfs-store", "No store files found for queue: {}", queue);
                return Ok(None);
            }
            Err(e) => {
                log::error!(target: "normfs-store", "Error finding last file ID for queue {}: {:?}", queue, e);
                return Err(StoreError::Path(e));
            }
            Ok(file) => {
                log::debug!(target: "normfs-store", "Last file ID for queue {}: {:?}", queue, file);
                file
            }
        };

        Ok(Some(last_file))
    }

    pub async fn recover(&self) -> Result<(), StoreError> {
        log::info!(target: "normfs-store", "Starting recovery: cleaning up temp folder");

        let temp_dir = self.root.join("tmp");

        if !temp_dir.exists() {
            log::debug!(target: "normfs-store", "Temp directory does not exist, nothing to recover");
            return Ok(());
        }

        let mut entries = fs::read_dir(&temp_dir).await?;
        let mut cleaned_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Only remove files with .tmp extension
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                match fs::remove_file(&path).await {
                    Ok(_) => {
                        log::debug!(target: "normfs-store", "Removed temp file: {:?}", path);
                        cleaned_count += 1;
                    }
                    Err(e) => {
                        log::warn!(target: "normfs-store", "Failed to remove temp file {:?}: {}", path, e);
                    }
                }
            }
        }

        if cleaned_count > 0 {
            log::info!(target: "normfs-store", "Recovery completed: removed {} temp files", cleaned_count);
        } else {
            log::info!(target: "normfs-store", "Recovery completed: no temp files found");
        }

        Ok(())
    }

    pub fn extract_wal_bytes(
        &self,
        queue: &QueueId,
        file_id: &UintN,
        content: bytes::Bytes,
        verify_signatures: bool,
    ) -> Result<bytes::Bytes, StoreError> {
        log::debug!(target: "normfs-store",
            "Extracting WAL bytes for queue '{}' from {} bytes of store content",
            queue, content.len());

        let (file_auth, auth_size) = header::FileAuthentication::from_bytes(&content)?;

        let content_after_auth = &content[auth_size..];
        let (store_header, header_size) = header::StoreHeader::from_bytes(content_after_auth)?;

        if verify_signatures {
            let header_bytes = &content_after_auth[..header_size];
            self.crypto_ctx
                .verify(header_bytes, &file_auth.header_signature)
                .map_err(|_| StoreError::SignatureVerificationFailed)?;

            let encrypted_content = &content_after_auth[header_size..];
            self.crypto_ctx
                .verify(encrypted_content, &file_auth.content_signature)
                .map_err(|_| StoreError::SignatureVerificationFailed)?;
        }

        let encrypted_content = &content_after_auth[header_size..];

        let wal_content_bytes = if store_header.is_encrypted() {
            if encrypted_content.len() < 12 {
                log::error!(target: "normfs-store",
                    "Encrypted content too short for queue '{}': {} bytes",
                    queue, content.len());
                return Err(StoreError::Decrypt);
            }

            let encrypted_bytes = bytes::Bytes::from(encrypted_content.to_vec());
            let nonce = encrypted_bytes.slice(0..12);
            let ciphertext = encrypted_bytes.slice(12..);

            self.crypto_ctx
                .decrypt(queue, file_id, &nonce, &ciphertext)
                .map_err(|e| {
                    log::error!(target: "normfs-store",
                        "Decryption failed for queue '{}': {:?}", queue, e);
                    StoreError::Decrypt
                })?
        } else {
            bytes::Bytes::from(encrypted_content.to_vec())
        };

        // Handle decompression if needed
        let wal_bytes = if store_header.is_compressed() {
            match store_header.compression {
                header::CompressionType::Gzip => {
                    bytes::Bytes::from(compression::zstd_decompress(wal_content_bytes.as_ref())?)
                }
                header::CompressionType::Xz => {
                    bytes::Bytes::from(compression::xz_decompress(wal_content_bytes.as_ref())?)
                }
                header::CompressionType::Zstd => {
                    bytes::Bytes::from(compression::zstd_decompress(wal_content_bytes.as_ref())?)
                }
                header::CompressionType::None => wal_content_bytes,
            }
        } else {
            wal_content_bytes
        };

        log::debug!(target: "normfs-store",
            "Extracted {} bytes of WAL content for queue '{}'",
            wal_bytes.len(), queue);

        Ok(wal_bytes)
    }

    /// Get the raw store file bytes (before decryption/decompression).
    /// Returns None if the file doesn't exist.
    pub async fn get_store_bytes(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<bytes::Bytes>, StoreError> {
        log::debug!(target: "normfs-store",
            "Getting store bytes for queue '{}', file {}",
            queue, file_id);

        let store_fs_path = queue.to_store_dir(&self.root);
        let file_path = file_id.to_file_path(store_fs_path.to_str().unwrap(), "store");

        match fs::read(&file_path).await {
            Ok(bytes) => {
                log::debug!(target: "normfs-store",
                    "Read {} bytes from store file for queue '{}', file {}",
                    bytes.len(), queue, file_id);
                Ok(Some(bytes::Bytes::from(bytes)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::debug!(target: "normfs-store",
                    "Store file not found for queue '{}', file {}",
                    queue, file_id);
                Ok(None)
            }
            Err(e) => {
                log::error!(target: "normfs-store",
                    "Error reading store file for queue '{}', file {}: {:?}",
                    queue, file_id, e);
                Err(e.into())
            }
        }
    }
}
