pub(crate) mod lookup;
mod mem;
pub mod server;
pub mod proto {
    include!("proto/normfs.rs");
}
mod config;
mod offload;
pub(crate) mod reader_fsm;

use bytes::Bytes;
use core::time::Duration;
use normfs_cloud::CloudDownloader;
use normfs_crypto::CryptoContext;
use normfs_store::PersistStore;
use normfs_wal::{WalFile, WalSettings, WalStore};
use std::collections::HashMap;
use std::sync::RwLock;
use std::{path::Path, sync::Arc};
use tokio::sync::Mutex;

pub use lookup::LookupError;
pub use normfs_cloud::CloudSettings;
pub use normfs_store::{StoreError, StoreWriteConfig};
pub use normfs_types::{DataSource, QueueId, ReadEntry, ReadPosition};
pub use normfs_wal::WalError;
use offload::disk_monitor::DiskMonitor;
pub use offload::disk_monitor::DiskMonitorConfig;

pub use crate::config::{QueueConfig, QueueMode, QueueSettings};

pub use uintn::{Error as UintNError, UintN, UintNType};

pub struct NormFS {
    wal: Arc<WalStore>,
    store: Arc<PersistStore>,
    mem: Arc<mem::MemStore>,
    disk_monitor: Option<Arc<DiskMonitor>>,
    _cloud_downloader: Option<Arc<CloudDownloader>>,
    crypto_ctx: Arc<CryptoContext>,
    settings: NormFsSettings,
    reader_fsm: reader_fsm::ReaderFSM,
    queue_resolver: normfs_types::QueueIdResolver,
    queue_init_locks: RwLock<HashMap<QueueId, Arc<Mutex<()>>>>,
}

#[derive(Debug)]
pub enum Error {
    Wal(WalError),
    Store(StoreError),
    Cloud(normfs_cloud::errors::CloudError),
    Io(std::io::Error),
    QueueNotFound,
    QueueEmpty,
    NotFound,
    ClientDisconnected,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Wal(e) => write!(f, "WAL error: {}", e),
            Error::Store(e) => write!(f, "Store error: {}", e),
            Error::Cloud(e) => write!(f, "S3 error: {}", e),
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::QueueNotFound => write!(f, "Queue not found"),
            Error::QueueEmpty => write!(f, "Queue is empty"),
            Error::NotFound => write!(f, "Entry not found"),
            Error::ClientDisconnected => write!(f, "Client disconnected"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Wal(e) => Some(e),
            Error::Store(e) => Some(e),
            Error::Cloud(e) => Some(e),
            Error::Io(e) => Some(e),
            Error::QueueNotFound => None,
            Error::QueueEmpty => None,
            Error::NotFound => None,
            Error::ClientDisconnected => None,
        }
    }
}

impl From<WalError> for Error {
    fn from(e: WalError) -> Self {
        Error::Wal(e)
    }
}

impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Error::Store(e)
    }
}

impl From<normfs_cloud::errors::CloudError> for Error {
    fn from(e: normfs_cloud::errors::CloudError) -> Self {
        Error::Cloud(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

#[derive(Debug, Clone)]
pub struct NormFsSettings {
    pub store_cfg: StoreWriteConfig,
    pub max_memory_usage: usize,
    /// WAL settings (used for disk monitor validation, etc.)
    pub wal_settings: WalSettings,
    pub max_disk_usage_per_queue: Option<u64>,
    pub cloud_settings: Option<CloudSettings>,
    pub queue_settings: QueueSettings,
}

impl Default for NormFsSettings {
    fn default() -> Self {
        Self {
            store_cfg: Default::default(),
            max_memory_usage: 256 * 1024 * 1024, // 256MB
            max_disk_usage_per_queue: None,
            wal_settings: Default::default(),
            cloud_settings: None,
            queue_settings: Default::default(),
        }
    }
}

impl NormFS {
    pub async fn new<P: AsRef<Path> + Send + 'static>(
        path: P,
        settings: NormFsSettings,
    ) -> Result<Self, Error> {
        log::debug!(target: "normfs", "Creating new NormFS at path: {:?}", path.as_ref());

        let (wal_entry_send, mut wal_entry_recv) = tokio::sync::mpsc::unbounded_channel();
        let (wal_complete_send, wal_complete_recv): (
            tokio::sync::mpsc::UnboundedSender<WalFile>,
            tokio::sync::mpsc::UnboundedReceiver<WalFile>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let crypto_ctx = Arc::new(CryptoContext::open(path.as_ref()).map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to open crypto context: {}",
                e
            )))
        })?);

        let instance_id = crypto_ctx.instance_id_hex();

        let queue_resolver = normfs_types::QueueIdResolver::new(instance_id);

        let wal = Arc::new(WalStore::new(
            path.as_ref(),
            wal_entry_send,
            wal_complete_send,
        ));

        let mut store = PersistStore::new(
            path.as_ref(),
            settings.store_cfg.clone(),
            crypto_ctx.clone(),
            wal.clone(),
        );

        store.recover().await?;

        let store_done_rx = store.start_writers(wal_complete_recv).await;

        let mem = Arc::new(mem::MemStore::new(settings.max_memory_usage));

        let mem_clone = mem.clone();
        tokio::spawn(async move {
            while let Some((queue_id, id)) = wal_entry_recv.recv().await {
                log::trace!(target: "normfs", "Processing WAL ack - Queue: '{}', Entry ID: {}", queue_id, id);
                mem_clone.ack(&queue_id, &id);
            }
        });

        // Create S3 client and extract prefix if settings are provided
        let (cloud_client, cloud_prefix) = if let Some(ref cloud_settings) = settings.cloud_settings
        {
            let endpoint = url::Url::parse(&cloud_settings.endpoint)
                .map_err(|e| Error::Cloud(normfs_cloud::errors::CloudError::InvalidUrl(e)))?;

            match normfs_cloud::S3Client::new(
                endpoint,
                cloud_settings.bucket.clone(),
                cloud_settings.region.clone(),
                cloud_settings.access_key.clone(),
                cloud_settings.secret_key.clone(),
            ) {
                Ok(client) => {
                    log::info!(target: "normfs",
                        "Created S3 client for bucket '{}' at endpoint '{}' with prefix '{}'",
                        cloud_settings.bucket, cloud_settings.endpoint, cloud_settings.prefix
                    );
                    (Some(Arc::new(client)), Some(cloud_settings.prefix.clone()))
                }
                Err(e) => {
                    log::error!(target: "normfs", "Failed to create S3 client: {}", e);
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        // Initialize S3 downloader if S3 client is available
        let cloud_downloader =
            if let (Some(client), Some(ref prefix)) = (&cloud_client, &cloud_prefix) {
                let full_prefix = if prefix.is_empty() {
                    String::new()
                } else {
                    prefix.clone()
                };
                log::info!(target: "normfs", "Creating S3 downloader with prefix: {}", full_prefix);
                Some(Arc::new(CloudDownloader::new(client.clone(), &full_prefix)))
            } else {
                log::info!(target: "normfs", "S3 downloader disabled");
                None
            };

        // Initialize disk monitor if enabled
        let disk_monitor = if settings.max_disk_usage_per_queue.is_some() {
            log::debug!(target: "normfs", "Disk monitor enabled, creating disk monitor instance");
            // Pass the shared S3 bucket and full prefix (with instance_id) to DiskMonitor
            let full_cloud_prefix = if let Some(ref prefix) = cloud_prefix {
                let instance_id = crypto_ctx.instance_id_hex();
                Some(if prefix.is_empty() {
                    instance_id.to_string()
                } else {
                    format!("{}/{}", prefix, instance_id)
                })
            } else {
                None
            };
            match DiskMonitor::new(path.as_ref(), cloud_client.clone(), full_cloud_prefix).await {
                Ok(monitor) => Some(Arc::new(monitor)),
                Err(e) => {
                    log::error!(target: "normfs", "Failed to create disk monitor: {}", e);
                    return Err(e);
                }
            }
        } else {
            log::info!(target: "normfs", "Disk monitor disabled");
            None
        };

        // Always consume store completions to prevent SendError on the sender side
        // Forward to offload queue if disk monitor is enabled
        let monitor_opt = disk_monitor.clone();
        tokio::spawn(async move {
            let mut store_done_rx = store_done_rx;
            while let Some((queue_id, file_id)) = store_done_rx.recv().await {
                if let Some(ref monitor) = monitor_opt {
                    log::debug!(target: "normfs",
                        "Received store completion for queue: {}, file_id: {:?}",
                        queue_id, file_id);

                    // Forward to offload queue
                    if let Err(e) = monitor
                        .enqueue_for_offload(&queue_id, file_id.clone())
                        .await
                    {
                        log::error!(target: "normfs",
                            "Failed to enqueue file for offload: queue={}, file_id={:?}, error={}",
                            queue_id, file_id, e);
                    }
                }
            }
            log::info!(target: "normfs", "Store completion forwarding task ended");
        });

        log::info!(target: "normfs", "NormFS initialized successfully (disk_monitor: {}, s3: {})",
            if settings.max_disk_usage_per_queue.is_some() { "enabled" } else { "disabled" },
            if cloud_downloader.is_some() { "enabled" } else { "disabled" });

        let store_arc = Arc::new(store);
        let reader_fsm = reader_fsm::ReaderFSM::new(
            wal.clone(),
            store_arc.clone(),
            mem.clone(),
            cloud_downloader.clone(),
        );

        Ok(Self {
            wal,
            store: store_arc,
            mem,
            disk_monitor,
            _cloud_downloader: cloud_downloader,
            crypto_ctx,
            settings: settings.clone(),
            reader_fsm,
            queue_resolver,
            queue_init_locks: RwLock::new(HashMap::new()),
        })
    }

    pub fn get_instance_id(&self) -> &str {
        self.crypto_ctx.instance_id_hex()
    }

    pub fn get_instance_id_bytes(&self) -> Bytes {
        self.crypto_ctx.instance_id_bytes()
    }

    /// Resolve a queue path to a QueueId with absolute path
    /// If the path is relative, it will be prefixed with /instance_id/
    /// If the path is absolute (starts with /), it will be used as-is
    pub fn resolve(&self, path: &str) -> QueueId {
        self.queue_resolver.resolve(path)
    }

    pub async fn ensure_queue_exists_for_read(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = {
            let locks = self.queue_init_locks.read().unwrap();
            if let Some(lock) = locks.get(queue).cloned() {
                lock
            } else {
                drop(locks);
                let mut locks = self.queue_init_locks.write().unwrap();
                locks
                    .entry(queue.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }
        };

        let _guard = queue_lock.lock().await;

        if self.mem.get_last_id(queue).is_some() {
            return Ok(());
        }

        log::info!(target: "normfs", "Auto-starting queue '{}' in readonly mode for read request", queue);
        self.start_queue(queue, QueueMode { readonly: true }).await
    }

    pub async fn ensure_queue_exists_for_write(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = {
            let locks = self.queue_init_locks.read().unwrap();
            if let Some(lock) = locks.get(queue).cloned() {
                lock
            } else {
                drop(locks);
                let mut locks = self.queue_init_locks.write().unwrap();
                locks
                    .entry(queue.clone())
                    .or_insert_with(|| Arc::new(Mutex::new(())))
                    .clone()
            }
        };

        let _guard = queue_lock.lock().await;

        let queue_exists = self.mem.get_last_id(queue).is_some();
        let has_writer = self.wal.has_writer(queue).await;

        if queue_exists && has_writer {
            return Ok(());
        }

        if queue_exists && !has_writer {
            log::info!(target: "normfs", "Restarting queue '{}' from readonly to write mode", queue);
        } else {
            log::info!(target: "normfs", "Auto-starting queue '{}' in write mode for write request", queue);
        }

        self.start_queue(queue, QueueMode { readonly: false }).await
    }

    fn get_config_for_queue(&self, queue: &QueueId) -> QueueConfig {
        self.settings.queue_settings.get_config(&queue.to_string())
    }

    /// Get the latest file ID across all sources (WAL, Store, S3).
    /// Returns the maximum file ID found, or None if no files exist in any source.
    async fn get_latest_file(&self, queue: &QueueId) -> Option<UintN> {
        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_file_id, store_file_id) = tokio::join!(
            self.wal.find_last_file_id(queue),
            self.store.find_last_file_id(queue)
        );

        // Log results from each source individually
        match &wal_file_id {
            Ok(id) => {
                log::info!(target: "normfs", "Queue '{}' - WAL latest file ID: {}", queue, id)
            }
            Err(e) => {
                log::info!(target: "normfs", "Queue '{}' - WAL has no files or error: {:?}", queue, e)
            }
        }

        match &store_file_id {
            Ok(id) => {
                log::info!(target: "normfs", "Queue '{}' - Store latest file ID: {}", queue, id)
            }
            Err(e) => {
                log::info!(target: "normfs", "Queue '{}' - Store has no files or error: {:?}", queue, e)
            }
        }

        log::info!(target: "normfs",
            "Queue '{}' - Latest file IDs summary: WAL={:?}, Store={:?}",
            queue,
            wal_file_id.as_ref().ok(),
            store_file_id.as_ref().ok()
        );

        // Find the maximum file ID among WAL and Store
        let mut max_file_id: Option<UintN> = None;

        if let Ok(id) = wal_file_id {
            max_file_id = Some(match max_file_id {
                Some(current_max) if id > current_max => id,
                Some(current_max) => current_max,
                None => id,
            });
        }

        if let Ok(id) = store_file_id {
            max_file_id = Some(match max_file_id {
                Some(current_max) if id > current_max => id,
                Some(current_max) => current_max,
                None => id,
            });
        }

        log::info!(target: "normfs",
            "Queue '{}' - Maximum file ID across WAL and Store: {:?}",
            queue,
            max_file_id
        );

        max_file_id
    }

    /// Get the last entry ID in a specific file across WAL and Store only (S3 is too slow).
    /// Queries WAL and Store in parallel and returns the maximum last entry ID found,
    /// or None if the file has no entries in any source.
    async fn get_file_end_all_sources(&self, queue: &QueueId, file_id: &UintN) -> Option<UintN> {
        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_end, store_end) = tokio::join!(
            self.wal.get_file_end(queue, file_id),
            self.store.get_file_end(queue, file_id)
        );

        // Log detailed results from each source
        match &wal_end {
            Ok(Some(id)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL has entries, last entry ID: {}", queue, file_id, id),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL file exists but has no entries", queue, file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - WAL query error: {:?}", queue, file_id, e),
        }

        match &store_end {
            Ok(Some(id)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store has entries, last entry ID: {}", queue, file_id, id),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store file exists but has no entries", queue, file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - Store query error: {:?}", queue, file_id, e),
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {:?} - Last entry IDs summary: WAL={:?}, Store={:?}",
            queue,
            file_id,
            wal_end.as_ref().ok().and_then(|o| o.as_ref()),
            store_end.as_ref().ok().and_then(|o| o.as_ref())
        );

        // Find the maximum last entry ID among WAL and Store
        let mut max_last_entry_id: Option<UintN> = None;
        let mut max_source = "none";

        if let Ok(Some(id)) = wal_end {
            match &max_last_entry_id {
                Some(current_max) if id > *current_max => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - WAL entry ID {} is now maximum (was {:?})",
                        queue, file_id, id, current_max);
                    max_last_entry_id = Some(id);
                    max_source = "WAL";
                }
                Some(_current_max) => {
                    log::debug!(target: "normfs",
                        "Queue '{}', File ID {} - WAL entry ID {} is not maximum",
                        queue, file_id, id);
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - WAL entry ID {} is first candidate",
                        queue, file_id, id);
                    max_last_entry_id = Some(id);
                    max_source = "WAL";
                }
            }
        }

        if let Ok(Some(id)) = store_end {
            match &max_last_entry_id {
                Some(current_max) if id > *current_max => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - Store entry ID {} is now maximum (was {:?})",
                        queue, file_id, id, current_max);
                    max_last_entry_id = Some(id);
                    max_source = "Store";
                }
                Some(_current_max) => {
                    log::debug!(target: "normfs",
                        "Queue '{}', File ID {} - Store entry ID {} is not maximum",
                        queue, file_id, id);
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - Store entry ID {} is first candidate",
                        queue, file_id, id);
                    max_last_entry_id = Some(id);
                    max_source = "Store";
                }
            }
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {} - Selected maximum last entry ID: {:?} from source: {}",
            queue,
            file_id,
            max_last_entry_id,
            max_source
        );

        max_last_entry_id
    }

    /// Get the format header from a specific file across all sources.
    /// Queries WAL and Store in parallel and returns the first valid header found,
    /// or None if the file doesn't exist in any source. (S3 is too slow for recovery)
    async fn get_file_header_all_sources(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Option<normfs_wal::WalHeader> {
        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_header, store_header) = tokio::join!(
            self.wal.get_file_header(queue, file_id),
            self.store.get_file_header(queue, file_id)
        );

        // Log individual source results
        match &wal_header {
            Ok(Some(header)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL header: data_size={}, id_size={}, entries_before={}",
                queue, file_id, header.data_size_bytes, header.id_size_bytes, header.num_entries_before),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL header not found", queue, file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - WAL header error: {:?}", queue, file_id, e),
        }

        match &store_header {
            Ok(Some(header)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store header: data_size={}, id_size={}, entries_before={}",
                queue, file_id, header.data_size_bytes, header.id_size_bytes, header.num_entries_before),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store header not found", queue, file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - Store header error: {:?}", queue, file_id, e),
        }

        // Return the first valid header found (prefer WAL, then Store)
        if let Ok(Some(header)) = wal_header {
            log::info!(target: "normfs",
                "Queue '{}', File ID {} - Selected WAL header for recovery",
                queue, file_id
            );
            return Some(header);
        }

        if let Ok(Some(header)) = store_header {
            log::info!(target: "normfs",
                "Queue '{}', File ID {} - Selected Store header for recovery",
                queue, file_id
            );
            return Some(header);
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {} - No header found in WAL or Store",
            queue, file_id
        );
        None
    }

    /// Continue a queue by walking backward from the latest file to find the last entry.
    /// Returns (file_id, header, last_entry_id) for starting the WAL writer.
    async fn continue_queue(
        &self,
        queue: &QueueId,
    ) -> Result<(UintN, normfs_wal::WalHeader, Option<UintN>), Error> {
        log::info!(target: "normfs", "Continuing queue: '{}'", queue);

        // Get the latest file ID across all sources
        let latest_file_id = match self.get_latest_file(queue).await {
            Some(id) => {
                log::info!(target: "normfs",
                    "Queue '{}' - Found latest file ID: {:?}",
                    queue, id
                );
                id
            }
            None => {
                log::info!(target: "normfs",
                    "Queue '{}' - No files found, starting fresh",
                    queue
                );
                return Ok((UintN::one(), Default::default(), None));
            }
        };

        // Walk backward from the latest file ID to find the first file with actual entries
        let mut current_file_id = latest_file_id.clone();

        loop {
            log::info!(target: "normfs",
                "Queue '{}' - Checking file ID {:?} for entries",
                queue, current_file_id
            );

            // Try to get the last entry ID in this file
            match self.get_file_end_all_sources(queue, &current_file_id).await {
                Some(last_entry_id) => {
                    log::info!(target: "normfs",
                        "Queue '{}' - Found file with entries: file_id={:?}, last_entry_id={:?}",
                        queue, current_file_id, last_entry_id
                    );

                    // Get the header from this file
                    let header = self
                        .get_file_header_all_sources(queue, &current_file_id)
                        .await
                        .unwrap_or_default();

                    log::info!(target: "normfs",
                        "Queue '{}' - File {:?} header: data_size={}, id_size={}, entries_before={}",
                        queue, current_file_id,
                        header.data_size_bytes, header.id_size_bytes, header.num_entries_before
                    );

                    // Decide where to write:
                    // - If current file (with entries) == latest file: write to latest + 1
                    // - If current file (with entries) < latest file: reuse empty latest file
                    let is_latest_file = current_file_id == latest_file_id;
                    let next_file_id = if is_latest_file {
                        // Latest file has entries, create new file
                        latest_file_id.increment()
                    } else {
                        // Found entries in older file, latest file is empty - reuse it
                        latest_file_id.clone()
                    };

                    let mut new_header = header;
                    new_header.num_entries_before = last_entry_id.increment();

                    log::info!(target: "normfs",
                        "Queue '{}' - Recovery decision: Found entries in file {}, will write to file {} {}",
                        queue, current_file_id, next_file_id,
                        if is_latest_file { "(new file)" } else { "(reusing empty latest file)" }
                    );

                    log::info!(target: "normfs",
                        "Queue '{}' - Starting WAL writer: file_id={}, num_entries_before={}, last_entry_id={:?}",
                        queue, next_file_id, new_header.num_entries_before, last_entry_id
                    );

                    return Ok((next_file_id, new_header, Some(last_entry_id)));
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}' - File {:?} has no entries, trying previous file",
                        queue, current_file_id
                    );

                    // File is empty or corrupted, move to previous file
                    if current_file_id == UintN::one() {
                        // We've reached the first file and it's empty - start fresh
                        log::info!(target: "normfs",
                            "Queue '{}' - Reached first file with no entries, starting fresh",
                            queue
                        );
                        return Ok((UintN::one(), Default::default(), None));
                    }

                    // Decrement to previous file
                    current_file_id = current_file_id.decrement().map_err(|e| {
                        log::error!(target: "normfs",
                            "Queue '{}' - Failed to decrement file ID: {:?}",
                            queue, e
                        );
                        Error::Store(normfs_store::StoreError::UintN(e))
                    })?;
                }
            }
        }
    }

    async fn start_queue(&self, queue: &QueueId, mode: QueueMode) -> Result<(), Error> {
        log::info!(target: "normfs", "========================================");
        log::info!(target: "normfs", "Starting queue: '{}' (readonly={})", queue, mode.readonly);
        log::info!(target: "normfs", "========================================");

        // Use the new backward search logic to find the correct file and entry to continue from
        let (file_id, header, last_entry_id) = self.continue_queue(queue).await?;

        log::info!(target: "normfs", "----------------------------------------");
        log::info!(target: "normfs", "Queue '{}' - Recovery complete:", queue);
        log::info!(target: "normfs", "  - Will write to file ID: {}", file_id);
        log::info!(target: "normfs", "  - Last entry ID in queue: {:?}", last_entry_id);
        log::info!(target: "normfs", "  - Header entries_before: {}", header.num_entries_before);
        log::info!(target: "normfs", "  - Next entry will have ID: {}", header.num_entries_before);
        log::info!(target: "normfs", "----------------------------------------");

        if !mode.readonly {
            let queue_config = self.get_config_for_queue(queue);

            let mut wal_settings = self.settings.wal_settings.clone();
            wal_settings.enable_fsync = queue_config.enable_fsync;
            wal_settings.compression_type = queue_config.compression_type;
            wal_settings.encryption_type = queue_config.encryption_type;

            self.wal
                .start_writer(
                    queue,
                    &file_id,
                    header,
                    wal_settings.clone(),
                    last_entry_id.clone(),
                )
                .await?;

            let wal = self.wal.clone();
            let queue_clone = queue.clone();
            let file_id_clone = file_id.clone();
            let compression_type = wal_settings.compression_type;
            let encryption_type = wal_settings.encryption_type;

            tokio::spawn(async move {
                if let Err(e) = wal
                    .process_old_files(
                        &queue_clone,
                        &file_id_clone,
                        compression_type,
                        encryption_type,
                    )
                    .await
                {
                    log::error!(target: "normfs",
                        "Failed to process old files for queue: {}",
                        e
                    );
                }
            });
        }

        self.mem.start_queue(queue, last_entry_id.clone());

        // Add queue to disk monitor if enabled
        if let (Some(disk_monitor), Some(max_size)) =
            (&self.disk_monitor, self.settings.max_disk_usage_per_queue)
        {
            let config = DiskMonitorConfig {
                max_size: max_size as usize,
                check_interval: Duration::from_secs(10), // Default check interval
                wal_settings: self.settings.wal_settings.clone(),
            };

            disk_monitor.add_queue(queue, config).await?;
            log::info!(target: "normfs", "Added queue '{}' to disk monitor with max_size: {}", queue, max_size);
        }

        log::info!(target: "normfs", "Queue '{}' started successfully, last_entry_id: {:?}", queue, last_entry_id);

        Ok(())
    }

    pub fn enqueue(&self, queue: &QueueId, data: Bytes) -> Result<UintN, Error> {
        let entry_id = self.mem.enqueue(queue, data.clone());

        log::debug!(target: "normfs", "Enqueuing entry - Queue: '{}', Entry ID: {}, Data size: {} bytes",
            queue, entry_id, data.len());

        self.wal.enqueue(queue, entry_id.clone(), data)?;

        log::trace!(target: "normfs", "Entry enqueued successfully - Queue: '{}', Entry ID: {}", queue, entry_id);

        Ok(entry_id)
    }

    pub fn enqueue_batch(&self, queue: &QueueId, data: Vec<Bytes>) -> Result<Vec<UintN>, Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        log::debug!(target: "normfs", "Enqueuing batch - Queue: '{}', Batch size: {} entries", queue, data.len());

        let entry_ids = self.mem.enqueue_batch(queue, data.clone());

        if let (Some(first_id), Some(last_id)) = (entry_ids.first(), entry_ids.last()) {
            log::debug!(target: "normfs", "Batch entry IDs - Queue: '{}', First ID: {}, Last ID: {}",
                queue, first_id, last_id);
        }

        let wal_entries: Vec<(UintN, Bytes)> = entry_ids
            .iter()
            .cloned()
            .zip(data.iter().cloned())
            .collect();

        self.wal.enqueue_batch(queue, wal_entries)?;

        log::trace!(target: "normfs", "Batch enqueued successfully - Queue: '{}', Count: {}", queue, entry_ids.len());

        Ok(entry_ids)
    }

    pub fn get_last_id(&self, queue: &QueueId) -> Result<UintN, Error> {
        match self.mem.get_last_id(queue) {
            Some(Some(id)) => Ok(id),
            Some(None) => Err(Error::QueueEmpty),
            None => Err(Error::QueueNotFound),
        }
    }

    pub fn subscribe(
        &self,
        queue: &QueueId,
        callback: normfs_types::SubscriberCallback,
    ) -> Result<usize, Error> {
        self.mem
            .subscribe(queue, callback)
            .ok_or(Error::QueueNotFound)
    }

    pub fn unsubscribe(&self, queue: &QueueId, subscriber_id: usize) {
        self.mem.unsubscribe(queue, subscriber_id);
    }

    pub async fn read(
        &self,
        queue: &QueueId,
        position: ReadPosition,
        limit: u64,
        step: u64,
        sender: tokio::sync::mpsc::Sender<ReadEntry>,
    ) -> Result<bool, Error> {
        log::debug!(target: "normfs",
            "Reading entries - Queue: '{}', Position: {:?}, Limit: {}, Step: {}",
            queue, position, limit, step);

        self.reader_fsm
            .read(queue.clone(), position, limit, step, sender)
            .await
    }

    pub async fn close(&self) -> Result<(), Error> {
        log::info!(target: "normfs", "Closing NormFS");

        // Close the store first to shut down writer workers
        self.store.close().await;

        // Then close the WAL
        self.wal.close().await?;

        log::info!(target: "normfs", "NormFS closed successfully");
        Ok(())
    }
}
