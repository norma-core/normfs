pub(crate) mod lookup;
mod mem;
pub mod server;
pub mod proto {
    include!("proto/normfs.rs");
}
mod config;
mod memory_pointers;
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
use tokio::task::JoinHandle;

pub use lookup::LookupError;
pub use normfs_cloud::CloudSettings;
pub use normfs_store::{StoreError, StoreWriteConfig};
pub use normfs_types::{DataSource, QueueId, ReadEntry, ReadPosition};
pub use normfs_wal::WalError;
use offload::disk_monitor::DiskMonitor;
pub use offload::disk_monitor::DiskMonitorConfig;

pub use crate::config::{PersistenceMode, PoolKind, QueueConfig, QueueMode, QueueSettings};

pub use uintn::{Error as UintNError, UintN, UintNType};

pub struct NormFS {
    path: std::path::PathBuf,
    wal: Option<Arc<WalStore>>,
    store: Option<Arc<PersistStore>>,
    mem: Arc<mem::MemStore>,
    disk_monitor: Option<Arc<DiskMonitor>>,
    _cloud_downloader: Option<Arc<CloudDownloader>>,
    memory_pointers: Option<Arc<memory_pointers::MemoryPointers>>,
    memory_pointer_task: Option<JoinHandle<()>>,
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
    /// The record does not fit a page, framing included, so no page can hold
    /// it. Refused before an id is taken — see [`NormFS::enqueue`].
    RecordTooLarge(usize),
    /// No page could take the record and the caller would not wait
    /// ([`NormFS::try_enqueue`]).
    WouldBlock,
    /// The queue was closed for good ([`NormFS::close_queue`]); a later
    /// write is an error. The data stays readable.
    QueueClosed,
    /// `max_memory_usage` cannot hold the two pages a single queue needs to
    /// work. Refused at construction rather than rounded up: rounding up would
    /// mean the process quietly using more memory than it was configured for.
    MemoryBelowFloor {
        max_memory_usage: usize,
        page_size: usize,
        needed: usize,
    },
    /// `mem_page_size` is below the smallest page the ring's contracts allow.
    /// Refused at construction; past this check the arena panics instead.
    PageBelowMinimum {
        page_size: usize,
        minimum: usize,
    },
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
            Error::RecordTooLarge(n) => write!(
                f,
                "Record of {n} bytes does not fit a memory page once framed"
            ),
            Error::WouldBlock => write!(f, "No page is free and the caller would not wait"),
            Error::QueueClosed => write!(f, "Queue is closed and accepts no more writes"),
            Error::MemoryBelowFloor {
                max_memory_usage,
                page_size,
                needed,
            } => write!(
                f,
                "max_memory_usage of {max_memory_usage} bytes is below the {needed} bytes a \
                 single queue needs at a page size of {page_size}; raise max_memory_usage or \
                 lower mem_page_size"
            ),
            Error::PageBelowMinimum { page_size, minimum } => write!(
                f,
                "mem_page_size of {page_size} bytes is below the {minimum} bytes a page needs \
                 to hold even an empty record"
            ),
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
            Error::RecordTooLarge(_) => None,
            Error::WouldBlock => None,
            Error::QueueClosed => None,
            Error::MemoryBelowFloor { .. } => None,
            Error::PageBelowMinimum { .. } => None,
        }
    }
}

/// Refuses a record no page can hold, **before** it is given an id.
///
/// This is the only place the size limit is enforced, and the timing is the
/// whole point. The writer used to discover the problem after the fact
/// (`WriterState::write`) and could only log it: the id had already been
/// returned to the caller, the writer's ordered buffer had already counted it,
/// and the pool had already stepped past it. The record was then simply absent
/// from the file while every id after it kept counting — and V1 derives entry
/// ids from position, so the result is not a missing record but every later
/// record answering to the wrong id.
///
/// Refusing it here costs the caller an error and costs the sequence nothing.
///
/// Three bounds, narrowest first:
///
/// * A page. A record is written into one page and never straddles two, so a
///   page is the ceiling — and it is the *encoded* entry that has to fit, not
///   the record: the frame is `[record_size varint32][record][crc32c u32 LE]`,
///   so a record of exactly `mem_page_size` is nine bytes too wide.
/// * `max_memory_usage`. Below two pages there is no arena to speak of; the
///   page bound is tighter than this in every sane configuration, but the two
///   are set independently so both are checked.
/// * The V1 frame itself, whose length prefix is a varint32.
fn check_framable(record: &Bytes, page_size: usize, max_memory_usage: usize) -> Result<(), Error> {
    // `max_record_len` is the pool's own arithmetic, not a copy of it: the two
    // sides of this limit must not be able to drift apart.
    let cap = normfs_wal::max_record_len(page_size).min(max_memory_usage);
    if record.len() > cap || u32::try_from(record.len()).is_err() {
        return Err(Error::RecordTooLarge(record.len()));
    }
    Ok(())
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

/// 32 KiB pages, so a passive queue's permanent 2-page floor is 64 KiB.
pub const DEFAULT_PASSIVE_PAGE_SIZE: usize = 32 * 1024;
/// Floors for ~128 rare queues before the private-floor fallback kicks in.
pub const DEFAULT_PASSIVE_MEMORY_USAGE: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct NormFsSettings {
    pub store_cfg: StoreWriteConfig,
    pub max_memory_usage: usize,
    /// Size of one memory page, and with it the largest record this instance
    /// accepts: a record is written into one page and never straddles two, so
    /// the cap is this minus the V1 framing.
    ///
    /// It is also the unit the arena shares between queues. Large pages cost
    /// fewer rotations and fewer flush runs; small ones divide the same
    /// `max_memory_usage` into more chunks, so more queues get a working
    /// allowance and the two-page floor each one holds while idle is smaller.
    pub mem_page_size: usize,
    /// Page size of the passive arena. Caps a passive queue's records the
    /// same way `mem_page_size` caps an active one's.
    pub mem_passive_page_size: usize,
    /// Passive arena budget, separate so rare queues and busy ones cannot
    /// eat each other's memory.
    pub max_passive_memory_usage: usize,
    /// WAL settings (used for disk monitor validation, etc.)
    pub wal_settings: WalSettings,
    pub max_disk_usage_per_queue: Option<u64>,
    pub cloud_settings: Option<CloudSettings>,
    pub queue_settings: QueueSettings,
    pub persistence_mode: PersistenceMode,
    pub memory_pointers_flush_interval: Duration,
}

impl Default for NormFsSettings {
    fn default() -> Self {
        Self {
            store_cfg: Default::default(),
            max_memory_usage: 256 * 1024 * 1024, // 256MB
            // The sweep says CPU and throughput are flat from 64 KiB to
            // 16 MiB, so the sharing unit decides: at 4 MiB this budget is 64
            // pages and four queues exhaust it, pushing every later queue
            // into a private out-of-budget pool. 256 KiB keeps ~500 queue
            // floors inside the budget. Deployments with wider records raise
            // this and max_memory_usage together, deliberately.
            mem_page_size: 256 * 1024,
            mem_passive_page_size: DEFAULT_PASSIVE_PAGE_SIZE,
            max_passive_memory_usage: DEFAULT_PASSIVE_MEMORY_USAGE,
            max_disk_usage_per_queue: None,
            wal_settings: Default::default(),
            cloud_settings: None,
            queue_settings: Default::default(),
            persistence_mode: PersistenceMode::Durable,
            memory_pointers_flush_interval: Duration::from_secs(5),
        }
    }
}

impl NormFsSettings {
    /// Every queue on the active arena: the pre-two-pool behavior.
    pub fn all_active() -> Self {
        Self {
            queue_settings: QueueSettings::all_active(),
            ..Self::default()
        }
    }
}

impl NormFS {
    pub async fn new<P: AsRef<Path> + Send + 'static>(
        path: P,
        settings: NormFsSettings,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        log::debug!(target: "normfs", "Creating new NormFS at path: {:?}", path);

        let crypto_ctx = Arc::new(CryptoContext::open(&path).map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to open crypto context: {}",
                e
            )))
        })?);

        let instance_id = crypto_ctx.instance_id_hex();

        let queue_resolver = normfs_types::QueueIdResolver::new(instance_id);

        let mem = Arc::new(mem::MemStore::with_pools(
            settings.max_memory_usage,
            settings.mem_page_size,
            settings.max_passive_memory_usage,
            settings.mem_passive_page_size,
        )?);

        if settings.persistence_mode == PersistenceMode::MemoryOnly {
            mem.evict_cache_on_full();
            if settings.cloud_settings.is_some() {
                log::warn!(target: "normfs", "Ignoring cloud settings in memory-only mode");
            }
            if settings.max_disk_usage_per_queue.is_some() {
                log::warn!(target: "normfs", "Ignoring disk monitor settings in memory-only mode");
            }

            let memory_pointers =
                Arc::new(memory_pointers::MemoryPointers::open(&path).map_err(Error::Io)?);
            let memory_pointer_task =
                Some(memory_pointers.spawn_flusher(settings.memory_pointers_flush_interval));
            let reader_fsm = reader_fsm::ReaderFSM::new(None, None, mem.clone(), None);

            log::info!(target: "normfs", "NormFS initialized in memory-only mode");

            return Ok(Self {
                path: path.clone(),
                wal: None,
                store: None,
                mem,
                disk_monitor: None,
                _cloud_downloader: None,
                memory_pointers: Some(memory_pointers),
                memory_pointer_task,
                crypto_ctx,
                settings: settings.clone(),
                reader_fsm,
                queue_resolver,
                queue_init_locks: RwLock::new(HashMap::new()),
            });
        }

        let (wal_entry_send, mut wal_entry_recv) = tokio::sync::mpsc::unbounded_channel();
        let (wal_complete_send, wal_complete_recv): (
            tokio::sync::mpsc::UnboundedSender<WalFile>,
            tokio::sync::mpsc::UnboundedReceiver<WalFile>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let wal = Arc::new(WalStore::new(&path, wal_entry_send, wal_complete_send));

        let mut store = PersistStore::new(
            &path,
            settings.store_cfg.clone(),
            crypto_ctx.clone(),
            wal.clone(),
        );

        store.recover().await?;

        let store_done_rx = store.start_writers(wal_complete_recv).await;

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
            match DiskMonitor::new(&path, cloud_client.clone(), cloud_prefix.clone()).await {
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
            Some(wal.clone()),
            Some(store_arc.clone()),
            mem.clone(),
            cloud_downloader.clone(),
        );

        Ok(Self {
            path: path.clone(),
            wal: Some(wal),
            store: Some(store_arc),
            mem,
            disk_monitor,
            _cloud_downloader: cloud_downloader,
            memory_pointers: None,
            memory_pointer_task: None,
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

    fn queue_init_lock(&self, queue: &QueueId) -> Arc<Mutex<()>> {
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
    }

    fn is_memory_only(&self) -> bool {
        self.settings.persistence_mode == PersistenceMode::MemoryOnly
    }

    // Consults the durable marker once and mirrors it into memory.
    fn queue_closed_durably(&self, queue: &QueueId) -> bool {
        if self.mem.is_closed(queue) {
            return true;
        }
        // A queue live in memory had its marker consulted when it started;
        // the disk stat stays off the per-request path.
        if self.mem.get_last_id(queue).is_some() {
            return false;
        }
        if queue.to_fs_path(&self.path).join("closed").is_file() {
            self.mem.mark_closed(queue);
            return true;
        }
        false
    }

    pub async fn ensure_queue_exists_for_read(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = self.queue_init_lock(queue);
        let _guard = queue_lock.lock().await;

        // Reads stay legal on a closed queue; this only loads the marker
        // so a follow here knows to end.
        self.queue_closed_durably(queue);

        if self.mem.get_last_id(queue).is_some() {
            return Ok(());
        }

        log::info!(target: "normfs", "Auto-starting queue '{}' in readonly mode for read request", queue);
        self.start_queue(queue, QueueMode { readonly: true }).await
    }

    pub async fn ensure_queue_exists_for_write(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = self.queue_init_lock(queue);
        let _guard = queue_lock.lock().await;

        if self.queue_closed_durably(queue) {
            return Err(Error::QueueClosed);
        }

        let queue_exists = self.mem.get_last_id(queue).is_some();
        if self.is_memory_only() {
            if queue_exists {
                return Ok(());
            }

            log::info!(target: "normfs", "Auto-starting queue '{}' in memory-only write mode", queue);
            return self.start_queue(queue, QueueMode { readonly: false }).await;
        }

        let has_writer = self
            .wal
            .as_ref()
            .expect("WAL backend must be available in durable mode")
            .has_writer(queue)
            .await;

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

    // From config, not the live queue: the size check runs on the first
    // write, which is what creates the queue.
    fn page_size_for(&self, queue: &QueueId) -> usize {
        match self.get_config_for_queue(queue).pool {
            config::PoolKind::Active => self.settings.mem_page_size,
            config::PoolKind::Passive => self.settings.mem_passive_page_size,
        }
    }

    /// Get the latest file ID across all sources (WAL, Store, S3).
    /// Returns the maximum file ID found, or None if no files exist in any source.
    async fn get_latest_file(&self, queue: &QueueId) -> Option<UintN> {
        let wal = self
            .wal
            .as_ref()
            .expect("WAL backend must be available in durable mode");
        let store = self
            .store
            .as_ref()
            .expect("Store backend must be available in durable mode");

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_file_id, store_file_id) =
            tokio::join!(wal.find_last_file_id(queue), store.find_last_file_id(queue));

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
        let wal = self
            .wal
            .as_ref()
            .expect("WAL backend must be available in durable mode");
        let store = self
            .store
            .as_ref()
            .expect("Store backend must be available in durable mode");

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_end, store_end) = tokio::join!(
            wal.get_file_end(queue, file_id),
            store.get_file_end(queue, file_id)
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
        let wal = self
            .wal
            .as_ref()
            .expect("WAL backend must be available in durable mode");
        let store = self
            .store
            .as_ref()
            .expect("Store backend must be available in durable mode");

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_header, store_header) = tokio::join!(
            wal.get_file_header(queue, file_id),
            store.get_file_header(queue, file_id)
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

    /// Reports ids that no file holds, walking down from the file recovery is
    /// resuming after.
    ///
    /// No entry body is read: file F's `num_entries_before` should be one past
    /// the last id of the file below it, and the difference when it is not is
    /// exactly what a failed closing flush lost.
    ///
    /// Nothing is deleted or set aside. Those records were fsynced and acked
    /// normally while the torn file waited for a retry a crash cut short, so
    /// discarding them would destroy acknowledged data to make the sequence
    /// contiguous -- and would not even suffice, since `get_latest_file` merges
    /// the WAL and the Store, the range index has no removal, and the files may
    /// already be offloaded.
    async fn report_id_chain_breaks(&self, queue: &QueueId, from: &UintN) {
        // `get_file_end` on a WAL file is a full frame scan; an archived one
        // answers from its store header, which is what the healthy case costs.
        const MAX_LINKS: u32 = 32;

        let mut upper = from.clone();
        for link in 0..MAX_LINKS {
            let Ok(lower) = upper.decrement() else {
                return;
            };
            let Some(header) = self.get_file_header_all_sources(queue, &upper).await else {
                return;
            };
            let Some(lower_last) = self.get_file_end_all_sources(queue, &lower).await else {
                // An empty file between two full ones is ordinary.
                upper = lower;
                continue;
            };
            let expected = lower_last.increment();
            if header.num_entries_before == expected {
                // Keep going down: the tear is at the *bottom* of the run of
                // files written after it, because the queue kept rotating while
                // the retry was stuck.
                upper = lower;
                continue;
            }
            log::error!(target: "normfs",
                "Queue '{}' - ids {}..{} reach no file: file {} ends at {} and file {} starts \
                 at {}. A closing flush lost them and the retry did not land before the \
                 process ended. Nothing is discarded to close the gap -- the records above it \
                 were reported durable -- so reads for those ids find nothing.",
                queue, expected, header.num_entries_before, lower, lower_last, upper,
                header.num_entries_before);

            if link + 1 == MAX_LINKS {
                log::error!(target: "normfs",
                    "Queue '{}' - stopped checking the id chain after {} links; there may be \
                     further gaps below file {}",
                    queue, MAX_LINKS, lower);
                return;
            }
            upper = lower;
        }
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

        // A file that could not be read is not an empty file: `get_file_end`
        // already reports "absent" and "no entries" as Ok(None), and the reuse
        // branch below hands its id to a writer that opens with truncate(true).
        let wal = self
            .wal
            .as_ref()
            .expect("WAL backend must be available in durable mode");
        let latest_unreadable = match wal.get_file_end(queue, &latest_file_id).await {
            Err(e) => {
                log::error!(target: "normfs",
                    "Queue '{}' - Latest file {} could not be read ({:?}); writing to the next \
                     file id rather than reusing it",
                    queue, latest_file_id, e);
                true
            }
            Ok(_) => false,
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
                    let next_file_id = if is_latest_file || latest_unreadable {
                        // Has entries, or could not be read: new file either way
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

                    self.report_id_chain_breaks(queue, &current_file_id).await;

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
                        let start_at = if latest_unreadable {
                            latest_file_id.increment()
                        } else {
                            UintN::one()
                        };
                        log::info!(target: "normfs",
                            "Queue '{}' - Reached first file with no entries, starting fresh at file {}",
                            queue, start_at
                        );
                        return Ok((start_at, Default::default(), None));
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

        if self.is_memory_only() {
            let last_entry_id = self
                .memory_pointers
                .as_ref()
                .and_then(|pointers| pointers.last_id(queue));
            let queue_config = self.get_config_for_queue(queue);
            self.mem.start_queue(
                queue,
                last_entry_id.clone(),
                mode.readonly,
                queue_config.pool,
            );
            log::info!(target: "normfs", "Memory-only queue '{}' started, last_entry_id: {:?}", queue, last_entry_id);
            return Ok(());
        }

        // Use the new backward search logic to find the correct file and entry to continue from
        let (file_id, header, last_entry_id) = self.continue_queue(queue).await?;

        log::info!(target: "normfs", "----------------------------------------");
        log::info!(target: "normfs", "Queue '{}' - Recovery complete:", queue);
        log::info!(target: "normfs", "  - Will write to file ID: {}", file_id);
        log::info!(target: "normfs", "  - Last entry ID in queue: {:?}", last_entry_id);
        log::info!(target: "normfs", "  - Header entries_before: {}", header.num_entries_before);
        log::info!(target: "normfs", "  - Next entry will have ID: {}", header.num_entries_before);
        log::info!(target: "normfs", "----------------------------------------");

        let queue_config = self.get_config_for_queue(queue);

        // Started first: the writer is handed this queue's page pool, so the
        // pool has to exist before it.
        self.mem.start_queue(
            queue,
            last_entry_id.clone(),
            mode.readonly,
            queue_config.pool,
        );

        if !mode.readonly {
            let mut wal_settings = self.settings.wal_settings.clone();
            wal_settings.enable_fsync = queue_config.enable_fsync;
            wal_settings.compression_type = queue_config.compression_type;
            wal_settings.encryption_type = queue_config.encryption_type;

            self.wal
                .as_ref()
                .expect("WAL backend must be available in durable mode")
                .start_writer_with_pool(
                    queue,
                    &file_id,
                    header,
                    wal_settings.clone(),
                    last_entry_id.clone(),
                    // Live. The records reach the file as pages, from the same
                    // memory they were accepted into. Rotation is decided at
                    // enqueue time, before the bytes enter a page, and the
                    // writer carries that decision out rather than making its
                    // own -- which is what keeps a page's bytes belonging to
                    // exactly one file.
                    self.mem.pool(queue),
                )
                .await?;

            let wal = self
                .wal
                .as_ref()
                .expect("WAL backend must be available in durable mode")
                .clone();
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

    /// Accepts a record, waiting if every page is occupied by records that are
    /// not yet on disk. That wait is the back-pressure: the queue declines to
    /// run ahead of the disk rather than dropping what it already took.
    pub async fn enqueue(&self, queue: &QueueId, data: Bytes) -> Result<UintN, Error> {
        if self.mem.is_closed(queue) {
            return Err(Error::QueueClosed);
        }
        check_framable(
            &data,
            self.page_size_for(queue),
            self.settings.max_memory_usage,
        )?;
        let Some((entry_id, placement)) = self.mem.enqueue_awaiting(queue, data.clone()).await
        else {
            // A close won the race after the check above. The record took
            // no id, so refusing it costs the sequence nothing.
            return Err(if self.mem.is_closed(queue) {
                Error::QueueClosed
            } else {
                Error::QueueNotFound
            });
        };

        log::debug!(target: "normfs", "Enqueuing entry - Queue: '{}', Entry ID: {}, Data size: {} bytes",
            queue, entry_id, data.len());

        if self.is_memory_only() {
            if let Some(pointers) = &self.memory_pointers {
                pointers.mark(queue, &entry_id).map_err(Error::Io)?;
            }
            self.mem.ack(queue, &entry_id);
            log::trace!(target: "normfs", "Memory-only entry enqueued successfully - Queue: '{}', Entry ID: {}", queue, entry_id);
            return Ok(entry_id);
        }

        self.wal
            .as_ref()
            .expect("WAL backend must be available in durable mode")
            .enqueue_pooled(queue, entry_id.clone(), data, placement)?;

        log::trace!(target: "normfs", "Entry enqueued successfully - Queue: '{}', Entry ID: {}", queue, entry_id);

        Ok(entry_id)
    }

    /// [`NormFS::enqueue`] for callers that cannot wait -- a capture thread, or
    /// a subscriber callback, which runs while its own queue holds the append
    /// gate. A refused record took no id.
    pub fn try_enqueue(&self, queue: &QueueId, data: Bytes) -> Result<UintN, Error> {
        if self.mem.is_closed(queue) {
            return Err(Error::QueueClosed);
        }
        check_framable(
            &data,
            self.page_size_for(queue),
            self.settings.max_memory_usage,
        )?;

        let (entry_id, placement) = match self
            .mem
            .try_enqueue(queue, data.clone())
            .ok_or(Error::QueueNotFound)?
        {
            mem::TryEnqueue::Placed(id, placement) => (id, placement),
            mem::TryEnqueue::Full => return Err(Error::WouldBlock),
            mem::TryEnqueue::Closed => return Err(Error::QueueClosed),
        };

        if self.is_memory_only() {
            if let Some(pointers) = &self.memory_pointers {
                pointers.mark(queue, &entry_id).map_err(Error::Io)?;
            }
            self.mem.ack(queue, &entry_id);
            return Ok(entry_id);
        }

        self.wal
            .as_ref()
            .expect("WAL backend must be available in durable mode")
            .enqueue_pooled(queue, entry_id.clone(), data, placement)?;

        Ok(entry_id)
    }

    pub async fn enqueue_batch(
        &self,
        queue: &QueueId,
        data: Vec<Bytes>,
    ) -> Result<Vec<UintN>, Error> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        if self.mem.is_closed(queue) {
            return Err(Error::QueueClosed);
        }
        let page_size = self.page_size_for(queue);
        for record in &data {
            check_framable(record, page_size, self.settings.max_memory_usage)?;
        }

        log::debug!(target: "normfs", "Enqueuing batch - Queue: '{}', Batch size: {} entries", queue, data.len());

        // Each record is placed exactly as a single enqueue would place it. It
        // has to be: a record that reached a page but was reported as not in
        // one would be written to the file twice — once from the writer's
        // buffer and once from its page.
        let Some(placed) = self.mem.enqueue_batch_awaiting(queue, data.clone()).await else {
            return Err(if self.mem.is_closed(queue) {
                Error::QueueClosed
            } else {
                Error::QueueNotFound
            });
        };
        let entry_ids: Vec<UintN> = placed.iter().map(|(id, _)| id.clone()).collect();

        if let (Some(first_id), Some(last_id)) = (entry_ids.first(), entry_ids.last()) {
            log::debug!(target: "normfs", "Batch entry IDs - Queue: '{}', First ID: {}, Last ID: {}",
                queue, first_id, last_id);
        }

        let wal_entries: Vec<(UintN, Bytes, normfs_wal::Placement)> = placed
            .into_iter()
            .zip(data.iter().cloned())
            .map(|((id, placement), d)| (id, d, placement))
            .collect();

        if self.is_memory_only() {
            if let Some(last_id) = entry_ids.last() {
                if let Some(pointers) = &self.memory_pointers {
                    pointers.mark(queue, last_id).map_err(Error::Io)?;
                }
                self.mem.ack(queue, last_id);
            }
            log::trace!(target: "normfs", "Memory-only batch enqueued successfully - Queue: '{}', Count: {}", queue, entry_ids.len());
            return Ok(entry_ids);
        }

        self.wal
            .as_ref()
            .expect("WAL backend must be available in durable mode")
            .enqueue_batch(queue, wal_entries)?;

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

    /// Closes a queue for good: no write is ever accepted again, reads stay,
    /// a follow ends at the last record. The order is the safety argument:
    /// refuse writes, flush and complete the file, then the marker, so a
    /// marker on disk implies the data reached it. Memory is released last.
    pub async fn close_queue(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = self.queue_init_lock(queue);
        let _guard = queue_lock.lock().await;

        log::info!(target: "normfs", "Closing queue '{}' for good", queue);
        let dir = queue.to_fs_path(&self.path);

        // Checks that change nothing come first, so a refused close leaves
        // no half-closed state. An unknown name is more likely a typo than
        // an intent, and "closed" is a reserved child name the same way
        // wal/ and store/ already are.
        if self.mem.get_last_id(queue).is_none() && !dir.exists() {
            return Err(Error::QueueNotFound);
        }

        if dir.join("closed").is_dir() {
            return Err(Error::Io(std::io::Error::other(
                "a child queue named 'closed' occupies this queue's marker path",
            )));
        }

        // Waits for the in-flight append: after this, everything accepted
        // is placed and nothing more can be.
        self.mem.begin_close(queue).await;

        if let Some(wal) = self.wal.as_ref() {
            wal.close_writer(queue).await?;
        }

        // The marker certifies everything accepted is on disk. Records a
        // failed flush stranded stay in the WAL file for recovery; the close
        // stays incomplete rather than certifying loss. Memory-only has no
        // disk to certify: its close only ends the write side.
        if self.wal.is_some() && !self.mem.is_fully_durable(queue) {
            return Err(Error::Wal(WalError::CloseIncomplete));
        }

        std::fs::create_dir_all(&dir)?;
        let marker = std::fs::File::create(dir.join("closed"))?;
        marker.sync_all()?;
        // The directory entry must survive a power cut too.
        std::fs::File::open(&dir)?.sync_all()?;

        self.mem.close_queue(queue);
        Ok(())
    }

    pub async fn close(&self) -> Result<(), Error> {
        log::info!(target: "normfs", "Closing NormFS");

        if let Some(pointers) = &self.memory_pointers {
            pointers.flush_if_dirty().map_err(Error::Io)?;
        }

        if let Some(task) = &self.memory_pointer_task {
            task.abort();
        }

        // Close the store first to shut down writer workers
        if let Some(store) = &self.store {
            store.close().await;
        }

        // Then close the WAL
        if let Some(wal) = &self.wal {
            wal.close().await?;
        }

        log::info!(target: "normfs", "NormFS closed successfully");
        Ok(())
    }
}
