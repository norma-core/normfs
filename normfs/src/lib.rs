mod bucket_check;
#[cfg(test)]
mod bucket_check_test;
pub(crate) mod lookup;
mod mem;
pub mod server;
pub mod proto {
    include!("proto/normfs.rs");
    pub mod system {
        include!("proto/normfs.system.rs");
    }
}
mod config;
mod memory_pointers;
#[cfg(test)]
mod memory_pointers_test;
mod offload;
pub(crate) mod reader_fsm;
mod system;

use bytes::Bytes;
use core::time::Duration;
use normfs_crypto::CryptoContext;
use normfs_fs::{Fs, FsConfig, Runs, TmpMode};
use normfs_store::layer::LayerError;
use normfs_store::{Layer, PersistStore};
use normfs_types::events::{EventSink, SystemEvent};
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

pub use crate::config::{
    ConfigError, Drainer, Persist, PoolKind, QueueConfig, QueueMode, QueueSettings,
};

pub use system::SYSTEM_QUEUE;
pub use uintn::{Error as UintNError, UintN, UintNType};

/// What [`NormFS::file_room`] saw of a queue's open file. A file is what a
/// queue is stored, offloaded and evicted by; a queue kept only in memory
/// has none, and its pages stand in.
#[derive(Debug, Clone, Copy)]
pub struct FileRoom {
    room: Option<usize>,
    file: normfs_wal::FileMark,
}

impl FileRoom {
    /// The widest record that still joins the open file, or `None` when the
    /// next record starts a new one.
    pub fn room(&self) -> Option<usize> {
        self.room
    }

    /// Whether a record of `record_len` bytes would be the first of a file.
    /// Also true for one wider than a page, which the append refuses.
    pub fn starts_file(&self, record_len: usize) -> bool {
        self.room.is_none_or(|room| record_len > room)
    }

    /// Whether no file started between this look and `later`.
    pub fn same_file(&self, later: &FileRoom) -> bool {
        self.file == later.file
    }
}

pub struct NormFS {
    path: std::path::PathBuf,
    fs: Fs,
    wal: Arc<WalStore>,
    store: Arc<PersistStore>,
    mem: Arc<mem::MemStore>,
    disk_monitor: Option<Arc<DiskMonitor>>,
    /// `None` without a bucket.
    offloaders: Option<Arc<offload::offloaders::Offloaders>>,
    cloud: Option<Arc<Layer>>,
    /// `None` without cloud settings; `new` refuses any rule that asks for cloud then.
    cloud_sink: Option<Arc<normfs_store::LayerSink>>,
    memory_pointers: Arc<memory_pointers::MemoryPointers>,
    memory_pointer_task: JoinHandle<()>,
    crypto_ctx: Arc<CryptoContext>,
    settings: NormFsSettings,
    reader_fsm: reader_fsm::ReaderFSM,
    queue_resolver: normfs_types::QueueIdResolver,
    queue_init_locks: RwLock<HashMap<QueueId, Arc<Mutex<()>>>>,
    /// The seed was made by this run: no earlier life wrote under this
    /// instance's id.
    fresh_instance: bool,
    bucket_down: std::sync::Mutex<Option<(std::time::Instant, String)>>,
    placer: Placer,
    system_queue: QueueId,
    events: EventSink,
    system_writer: system::Writer,
}

#[derive(Debug)]
pub enum Error {
    Wal(WalError),
    Store(StoreError),
    Config(ConfigError),
    Cloud(LayerError),
    Io(std::io::Error),
    QueueNotFound,
    QueueEmpty,
    NotFound,
    ClientDisconnected,
    /// The record does not fit a page, framing included, so no page can hold
    /// it. Refused before an id is taken — see [`NormFS::enqueue`].
    RecordTooLarge(usize),
    /// No page could take the record within the wait the caller allowed
    /// ([`NormFS::try_enqueue`], [`NormFS::enqueue_timeout`]). It took no id.
    WouldBlock,
    /// [`NormFS::try_enqueue_in`]: the file the caller saw has ended, or the
    /// record no longer fits it. It took no id.
    NotInFile,
    /// The queue is closed ([`NormFS::close_queue`]): writes are refused
    /// until it is started for write again. The data stays readable.
    QueueClosed,
    /// The queue is NormFS's own ([`SYSTEM_QUEUE`]): it can be read, but
    /// only NormFS writes or closes it.
    ReservedQueue,
    /// `max_memory_usage` cannot hold the two pages a single queue needs to
    /// work. Refused at construction rather than rounded up: rounding up would
    /// mean the process quietly using more memory than it was configured for.
    MemoryBelowFloor {
        max_memory_usage: usize,
        page_size: usize,
        needed: usize,
    },
    /// A queue could not list the bucket on start and nothing local says what
    /// it holds: a cloud-direct queue of an existing instance with no record,
    /// or a local queue after a cloud-direct life, whose file numbers have to
    /// follow the bucket's.
    BucketUnreachable {
        queue: String,
        cause: String,
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
            Error::Config(e) => write!(f, "configuration error: {}", e),
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
            Error::WouldBlock => {
                write!(f, "No page became free within the wait the caller allowed")
            }
            Error::NotInFile => write!(f, "Record would not join the file the caller saw"),
            Error::QueueClosed => write!(f, "Queue is closed and accepts no more writes"),
            Error::ReservedQueue => write!(f, "Queue is written by NormFS itself"),
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
            Error::BucketUnreachable { queue, cause } => {
                write!(f, "queue '{queue}' needs the bucket to start: {cause}")
            }
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
            Error::Config(e) => Some(e),
            Error::Cloud(e) => Some(e),
            Error::Io(e) => Some(e),
            Error::QueueNotFound => None,
            Error::QueueEmpty => None,
            Error::NotFound => None,
            Error::ClientDisconnected => None,
            Error::RecordTooLarge(_) => None,
            Error::WouldBlock => None,
            Error::NotInFile => None,
            Error::QueueClosed => None,
            Error::ReservedQueue => None,
            Error::MemoryBelowFloor { .. } => None,
            Error::BucketUnreachable { .. } => None,
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

impl From<ConfigError> for Error {
    fn from(e: ConfigError) -> Self {
        Error::Config(e)
    }
}

impl From<normfs_cloud::errors::CloudError> for Error {
    fn from(e: normfs_cloud::errors::CloudError) -> Self {
        Error::Cloud(LayerError::Backend(e.into()))
    }
}

impl From<normfs_fs::FsError> for Error {
    fn from(e: normfs_fs::FsError) -> Self {
        Error::Io(e.into())
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// How long a cloud-direct queue's start waits on the bucket before it
/// resumes from what this instance recorded. Finding the last file lists
/// every directory of the queue, a few dozen requests on a big one, so this
/// is not a connect timeout; a dead link fails at the client's own.
const BUCKET_CHECK_TIMEOUT: Duration = Duration::from_secs(30);

/// After a failed check, how long later starts go without the bucket at once
/// rather than each waiting it out. The first file still waits for it.
const BUCKET_DOWN_FOR: Duration = Duration::from_secs(60);

/// The bucket's failure, worded for the log and for [`Error::BucketUnreachable`].
/// `true` when the bucket answered with a refusal rather than not at all.
fn bucket_cause(e: &Error) -> (String, bool) {
    use normfs_store::BackendError;
    match e {
        Error::Cloud(LayerError::Backend(BackendError::Status(code)))
            if (400..500).contains(code) =>
        {
            let cause = format!(
                "the bucket refused the listing (HTTP {code}): check the credentials and the \
                 bucket's permissions"
            );
            (cause, true)
        }
        e => (format!("the bucket did not answer: {e}"), false),
    }
}

struct CloudResume {
    landed: Option<(UintN, UintN)>,
    /// The bucket did not answer: the first file id is still to be checked.
    unchecked: bool,
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
    pub memory_pointers_flush_interval: Duration,
    /// Store and cloud files a read may hold decoded at once. A file is decoded
    /// whole, so this many file sizes bound what cold reads hold.
    pub cold_read_files: usize,
    /// Record cloud uploads, file metadata and failures in [`SYSTEM_QUEUE`].
    /// The path stays reserved when this is off.
    pub system_queue: bool,
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
            memory_pointers_flush_interval: Duration::from_secs(5),
            cold_read_files: 2,
            system_queue: true,
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

/// The steps of an append that follow the id, apart from the queue they
/// belong to: what `NormFS` runs for a client and the system queue writer
/// runs for itself.
#[derive(Clone)]
pub(crate) struct Placer {
    mem: Arc<mem::MemStore>,
    wal: Arc<WalStore>,
    memory_pointers: Arc<memory_pointers::MemoryPointers>,
    queue_settings: Arc<QueueSettings>,
}

impl Placer {
    /// What a record needs once it is in a page: a memory queue acks it here,
    /// a WAL queue tells its writer, a page-per-file queue nothing -- its
    /// writer takes the page whole and the ack comes back from the sink.
    fn after_place(
        &self,
        queue: &QueueId,
        entry_id: &UintN,
        data: Bytes,
        placement: normfs_wal::Placement,
    ) -> Result<(), Error> {
        match self
            .queue_settings
            .get_config(queue.as_str())
            .persist
            .drainer()
        {
            Drainer::None => {
                self.memory_pointers
                    .mark(queue, entry_id)
                    .map_err(Error::Io)?;
                self.mem.ack(queue, entry_id);
            }
            Drainer::Wal => {
                self.wal
                    .enqueue_pooled(queue, entry_id.clone(), data, placement)?;
            }
            Drainer::Page => {}
        }
        Ok(())
    }

    /// Waits for a page. `None` when the queue is closed or not started, or
    /// its drainer refused the record.
    pub(crate) async fn append(&self, queue: &QueueId, data: Bytes) -> Option<UintN> {
        let (id, placement) = self.mem.enqueue_awaiting(queue, data.clone()).await?;
        match self.after_place(queue, &id, data, placement) {
            Ok(()) => Some(id),
            Err(e) => {
                log::warn!(target: "normfs", "queue '{}': record {id} placed but not handed on: {e}", queue.short());
                None
            }
        }
    }
}

impl NormFS {
    pub async fn new<P: AsRef<Path> + Send + 'static>(
        path: P,
        mut settings: NormFsSettings,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        log::debug!(target: "normfs", "Creating new NormFS at path: {:?}", path);

        let fs = Fs::new(FsConfig::default()).map_err(Error::Io)?;
        fs.mkdir_all(&path).await?;

        let crypto_path = path.clone();
        let (fresh_instance, crypto_ctx) = fs
            .run_blocking(move || {
                let fresh = !CryptoContext::exists(&crypto_path);
                let ctx = CryptoContext::open(&crypto_path).map_err(|e| {
                    std::io::Error::other(format!("Failed to open crypto context: {}", e))
                })?;
                Ok((fresh, Arc::new(ctx)))
            })
            .await?;

        let instance_id = crypto_ctx.instance_id_hex();

        let queue_resolver = normfs_types::QueueIdResolver::new(instance_id);
        let (system, system_rx) = system::SystemQueue::new(queue_resolver.resolve(SYSTEM_QUEUE));
        let events = if settings.system_queue {
            system.sink()
        } else {
            normfs_types::events::discard()
        };

        let mem = Arc::new(mem::MemStore::with_pools(
            settings.max_memory_usage,
            settings.mem_page_size,
            settings.max_passive_memory_usage,
            settings.mem_passive_page_size,
        )?);

        settings.queue_settings.validate()?;
        let cloud_rules = settings.queue_settings.cloud_rules();
        if settings.cloud_settings.is_none() {
            if let Some(pattern) = cloud_rules.first() {
                return Err(ConfigError::CloudWithoutSettings {
                    pattern: pattern.clone(),
                }
                .into());
            }
        }
        if settings.system_queue {
            let config = system::queue_config(
                &settings.queue_settings.default_config,
                settings.cloud_settings.is_some(),
            );
            settings.queue_settings = settings
                .queue_settings
                .clone()
                .with_override(system.queue().as_str(), config);
        }

        let memory_pointers = Arc::new(
            memory_pointers::MemoryPointers::open(fs.clone(), &path)
                .await
                .map_err(Error::Io)?,
        );
        let memory_pointer_task =
            memory_pointers.spawn_flusher(settings.memory_pointers_flush_interval);

        let (wal_entry_send, mut wal_entry_recv) = tokio::sync::mpsc::unbounded_channel();
        let (wal_complete_send, wal_complete_recv): (
            tokio::sync::mpsc::UnboundedSender<WalFile>,
            tokio::sync::mpsc::UnboundedReceiver<WalFile>,
        ) = tokio::sync::mpsc::unbounded_channel();

        let wal = Arc::new(WalStore::with_fs(
            &path,
            wal_entry_send.clone(),
            wal_complete_send,
            fs.clone(),
        ));

        // Page-per-file queues pack one page at a time, so a slot holds the
        // larger page; one slot per store worker caps packs in flight.
        let packer = normfs_store::Packer::new(
            settings.store_cfg.num_workers,
            normfs_wal::WAL_HEADER_V1_MAX_SIZE
                + settings.mem_page_size.max(settings.mem_passive_page_size),
        )
        .map_err(Error::Io)?;
        // A WAL file overshoots `max_file_size` by at most the tail of a page.
        let wal_packer = normfs_store::Packer::new(
            settings.store_cfg.num_workers,
            normfs_wal::WAL_HEADER_V1_MAX_SIZE
                + settings.wal_settings.max_file_size
                + settings.mem_page_size.max(settings.mem_passive_page_size),
        )
        .map_err(Error::Io)?;
        let store = PersistStore::new(
            &path,
            settings.store_cfg.clone(),
            crypto_ctx.clone(),
            wal.clone(),
            fs.clone(),
            wal_entry_send.clone(),
        )
        .with_events(events.clone())
        .with_packer(Arc::new(packer))
        .with_wal_packer(Arc::new(wal_packer));

        store.recover().await?;

        let store_done_rx = store.start_writers(wal_complete_recv).await;

        let mem_clone = mem.clone();
        tokio::spawn(async move {
            while let Some((queue_id, id)) = wal_entry_recv.recv().await {
                log::trace!(target: "normfs", "Processing WAL ack - Queue: '{}', Entry ID: {}", queue_id.short(), id);
                mem_clone.ack(&queue_id, &id);
            }
        });

        // Create S3 client and extract prefix if settings are provided
        let (cloud_client, cloud_prefix) = if let Some(ref cloud_settings) = settings.cloud_settings
        {
            let endpoint = url::Url::parse(&cloud_settings.endpoint)
                .map_err(|e| Error::from(normfs_cloud::errors::CloudError::InvalidUrl(e)))?;

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
                    if let Some(pattern) = cloud_rules.first() {
                        return Err(ConfigError::CloudWithoutSettings {
                            pattern: pattern.clone(),
                        }
                        .into());
                    }
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        // Files read back from the bucket have their signatures checked whole.
        let cloud = if let (Some(client), Some(prefix)) = (&cloud_client, &cloud_prefix) {
            log::info!(target: "normfs", "Cloud layer under prefix: {}", prefix);
            let backend = normfs_cloud::S3Store::new(client.clone(), prefix);
            Some(Arc::new(Layer::new(Arc::new(backend), None, true)))
        } else {
            log::info!(target: "normfs", "Cloud layer disabled");
            None
        };

        // Initialize disk monitor if enabled
        let store_arc = Arc::new(store);

        let disk_monitor = if settings.max_disk_usage_per_queue.is_some() {
            log::debug!(target: "normfs", "Disk monitor enabled, creating disk monitor instance");
            let store = store_arc.clone();
            let forget_range: offload::disk_monitor::ForgetRange =
                Arc::new(move |queue, file_id| store.forget_file_range(queue, file_id));
            match DiskMonitor::new(
                fs.clone(),
                &path,
                Some(forget_range),
                store_arc.disk_usage(),
                events.clone(),
            )
            .await
            {
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

        let offloaders = cloud.clone().map(|to| {
            Arc::new(offload::offloaders::Offloaders::new(
                store_arc.local().clone(),
                to,
                wal.backend().clone(),
                events.clone(),
            ))
        });

        // Always consume store completions to prevent SendError on the sender side
        let offloaders_rx = offloaders.clone();
        tokio::spawn(async move {
            let mut store_done_rx = store_done_rx;
            while let Some((queue_id, file_id)) = store_done_rx.recv().await {
                if let Some(offloaders) = &offloaders_rx {
                    offloaders.file_landed(&queue_id, file_id).await;
                }
            }
            log::info!(target: "normfs", "Store completion forwarding task ended");
        });

        log::info!(target: "normfs", "NormFS initialized successfully (disk_monitor: {}, s3: {})",
            if settings.max_disk_usage_per_queue.is_some() { "enabled" } else { "disabled" },
            if cloud.is_some() { "enabled" } else { "disabled" });

        // A queue whose first layer is the bucket keeps nothing local to list
        // on restart, so its landings are recorded.
        let cloud_sink = cloud.as_ref().map(|cloud| {
            let after = normfs_store::AfterLanding::Record(memory_pointers.clone());
            Arc::new(normfs_store::LayerSink::new(
                cloud.clone(),
                after,
                events.clone(),
            ))
        });
        let reader_fsm = reader_fsm::ReaderFSM::new(
            wal.clone(),
            store_arc.clone(),
            mem.clone(),
            cloud.clone(),
            Arc::new(settings.queue_settings.clone()),
            memory_pointers.clone(),
            settings.cold_read_files,
        );

        let placer = Placer {
            mem: mem.clone(),
            wal: wal.clone(),
            memory_pointers: memory_pointers.clone(),
            queue_settings: Arc::new(settings.queue_settings.clone()),
        };
        let normfs = Self {
            path: path.clone(),
            fs,
            wal,
            store: store_arc,
            mem,
            disk_monitor,
            offloaders,
            cloud,
            cloud_sink,
            memory_pointers,
            memory_pointer_task,
            crypto_ctx,
            settings: settings.clone(),
            reader_fsm,
            queue_resolver,
            queue_init_locks: RwLock::new(HashMap::new()),
            fresh_instance,
            bucket_down: std::sync::Mutex::new(None),
            placer,
            system_queue: system.queue().clone(),
            events,
            system_writer: system::Writer::idle(),
        };
        if normfs.settings.system_queue {
            let queue = &normfs.system_queue;
            normfs.ensure_queue_exists_for_write(queue).await?;
            let max_record = normfs_wal::max_record_len(normfs.page_size_for(queue))
                .min(normfs.settings.max_memory_usage);
            normfs
                .system_writer
                .run(system, system_rx, max_record, normfs.placer.clone());
        }
        Ok(normfs)
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

    fn persist_for(&self, queue: &QueueId) -> Persist {
        self.get_config_for_queue(queue).persist
    }

    // Consults the durable marker once and mirrors it into memory.
    async fn queue_closed_durably(&self, queue: &QueueId) -> bool {
        if self.mem.is_closed(queue) {
            return true;
        }
        // A queue live in memory had its marker consulted when it started;
        // the disk stat stays off the per-request path.
        if self.mem.get_last_id(queue).is_some() {
            return false;
        }
        let marker = queue.to_fs_path(&self.path).join("closed");
        if self
            .fs
            .stat(&marker)
            .await
            .ok()
            .flatten()
            .is_some_and(|m| m.is_file())
        {
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
        self.queue_closed_durably(queue).await;

        if self.mem.get_last_id(queue).is_some() {
            return Ok(());
        }

        log::info!(target: "normfs", "Auto-starting queue '{}' in readonly mode for read request", queue.short());
        self.start_queue(queue, QueueMode { readonly: true }).await
    }

    pub async fn ensure_queue_exists_for_write(&self, queue: &QueueId) -> Result<(), Error> {
        let queue_lock = self.queue_init_lock(queue);
        let _guard = queue_lock.lock().await;

        // A retry still owns its file id until the old writer has finished.
        if self.store.page_writer_is_closing(queue) && !self.store.close_page_writer(queue).await {
            return Err(StoreError::CloseIncomplete.into());
        }

        if self.queue_closed_durably(queue).await {
            self.reopen_queue(queue).await?;
        }

        let queue_exists = self.mem.get_last_id(queue).is_some();
        let has_writer = match self.persist_for(queue).drainer() {
            Drainer::None => true,
            Drainer::Wal => self.wal.has_writer(queue).await,
            Drainer::Page => self.store.has_page_writer(queue),
        };

        if queue_exists && has_writer {
            return Ok(());
        }

        if queue_exists && !has_writer {
            log::info!(target: "normfs", "Restarting queue '{}' from readonly to write mode", queue.short());
        } else {
            log::info!(target: "normfs", "Auto-starting queue '{}' in write mode for write request", queue.short());
        }

        self.start_queue(queue, QueueMode { readonly: false }).await
    }

    /// Undoes a close so the queue can be started for write. The marker goes
    /// first and is synced, so a crash here leaves the queue closed rather
    /// than half-open; the start that follows recovers the last id from the
    /// files the close completed.
    async fn reopen_queue(&self, queue: &QueueId) -> Result<(), Error> {
        log::info!(target: "normfs", "Reopening closed queue '{}' for write", queue.short());
        let dir = queue.to_fs_path(&self.path);
        if let Err(e) = self.fs.remove_durable(&dir.join("closed"), true).await {
            let e = std::io::Error::from(e);
            // No directory: nothing was closed, and there is nothing to sync.
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        self.mem.reopen(queue);
        Ok(())
    }

    fn refuse_reserved(&self, queue: &QueueId) -> Result<(), Error> {
        if queue == &self.system_queue {
            return Err(Error::ReservedQueue);
        }
        Ok(())
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
        let wal = &self.wal;
        let store = &self.store;

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_file_id, store_file_id) =
            tokio::join!(wal.find_last_file_id(queue), store.find_last_file_id(queue));

        // Log results from each source individually
        match &wal_file_id {
            Ok(id) => {
                log::info!(target: "normfs", "Queue '{}' - WAL latest file ID: {}", queue.short(), id)
            }
            Err(e) => {
                log::info!(target: "normfs", "Queue '{}' - WAL has no files or error: {:?}", queue.short(), e)
            }
        }

        match &store_file_id {
            Ok(id) => {
                log::info!(target: "normfs", "Queue '{}' - Store latest file ID: {}", queue.short(), id)
            }
            Err(e) => {
                log::info!(target: "normfs", "Queue '{}' - Store has no files or error: {:?}", queue.short(), e)
            }
        }

        log::info!(target: "normfs",
            "Queue '{}' - Latest file IDs summary: WAL={:?}, Store={:?}",
            queue.short(),
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
            queue.short(),
            max_file_id
        );

        max_file_id
    }

    /// Get the last entry ID in a specific file across WAL and Store only (S3 is too slow).
    /// Queries WAL and Store in parallel and returns the maximum last entry ID found,
    /// or None if the file has no entries in any source.
    async fn get_file_end_all_sources(&self, queue: &QueueId, file_id: &UintN) -> Option<UintN> {
        let wal = &self.wal;
        let store = &self.store;

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_end, store_end) = tokio::join!(
            wal.get_file_end(queue, file_id),
            store.get_file_end(queue, file_id)
        );

        // Log detailed results from each source
        match &wal_end {
            Ok(Some(id)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL has entries, last entry ID: {}", queue.short(), file_id, id),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL file exists but has no entries", queue.short(), file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - WAL query error: {:?}", queue.short(), file_id, e),
        }

        match &store_end {
            Ok(Some(id)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store has entries, last entry ID: {}", queue.short(), file_id, id),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store file exists but has no entries", queue.short(), file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - Store query error: {:?}", queue.short(), file_id, e),
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {:?} - Last entry IDs summary: WAL={:?}, Store={:?}",
            queue.short(),
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
                        queue.short(), file_id, id, current_max);
                    max_last_entry_id = Some(id);
                    max_source = "WAL";
                }
                Some(_current_max) => {
                    log::debug!(target: "normfs",
                        "Queue '{}', File ID {} - WAL entry ID {} is not maximum",
                        queue.short(), file_id, id);
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - WAL entry ID {} is first candidate",
                        queue.short(), file_id, id);
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
                        queue.short(), file_id, id, current_max);
                    max_last_entry_id = Some(id);
                    max_source = "Store";
                }
                Some(_current_max) => {
                    log::debug!(target: "normfs",
                        "Queue '{}', File ID {} - Store entry ID {} is not maximum",
                        queue.short(), file_id, id);
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}', File ID {} - Store entry ID {} is first candidate",
                        queue.short(), file_id, id);
                    max_last_entry_id = Some(id);
                    max_source = "Store";
                }
            }
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {} - Selected maximum last entry ID: {:?} from source: {}",
            queue.short(),
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
        let wal = &self.wal;
        let store = &self.store;

        // Query WAL and Store only (S3 is too slow for recovery)
        let (wal_header, store_header) = tokio::join!(
            wal.get_file_header(queue, file_id),
            store.get_file_header(queue, file_id)
        );

        // Log individual source results
        match &wal_header {
            Ok(Some(header)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL header: data_size={}, id_size={}, entries_before={}",
                queue.short(), file_id, header.data_size_bytes, header.id_size_bytes, header.num_entries_before),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - WAL header not found", queue.short(), file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - WAL header error: {:?}", queue.short(), file_id, e),
        }

        match &store_header {
            Ok(Some(header)) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store header: data_size={}, id_size={}, entries_before={}",
                queue.short(), file_id, header.data_size_bytes, header.id_size_bytes, header.num_entries_before),
            Ok(None) => log::info!(target: "normfs",
                "Queue '{}', File ID {} - Store header not found", queue.short(), file_id),
            Err(e) => log::debug!(target: "normfs",
                "Queue '{}', File ID {} - Store header error: {:?}", queue.short(), file_id, e),
        }

        // Return the first valid header found (prefer WAL, then Store)
        if let Ok(Some(header)) = wal_header {
            log::info!(target: "normfs",
                "Queue '{}', File ID {} - Selected WAL header for recovery",
                queue.short(), file_id
            );
            return Some(header);
        }

        if let Ok(Some(header)) = store_header {
            log::info!(target: "normfs",
                "Queue '{}', File ID {} - Selected Store header for recovery",
                queue.short(), file_id
            );
            return Some(header);
        }

        log::info!(target: "normfs",
            "Queue '{}', File ID {} - No header found in WAL or Store",
            queue.short(), file_id
        );
        None
    }

    /// Reports ids that no file holds, or that two files hold, walking down from
    /// the file recovery is resuming after.
    ///
    /// No entry body is read: file F's `num_entries_before` should be one past
    /// the last id of the file below it. Above it, the difference is exactly what
    /// a failed closing flush lost; below it, a second process recovered the
    /// queue while the first was still writing it.
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
            let lower = match upper.decrement() {
                Ok(lower) if !lower.is_zero() => lower,
                _ => return,
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
            if header.num_entries_before < expected {
                // Only ids both files hold: the later start to the earlier end.
                let lower_first = self
                    .get_file_header_all_sources(queue, &lower)
                    .await
                    .map(|h| h.num_entries_before);
                let upper_last = self.get_file_end_all_sources(queue, &upper).await;
                if let (Some(lower_first), Some(upper_last)) = (lower_first, upper_last) {
                    let first = header.num_entries_before.clone().max(lower_first);
                    let last = upper_last.min(lower_last.clone());
                    if first <= last {
                        log::error!(target: "normfs",
                            "Queue '{}' - ids {}..={} are in two files: file {} ends at {} and \
                             file {} starts at {}. Another process was still writing the queue \
                             when this one recovered it, so both wrote those ids; a read gets \
                             one of the two.",
                            queue.short(), first, last, lower, lower_last, upper,
                            header.num_entries_before);
                    }
                }
            } else {
                log::error!(target: "normfs",
                    "Queue '{}' - ids {}..{} reach no file: file {} ends at {} and file {} \
                     starts at {}. A closing flush lost them and the retry did not land before \
                     the process ended. Nothing is discarded to close the gap -- the records \
                     above it were reported durable -- so reads for those ids find nothing.",
                    queue.short(), expected, header.num_entries_before, lower, lower_last, upper,
                    header.num_entries_before);
            }

            if link + 1 == MAX_LINKS {
                log::error!(target: "normfs",
                    "Queue '{}' - stopped checking the id chain after {} links; there may be \
                     further gaps below file {}",
                    queue.short(), MAX_LINKS, lower);
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
        log::info!(target: "normfs", "Continuing queue: '{}'", queue.short());

        // Get the latest file ID across all sources
        let latest_file_id = match self.get_latest_file(queue).await {
            Some(id) => {
                log::info!(target: "normfs",
                    "Queue '{}' - Found latest file ID: {:?}",
                    queue.short(), id
                );
                id
            }
            None => {
                log::info!(target: "normfs",
                    "Queue '{}' - No files found, starting fresh",
                    queue.short()
                );
                return Ok((UintN::one(), Default::default(), None));
            }
        };

        // A file that could not be read is not an empty file: `get_file_end`
        // already reports "absent" and "no entries" as Ok(None), and the reuse
        // branch below hands its id to a writer that truncates a `.wal` or
        // renames a `.store` over it. Both sources are asked: in store mode
        // the latest file is a store file the WAL has never seen.
        let (wal_end, store_end) = tokio::join!(
            self.wal.get_file_end(queue, &latest_file_id),
            self.store.get_file_end(queue, &latest_file_id)
        );
        let unreadable = match (&wal_end, &store_end) {
            (Err(e), _) => Some(format!("{e:?}")),
            (_, Err(e)) => Some(format!("{e:?}")),
            _ => None,
        };
        if let Some(e) = &unreadable {
            log::error!(target: "normfs",
                "Queue '{}' - Latest file {} could not be read ({}); writing to the next \
                 file id rather than reusing it",
                queue.short(), latest_file_id, e);
        }
        let latest_unreadable = unreadable.is_some();

        // Walk backward from the latest file ID to find the first file with actual entries
        let mut current_file_id = latest_file_id.clone();

        loop {
            log::info!(target: "normfs",
                "Queue '{}' - Checking file ID {:?} for entries",
                queue.short(), current_file_id
            );

            // Try to get the last entry ID in this file
            match self.get_file_end_all_sources(queue, &current_file_id).await {
                Some(last_entry_id) => {
                    log::info!(target: "normfs",
                        "Queue '{}' - Found file with entries: file_id={:?}, last_entry_id={:?}",
                        queue.short(), current_file_id, last_entry_id
                    );

                    // Get the header from this file
                    let header = self
                        .get_file_header_all_sources(queue, &current_file_id)
                        .await
                        .unwrap_or_default();

                    log::info!(target: "normfs",
                        "Queue '{}' - File {:?} header: data_size={}, id_size={}, entries_before={}",
                        queue.short(), current_file_id,
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
                        queue.short(), current_file_id, next_file_id,
                        if is_latest_file { "(new file)" } else { "(reusing empty latest file)" }
                    );

                    log::info!(target: "normfs",
                        "Queue '{}' - Starting WAL writer: file_id={}, num_entries_before={}, last_entry_id={:?}",
                        queue.short(), next_file_id, new_header.num_entries_before, last_entry_id
                    );

                    self.report_id_chain_breaks(queue, &current_file_id).await;

                    return Ok((next_file_id, new_header, Some(last_entry_id)));
                }
                None => {
                    log::info!(target: "normfs",
                        "Queue '{}' - File {:?} has no entries, trying previous file",
                        queue.short(), current_file_id
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
                            queue.short(), start_at
                        );
                        return Ok((start_at, Default::default(), None));
                    }

                    // Decrement to previous file
                    current_file_id = current_file_id.decrement().map_err(|e| {
                        log::error!(target: "normfs",
                            "Queue '{}' - Failed to decrement file ID: {:?}",
                            queue.short(), e
                        );
                        Error::Store(normfs_store::StoreError::UintN(e))
                    })?;
                }
            }
        }
    }

    /// The last id and file earlier cloud lives of this queue used. The bucket
    /// is asked when the queue is cloud-direct now, or was in an earlier life
    /// -- a store queue that never was does not wait on S3 to start.
    ///
    /// A cloud-direct queue does not wait for a bucket that does not answer
    /// if this instance has a record of it: its ids then come from the reserve
    /// and its first file id from the bucket, once it answers. Without a
    /// record nothing proves which ids are free, so the start fails.
    async fn cloud_landed(&self, queue: &QueueId, persist: Persist) -> Result<CloudResume, Error> {
        let landed = self.memory_pointers.last_landed(queue);
        let recorded = self.memory_pointers.last_id(queue).is_some();
        let resume = |landed, unchecked| CloudResume { landed, unchecked };
        let Some(cloud) = self.cloud.as_ref() else {
            if persist.cloud || !recorded || self.memory_pointers.is_settled(queue) {
                return Ok(resume(landed, false));
            }
            return Err(self.local_needs_bucket(queue, "no bucket is configured".into()));
        };
        if !persist.cloud || persist.store {
            // A settled pointer names the bucket's last file; otherwise the
            // one it names may be behind, after a crash or an unrecorded
            // landing, and local files numbered after it would be offloaded
            // over objects.
            if !recorded || self.memory_pointers.is_settled(queue) {
                return Ok(resume(landed, false));
            }
            let checked =
                tokio::time::timeout(BUCKET_CHECK_TIMEOUT, self.bucket_landed(cloud, queue)).await;
            return match checked {
                Ok(Ok(found)) => Ok(resume(found.or(landed), false)),
                Ok(Err(e)) => Err(self.local_needs_bucket(queue, bucket_cause(&e).0)),
                Err(_) => Err(self.local_needs_bucket(
                    queue,
                    format!("the bucket did not answer within {BUCKET_CHECK_TIMEOUT:?}"),
                )),
            };
        }
        // A seed made by this run is a namespace nothing was written to.
        // Only for its first start in this run: after that the pointer says
        // what it wrote.
        if self.fresh_instance && queue.as_str().starts_with(&self.instance_prefix()) && !recorded {
            self.memory_pointers
                .record(queue)
                .await
                .map_err(Error::Io)?;
            return Ok(resume(None, false));
        }
        let mut refused = false;
        let cause = match self.bucket_down() {
            Some(cause) => cause,
            None => {
                let checked =
                    tokio::time::timeout(BUCKET_CHECK_TIMEOUT, self.bucket_landed(cloud, queue))
                        .await;
                let cause = match checked {
                    Ok(Ok(found)) => {
                        self.set_bucket_down(None);
                        self.memory_pointers
                            .record(queue)
                            .await
                            .map_err(Error::Io)?;
                        // A hint past the bucket's last file means the
                        // listing is behind; the hint wins.
                        let landed = match (found, landed) {
                            (Some((last, file)), Some((_, hint))) if hint > file => {
                                Some((last, hint))
                            }
                            (Some(found), _) => Some(found),
                            (None, landed) => landed,
                        };
                        return Ok(resume(landed, false));
                    }
                    Ok(Err(e)) => {
                        let cause;
                        (cause, refused) = bucket_cause(&e);
                        cause
                    }
                    Err(_) => format!("the bucket did not answer within {BUCKET_CHECK_TIMEOUT:?}"),
                };
                self.set_bucket_down(Some(cause.clone()));
                cause
            }
        };
        if !recorded {
            return Err(Error::BucketUnreachable {
                queue: queue.to_string(),
                cause: format!("it has no local record, and {cause}"),
            });
        }
        let level = if refused {
            log::Level::Error
        } else {
            log::Level::Warn
        };
        log::log!(target: "normfs", level,
            "Queue '{}' - starting without the bucket ({}); resuming from the local pointer {:?}",
            queue, cause, landed);
        Ok(resume(landed, true))
    }

    fn local_needs_bucket(&self, queue: &QueueId, cause: String) -> Error {
        Error::BucketUnreachable {
            queue: queue.to_string(),
            cause: format!(
                "its last cloud-direct life ended without recording its last file, which local \
                 files must be numbered after, and {cause}"
            ),
        }
    }

    fn instance_prefix(&self) -> String {
        format!("/{}/", self.crypto_ctx.instance_id_hex())
    }

    /// Why the bucket failed a recent start, while that is recent enough to
    /// spare the next queue the same wait.
    fn bucket_down(&self) -> Option<String> {
        let down = self.bucket_down.lock().unwrap();
        let (since, cause) = down.as_ref()?;
        (since.elapsed() < BUCKET_DOWN_FOR).then(|| format!("{cause}, {:?} ago", since.elapsed()))
    }

    fn set_bucket_down(&self, cause: Option<String>) {
        *self.bucket_down.lock().unwrap() = cause.map(|c| (std::time::Instant::now(), c));
    }

    /// The bucket's last file for this queue and the last id in it, noted in
    /// the pointer as exact, since readers bound their file walk by it.
    async fn bucket_landed(
        &self,
        cloud: &Layer,
        queue: &QueueId,
    ) -> Result<Option<(UintN, UintN)>, Error> {
        let max_file = cloud
            .last_file_id(queue)
            .await
            .map_err(|e| Error::Cloud(e.into()))?;
        let Some(max_file) = max_file else {
            self.memory_pointers.settle(queue);
            return Ok(None);
        };
        let (_, last) = cloud
            .get_file_range(queue, &max_file)
            .await
            .map_err(Error::Cloud)?
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("cloud file {max_file} has no recoverable range for queue {queue}"),
                )
            })?;
        self.memory_pointers
            .settle_from_bucket(queue, &last, &max_file)
            .await
            .map_err(Error::Io)?;
        Ok(Some((last, max_file)))
    }

    /// Where a queue resumes: after every file and id any earlier life used,
    /// whatever its persistence was then. Local files and a cloud-direct
    /// life's objects share one id space and one file id space, so resuming
    /// from one source alone hands out an id again or writes a file over one
    /// that holds acked records.
    async fn resume_point(
        &self,
        queue: &QueueId,
        persist: Persist,
    ) -> Result<(UintN, normfs_wal::WalHeader, Option<UintN>, bool), Error> {
        let (mut file_id, mut header, mut last_id) = self.continue_queue(queue).await?;

        let cloud = self.cloud_landed(queue, persist).await?;
        if let Some((last, file)) = cloud.landed {
            if file >= file_id {
                file_id = file.increment();
            }
            if last_id.as_ref().is_none_or(|l| last > *l) {
                last_id = Some(last);
            }
        }
        // Past the reserve too, even when the bucket answered: ids an earlier
        // life gave out and never landed are not given out again.
        if let Some(last) = self.memory_pointers.used_id(queue) {
            if last_id.as_ref().is_none_or(|l| last > *l) {
                last_id = Some(last);
            }
        }

        if let Some(last) = &last_id {
            header.num_entries_before = last.increment();
        }
        Ok((file_id, header, last_id, cloud.unchecked))
    }

    fn report_started(
        &self,
        queue: &QueueId,
        readonly: bool,
        persist: Persist,
        last_id: Option<UintN>,
    ) {
        self.events.emit(SystemEvent::QueueStarted {
            queue: queue.clone(),
            readonly,
            wal: persist.wal,
            store: persist.store,
            cloud: persist.cloud,
            last_id,
        });
    }

    async fn start_queue(&self, queue: &QueueId, mode: QueueMode) -> Result<(), Error> {
        log::info!(target: "normfs", "========================================");
        log::info!(target: "normfs", "Starting queue: '{}' (readonly={})", queue.short(), mode.readonly);
        log::info!(target: "normfs", "========================================");

        let queue_config = self.get_config_for_queue(queue);
        let persist = queue_config.persist;
        if persist.is_memory() {
            // Past what an earlier cloud life recorded, which may be a reserve,
            // and past a local life's files.
            let (_, _, local) = self.continue_queue(queue).await?;
            let last_entry_id = self.memory_pointers.used_id(queue).max(local);
            self.mem.start_queue_with(
                queue,
                last_entry_id.clone(),
                mode.readonly,
                queue_config.pool,
                true,
            );
            log::info!(target: "normfs", "Memory-only queue '{}' started, last_entry_id: {:?}", queue.short(), last_entry_id);
            self.report_started(queue, mode.readonly, persist, last_entry_id);
            return Ok(());
        }

        if persist.cloud
            && queue
                .as_str()
                .split('/')
                .filter(|c| !c.is_empty())
                .skip(1)
                .any(normfs_cloud::is_id_component)
        {
            return Err(ConfigError::CloudQueuePathLooksLikeIds {
                queue: queue.to_string(),
            }
            .into());
        }

        let (file_id, header, last_entry_id, unchecked) = self.resume_point(queue, persist).await?;

        log::info!(target: "normfs", "----------------------------------------");
        log::info!(target: "normfs", "Queue '{}' - Recovery complete:", queue.short());
        log::info!(target: "normfs", "  - Will write to file ID: {}", file_id);
        log::info!(target: "normfs", "  - Last entry ID in queue: {:?}", last_entry_id);
        log::info!(target: "normfs", "  - Header entries_before: {}", header.num_entries_before);
        log::info!(target: "normfs", "  - Next entry will have ID: {}", header.num_entries_before);
        log::info!(target: "normfs", "----------------------------------------");

        // Started first: the writer is handed this queue's page pool, so the
        // pool has to exist before it.
        self.mem.start_queue(
            queue,
            last_entry_id.clone(),
            mode.readonly,
            queue_config.pool,
        );

        if !mode.readonly {
            if let Some(pool) = self.mem.pool(queue) {
                let (events, queue) = (self.events.clone(), queue.clone());
                pool.set_stall_listener(Arc::new(move |stall| {
                    events.emit(SystemEvent::PoolStalled {
                        queue: queue.clone(),
                        waits: stall.waits,
                        stalled_for: stall.stalled_for,
                        resumed: stall.resumed,
                    })
                }));
            }

            let mut wal_settings = self.settings.wal_settings.clone();
            wal_settings.enable_fsync = queue_config.enable_fsync;
            wal_settings.compression_type = queue_config.compression_type;
            wal_settings.encryption_type = queue_config.encryption_type;

            match persist.drainer() {
                Drainer::None => unreachable!("memory queues returned above"),
                Drainer::Wal => {
                    self.wal
                        .start_writer_with_pool(
                            queue,
                            &file_id,
                            header,
                            wal_settings.clone(),
                            last_entry_id.clone(),
                            // Rotation is decided at enqueue, before the bytes enter a
                            // page, and the writer only carries it out: that keeps a
                            // page's bytes in exactly one file.
                            self.mem.pool(queue),
                        )
                        .await?;
                }
                Drainer::Page => {
                    let pool = self.mem.pool(queue).ok_or(Error::QueueNotFound)?;
                    // A sealed page lands in the queue's first layer: the local
                    // store when it keeps one, else the bucket.
                    let sink: Arc<dyn normfs_store::SealedFileSink> = if persist.store {
                        // `continue_queue` may hand back the latest WAL file
                        // for reuse when it is header-only. This writer never
                        // writes a `.wal`, so that file would sit beside the
                        // store file of the same id forever.
                        match self.wal.delete_wal_file(queue, &file_id).await {
                            Ok(()) => log::info!(target: "normfs",
                                "Queue '{}': removed empty WAL file {} in favour of a store file", queue.short(), file_id),
                            Err(WalError::IoError(e))
                                if e.kind() == std::io::ErrorKind::NotFound => {}
                            Err(e) => log::warn!(target: "normfs",
                                "Queue '{}': could not remove WAL file {}: {}", queue.short(), file_id, e),
                        }
                        self.store.local_sink(wal_settings.enable_fsync)
                    } else {
                        let sink = self.cloud_sink.clone().ok_or_else(|| {
                            Error::Config(ConfigError::CloudWithoutSettings {
                                pattern: queue.to_string(),
                            })
                        })?;
                        match (&self.cloud, unchecked) {
                            (Some(cloud), true) => Arc::new(bucket_check::BucketCheck::new(
                                sink,
                                cloud.clone(),
                                self.memory_pointers.clone(),
                                last_entry_id.clone(),
                            )),
                            _ => sink,
                        }
                    };
                    self.store.start_page_writer(
                        queue,
                        &file_id,
                        header,
                        normfs_store::PageWriterSettings {
                            compression: wal_settings.compression_type,
                            encryption: wal_settings.encryption_type,
                            retry_delay: wal_settings.flush_retry_delay,
                            close_max_attempts: wal_settings.flush_max_retries,
                        },
                        pool,
                        sink,
                    );
                }
            }

            // Files a previous life in WAL mode left behind are migrated
            // either way; a store-mode queue has none of its own.
            let wal = self.wal.clone();
            let queue_clone = queue.clone();
            let file_id_clone = file_id.clone();
            let compression_type = wal_settings.compression_type;
            let encryption_type = wal_settings.encryption_type;

            // Retried: an old file a failed listing misses is never migrated.
            // A queue's pool lives as long as that opening of the queue, so
            // the retries end when it is closed and do not double on reopen.
            let mem = self.mem.clone();
            let opened = self.mem.pool(queue).map(|p| Arc::downgrade(&p));
            tokio::spawn(async move {
                let mut failures: u32 = 0;
                while let Err(e) = wal
                    .process_old_files(
                        &queue_clone,
                        &file_id_clone,
                        compression_type,
                        encryption_type,
                    )
                    .await
                {
                    failures = failures.saturating_add(1);
                    if failures.is_power_of_two() {
                        log::error!(target: "normfs",
                            "Failed to process old files for queue {} ({} tries): {}, \
                             retrying in 1 second",
                            queue_clone.short(), failures, e
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    let open = opened.as_ref().is_some_and(|pool| {
                        mem.pool(&queue_clone)
                            .is_some_and(|p| std::ptr::eq(pool.as_ptr(), Arc::as_ptr(&p)))
                    });
                    if !open {
                        break;
                    }
                }
            });
        }

        let offloader = match (&self.offloaders, persist.store && persist.cloud) {
            (Some(offloaders), true) => Some(offloaders.start(queue).await),
            _ => None,
        };

        // The disk monitor watches local store files; a cloud-direct queue
        // has none.
        if let (Some(disk_monitor), Some(max_size), true) = (
            &self.disk_monitor,
            self.settings.max_disk_usage_per_queue,
            persist.store,
        ) {
            let config = DiskMonitorConfig {
                max_size: max_size as usize,
                check_interval: Duration::from_secs(10), // Default check interval
                wal_settings: self.settings.wal_settings.clone(),
                offload: persist.cloud,
            };

            disk_monitor.add_queue(queue, config, offloader).await?;
            log::info!(target: "normfs", "Added queue '{}' to disk monitor with max_size: {}", queue.short(), max_size);
        }

        log::info!(target: "normfs", "Queue '{}' started successfully, last_entry_id: {:?}", queue.short(), last_entry_id);
        self.report_started(queue, mode.readonly, persist, last_entry_id);

        Ok(())
    }

    /// Accepts a record, waiting if every page is occupied by records that are
    /// not yet on disk. That wait is the back-pressure: the queue declines to
    /// run ahead of the disk rather than dropping what it already took.
    pub async fn enqueue(&self, queue: &QueueId, data: Bytes) -> Result<UintN, Error> {
        self.refuse_reserved(queue)?;
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
            queue.short(), entry_id, data.len());

        self.after_place(queue, &entry_id, data, placement)?;

        log::trace!(target: "normfs", "Entry enqueued successfully - Queue: '{}', Entry ID: {}", queue.short(), entry_id);

        Ok(entry_id)
    }

    /// [`NormFS::enqueue`] for callers that cannot wait -- a capture thread, or
    /// a subscriber callback, which runs while its own queue holds the append
    /// gate. A refused record took no id.
    pub fn try_enqueue(&self, queue: &QueueId, data: Bytes) -> Result<UintN, Error> {
        self.try_enqueue_within(queue, data, None)
    }

    /// [`NormFS::try_enqueue`] for a record that has to land in the file `room`
    /// saw, such as a delta frame that needs its keyframe in the same file. The
    /// check and the append are one step, so a flush cannot seal the file
    /// between them; [`Error::NotInFile`] when it already has or the record no
    /// longer fits.
    pub fn try_enqueue_in(
        &self,
        queue: &QueueId,
        room: &FileRoom,
        data: Bytes,
    ) -> Result<UintN, Error> {
        self.try_enqueue_within(queue, data, Some(room.file))
    }

    fn try_enqueue_within(
        &self,
        queue: &QueueId,
        data: Bytes,
        within: Option<normfs_wal::FileMark>,
    ) -> Result<UintN, Error> {
        self.refuse_reserved(queue)?;
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
            .try_enqueue(queue, data.clone(), within)
            .ok_or(Error::QueueNotFound)?
        {
            mem::TryEnqueue::Placed(id, placement) => (id, placement),
            mem::TryEnqueue::Full => return Err(Error::WouldBlock),
            mem::TryEnqueue::Closed => return Err(Error::QueueClosed),
            mem::TryEnqueue::NotInFile => return Err(Error::NotInFile),
        };

        self.after_place(queue, &entry_id, data, placement)?;

        Ok(entry_id)
    }

    /// [`NormFS::enqueue`] that gives up after `wait` with [`Error::WouldBlock`].
    /// The refused record took no id: a dropped `enqueue` consumes nothing.
    pub async fn enqueue_timeout(
        &self,
        queue: &QueueId,
        data: Bytes,
        wait: Duration,
    ) -> Result<UintN, Error> {
        match tokio::time::timeout(wait, self.enqueue(queue, data)).await {
            Ok(outcome) => outcome,
            Err(_elapsed) => Err(Error::WouldBlock),
        }
    }

    pub async fn enqueue_batch(
        &self,
        queue: &QueueId,
        data: Vec<Bytes>,
    ) -> Result<Vec<UintN>, Error> {
        self.refuse_reserved(queue)?;
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

        log::debug!(target: "normfs", "Enqueuing batch - Queue: '{}', Batch size: {} entries", queue.short(), data.len());

        let Some(entry_ids) = self
            .mem
            .enqueue_batch_awaiting(queue, data, |id, data, placement| {
                self.after_place(queue, id, data, placement)
            })
            .await?
        else {
            return Err(if self.mem.is_closed(queue) {
                Error::QueueClosed
            } else {
                Error::QueueNotFound
            });
        };

        log::trace!(target: "normfs", "Batch enqueued successfully - Queue: '{}', Count: {}", queue.short(), entry_ids.len());

        Ok(entry_ids)
    }

    fn after_place(
        &self,
        queue: &QueueId,
        entry_id: &UintN,
        data: Bytes,
        placement: normfs_wal::Placement,
    ) -> Result<(), Error> {
        self.placer.after_place(queue, entry_id, data, placement)
    }

    /// Lands everything a queue has accepted. On a page-per-file queue this
    /// seals the open page into a store file; on a WAL queue there is
    /// nothing to do -- records reach the file within `write_interval` --
    /// and a memory queue has nowhere to land.
    pub async fn flush_queue(&self, queue: &QueueId) -> Result<(), Error> {
        match self.persist_for(queue).drainer() {
            Drainer::Page => Ok(self.store.flush_page_writer(queue).await?),
            Drainer::Wal | Drainer::None => Ok(()),
        }
    }

    /// Where the queue's next record would land. Only its writer can act on
    /// the answer: any append, a flush or a close moves it.
    pub fn file_room(&self, queue: &QueueId) -> Result<FileRoom, Error> {
        self.refuse_reserved(queue)?;
        match self.mem.file_room(queue) {
            Some((room, file)) => Ok(FileRoom { room, file }),
            None => Err(Error::QueueNotFound),
        }
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
            queue.short(), position, limit, step);

        self.reader_fsm
            .read(queue.clone(), position, limit, step, sender)
            .await
    }

    /// Closes a queue: writes are refused until the next start for write,
    /// reads stay, a follow ends at the last record, and the memory goes back
    /// to the arena. The order is the safety argument: refuse writes, flush
    /// and complete the file, then the marker, so a marker on disk implies
    /// the data reached it. Memory is released last.
    pub async fn close_queue(&self, queue: &QueueId) -> Result<(), Error> {
        self.refuse_reserved(queue)?;
        let queue_lock = self.queue_init_lock(queue);
        let _guard = queue_lock.lock().await;

        log::info!(target: "normfs", "Closing queue '{}'", queue.short());
        let dir = queue.to_fs_path(&self.path);

        // Checks that change nothing come first, so a refused close leaves
        // no half-closed state. An unknown name is more likely a typo than
        // an intent, and "closed" is a reserved child name the same way
        // wal/ and store/ already are.
        if self.mem.get_last_id(queue).is_none() && self.fs.stat(&dir).await?.is_none() {
            return Err(Error::QueueNotFound);
        }

        if self
            .fs
            .stat(&dir.join("closed"))
            .await?
            .is_some_and(|m| m.is_dir())
        {
            return Err(Error::Io(std::io::Error::other(
                "a child queue named 'closed' occupies this queue's marker path",
            )));
        }

        // Waits for the in-flight append: after this, everything accepted
        // is placed and nothing more can be.
        self.mem.begin_close(queue).await;

        let drainer = self.persist_for(queue).drainer();
        match drainer {
            Drainer::Wal => self.wal.close_writer(queue).await?,
            Drainer::Page => {
                if !self.store.close_page_writer(queue).await {
                    return Err(Error::Wal(WalError::CloseIncomplete));
                }
            }
            Drainer::None => {}
        }

        // The marker certifies everything accepted is on disk. Records a
        // failed flush stranded stay in the WAL file for recovery, and a
        // store file that did not land keeps retrying; the close stays
        // incomplete rather than certifying loss. Memory-only has no disk to
        // certify: its close only ends the write side.
        if drainer != Drainer::None && !self.mem.is_fully_durable(queue) {
            return Err(Error::Wal(WalError::CloseIncomplete));
        }

        // The marker and its directory entry, both synced: a CREATE plan.
        self.fs.mkdir_all(&dir).await?;
        self.fs
            .create_durable(&dir.join("closed"), Runs::default(), TmpMode::Trunc, true)
            .await?;

        // Everything accepted has landed, so its reserve can come down.
        self.memory_pointers.settle_landed_queue(queue);
        let last_id = self.mem.closed_last_id(queue);
        self.mem.close_queue(queue);
        self.events.emit(SystemEvent::QueueClosed {
            queue: queue.clone(),
            last_id,
        });
        Ok(())
    }

    pub async fn close(&self) -> Result<(), Error> {
        log::info!(target: "normfs", "Closing NormFS");

        // Before the store closes, so what it holds lands with the rest.
        self.system_writer.stop().await;

        // Store first: page writers land their tails, and the migration
        // workers must outlive the WAL writers' last rotation.
        let store_result = self.store.close().await;
        let wal_result = self.wal.close().await;
        self.memory_pointer_task.abort();
        if store_result.is_ok() {
            self.memory_pointers.settle_landed();
        }
        self.memory_pointers.flush_all().await.map_err(Error::Io)?;
        store_result?;
        wal_result?;

        log::info!(target: "normfs", "NormFS closed successfully");
        Ok(())
    }
}
