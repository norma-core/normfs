use std::ffi::CString;
use std::io;
use std::os::raw::{c_char, c_int};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time;
use uintn::UintN;

use crate::Error;
use normfs_cloud::offloader::QueueOffloader;
use normfs_cloud::S3Client;
use normfs_store::{DiskUsage, StoreError};
use normfs_types::QueueId;
use normfs_wal::WalSettings;

// Mirrors c/include/normfs/disk_monitor.h.
const NORMFS_DISK_ID_MAX: usize = 48;

const NORMFS_DISK_OK: c_int = 0;
const NORMFS_DISK_ERR_INVALID_ARG: c_int = 1;
const NORMFS_DISK_ERR_PATH_TOO_LONG: c_int = 2;
const NORMFS_DISK_ERR_ID_OVERFLOW: c_int = 3;
const NORMFS_DISK_ERR_TOO_DEEP: c_int = 4;
const NORMFS_DISK_ERR_TOO_MANY: c_int = 5;
const NORMFS_DISK_ERR_NOT_FOUND: c_int = 6;
const NORMFS_DISK_ERR_IO: c_int = 7;

const NORMFS_DISK_STOP_MORE: c_int = 0;
const NORMFS_DISK_STOP_FREED: c_int = 1;
const NORMFS_DISK_STOP_GAP: c_int = 2;
const NORMFS_DISK_STOP_BOUND: c_int = 3;
const NORMFS_DISK_STOP_ERROR: c_int = 4;

#[repr(C)]
#[derive(Clone, Copy)]
struct CDiskResult {
    os_error: c_int,
    status: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CDiskId {
    hex: [u8; NORMFS_DISK_ID_MAX],
    len: usize,
}

#[repr(C)]
struct CDiskScan {
    total: u64,
    min: CDiskId,
    has_min: c_int,
}

#[repr(C)]
struct CDiskEvictReq {
    store_dir: *const c_char,
    store_dir_len: usize,
    wal_dir: *const c_char,
    wal_dir_len: usize,
    next: CDiskId,
    bound: CDiskId,
    has_bound: c_int,
    to_free: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CDiskEvent {
    id: CDiskId,
    size: u64,
    freed: u64,
    kind: c_int,
    deleted: c_int,
    os_error: c_int,
}

unsafe extern "C" {
    #[cfg(test)]
    fn normfs_disk_path(
        dir: *const c_char,
        dir_len: usize,
        id: *const CDiskId,
        kind: c_int,
        out: *mut u8,
        out_len: usize,
        used: *mut usize,
    ) -> CDiskResult;

    fn normfs_disk_scan(
        dir: *const c_char,
        dir_len: usize,
        kind: c_int,
        out: *mut CDiskScan,
    ) -> CDiskResult;

    fn normfs_disk_evict(
        req: *mut CDiskEvictReq,
        events: *mut CDiskEvent,
        cap: usize,
        count: *mut usize,
        stop: *mut c_int,
    ) -> CDiskResult;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileKind {
    Store,
    Wal,
}

impl FileKind {
    fn c(self) -> c_int {
        match self {
            FileKind::Store => 0,
            FileKind::Wal => 1,
        }
    }

    fn from_c(kind: c_int) -> io::Result<Self> {
        match kind {
            0 => Ok(FileKind::Store),
            1 => Ok(FileKind::Wal),
            other => Err(io::Error::other(format!(
                "unknown file kind {} from the C disk layer",
                other
            ))),
        }
    }
}

impl std::fmt::Display for FileKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileKind::Store => write!(f, "store"),
            FileKind::Wal => write!(f, "WAL"),
        }
    }
}

fn map_status(r: CDiskResult) -> io::Result<()> {
    match r.status {
        NORMFS_DISK_OK => Ok(()),
        NORMFS_DISK_ERR_IO if r.os_error != 0 => Err(io::Error::from_raw_os_error(r.os_error)),
        NORMFS_DISK_ERR_IO => Err(io::Error::other(
            "the C disk layer reported a failure without an errno",
        )),
        NORMFS_DISK_ERR_NOT_FOUND => Err(io::Error::from(io::ErrorKind::NotFound)),
        NORMFS_DISK_ERR_INVALID_ARG => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file id or kind rejected by the C disk layer",
        )),
        NORMFS_DISK_ERR_PATH_TOO_LONG => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "queue path exceeds the C layer's path limit",
        )),
        NORMFS_DISK_ERR_ID_OVERFLOW => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file id past the layout's 48 hex digits",
        )),
        NORMFS_DISK_ERR_TOO_DEEP | NORMFS_DISK_ERR_TOO_MANY => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "queue directory is not a normfs layout",
        )),
        other => Err(io::Error::other(format!(
            "unknown status {} from the C disk layer",
            other
        ))),
    }
}

fn c_dir(dir: &Path) -> io::Result<CString> {
    CString::new(dir.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "queue directory path contains an interior NUL",
        )
    })
}

fn c_id(id: &UintN) -> io::Result<CDiskId> {
    let hex = format!("{:x}", id.to_ubig());
    if hex.len() > NORMFS_DISK_ID_MAX {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file id past the layout's 48 hex digits",
        ));
    }
    let mut out = CDiskId {
        hex: [0u8; NORMFS_DISK_ID_MAX],
        len: hex.len(),
    };
    out.hex[..hex.len()].copy_from_slice(hex.as_bytes());
    Ok(out)
}

fn from_c_id(id: &CDiskId) -> io::Result<UintN> {
    let hex = id
        .hex
        .get(..id.len)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .ok_or_else(|| io::Error::other("malformed file id from the C disk layer"))?;
    UintN::from_hex_digits(hex)
        .map_err(|_| io::Error::other("malformed file id from the C disk layer"))
}

#[cfg(test)]
pub(crate) fn file_path(dir: &Path, kind: FileKind, id: &UintN) -> io::Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;

    let dir_c = c_dir(dir)?;
    let id_c = c_id(id)?;
    let mut out = vec![0u8; 4096];
    let mut used = 0usize;

    // SAFETY: dir_c is NUL-terminated and outlives the call; out and used
    // are distinct live allocations, as the contract's \separated needs.
    let r = unsafe {
        normfs_disk_path(
            dir_c.as_ptr(),
            dir_c.as_bytes().len(),
            &id_c,
            kind.c(),
            out.as_mut_ptr(),
            out.len(),
            &mut used,
        )
    };
    map_status(r)?;
    out.truncate(used);
    Ok(PathBuf::from(std::ffi::OsString::from_vec(out)))
}

pub(crate) struct DirScan {
    pub total: u64,
    pub min: Option<UintN>,
}

pub(crate) fn scan(dir: &Path, kind: FileKind) -> io::Result<DirScan> {
    let dir_c = c_dir(dir)?;
    let mut out = CDiskScan {
        total: 0,
        min: CDiskId {
            hex: [0u8; NORMFS_DISK_ID_MAX],
            len: 0,
        },
        has_min: 0,
    };

    // SAFETY: as in file_path.
    let r = unsafe { normfs_disk_scan(dir_c.as_ptr(), dir_c.as_bytes().len(), kind.c(), &mut out) };
    map_status(r)?;
    let min = if out.has_min == 1 {
        Some(from_c_id(&out.min)?)
    } else {
        None
    };
    Ok(DirScan {
        total: out.total,
        min,
    })
}

pub(crate) struct EvictEvent {
    pub id: UintN,
    pub kind: FileKind,
    pub size: u64,
    /// Bytes deleted by the whole eviction up to and including this event.
    pub freed: u64,
    pub result: io::Result<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stop {
    Freed,
    /// An id with neither file: the end of the queue's files.
    Gap,
    /// The next id is past the offloaded bound.
    Bound,
    /// The last event's file could not be sized or removed; `next` is its id.
    Error,
}

pub(crate) struct Eviction {
    pub events: Vec<EvictEvent>,
    pub stop: Stop,
    pub next: UintN,
}

/// `bound` is the highest id that may go; `None` lets every id go.
pub(crate) fn evict(
    store_dir: &Path,
    wal_dir: &Path,
    start: &UintN,
    bound: Option<&UintN>,
    to_free: u64,
) -> io::Result<Eviction> {
    let store_c = c_dir(store_dir)?;
    let wal_c = c_dir(wal_dir)?;
    let zero = CDiskId {
        hex: [0u8; NORMFS_DISK_ID_MAX],
        len: 0,
    };
    let mut req = CDiskEvictReq {
        store_dir: store_c.as_ptr(),
        store_dir_len: store_c.as_bytes().len(),
        wal_dir: wal_c.as_ptr(),
        wal_dir_len: wal_c.as_bytes().len(),
        next: c_id(start)?,
        bound: match bound {
            Some(b) => c_id(b)?,
            None => zero,
        },
        has_bound: bound.is_some() as c_int,
        to_free,
    };
    let mut buf = [CDiskEvent {
        id: zero,
        size: 0,
        freed: 0,
        kind: 0,
        deleted: 0,
        os_error: 0,
    }; 64];
    let mut events = Vec::new();
    let mut freed = 0u64;

    let stop = loop {
        let mut count = 0usize;
        let mut stop: c_int = NORMFS_DISK_STOP_MORE;

        // SAFETY: store_c and wal_c outlive req, which outlives the call; req,
        // buf, count and stop are distinct live locals.
        let r = unsafe {
            normfs_disk_evict(&mut req, buf.as_mut_ptr(), buf.len(), &mut count, &mut stop)
        };
        let base = freed;
        for event in &buf[..count.min(buf.len())] {
            freed = base.saturating_add(event.freed);
            events.push(EvictEvent {
                id: from_c_id(&event.id)?,
                kind: FileKind::from_c(event.kind)?,
                size: event.size,
                freed,
                result: if event.deleted == 1 {
                    Ok(())
                } else if event.os_error != 0 {
                    Err(io::Error::from_raw_os_error(event.os_error))
                } else {
                    Err(io::Error::other(
                        "the C disk layer skipped a file without an errno",
                    ))
                },
            });
        }
        map_status(r)?;

        match stop {
            NORMFS_DISK_STOP_MORE => continue,
            NORMFS_DISK_STOP_FREED => break Stop::Freed,
            NORMFS_DISK_STOP_GAP => break Stop::Gap,
            NORMFS_DISK_STOP_BOUND => break Stop::Bound,
            NORMFS_DISK_STOP_ERROR => break Stop::Error,
            other => {
                return Err(io::Error::other(format!(
                    "unknown stop reason {} from the C disk layer",
                    other
                )));
            }
        }
    };

    Ok(Eviction {
        events,
        stop,
        next: from_c_id(&req.next)?,
    })
}

/// The C calls are blocking syscalls; keep them off the runtime threads.
async fn blocking<T, F>(f: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Io(io::Error::other(e)))?
        .map_err(Error::Io)
}

#[derive(Debug, Clone)]
pub struct DiskMonitorConfig {
    /// Maximum size in bytes for a queue (store + wal combined)
    pub max_size: usize,
    /// Monitoring interval
    pub check_interval: Duration,
    /// WAL settings to validate minimum size
    pub wal_settings: WalSettings,
}

impl DiskMonitorConfig {
    pub fn validate(&self) -> Result<(), Error> {
        let min_size = self.wal_settings.max_file_size * 3;
        if self.max_size < min_size {
            return Err(Error::Store(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Queue max_size ({}) must be at least 3x WAL file size ({})",
                    self.max_size, min_size
                ),
            ))));
        }
        Ok(())
    }
}

pub type ForgetRange = Arc<dyn Fn(&QueueId, &UintN) + Send + Sync>;

const CHECK_INTERVAL: Duration = Duration::from_secs(60);
/// Store bytes are tracked from publication and deletion; a full walk this
/// often catches anything else that touched the directory.
const RESCAN_EVERY_TICKS: u64 = 60;

struct QueueMonitor {
    queue_id: QueueId,
    config: DiskMonitorConfig,
    store_dir: PathBuf,
    wal_dir: PathBuf,
    offloader: Option<QueueOffloader>,
    store_bytes: Arc<Mutex<u64>>,
    /// The last eviction's `next`. Publication takes `store_bytes`' lock, so
    /// cleanup starts here instead of walking the store for its oldest file.
    cursor: std::sync::Mutex<Option<UintN>>,
    forget_range: Option<ForgetRange>,
}

fn earliest(a: Option<UintN>, b: Option<UintN>) -> Option<UintN> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl QueueMonitor {
    async fn new(
        queue_id: QueueId,
        config: DiskMonitorConfig,
        root_path: PathBuf,
        client: Option<Arc<S3Client>>,
        prefix: Option<&str>,
        forget_range: Option<ForgetRange>,
        disk_usage: Arc<DiskUsage>,
    ) -> Result<Self, Error> {
        let offloader = if let (Some(client), Some(prefix)) = (client, prefix) {
            Some(QueueOffloader::new(queue_id.clone(), root_path.clone(), client, prefix).await)
        } else {
            None
        };

        let store_dir = queue_id.to_store_dir(&root_path);
        let wal_dir = queue_id.to_wal_dir(&root_path);
        let store_bytes = disk_usage.queue(&queue_id);

        let monitor = Self {
            queue_id,
            config,
            store_dir,
            wal_dir,
            offloader,
            store_bytes,
            cursor: std::sync::Mutex::new(None),
            forget_range,
        };
        monitor.rescan_store().await?;
        Ok(monitor)
    }

    async fn scan_dir(dir: &Path, kind: FileKind) -> Result<DirScan, Error> {
        let dir = dir.to_path_buf();
        blocking(move || scan(&dir, kind)).await
    }

    async fn rescan_store(&self) -> Result<(), Error> {
        let mut tracked = self.store_bytes.clone().lock_owned().await;
        let dir = self.store_dir.clone();
        let min = blocking(move || {
            let store = scan(&dir, FileKind::Store)?;
            *tracked = store.total;
            Ok(store.min)
        })
        .await?;
        *self.cursor.lock().unwrap() = min;
        Ok(())
    }

    /// Only the minimum is wanted, so the walk runs without the lock.
    async fn locate_oldest(&self, wal_min: Option<UintN>) -> Result<Option<UintN>, Error> {
        let store = Self::scan_dir(&self.store_dir, FileKind::Store).await?;
        Ok(earliest(store.min, wal_min))
    }

    async fn get_queue_size(&self) -> Result<u64, Error> {
        let wal = Self::scan_dir(&self.wal_dir, FileKind::Wal).await?.total;
        Ok((*self.store_bytes.lock().await).saturating_add(wal))
    }

    /// `None` when the queue is already under its limit.
    async fn evict_from(
        &self,
        start: UintN,
        bound: Option<UintN>,
        wal_total: u64,
    ) -> Result<Option<Eviction>, Error> {
        let max_size = self.config.max_size as u64;
        let mut tracked = self.store_bytes.clone().lock_owned().await;
        let store_dir = self.store_dir.clone();
        let wal_dir = self.wal_dir.clone();
        blocking(move || {
            let to_free = tracked.saturating_add(wal_total).saturating_sub(max_size);
            if to_free == 0 {
                return Ok(None);
            }
            let result = evict(&store_dir, &wal_dir, &start, bound.as_ref(), to_free);
            match &result {
                Ok(eviction) => {
                    for event in &eviction.events {
                        if event.kind == FileKind::Store && event.result.is_ok() {
                            *tracked = tracked.saturating_sub(event.size);
                        }
                    }
                }
                // Deletions before the failure are not reported back.
                Err(_) => *tracked = scan(&store_dir, FileKind::Store)?.total,
            }
            result.map(Some)
        })
        .await
    }

    async fn cleanup_oldest_files(
        &self,
        current_size: u64,
        wal_scan: DirScan,
    ) -> Result<(), Error> {
        let max_size = self.config.max_size as u64;
        let to_free = current_size.saturating_sub(max_size);
        if to_free == 0 {
            return Ok(());
        }

        log::info!(
            target: "normfs::disk_monitor",
            "Queue '{}' size {} exceeds limit {}, need to free {} bytes",
            self.queue_id,
            current_size,
            max_size,
            to_free
        );

        let bound = match &self.offloader {
            None => None,
            Some(offloader) => match offloader.get_latest_offloaded_id().await {
                Some(id) => Some(id),
                None => {
                    log::warn!(
                        target: "normfs::disk_monitor",
                        "Queue '{}' over its limit but nothing is offloaded to S3 yet; deleting nothing",
                        self.queue_id
                    );
                    return Ok(());
                }
            },
        };

        let remembered = self.cursor.lock().unwrap().clone();
        let mut from_cursor = remembered.is_some();
        let mut start = match remembered {
            Some(cursor) => earliest(Some(cursor), wal_scan.min.clone()),
            None => self.locate_oldest(wal_scan.min.clone()).await?,
        };

        let eviction = loop {
            let Some(from) = start.clone() else {
                log::warn!(
                    target: "normfs::disk_monitor",
                    "No files found to delete for queue '{}'",
                    self.queue_id
                );
                return Ok(());
            };
            let eviction = match self.evict_from(from, bound.clone(), wal_scan.total).await {
                Ok(Some(eviction)) => eviction,
                Ok(None) => return Ok(()),
                Err(e) => {
                    *self.cursor.lock().unwrap() = None;
                    return Err(e);
                }
            };
            // Nothing at the cursor: files removed by hand, or a WAL file
            // older than the store.
            if from_cursor && eviction.stop == Stop::Gap && eviction.events.is_empty() {
                let oldest = self.locate_oldest(wal_scan.min.clone()).await?;
                if oldest != start {
                    start = oldest;
                    from_cursor = false;
                    continue;
                }
            }
            break eviction;
        };
        *self.cursor.lock().unwrap() = Some(eviction.next.clone());

        for event in eviction.events {
            match event.result {
                Ok(()) => {
                    if event.kind == FileKind::Store {
                        if let Some(forget) = &self.forget_range {
                            forget(&self.queue_id, &event.id);
                        }
                    }
                    log::info!(
                        target: "normfs::disk_monitor",
                        "Deleted {} file {} ({} bytes) of queue '{}', size now: {} bytes",
                        event.kind,
                        event.id,
                        event.size,
                        self.queue_id,
                        current_size.saturating_sub(event.freed)
                    );
                }
                Err(e) => log::error!(
                    target: "normfs::disk_monitor",
                    "Failed to delete {} file {} of queue '{}': {}",
                    event.kind,
                    event.id,
                    self.queue_id,
                    e
                ),
            }
        }

        match eviction.stop {
            Stop::Freed => {}
            Stop::Gap => log::info!(
                target: "normfs::disk_monitor",
                "No more files to delete for queue '{}', stopping cleanup at id {}",
                self.queue_id,
                eviction.next
            ),
            Stop::Bound => log::warn!(
                target: "normfs::disk_monitor",
                "Skipping deletion of file {} of queue '{}' - not yet offloaded to S3",
                eviction.next,
                self.queue_id
            ),
            Stop::Error => log::warn!(
                target: "normfs::disk_monitor",
                "Cleanup of queue '{}' holds at id {} until it can be deleted",
                self.queue_id,
                eviction.next
            ),
        }

        Ok(())
    }

    async fn check_and_cleanup(&self, rescan: bool) -> Result<(), Error> {
        if rescan {
            self.rescan_store().await?;
        }
        let wal_scan = Self::scan_dir(&self.wal_dir, FileKind::Wal).await?;
        let current_size = (*self.store_bytes.lock().await).saturating_add(wal_scan.total);

        if current_size > self.config.max_size as u64 {
            self.cleanup_oldest_files(current_size, wal_scan).await?;
        } else {
            log::debug!(
                target: "normfs::disk_monitor",
                "Queue '{}' size {} is within limit {}",
                self.queue_id,
                current_size,
                self.config.max_size
            );
        }

        Ok(())
    }
}

pub struct DiskMonitor {
    monitors: Arc<RwLock<std::collections::HashMap<QueueId, QueueMonitor>>>,
    root_path: PathBuf,
    _handle: Option<tokio::task::JoinHandle<()>>,
    client: Option<Arc<S3Client>>,
    prefix: Option<String>,
    forget_range: Option<ForgetRange>,
    disk_usage: Arc<DiskUsage>,
}

impl DiskMonitor {
    pub async fn new(
        root_path: impl AsRef<Path>,
        client: Option<Arc<S3Client>>,
        prefix: Option<String>,
        forget_range: Option<ForgetRange>,
        disk_usage: Arc<DiskUsage>,
    ) -> Result<Self, Error> {
        let monitors: Arc<RwLock<std::collections::HashMap<QueueId, QueueMonitor>>> =
            Arc::new(RwLock::new(std::collections::HashMap::new()));
        let monitors_clone = monitors.clone();

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(CHECK_INTERVAL);
            let mut ticks: u64 = 0;
            // The first tick is immediate; add_queue already checked.
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        ticks += 1;
                        let rescan = ticks.is_multiple_of(RESCAN_EVERY_TICKS);
                        let monitors = monitors_clone.read().await;
                        for (queue_id, monitor) in monitors.iter() {
                            if let Err(e) = monitor.check_and_cleanup(rescan).await {
                                log::error!(
                                    target: "normfs::disk_monitor",
                                    "Error checking queue '{}': {}",
                                    queue_id,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            monitors,
            root_path: root_path.as_ref().to_path_buf(),
            _handle: Some(handle),
            client,
            prefix,
            forget_range,
            disk_usage,
        })
    }

    pub async fn store_file_done(&self, queue_id: &QueueId, file_id: UintN) -> Result<(), Error> {
        let monitors = self.monitors.read().await;
        if let Some(monitor) = monitors.get(queue_id) {
            if let Some(ref offloader) = monitor.offloader {
                if let Err(e) = offloader.enqueue_file(file_id.clone()).await {
                    log::error!(
                        target: "normfs::disk_monitor",
                        "Failed to enqueue file for offload: queue={}, file_id={:?}, error={}",
                        queue_id, file_id, e
                    );
                }
                Ok(())
            } else {
                log::debug!(
                    target: "normfs::disk_monitor",
                    "No offloader configured for queue '{}', skipping offload for file {:?}",
                    queue_id, file_id
                );
                Ok(())
            }
        } else {
            log::warn!(
                target: "normfs::disk_monitor",
                "Queue '{}' not found in disk monitor, cannot enqueue file {:?} for offload",
                queue_id, file_id
            );
            Err(Error::Store(StoreError::FileNotFound))
        }
    }

    pub async fn add_queue(
        &self,
        queue_id: &QueueId,
        config: DiskMonitorConfig,
    ) -> Result<(), Error> {
        config.validate()?;

        let monitor = QueueMonitor::new(
            queue_id.clone(),
            config,
            self.root_path.clone(),
            self.client.clone(),
            self.prefix.as_deref(),
            self.forget_range.clone(),
            self.disk_usage.clone(),
        )
        .await?;

        monitor.check_and_cleanup(false).await?;

        let mut monitors = self.monitors.write().await;
        let has_offloader = monitor.offloader.is_some();
        monitors.insert(queue_id.clone(), monitor);

        log::info!(
            target: "normfs::disk_monitor",
            "Added disk monitoring for queue '{}' with max_size: {} (S3 offloading: {})",
            queue_id,
            monitors.get(queue_id).unwrap().config.max_size,
            if has_offloader { "enabled" } else { "disabled" }
        );

        Ok(())
    }
}

#[cfg(test)]
#[path = "disk_monitor_test.rs"]
mod disk_monitor_test;
