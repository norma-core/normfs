//! The filesystem layer: every write and every directory scan in NormFS goes
//! through here, on an executor that is not a tokio worker thread.
//!
//! Each durable operation is a plan from the proved planner in
//! `c/include/normfs/fs_plan.h`: the planner names the syscalls and their
//! order, and holds the crash invariant across them; the executor performs
//! them. `verify/fs.md` states what is assumed of the kernel and what is
//! proved on that basis.
//!
//! A job, once submitted, runs to completion whether or not the future that
//! submitted it is still polled. What that buys is in [`Fs::publish`].

use std::collections::HashSet;
use std::fs::{File, Metadata};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::{Semaphore, oneshot};

mod dir_sync;
mod directory;
mod executor;
pub mod fault;
mod plan;
mod pool;
mod read;
mod scan;
pub use read::ReadFile;

#[cfg(test)]
mod dir_sync_test;
#[cfg(test)]
mod directory_test;
#[cfg(test)]
mod plan_test;
#[cfg(test)]
mod pool_test;
#[cfg(test)]
mod read_test;
#[cfg(test)]
mod scan_test;

pub use executor::Accounting;
pub use plan::{Op, PlanError, TmpMode};
pub use scan::{Scan, ScanResult};

use executor::{Executor, Job, PlanJob, Resources, Task};
use plan::Plan;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Pool,
}

#[derive(Debug, Clone)]
pub struct FsConfig {
    pub backend: Backend,
    /// Executor threads. Zero picks a default sized for the machine.
    pub threads: usize,
}

impl Default for FsConfig {
    fn default() -> Self {
        FsConfig {
            backend: Backend::Pool,
            threads: 0,
        }
    }
}

/// One thread per core up to eight. On the four-core rover that is four;
/// on a many-core host with an SSD the gate benchmark lost 11% of publish
/// throughput at four threads and 5% at eight against tokio's unbounded
/// pool, at 40% less CPU either way.
fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2)
        .clamp(2, 8)
}

#[derive(Debug)]
pub enum FsError {
    Os(io::Error),
    ExecutorGone,
    JobPanicked,
    Plan(PlanError),
}

impl std::fmt::Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsError::Os(e) => write!(f, "IO error: {e}"),
            FsError::JobPanicked => write!(f, "fs job panicked"),
            FsError::ExecutorGone => write!(f, "the fs executor has shut down"),
            FsError::Plan(e) => write!(f, "fs planner: {e}"),
        }
    }
}

impl std::error::Error for FsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FsError::Os(e) => Some(e),
            FsError::ExecutorGone | FsError::JobPanicked => None,
            FsError::Plan(e) => Some(e),
        }
    }
}

impl From<io::Error> for FsError {
    fn from(e: io::Error) -> Self {
        FsError::Os(e)
    }
}

impl From<FsError> for io::Error {
    fn from(e: FsError) -> Self {
        match e {
            FsError::Os(e) => e,
            other => io::Error::other(other),
        }
    }
}

/// Bytes handed to the kernel in order, each run one iovec.
#[derive(Debug, Clone, Default)]
pub struct Runs(pub Vec<Bytes>);

impl Runs {
    pub fn total(&self) -> u64 {
        self.0.iter().map(|b| b.len() as u64).sum()
    }
}

pub struct PublishSpec {
    /// Must name a different inode from `dst`; callers exclude hard-link and parent aliases.
    pub tmp: PathBuf,
    pub dst: PathBuf,
    pub runs: Runs,
    pub tmp_mode: TmpMode,
    /// Off, the fsync steps are skipped: the rename is still atomic, and
    /// nothing else is promised.
    pub sync: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PublishReport {
    /// The length of the file the publish replaced, if there was one.
    pub old_len: Option<u64>,
    pub new_len: u64,
}

#[derive(Debug)]
pub enum AppendOutcome {
    /// Written and synced: the bytes are on the medium.
    Committed,
    /// Not committed. `restored` says the file was cut back to where the
    /// append started, so the same append can be tried again; without it,
    /// [`Fs::restore`] first.
    Failed { err: io::Error, restored: bool },
}

#[derive(Clone)]
pub struct Fs {
    exec: Arc<dyn Executor>,
    slots: Arc<Semaphore>,
    // Avoid repeated pool round trips for already provisioned store directories.
    provisioned: Arc<Mutex<Provisioned>>,
}

#[derive(Default)]
struct Provisioned {
    dirs: HashSet<PathBuf>,
    /// Advanced by every removal, so a creation that overlapped one cannot
    /// cache a directory the removal may have deleted.
    generation: u64,
}

impl Provisioned {
    fn forget(&mut self, path: &Path) {
        self.dirs.retain(|known| !known.starts_with(path));
        self.generation += 1;
    }
}

impl std::fmt::Debug for Fs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fs")
            .field("backend", &self.backend())
            .finish_non_exhaustive()
    }
}

impl Fs {
    pub fn new(cfg: FsConfig) -> io::Result<Fs> {
        let threads = if cfg.threads == 0 {
            default_threads()
        } else {
            cfg.threads
        };
        let exec: Arc<dyn Executor> = match cfg.backend {
            Backend::Pool => pool::Pool::start(threads)?,
        };
        log::info!(target: "normfs-fs", "fs executor: {} with {threads} threads", exec.name());
        Ok(Fs {
            exec,
            slots: Arc::new(Semaphore::new(threads.saturating_mul(32).clamp(1, 1024))),
            provisioned: Arc::new(Mutex::new(Provisioned::default())),
        })
    }

    pub fn backend(&self) -> &'static str {
        self.exec.name()
    }

    async fn submit(&self, task: Task) -> Result<(), FsError> {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| FsError::ExecutorGone)?;
        self.exec.submit(Job { task, permit })
    }

    async fn run_plan(
        &self,
        plan: Plan,
        res: Resources,
        then: Option<Accounting>,
    ) -> Result<executor::Finished, FsError> {
        let (reply, done) = oneshot::channel();
        self.submit(Task::Plan(PlanJob {
            plan,
            res,
            then,
            reply,
        }))
        .await?;
        done.await.map_err(|_| FsError::ExecutorGone)?
    }

    /// Writes `runs` at `at` in `file` and syncs. `file` must be synced and
    /// unwritten past `at`: true after a `Committed` append of the bytes up
    /// to `at`, after a `Failed { restored: true }` one, and after
    /// [`Fs::restore`].
    ///
    /// With `sync` off the write is not followed by fsync and `Committed`
    /// means only that the kernel took the bytes.
    pub async fn append_sync(
        &self,
        file: Arc<File>,
        path: &Path,
        at: u64,
        runs: Runs,
        sync: bool,
    ) -> Result<AppendOutcome, FsError> {
        if runs.total() == 0 {
            return Ok(AppendOutcome::Committed);
        }
        let handle = file.clone();
        let ino = self.run_blocking(move || inode(&handle)).await?;
        self.append_sync_with_inode(file, ino, path, at, runs, sync)
            .await
    }

    /// Like [`Fs::append_sync`], using the inode returned by creation or metadata.
    /// `ino` must belong to this open file; holding the file prevents inode reuse.
    pub async fn append_sync_with_inode(
        &self,
        file: Arc<File>,
        ino: u64,
        path: &Path,
        at: u64,
        runs: Runs,
        sync: bool,
    ) -> Result<AppendOutcome, FsError> {
        let total = runs.total();
        if total == 0 {
            return Ok(AppendOutcome::Committed);
        }
        let plan = Plan::append(path, ino, at, total)?;
        let res = Resources {
            file: Some(file),
            runs: runs.0,
            sync,
        };
        let fin = self.run_plan(plan, res, None).await?;
        Ok(match fin.plan.next()? {
            Op::Done => AppendOutcome::Committed,
            _ => AppendOutcome::Failed {
                err: io::Error::from_raw_os_error(fin.plan.os_error()),
                restored: fin.plan.restored(),
            },
        })
    }

    /// Cuts `file` back to `at`, for an append that failed unrestored.
    pub async fn restore(&self, file: Arc<File>, path: &Path, at: u64) -> Result<(), FsError> {
        let handle = file.clone();
        let ino = self.run_blocking(move || inode(&handle)).await?;
        self.restore_with_inode(file, ino, path, at).await
    }

    /// Like [`Fs::restore`], with the inode of this open file already known.
    pub async fn restore_with_inode(
        &self,
        file: Arc<File>,
        ino: u64,
        path: &Path,
        at: u64,
    ) -> Result<(), FsError> {
        let plan = Plan::restore(path, ino, at)?;
        let res = Resources {
            file: Some(file),
            runs: Vec::new(),
            sync: true,
        };
        let fin = self.run_plan(plan, res, None).await?;
        finished(&fin.plan)
    }

    /// Writes `spec.runs` to `spec.tmp`, syncs, renames it to `spec.dst` and
    /// syncs the directory. `then`, if given, runs on the executor's thread
    /// once the rename has succeeded and before this returns: bookkeeping
    /// put there cannot be separated from the rename by a dropped future.
    /// It runs even when the directory sync then fails and this returns an
    /// error, because `spec.dst` already holds the new file.
    /// A callback panic is logged and does not change the result.
    pub async fn publish(
        &self,
        spec: PublishSpec,
        then: Option<Accounting>,
    ) -> Result<PublishReport, FsError> {
        let total = spec.runs.total();
        let plan = Plan::publish(&spec.tmp, &spec.dst, spec.tmp_mode, total.max(1))?;
        let runs = if total == 0 {
            // The planner requires a nonempty publication.
            return Err(FsError::Os(io::Error::new(
                io::ErrorKind::InvalidInput,
                "publish of an empty file",
            )));
        } else {
            spec.runs.0
        };
        let res = Resources {
            file: None,
            runs,
            sync: spec.sync,
        };
        let fin = self.run_plan(plan, res, then).await?;
        finished(&fin.plan)?;
        Ok(PublishReport {
            old_len: fin.plan.old_len(),
            new_len: total,
        })
    }

    /// Creates `path` holding `runs` (possibly none), syncs it and its
    /// directory entry, and returns it open for writing.
    pub async fn create_durable(
        &self,
        path: &Path,
        runs: Runs,
        mode: TmpMode,
        sync: bool,
    ) -> Result<File, FsError> {
        self.create_durable_with_inode(path, runs, mode, sync)
            .await
            .map(|(file, _)| file)
    }

    /// Creates a file and returns its inode from the same OPEN completion.
    pub async fn create_durable_with_inode(
        &self,
        path: &Path,
        runs: Runs,
        mode: TmpMode,
        sync: bool,
    ) -> Result<(File, u64), FsError> {
        let total = runs.total();
        let plan = Plan::create(path, mode, total)?;
        let res = Resources {
            file: None,
            runs: runs.0,
            sync,
        };
        let fin = self.run_plan(plan, res, None).await?;
        finished(&fin.plan)?;
        fin.file
            .map(|file| (file, fin.plan.inode()))
            .ok_or_else(|| FsError::Os(io::Error::other("create finished without a file")))
    }

    /// Unlinks `path` and syncs its directory entry. `Ok(false)` when there
    /// was nothing to unlink; the directory is synced regardless, so a name
    /// removed earlier without a sync becomes durably absent too.
    pub async fn remove_durable(&self, path: &Path, sync: bool) -> Result<bool, FsError> {
        let plan = Plan::remove(path)?;
        let res = Resources {
            file: None,
            runs: Vec::new(),
            sync,
        };
        let fin = self.run_plan(plan, res, None).await?;
        finished(&fin.plan)?;
        Ok(!fin.absent)
    }

    /// Runs `f` on an executor thread.
    pub async fn run_blocking<T, F>(&self, f: F) -> Result<T, FsError>
    where
        T: Send + 'static,
        F: FnOnce() -> io::Result<T> + Send + 'static,
    {
        let (reply, done) = oneshot::channel();
        self.submit(Task::Blocking(Box::new(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
                .map_err(|_| FsError::JobPanicked)
                .and_then(|r| r.map_err(FsError::Os));
            let _ = reply.send(result);
        })))
        .await?;
        done.await.map_err(|_| FsError::ExecutorGone)?
    }

    pub async fn open_read(&self, path: &Path) -> io::Result<ReadFile> {
        let path = path.to_path_buf();
        let file = self.run_blocking(move || File::open(path)).await?;
        Ok(ReadFile::new(self.clone(), file))
    }

    pub async fn metadata(&self, file: Arc<File>) -> Result<Metadata, FsError> {
        self.run_blocking(move || file.metadata()).await
    }

    pub async fn directory_size(&self, path: &Path) -> Result<u64, FsError> {
        let root = path.to_path_buf();
        self.run_blocking(move || {
            let mut total = 0u64;
            let mut dirs = vec![root];
            while let Some(dir) = dirs.pop() {
                let entries = match std::fs::read_dir(dir) {
                    Ok(entries) => entries,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                for entry in entries {
                    let entry = entry?;
                    let metadata = match entry.metadata() {
                        Ok(m) => m,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e),
                    };
                    if metadata.is_dir() {
                        dirs.push(entry.path());
                    } else if metadata.is_file() {
                        total = total.saturating_add(metadata.len());
                    }
                }
            }
            Ok(total)
        })
        .await
    }

    pub async fn read_whole(&self, path: &Path) -> Result<Bytes, FsError> {
        let path = path.to_path_buf();
        self.run_blocking(move || std::fs::read(&path).map(Bytes::from))
            .await
    }

    /// `None` when nothing is at `path`.
    pub async fn stat(&self, path: &Path) -> Result<Option<Metadata>, FsError> {
        let path = path.to_path_buf();
        self.run_blocking(move || match std::fs::symlink_metadata(&path) {
            Ok(m) => Ok(Some(m)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        })
        .await
    }

    /// Creates durable directories below an already durable, provisioned ancestor.
    pub async fn mkdir_all(&self, path: &Path) -> Result<(), FsError> {
        let generation = {
            let provisioned = self.provisioned.lock().unwrap();
            if provisioned.dirs.contains(path) {
                return Ok(());
            }
            provisioned.generation
        };
        let key = path.to_path_buf();
        let path = key.clone();
        self.run_blocking(move || directory::mkdir_all(&path))
            .await?;
        let mut provisioned = self.provisioned.lock().unwrap();
        if provisioned.generation == generation {
            provisioned.dirs.insert(key);
        }
        Ok(())
    }

    /// unlink without a directory sync: for eviction, where the name coming
    /// back after a crash costs nothing but another eviction.
    pub async fn unlink(&self, path: &Path) -> Result<(), FsError> {
        let path = path.to_path_buf();
        self.run_blocking(move || std::fs::remove_file(&path)).await
    }

    pub async fn remove_dir_all(&self, path: &Path) -> Result<(), FsError> {
        self.provisioned.lock().unwrap().forget(path);
        let provisioned = self.provisioned.clone();
        let path = path.to_path_buf();
        self.run_blocking(move || {
            let removed = std::fs::remove_dir_all(&path);
            // A creation that finished while the removal ran may have cached
            // its directory. Clearing here, on the executor, holds even if the
            // caller stopped waiting.
            provisioned.lock().unwrap().forget(&path);
            removed
        })
        .await
    }

    /// The ids of the `.{ext}` files under `dir` in the 3-hex-chunk layout.
    pub async fn scan_ids(
        &self,
        dir: &Path,
        ext: &'static str,
        which: Scan,
    ) -> Result<ScanResult, FsError> {
        let dir = dir.to_path_buf();
        self.run_blocking(move || scan::scan_ids(&dir, ext, which))
            .await
    }
}

fn inode(file: &File) -> io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    Ok(file.metadata()?.ino())
}

fn finished(plan: &Plan) -> Result<(), FsError> {
    match plan.next()? {
        Op::Done => Ok(()),
        _ => Err(FsError::Os(io::Error::from_raw_os_error(plan.os_error()))),
    }
}
