use bytes::Bytes;
use normfs_fs::{AppendOutcome, Fs, ReadFile, Runs, Scan, ScanResult, TmpMode};
use normfs_types::{End, QueueId};
use std::fs::File;
use std::future::Future;
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;
use uintn::UintN;

use crate::PackSlot;

pub type WalFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// A WAL file being read: a stream that can go back to an offset.
pub trait WalRead: AsyncRead + Send + Unpin {
    fn seek_to(&mut self, offset: u64) -> WalFuture<'_, ()>;
}

impl WalRead for ReadFile {
    fn seek_to(&mut self, offset: u64) -> WalFuture<'_, ()> {
        Box::pin(async move {
            self.seek(io::SeekFrom::Start(offset)).await?;
            Ok(())
        })
    }
}

pub type WalReader = Box<dyn WalRead>;

/// How an append ended.
#[derive(Debug)]
pub enum Appended {
    /// The bytes are durable.
    Committed,
    /// Nothing is committed. `restored` says the file is back at the length
    /// the append started from, so the same append may be tried again;
    /// without it, [`AppendTarget::restore`] first.
    Failed { err: io::Error, restored: bool },
}

/// What [`WalBackend::read_into`] put in the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    Read(usize),
    /// The file is this long and does not fit; nothing was read.
    TooLarge(u64),
}

/// One WAL file, open to be appended to at a known length.
pub trait AppendTarget: Send + Sync {
    /// For logs.
    fn name(&self) -> &str;

    /// Writes `runs` at `at`. `Committed` means they are durable there, and
    /// an attempt that is not committed leaves no bytes past `at` it does
    /// not report.
    fn append(&self, at: u64, runs: Vec<Bytes>, sync: bool) -> WalFuture<'_, Appended>;

    /// Cuts the file back to `at` and makes that durable.
    fn restore(&self, at: u64) -> WalFuture<'_, ()>;

    fn size(&self) -> WalFuture<'_, u64>;
}

/// Where WAL files live, addressed by `(queue, file_id)`. Absence is
/// `Ok(None)` or `NotFound`, as each method says; errors keep their OS code.
pub trait WalBackend: Send + Sync {
    /// Makes room for the queue's files.
    fn prepare<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, ()>;

    /// A new file holding only `header`, replacing any of that id. `Ok`
    /// means the file and its header are durable.
    fn create<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        header: Bytes,
        sync: bool,
    ) -> WalFuture<'a, Arc<dyn AppendTarget>>;

    /// An existing file, to append to. `NotFound` when there is none: it is
    /// never created here.
    fn reopen<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> WalFuture<'a, Arc<dyn AppendTarget>>;

    /// The file as a stream, and its length.
    fn open_read<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> WalFuture<'a, Option<(WalReader, u64)>>;

    fn read<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> WalFuture<'a, Option<Bytes>>;

    /// The whole file into the first `cap` bytes of `slot`, without a copy
    /// of its own. The slot is handed back either way.
    fn read_into<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        slot: PackSlot,
        cap: usize,
    ) -> WalFuture<'a, (PackSlot, Fill)>;

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> WalFuture<'a, Option<UintN>>;

    fn list<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, Vec<UintN>>;

    fn delete<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> WalFuture<'a, ()>;

    /// Removes every file of the queue.
    fn clear<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, ()>;
}

/// WAL files under a local directory, through the fs pool and its plans.
pub struct LocalWal {
    fs: Fs,
    root: PathBuf,
}

impl LocalWal {
    pub fn new(fs: Fs, root: impl AsRef<Path>) -> Self {
        Self {
            fs,
            root: root.as_ref().to_path_buf(),
        }
    }

    fn path(&self, queue: &QueueId, file_id: &UintN) -> PathBuf {
        queue.to_wal_path(&self.root, file_id)
    }

    async fn scan(&self, queue: &QueueId, which: Scan) -> io::Result<ScanResult> {
        // A lookup creates nothing: a queue that never wrote a WAL file has
        // no WAL directory, and a restart reads that absence.
        Ok(self
            .fs
            .scan_ids(&queue.to_wal_dir(&self.root), "wal", which)
            .await?)
    }
}

struct LocalAppend {
    fs: Fs,
    file: Arc<File>,
    inode: u64,
    path: PathBuf,
    name: String,
}

impl LocalAppend {
    fn new(fs: Fs, file: Arc<File>, inode: u64, path: PathBuf) -> Self {
        let name = path.display().to_string();
        Self {
            fs,
            file,
            inode,
            path,
            name,
        }
    }
}

impl AppendTarget for LocalAppend {
    fn name(&self) -> &str {
        &self.name
    }

    fn append(&self, at: u64, runs: Vec<Bytes>, sync: bool) -> WalFuture<'_, Appended> {
        Box::pin(async move {
            let outcome = self
                .fs
                .append_sync_with_inode(
                    self.file.clone(),
                    self.inode,
                    &self.path,
                    at,
                    Runs(runs),
                    sync,
                )
                .await?;
            Ok(match outcome {
                AppendOutcome::Committed => Appended::Committed,
                AppendOutcome::Failed { err, restored } => Appended::Failed { err, restored },
            })
        })
    }

    fn restore(&self, at: u64) -> WalFuture<'_, ()> {
        Box::pin(async move {
            self.fs
                .restore_with_inode(self.file.clone(), self.inode, &self.path, at)
                .await?;
            Ok(())
        })
    }

    fn size(&self) -> WalFuture<'_, u64> {
        Box::pin(async move { Ok(self.fs.metadata(self.file.clone()).await?.len()) })
    }
}

/// A local file at `path` holding only `header`, durable once this returns.
pub(crate) async fn create_at(
    fs: &Fs,
    path: PathBuf,
    header: Bytes,
    sync: bool,
) -> io::Result<Arc<dyn AppendTarget>> {
    if let Some(parent) = path.parent() {
        fs.mkdir_all(parent).await?;
    }
    let (file, inode) = fs
        .create_durable_with_inode(&path, Runs(vec![header]), TmpMode::Trunc, sync)
        .await?;
    Ok(Arc::new(LocalAppend::new(
        fs.clone(),
        Arc::new(file),
        inode,
        path,
    )))
}

fn absent<T>(r: io::Result<T>) -> io::Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

impl WalBackend for LocalWal {
    fn prepare<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, ()> {
        Box::pin(async move { Ok(self.fs.mkdir_all(&queue.to_wal_dir(&self.root)).await?) })
    }

    fn create<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        header: Bytes,
        sync: bool,
    ) -> WalFuture<'a, Arc<dyn AppendTarget>> {
        Box::pin(create_at(&self.fs, self.path(queue, file_id), header, sync))
    }

    fn reopen<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> WalFuture<'a, Arc<dyn AppendTarget>> {
        Box::pin(async move {
            let path = self.path(queue, file_id);
            let open = path.clone();
            let file = self
                .fs
                .run_blocking(move || std::fs::OpenOptions::new().write(true).open(&open))
                .await?;
            let file = Arc::new(file);
            let inode = self.fs.metadata(file.clone()).await?.ino();
            let target: Arc<dyn AppendTarget> =
                Arc::new(LocalAppend::new(self.fs.clone(), file, inode, path));
            Ok(target)
        })
    }

    fn open_read<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> WalFuture<'a, Option<(WalReader, u64)>> {
        Box::pin(async move {
            let Some(file) = absent(self.fs.open_read(&self.path(queue, file_id)).await)? else {
                return Ok(None);
            };
            let len = file.metadata().await?.len();
            let reader: WalReader = Box::new(file);
            Ok(Some((reader, len)))
        })
    }

    fn read<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> WalFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let read = self.fs.read_whole(&self.path(queue, file_id)).await;
            absent(read.map_err(io::Error::from))
        })
    }

    fn read_into<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        mut slot: PackSlot,
        cap: usize,
    ) -> WalFuture<'a, (PackSlot, Fill)> {
        Box::pin(async move {
            let path = self.path(queue, file_id);
            Ok(self
                .fs
                .run_blocking(move || {
                    let file = File::open(&path)?;
                    let len = file.metadata()?.len();
                    if len > cap.min(slot.buf().len()) as u64 {
                        return Ok((slot, Fill::TooLarge(len)));
                    }
                    let len = len as usize;
                    file.read_exact_at(&mut slot.buf()[..len], 0)?;
                    Ok((slot, Fill::Read(len)))
                })
                .await?)
        })
    }

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> WalFuture<'a, Option<UintN>> {
        Box::pin(async move {
            let which = match end {
                End::Min => Scan::Min,
                End::Max => Scan::Max,
            };
            match self.scan(queue, which).await? {
                ScanResult::One(id) => Ok(Some(id)),
                _ => Ok(None),
            }
        })
    }

    fn list<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, Vec<UintN>> {
        Box::pin(async move {
            match self.scan(queue, Scan::All).await? {
                ScanResult::All(ids) => Ok(ids),
                _ => Ok(Vec::new()),
            }
        })
    }

    fn delete<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> WalFuture<'a, ()> {
        Box::pin(async move { Ok(self.fs.unlink(&self.path(queue, file_id)).await?) })
    }

    fn clear<'a>(&'a self, queue: &'a QueueId) -> WalFuture<'a, ()> {
        Box::pin(async move {
            let dir = queue.to_wal_dir(&self.root);
            self.fs.remove_dir_all(&dir).await?;
            self.fs.mkdir_all(&dir).await?;
            Ok(())
        })
    }
}
