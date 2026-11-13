use bytes::Bytes;
use normfs_fs::{AppendOutcome, Fs, PublishSpec, ReadFile, Runs, Scan, ScanResult, TmpMode};
use normfs_types::events::UploadFailure;
use normfs_types::{DataSource, QueueId};
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt};
use uintn::UintN;

use crate::PackSlot;

pub use normfs_types::End;

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BackendError>> + Send + 'a>>;

/// A file on its way to a backend.
pub enum Body {
    Runs(Vec<Bytes>),
    /// A bucket reads it as it sends it, so a file of any size takes a chunk
    /// of memory; the local store reads it whole first.
    Stream {
        file: ReadFile,
        len: u64,
    },
}

impl Body {
    pub fn len(&self) -> u64 {
        match self {
            Body::Runs(runs) => runs.iter().map(|r| r.len() as u64).sum(),
            Body::Stream { len, .. } => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug)]
pub enum BackendError {
    /// Raw OS errors survive, so callers can still classify on `kind()`.
    Io(io::Error),
    /// The bucket answered with a status the operation does not accept.
    Status(u16),
    /// Nothing was found right after a put.
    Missing,
    SizeMismatch {
        local: u64,
        remote: u64,
    },
    Remote(Box<dyn std::error::Error + Send + Sync>),
    /// The backend cannot do this, as a bucket cannot append to an object.
    Unsupported(&'static str),
}

impl BackendError {
    /// What the system queue records for a failed upload.
    pub fn failure(&self) -> UploadFailure {
        match self {
            BackendError::Status(code) => UploadFailure::Status(*code),
            BackendError::Missing => UploadFailure::Missing,
            BackendError::SizeMismatch { local, remote } => UploadFailure::SizeMismatch {
                local: *local,
                remote: *remote,
            },
            BackendError::Io(_) | BackendError::Remote(_) | BackendError::Unsupported(_) => {
                UploadFailure::Network
            }
        }
    }
}

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendError::Io(e) => write!(f, "IO error: {e}"),
            BackendError::Status(code) => write!(f, "unexpected response code: {code}"),
            BackendError::Missing => write!(f, "not found after upload"),
            BackendError::SizeMismatch { local, remote } => {
                write!(
                    f,
                    "size mismatch after upload: local={local}, remote={remote}"
                )
            }
            BackendError::Remote(e) => write!(f, "request failed: {e}"),
            BackendError::Unsupported(op) => write!(f, "{op} is not supported by this backend"),
        }
    }
}

impl std::error::Error for BackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BackendError::Io(e) => Some(e),
            BackendError::Remote(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for BackendError {
    fn from(e: io::Error) -> Self {
        BackendError::Io(e)
    }
}

impl From<normfs_fs::FsError> for BackendError {
    fn from(e: normfs_fs::FsError) -> Self {
        BackendError::Io(e.into())
    }
}

impl From<BackendError> for io::Error {
    fn from(e: BackendError) -> Self {
        match e {
            BackendError::Io(e) => e,
            BackendError::Unsupported(_) => io::Error::new(io::ErrorKind::Unsupported, e),
            other => io::Error::other(other),
        }
    }
}

/// A file being read: a stream that can go back to an offset.
pub trait FileRead: AsyncRead + Send + Unpin {
    fn seek_to(&mut self, offset: u64) -> BackendFuture<'_, ()>;
}

impl FileRead for ReadFile {
    fn seek_to(&mut self, offset: u64) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.seek(io::SeekFrom::Start(offset)).await?;
            Ok(())
        })
    }
}

impl FileRead for io::Cursor<Bytes> {
    fn seek_to(&mut self, offset: u64) -> BackendFuture<'_, ()> {
        self.set_position(offset);
        Box::pin(async { Ok(()) })
    }
}

pub type Reader = Box<dyn FileRead>;

/// How an append ended.
#[derive(Debug)]
pub enum Appended {
    Committed,
    /// Nothing is committed. `restored` says the file is back at the length
    /// the append started from, so the same append may be tried again;
    /// without it, [`AppendTarget::restore`] first.
    Failed {
        err: io::Error,
        restored: bool,
    },
}

/// What [`Backend::read_into`] put in the slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fill {
    Read(usize),
    /// The file is this long and does not fit; nothing was read.
    TooLarge(u64),
}

/// One file, open to be appended to at a known length.
pub trait AppendTarget: Send + Sync {
    fn name(&self) -> &str;

    /// Writes `runs` at `at`. `Committed` means they are durable there, and
    /// an attempt that is not committed leaves no bytes past `at` it does
    /// not report.
    fn append(&self, at: u64, runs: Vec<Bytes>, sync: bool) -> BackendFuture<'_, Appended>;

    /// Cuts the file back to `at` and makes that durable.
    fn restore(&self, at: u64) -> BackendFuture<'_, ()>;

    fn size(&self) -> BackendFuture<'_, u64>;
}

fn unsupported<'a, T: Send + 'a>(op: &'static str) -> BackendFuture<'a, T> {
    Box::pin(async move { Err(BackendError::Unsupported(op)) })
}

/// Where a queue's files live, store files and WAL files alike, addressed by
/// `(queue, file_id)`.
///
/// `put` returns `Ok` only once the file is safe there: synced and renamed on
/// disk, or put and its size read back from the bucket. Absence is `Ok(None)`,
/// never an error. A backend that cannot append, delete or list answers
/// [`BackendError::Unsupported`], which is what the defaults do.
pub trait Backend: Send + Sync {
    /// How entries read from this backend are reported.
    fn source(&self) -> DataSource;

    /// The file's name in this backend, for logs and events.
    fn key(&self, queue: &QueueId, file_id: &UintN) -> String;

    fn put<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        body: Body,
    ) -> BackendFuture<'a, ()>;

    fn get<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Bytes>>;

    /// The whole file as a body another backend can take.
    fn body<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Body>>;

    /// Up to `len` bytes from `offset`; fewer when the file is shorter.
    fn get_range<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        offset: u64,
        len: u64,
    ) -> BackendFuture<'a, Option<Bytes>>;

    fn size<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN)
    -> BackendFuture<'a, Option<u64>>;

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>>;

    fn list<'a>(&'a self, _queue: &'a QueueId) -> BackendFuture<'a, Vec<UintN>> {
        unsupported("list")
    }

    fn delete<'a>(&'a self, _queue: &'a QueueId, _file_id: &'a UintN) -> BackendFuture<'a, ()> {
        unsupported("delete")
    }

    /// Removes every file of the queue.
    fn clear<'a>(&'a self, _queue: &'a QueueId) -> BackendFuture<'a, ()> {
        unsupported("clear")
    }

    /// Makes room for the queue's files before the first append.
    fn prepare<'a>(&'a self, _queue: &'a QueueId) -> BackendFuture<'a, ()> {
        unsupported("append")
    }

    /// A new file holding only `header`, replacing any of that id, to append
    /// to. `Ok` means the file and its header are durable.
    fn create<'a>(
        &'a self,
        _queue: &'a QueueId,
        _file_id: &'a UintN,
        _header: Bytes,
        _sync: bool,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        unsupported("append")
    }

    /// An existing file, to append to. `NotFound` when there is none: it is
    /// never created here.
    fn reopen<'a>(
        &'a self,
        _queue: &'a QueueId,
        _file_id: &'a UintN,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        unsupported("append")
    }

    /// The file as a stream, and its length.
    fn open_read<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<(Reader, u64)>> {
        Box::pin(async move {
            let Some(data) = self.get(queue, file_id).await? else {
                return Ok(None);
            };
            let len = data.len() as u64;
            let reader: Reader = Box::new(io::Cursor::new(data));
            Ok(Some((reader, len)))
        })
    }

    /// The whole file into the first `cap` bytes of `slot`. The slot is
    /// handed back either way.
    fn read_into<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        mut slot: PackSlot,
        cap: usize,
    ) -> BackendFuture<'a, (PackSlot, Fill)> {
        Box::pin(async move {
            let Some(data) = self.get(queue, file_id).await? else {
                return Err(BackendError::Io(io::ErrorKind::NotFound.into()));
            };
            if data.len() > cap.min(slot.buf().len()) {
                return Ok((slot, Fill::TooLarge(data.len() as u64)));
            }
            slot.buf()[..data.len()].copy_from_slice(&data);
            Ok((slot, Fill::Read(data.len())))
        })
    }
}

/// How a local directory names its files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Layout {
    Store,
    Wal,
}

impl Layout {
    fn ext(self) -> &'static str {
        match self {
            Layout::Store => "store",
            Layout::Wal => "wal",
        }
    }
}

/// The first and last file id of each queue, kept as files come and go so
/// that finding them does not list the directory. Shared by every backend on
/// the same directory. Files only appear through them, but eviction removes
/// files behind them, so a remembered end is checked to still be there.
#[derive(Default)]
pub struct FileEnds(Mutex<HashMap<QueueId, Ends>>);

/// `None` is not known; `Some(None)` is known empty.
#[derive(Default)]
struct Ends {
    /// Bumped by every change, so a listing that overlapped one is not kept.
    generation: u64,
    min: Option<Option<UintN>>,
    max: Option<Option<UintN>>,
    /// Puts and creates under way: their file may be on disk before the ends
    /// hear of it, so until they finish the directory is listed instead.
    landing: u64,
}

impl Ends {
    fn side(&mut self, end: End) -> &mut Option<Option<UintN>> {
        match end {
            End::Min => &mut self.min,
            End::Max => &mut self.max,
        }
    }
}

impl FileEnds {
    /// The end if known, else the generation a listing must still find.
    fn known(&self, queue: &QueueId, end: End) -> Result<Option<UintN>, u64> {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        let generation = ends.generation;
        match ends.landing {
            0 => ends.side(end).clone().ok_or(generation),
            _ => Err(generation),
        }
    }

    fn learn(&self, queue: &QueueId, end: End, generation: u64, found: Option<UintN>) {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        if ends.generation == generation && ends.landing == 0 {
            *ends.side(end) = Some(found);
        }
    }

    fn landing(&self, queue: &QueueId) {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        ends.generation += 1;
        ends.landing += 1;
    }

    /// A put or create finished: `Some` with its file, `None` when it failed
    /// and may or may not have left one.
    /// One step with the counter, so the cache never reopens before it
    /// holds the file.
    fn landed(&self, queue: &QueueId, file_id: Option<&UintN>) {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        ends.landing = ends.landing.saturating_sub(1);
        ends.generation += 1;
        let Some(file_id) = file_id else {
            ends.min = None;
            ends.max = None;
            return;
        };
        if let Some(min) = &mut ends.min
            && min.as_ref().is_none_or(|m| file_id < m)
        {
            *min = Some(file_id.clone());
        }
        if let Some(max) = &mut ends.max
            && max.as_ref().is_none_or(|m| file_id > m)
        {
            *max = Some(file_id.clone());
        }
    }

    fn removed(&self, queue: &QueueId, file_id: &UintN) {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        ends.generation += 1;
        for side in [&mut ends.min, &mut ends.max] {
            if side.as_ref().is_some_and(|s| s.as_ref() == Some(file_id)) {
                *side = None;
            }
        }
    }

    /// Forgets both ends; the generation a listing must then still find.
    fn forget(&self, queue: &QueueId) -> u64 {
        let mut map = self.0.lock().unwrap();
        let ends = map.entry(queue.clone()).or_default();
        ends.generation += 1;
        ends.min = None;
        ends.max = None;
        ends.generation
    }
}

/// A landing task's result. Its panic goes on unwinding in the caller; the
/// ends stay closed to the cache for that queue, as the landing never finished.
fn joined<T>(joined: Result<T, tokio::task::JoinError>) -> Result<T, BackendError> {
    match joined {
        Ok(result) => Ok(result),
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(e) => Err(io::Error::other(e).into()),
    }
}

/// Accounts a local put; the store's disk usage implements it.
pub trait Publisher: Send + Sync {
    fn publish<'a>(
        &'a self,
        fs: &'a Fs,
        queue: &'a QueueId,
        spec: PublishSpec,
    ) -> BackendFuture<'a, ()>;
}

/// Files under a local directory, through the fs pool and its plans: the
/// store's or the WAL's, as `layout` says.
pub struct Local {
    fs: Fs,
    root: PathBuf,
    layout: Layout,
    sync: bool,
    publisher: Option<Arc<dyn Publisher>>,
    ends: Arc<FileEnds>,
}

impl Local {
    pub fn wal(fs: Fs, root: impl AsRef<Path>) -> Self {
        Self {
            fs,
            root: root.as_ref().to_path_buf(),
            layout: Layout::Wal,
            sync: true,
            publisher: None,
            ends: Arc::default(),
        }
    }

    /// Puts are synced when `sync` says so and accounted by `publisher`.
    pub fn store(
        fs: Fs,
        root: impl AsRef<Path>,
        sync: bool,
        publisher: Arc<dyn Publisher>,
    ) -> Self {
        Self {
            fs,
            root: root.as_ref().to_path_buf(),
            layout: Layout::Store,
            sync,
            publisher: Some(publisher),
            ends: Arc::default(),
        }
    }

    /// Shares `ends` with the other backends on this directory.
    pub fn with_ends(mut self, ends: Arc<FileEnds>) -> Self {
        self.ends = ends;
        self
    }

    pub fn ends(&self) -> Arc<FileEnds> {
        self.ends.clone()
    }

    #[cfg(test)]
    pub(crate) fn landing(&self, queue: &QueueId) -> bool {
        let map = self.ends.0.lock().unwrap();
        map.get(queue).is_some_and(|ends| ends.landing > 0)
    }

    pub fn path(&self, queue: &QueueId, file_id: &UintN) -> PathBuf {
        match self.layout {
            Layout::Store => queue.to_store_path(&self.root, file_id),
            Layout::Wal => queue.to_wal_path(&self.root, file_id),
        }
    }

    fn dir(&self, queue: &QueueId) -> PathBuf {
        match self.layout {
            Layout::Store => queue.to_store_dir(&self.root),
            Layout::Wal => queue.to_wal_dir(&self.root),
        }
    }

    /// Store temp files go to `<root>/tmp`, which recovery empties; a WAL
    /// temp file sits beside its file, where scans skip it by extension.
    fn tmp_path(&self, dst: &Path) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "{}-{}-{}.tmp",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        match self.layout {
            Layout::Store => self.root.join("tmp").join(name),
            Layout::Wal => dst.with_file_name(name),
        }
    }

    async fn scan(&self, queue: &QueueId, which: Scan) -> io::Result<ScanResult> {
        // A lookup creates nothing: a queue that never wrote a file here has
        // no directory, and a restart reads that absence.
        Ok(self
            .fs
            .scan_ids(&self.dir(queue), self.layout.ext(), which)
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

    fn append(&self, at: u64, runs: Vec<Bytes>, sync: bool) -> BackendFuture<'_, Appended> {
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

    fn restore(&self, at: u64) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.fs
                .restore_with_inode(self.file.clone(), self.inode, &self.path, at)
                .await?;
            Ok(())
        })
    }

    fn size(&self) -> BackendFuture<'_, u64> {
        Box::pin(async move { Ok(self.fs.metadata(self.file.clone()).await?.len()) })
    }
}

/// A local file at `path` holding only `header`, durable once this returns.
pub(crate) async fn create_at(
    fs: &Fs,
    path: PathBuf,
    header: Bytes,
    sync: bool,
) -> Result<Arc<dyn AppendTarget>, BackendError> {
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

fn absent<T>(r: io::Result<T>) -> Result<Option<T>, BackendError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl Backend for Local {
    fn source(&self) -> DataSource {
        match self.layout {
            Layout::Store => DataSource::DiskStore,
            Layout::Wal => DataSource::DiskWal,
        }
    }

    fn key(&self, queue: &QueueId, file_id: &UintN) -> String {
        self.path(queue, file_id).to_string_lossy().into_owned()
    }

    fn put<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        body: Body,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let runs = match body {
                Body::Runs(runs) => runs,
                Body::Stream { mut file, len } => {
                    let mut buf = Vec::with_capacity(len as usize);
                    file.read_to_end(&mut buf).await?;
                    vec![Bytes::from(buf)]
                }
            };
            let dst = self.path(queue, file_id);
            let parent = dst
                .parent()
                .ok_or_else(|| io::Error::other("file path has no parent"))?;
            self.fs.mkdir_all(parent).await?;
            let tmp = self.tmp_path(&dst);
            if let Some(tmp_dir) = tmp.parent() {
                self.fs.mkdir_all(tmp_dir).await?;
            }
            let len: usize = runs.iter().map(Bytes::len).sum();
            let spec = PublishSpec {
                tmp,
                dst: dst.clone(),
                runs: Runs(runs),
                tmp_mode: TmpMode::Excl,
                sync: self.sync,
            };
            // On its own task, so a caller that gives up cannot leave the file
            // landed and the ends not told.
            let (fs, publisher, ends) =
                (self.fs.clone(), self.publisher.clone(), self.ends.clone());
            let (q, id) = (queue.clone(), file_id.clone());
            ends.landing(queue);
            let landing = tokio::spawn(async move {
                let published = match &publisher {
                    Some(publisher) => publisher.publish(&fs, &q, spec).await,
                    None => fs.publish(spec, None).await.map(drop).map_err(Into::into),
                };
                // A failed put may still have left the file.
                ends.landed(&q, published.is_ok().then_some(&id));
                published
            });
            joined(landing.await)??;
            log::debug!(target: "normfs-wal",
                "Put file for queue {}, file {}: {} bytes at {:?}", queue.short(), file_id, len, dst);
            Ok(())
        })
    }

    fn get<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let read = self.fs.read_whole(&self.path(queue, file_id)).await;
            absent(read.map_err(io::Error::from))
        })
    }

    fn body<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Body>> {
        Box::pin(async move {
            let Some(file) = absent(self.fs.open_read(&self.path(queue, file_id)).await)? else {
                return Ok(None);
            };
            let len = file.metadata().await?.len();
            Ok(Some(Body::Stream { file, len }))
        })
    }

    fn get_range<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        offset: u64,
        len: u64,
    ) -> BackendFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            let Some(mut file) = absent(self.fs.open_read(&self.path(queue, file_id)).await)?
            else {
                return Ok(None);
            };
            if offset > 0 {
                file.seek(io::SeekFrom::Start(offset)).await?;
            }
            let mut buf = Vec::with_capacity(len as usize);
            file.take(len).read_to_end(&mut buf).await?;
            Ok(Some(Bytes::from(buf)))
        })
    }

    fn size<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<u64>> {
        Box::pin(async move {
            let meta = self.fs.stat(&self.path(queue, file_id)).await?;
            Ok(meta.map(|m| m.size()))
        })
    }

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>> {
        Box::pin(async move {
            let generation = match self.ends.known(queue, end) {
                Ok(None) => return Ok(None),
                Ok(Some(id)) => {
                    if self.fs.stat(&self.path(queue, &id)).await?.is_some() {
                        return Ok(Some(id));
                    }
                    self.ends.forget(queue)
                }
                Err(generation) => generation,
            };
            let which = match end {
                End::Min => Scan::Min,
                End::Max => Scan::Max,
            };
            let found = match self.scan(queue, which).await? {
                ScanResult::One(id) => Some(id),
                _ => None,
            };
            self.ends.learn(queue, end, generation, found.clone());
            Ok(found)
        })
    }

    fn list<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, Vec<UintN>> {
        Box::pin(async move {
            match self.scan(queue, Scan::All).await? {
                ScanResult::All(ids) => Ok(ids),
                _ => Ok(Vec::new()),
            }
        })
    }

    fn delete<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let unlinked = self.fs.unlink(&self.path(queue, file_id)).await;
            self.ends.removed(queue, file_id);
            Ok(unlinked?)
        })
    }

    fn clear<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let dir = self.dir(queue);
            let removed = self.fs.remove_dir_all(&dir).await;
            self.ends.forget(queue);
            removed?;
            self.fs.mkdir_all(&dir).await?;
            Ok(())
        })
    }

    fn prepare<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, ()> {
        Box::pin(async move { Ok(self.fs.mkdir_all(&self.dir(queue)).await?) })
    }

    fn create<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        header: Bytes,
        sync: bool,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        Box::pin(async move {
            let (fs, ends, path) = (
                self.fs.clone(),
                self.ends.clone(),
                self.path(queue, file_id),
            );
            let (q, id) = (queue.clone(), file_id.clone());
            ends.landing(queue);
            // As in put: the ends hear of the file even if the caller gives up.
            let creating = tokio::spawn(async move {
                let created = create_at(&fs, path, header, sync).await;
                ends.landed(&q, created.is_ok().then_some(&id));
                created
            });
            joined(creating.await)?
        })
    }

    fn reopen<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        Box::pin(async move {
            let path = self.path(queue, file_id);
            let file = self.fs.open(&path, true).await?;
            let file = Arc::new(file);
            let inode = self.fs.inode(file.clone()).await?;
            let target: Arc<dyn AppendTarget> =
                Arc::new(LocalAppend::new(self.fs.clone(), file, inode, path));
            Ok(target)
        })
    }

    fn open_read<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<(Reader, u64)>> {
        Box::pin(async move {
            let Some(file) = absent(self.fs.open_read(&self.path(queue, file_id)).await)? else {
                return Ok(None);
            };
            let len = file.metadata().await?.len();
            let reader: Reader = Box::new(file);
            Ok(Some((reader, len)))
        })
    }

    fn read_into<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        mut slot: PackSlot,
        cap: usize,
    ) -> BackendFuture<'a, (PackSlot, Fill)> {
        Box::pin(async move {
            let path = self.path(queue, file_id);
            let fs = self.fs.clone();
            Ok(self
                .fs
                .run_blocking(move || {
                    let file = fs.open_blocking(&path, false)?;
                    let len = file.metadata()?.len();
                    if len > cap.min(slot.buf().len()) as u64 {
                        return Ok((slot, Fill::TooLarge(len)));
                    }
                    let len = len as usize;
                    normfs_fs::read_exact_at(&file, &mut slot.buf()[..len], 0)?;
                    Ok((slot, Fill::Read(len)))
                })
                .await?)
        })
    }
}
