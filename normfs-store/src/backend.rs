use bytes::Bytes;
use normfs_fs::{Fs, ReadFile, Scan, ScanResult};
use normfs_types::events::UploadFailure;
use normfs_types::{DataSource, QueueId};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use uintn::UintN;

use crate::DiskUsage;
use crate::store_file::publish_local;

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, BackendError>> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum End {
    Min,
    Max,
}

/// A store file on its way to a backend.
pub enum Body {
    Runs(Vec<Bytes>),
    /// Read as it is sent, so a file of any size takes a chunk of memory.
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
            BackendError::Io(_) | BackendError::Remote(_) => UploadFailure::Network,
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
            other => io::Error::other(other),
        }
    }
}

/// Where a queue's store files live, addressed by `(queue, file_id)`.
///
/// `put` returns `Ok` only once the file is safe there: synced and renamed on
/// disk, or put and its size read back from the bucket. Absence is `Ok(None)`,
/// never an error.
pub trait StoreBackend: Send + Sync {
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

    /// The lowest or highest file id the queue has here.
    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>>;
}

/// The local store directory, through the fs pool.
pub struct LocalStore {
    fs: Fs,
    root: PathBuf,
    fsync: bool,
    usage: Arc<DiskUsage>,
}

impl LocalStore {
    pub fn new(fs: Fs, root: impl AsRef<Path>, fsync: bool, usage: Arc<DiskUsage>) -> Self {
        Self {
            fs,
            root: root.as_ref().to_path_buf(),
            fsync,
            usage,
        }
    }

    pub fn path(&self, queue: &QueueId, file_id: &UintN) -> PathBuf {
        queue.to_store_path(&self.root, file_id)
    }
}

fn absent<T>(r: io::Result<T>) -> Result<Option<T>, BackendError> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

impl StoreBackend for LocalStore {
    fn source(&self) -> DataSource {
        DataSource::DiskStore
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
            publish_local(
                &self.fs,
                &self.root,
                queue,
                file_id,
                runs,
                self.fsync,
                &self.usage,
            )
            .await?;
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
            Ok(meta.map(|m| m.len()))
        })
    }

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>> {
        Box::pin(async move {
            let which = match end {
                End::Min => Scan::Min,
                End::Max => Scan::Max,
            };
            // A lookup creates nothing: a queue that never wrote a store file
            // has no store directory, and a restart reads that absence.
            let dir = queue.to_store_dir(&self.root);
            match self.fs.scan_ids(&dir, "store", which).await? {
                ScanResult::One(id) => Ok(Some(id)),
                _ => Ok(None),
            }
        })
    }
}
