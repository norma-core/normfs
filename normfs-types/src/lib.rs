use bytes::Bytes;
use uintn::UintN;

/// Callback type for subscription notifications
pub type SubscriberCallback = Box<dyn Fn(&[(UintN, Bytes)]) -> bool + Send + Sync>;

/// Queue identifier with support for absolute and relative paths
/// The path field always contains the absolute path (with instance_id embedded for relative paths)
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueId {
    path: String,
}

impl std::fmt::Debug for QueueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl std::fmt::Display for QueueId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl QueueId {
    /// Get the absolute queue path as a string slice
    pub fn as_str(&self) -> &str {
        &self.path
    }

    /// Get the queue path as a base for cryptographic key derivation
    pub fn to_key_derivation_base(&self) -> &str {
        &self.path
    }

    /// Get the queue path for cloud storage with base prefix
    /// Example: "prefix/instance_id/queue_name/"
    pub fn to_cloud_queue_path(&self, base_prefix: &str) -> String {
        format!("{}{}/", base_prefix, self.path.trim_start_matches('/'))
    }

    fn to_fs_path(&self, prefix: &std::path::Path) -> std::path::PathBuf {
        let relative = self.path.trim_start_matches('/');
        prefix.join(relative)
    }

    /// Construct the full filesystem path to the store directory
    /// Example: /base/instance_id/queue_name/store
    pub fn to_store_dir(&self, base: &std::path::Path) -> std::path::PathBuf {
        self.to_fs_path(base).join("store")
    }

    /// Construct the full filesystem path to a store file
    /// Example: /base/instance_id/queue_name/store/abc/def/123.store
    pub fn to_store_path(&self, base: &std::path::Path, file_id: &UintN) -> std::path::PathBuf {
        let queue_path = self.to_store_dir(base);
        file_id.to_file_path(&queue_path.to_string_lossy(), "store")
    }

    /// Construct the full filesystem path to the WAL directory
    /// Example: /base/instance_id/queue_name/wal
    pub fn to_wal_dir(&self, base: &std::path::Path) -> std::path::PathBuf {
        self.to_fs_path(base).join("wal")
    }

    /// Construct the full filesystem path to a WAL file
    /// Example: /base/instance_id/queue_name/wal/123.wal
    pub fn to_wal_path(&self, base: &std::path::Path, file_id: &UintN) -> std::path::PathBuf {
        let queue_path = self.to_wal_dir(base);
        file_id.to_file_path(&queue_path.to_string_lossy(), "wal")
    }

    /// Construct the cloud storage key for a store file (always uses forward slashes)
    /// Example: prefix/instance_id/queue_name/abc/def/123.store
    /// Note: Only store files are stored in cloud storage, not WAL files
    pub fn to_cloud_key(&self, base_prefix: &str, file_id: &UintN) -> String {
        let queue_path = self.path.trim_start_matches('/');
        let file_path_buf = file_id.to_file_path("", "store");
        let file_path = file_path_buf.to_str().unwrap_or("").trim_start_matches('/');

        if base_prefix.is_empty() {
            format!("{}/{}", queue_path, file_path)
        } else {
            format!("{}{}/{}", base_prefix, queue_path, file_path)
        }
    }
}

/// Resolver for QueueId operations that require instance_id
#[derive(Debug, Clone)]
pub struct QueueIdResolver {
    instance_id: String,
}

impl QueueIdResolver {
    pub fn new(instance_id: impl Into<String>) -> Self {
        Self {
            instance_id: instance_id.into(),
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Resolve a queue path to a QueueId with absolute path
    /// Relative paths are prefixed with /instance_id/, absolute paths (starting with /) are used as-is
    pub fn resolve(&self, path: &str) -> QueueId {
        let absolute_path = if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{}/{}", self.instance_id, path)
        };

        QueueId {
            path: absolute_path,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSource {
    None,
    Cloud,
    DiskStore,
    DiskWal,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum CompressionType {
    None = 0,
    Gzip = 1,
    Xz = 2,
    Zstd = 3,
}

impl TryFrom<u64> for CompressionType {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Gzip),
            2 => Ok(CompressionType::Xz),
            3 => Ok(CompressionType::Zstd),
            _ => Err(format!("Unsupported compression type: {}", value)),
        }
    }
}

impl From<CompressionType> for u64 {
    fn from(v: CompressionType) -> Self {
        v as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum EncryptionType {
    None = 0,
    Aes = 1,
}

impl TryFrom<u64> for EncryptionType {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(EncryptionType::None),
            1 => Ok(EncryptionType::Aes),
            _ => Err(format!("Unsupported encryption type: {}", value)),
        }
    }
}

impl From<EncryptionType> for u64 {
    fn from(v: EncryptionType) -> Self {
        v as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPosition {
    Absolute(UintN),
    ShiftFromTail(UintN),
}

impl ReadPosition {
    pub fn absolute(offset: UintN) -> Self {
        Self::Absolute(offset)
    }

    pub fn shift_from_tail(offset: UintN) -> Self {
        Self::ShiftFromTail(offset)
    }

    pub fn offset(&self) -> &UintN {
        match self {
            Self::Absolute(offset) => offset,
            Self::ShiftFromTail(offset) => offset,
        }
    }

    pub fn is_absolute(&self) -> bool {
        matches!(self, Self::Absolute(_))
    }

    pub fn is_tail_relative(&self) -> bool {
        matches!(self, Self::ShiftFromTail(_))
    }
}

#[derive(Debug, Clone)]
pub struct ReadEntry {
    pub id: UintN,
    pub data: Bytes,
    pub source: DataSource,
}

impl ReadEntry {
    pub fn new(id: UintN, data: Bytes, source: DataSource) -> Self {
        Self { id, data, source }
    }
}

#[cfg(test)]
mod queue_id_test;
