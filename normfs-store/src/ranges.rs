use crate::header::{FileAuthentication, StoreHeader, StoreHeaderError};
use normfs_crypto::CryptoContext;
use normfs_types::QueueId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use uintn::{Error as UintNError, UintN, paths};

pub struct RangeStore {
    root: PathBuf,
    ranges: RwLock<HashMap<String, (UintN, UintN)>>,
    crypto_ctx: Arc<CryptoContext>,
    verify_signatures: bool,
}

#[derive(Debug)]
pub enum RangeStoreError {
    Io(tokio::io::Error),
    Header(StoreHeaderError),
    Path(paths::PathError),
    UintN(UintNError),
    FileNotFound,
    SignatureVerificationFailed,
}

impl std::fmt::Display for RangeStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RangeStoreError::Io(e) => write!(f, "IO error: {}", e),
            RangeStoreError::Header(e) => write!(f, "Header error: {}", e),
            RangeStoreError::Path(e) => write!(f, "Path error: {}", e),
            RangeStoreError::UintN(e) => write!(f, "UintN error: {}", e),
            RangeStoreError::FileNotFound => write!(f, "File not found"),
            RangeStoreError::SignatureVerificationFailed => {
                write!(f, "Signature verification failed")
            }
        }
    }
}

impl std::error::Error for RangeStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RangeStoreError::Io(e) => Some(e),
            RangeStoreError::Header(e) => Some(e),
            RangeStoreError::Path(e) => Some(e),
            RangeStoreError::UintN(e) => Some(e),
            RangeStoreError::FileNotFound => None,
            RangeStoreError::SignatureVerificationFailed => None,
        }
    }
}

impl From<tokio::io::Error> for RangeStoreError {
    fn from(e: tokio::io::Error) -> Self {
        RangeStoreError::Io(e)
    }
}

impl From<StoreHeaderError> for RangeStoreError {
    fn from(e: StoreHeaderError) -> Self {
        RangeStoreError::Header(e)
    }
}

impl From<paths::PathError> for RangeStoreError {
    fn from(e: paths::PathError) -> Self {
        RangeStoreError::Path(e)
    }
}

impl From<UintNError> for RangeStoreError {
    fn from(e: UintNError) -> Self {
        RangeStoreError::UintN(e)
    }
}

impl RangeStore {
    pub fn new(
        root: impl AsRef<Path>,
        crypto_ctx: Arc<CryptoContext>,
        verify_signatures: bool,
    ) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            ranges: RwLock::new(HashMap::new()),
            crypto_ctx,
            verify_signatures,
        }
    }

    fn key(queue_id: &QueueId, file_id: &UintN) -> String {
        format!("{}-{}", queue_id, file_id)
    }

    async fn read_range_from_fs(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, RangeStoreError> {
        let file_path = queue_id.to_store_path(&self.root, file_id);

        log::debug!(target: "normfs-store",
            "Reading range from filesystem for queue: {}, file_id: {:?}, path: {:?}",
            queue_id, file_id, file_path);

        let file = match File::open(&file_path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::debug!(target: "normfs-store",
                    "Store file not found for queue: {}, file_id: {:?}", queue_id, file_id);
                return Err(RangeStoreError::FileNotFound);
            }
            Err(e) => {
                log::error!(target: "normfs-store",
                    "Error opening store file for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e);
                return Err(RangeStoreError::Io(e));
            }
        };

        // Read FileAuthentication (152 bytes) + StoreHeader (~50 bytes) + some buffer
        let mut buffer = Vec::with_capacity(256);
        file.take(256).read_to_end(&mut buffer).await?;

        let (file_auth, auth_size) = FileAuthentication::from_bytes(&buffer)?;

        let content_after_auth = &buffer[auth_size..];
        let (header, header_size) = StoreHeader::from_bytes(content_after_auth)?;

        if self.verify_signatures {
            let header_bytes = &content_after_auth[..header_size];
            self.crypto_ctx
                .verify(header_bytes, &file_auth.header_signature)
                .map_err(|_| RangeStoreError::SignatureVerificationFailed)?;
        }

        log::debug!(target: "normfs-store",
            "Read header for queue: {}, file_id: {:?}, entries_before: {:?}, num_entries: {:?}",
            queue_id, file_id, header.num_entries_before, header.num_entries);

        if header.num_entries.is_zero() {
            log::debug!(target: "normfs-store",
                "Empty file (zero entries) for queue: {}, file_id: {:?}", queue_id, file_id);
            return Ok(None);
        }

        let first_id = header.num_entries_before;
        let num_entries_minus_one = header.num_entries.sub(&UintN::one())?;
        let last_id = first_id.add(&num_entries_minus_one);

        let range = (first_id.clone(), last_id.clone());

        log::debug!(target: "normfs-store",
            "Calculated range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
            queue_id, file_id, first_id, last_id);

        Ok(Some(range))
    }

    pub async fn get_range(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, RangeStoreError> {
        let key = Self::key(queue_id, file_id);

        log::debug!(target: "normfs-store",
            "Getting range for queue: {}, file_id: {:?}, key: {}",
            queue_id, file_id, key);

        {
            let ranges = self.ranges.read().unwrap();
            if let Some(range) = ranges.get(&key) {
                log::debug!(target: "normfs-store",
                    "Found cached range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
                    queue_id, file_id, range.0, range.1);
                return Ok(Some(range.clone()));
            }
        }

        log::debug!(target: "normfs-store",
            "Range not in cache, reading from filesystem for queue: {}, file_id: {:?}",
            queue_id, file_id);

        let range_result = self.read_range_from_fs(queue_id, file_id).await;

        match range_result {
            Ok(range) => {
                if let Some(range_val) = &range {
                    let mut ranges = self.ranges.write().unwrap();
                    ranges.insert(key.clone(), range_val.clone());
                    log::debug!(target: "normfs-store",
                        "Cached range for queue: {}, file_id: {:?}, range: {:?} to {:?}",
                        queue_id, file_id, range_val.0, range_val.1);
                }
                Ok(range)
            }
            Err(RangeStoreError::FileNotFound) => {
                log::debug!(target: "normfs-store",
                    "File not found for queue: {}, file_id: {:?}, returning None",
                    queue_id, file_id);
                Ok(None)
            }
            Err(e) => {
                log::error!(target: "normfs-store",
                    "Error getting range for queue: {}, file_id: {:?}: {:?}",
                    queue_id, file_id, e);
                Err(e)
            }
        }
    }

    pub async fn record_range(
        &self,
        queue_id: &QueueId,
        file_id: &UintN,
        first_id: &UintN,
        last_id: &UintN,
    ) -> Result<(), RangeStoreError> {
        let key = Self::key(queue_id, file_id);

        log::debug!(target: "normfs-store",
            "Recording range for queue: {}, file_id: {:?}, range: {:?} to {:?}, key: {}",
            queue_id, file_id, first_id, last_id, key);

        let mut ranges = self.ranges.write().unwrap();
        ranges.insert(key, (first_id.clone(), last_id.clone()));

        log::debug!(target: "normfs-store",
            "Successfully recorded range for queue: {}, file_id: {:?}",
            queue_id, file_id);

        Ok(())
    }
}
