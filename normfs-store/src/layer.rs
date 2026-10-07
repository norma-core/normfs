use bytes::Bytes;
use normfs_crypto::CryptoContext;
use normfs_types::{BoundedMap, DataSource, QueueId};
use std::sync::{Arc, RwLock};
use uintn::{Error as UintNError, UintN};

use crate::backend::{Backend, BackendError, End};
use crate::header::{FileAuthentication, StoreHeaderError};
use crate::store_file::HEAD_LEN;
use crate::store_header_v1::{AnyStoreHeader, AnyStoreHeaderError};

const RANGE_CACHE_CAP: usize = 4096;

/// One place a queue's store files live, with what is known about them: the
/// entry range of each file, cached so a lookup reads a header once.
pub struct Layer {
    backend: Arc<dyn Backend>,
    ranges: RwLock<BoundedMap<String, (UintN, UintN)>>,
    verify_headers: Option<Arc<CryptoContext>>,
    verify_bodies: bool,
}

#[derive(Debug)]
pub enum LayerError {
    Backend(BackendError),
    Header(StoreHeaderError),
    AnyHeader(AnyStoreHeaderError),
    UintN(UintNError),
    SignatureVerificationFailed,
}

impl std::fmt::Display for LayerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayerError::Backend(e) => write!(f, "{e}"),
            LayerError::Header(e) => write!(f, "Header error: {e}"),
            LayerError::AnyHeader(e) => write!(f, "Header error: {e}"),
            LayerError::UintN(e) => write!(f, "UintN error: {e}"),
            LayerError::SignatureVerificationFailed => write!(f, "Signature verification failed"),
        }
    }
}

impl std::error::Error for LayerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LayerError::Backend(e) => Some(e),
            LayerError::Header(e) => Some(e),
            LayerError::AnyHeader(e) => Some(e),
            LayerError::UintN(e) => Some(e),
            LayerError::SignatureVerificationFailed => None,
        }
    }
}

impl From<BackendError> for LayerError {
    fn from(e: BackendError) -> Self {
        LayerError::Backend(e)
    }
}

impl From<StoreHeaderError> for LayerError {
    fn from(e: StoreHeaderError) -> Self {
        LayerError::Header(e)
    }
}

impl From<AnyStoreHeaderError> for LayerError {
    fn from(e: AnyStoreHeaderError) -> Self {
        LayerError::AnyHeader(e)
    }
}

impl From<UintNError> for LayerError {
    fn from(e: UintNError) -> Self {
        LayerError::UintN(e)
    }
}

impl Layer {
    /// `verify_headers` checks the header signature on every range read;
    /// `verify_bodies` asks readers to check whole files they take from here.
    pub fn new(
        backend: Arc<dyn Backend>,
        verify_headers: Option<Arc<CryptoContext>>,
        verify_bodies: bool,
    ) -> Self {
        Self {
            backend,
            ranges: RwLock::new(BoundedMap::new(RANGE_CACHE_CAP)),
            verify_headers,
            verify_bodies,
        }
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    pub fn source(&self) -> DataSource {
        self.backend.source()
    }

    pub fn verifies_bodies(&self) -> bool {
        self.verify_bodies
    }

    fn key(queue: &QueueId, file_id: &UintN) -> String {
        format!("{}-{}", queue, file_id)
    }

    pub async fn first_file_id(&self, queue: &QueueId) -> Result<Option<UintN>, BackendError> {
        self.backend.find(queue, End::Min).await
    }

    pub async fn last_file_id(&self, queue: &QueueId) -> Result<Option<UintN>, BackendError> {
        self.backend.find(queue, End::Max).await
    }

    pub async fn get_store_bytes(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<Bytes>, BackendError> {
        self.backend.get(queue, file_id).await
    }

    /// The ids the file holds, first and last; `None` when there is no such
    /// file or it holds nothing.
    pub async fn get_file_range(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, LayerError> {
        let key = Self::key(queue, file_id);
        if let Some(range) = self.ranges.read().unwrap().get(&key) {
            return Ok(Some(range.clone()));
        }
        let range = self.read_range(queue, file_id).await?;
        if let Some(range) = &range {
            self.ranges.write().unwrap().insert(key, range.clone());
        }
        Ok(range)
    }

    async fn read_range(
        &self,
        queue: &QueueId,
        file_id: &UintN,
    ) -> Result<Option<(UintN, UintN)>, LayerError> {
        let Some(head) = self
            .backend
            .get_range(queue, file_id, 0, HEAD_LEN as u64)
            .await?
        else {
            log::debug!(target: "normfs-store",
                "No store file {} for queue {} in {:?}", file_id, queue, self.source());
            return Ok(None);
        };

        let (file_auth, auth_size) = FileAuthentication::from_bytes(&head)?;
        let after_auth = &head[auth_size..];
        let (header, header_size) = AnyStoreHeader::from_bytes(after_auth)?;

        if let Some(crypto) = &self.verify_headers {
            crypto
                .verify(&after_auth[..header_size], &file_auth.header_signature)
                .map_err(|_| LayerError::SignatureVerificationFailed)?;
        }

        if header.num_entries().is_zero() {
            return Ok(None);
        }
        let first = header.num_entries_before();
        let last = first.add(&header.num_entries().sub(&UintN::one())?);
        Ok(Some((first, last)))
    }

    pub fn record_range(&self, queue: &QueueId, file_id: &UintN, first: &UintN, last: &UintN) {
        self.ranges
            .write()
            .unwrap()
            .insert(Self::key(queue, file_id), (first.clone(), last.clone()));
    }

    pub fn forget(&self, queue: &QueueId, file_id: &UintN) {
        self.ranges
            .write()
            .unwrap()
            .remove(&Self::key(queue, file_id));
    }

    /// The first entry id of the queue's oldest file here.
    pub async fn get_queue_start(&self, queue: &QueueId) -> Result<Option<UintN>, LayerError> {
        match self.first_file_id(queue).await? {
            Some(first) => Ok(self.get_file_range(queue, &first).await?.map(|r| r.0)),
            None => Ok(None),
        }
    }
}
