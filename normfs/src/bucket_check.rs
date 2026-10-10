use normfs_store::store_file::SealedFile;
use normfs_store::{Layer, SealedFileSink};
use normfs_types::QueueId;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use uintn::UintN;

use crate::memory_pointers::MemoryPointers;

/// The sink of a cloud-direct writer started while the bucket could not be
/// listed. Its ids are safe: they come after every id an upload was started
/// for. Its file ids are not until the bucket answers, since the upload a
/// previous life ended in may have landed unrecorded, so the first file waits
/// for a listing and goes after whatever the bucket holds.
pub(crate) struct BucketCheck {
    inner: Arc<dyn SealedFileSink>,
    cloud: Arc<Layer>,
    pointers: Arc<MemoryPointers>,
    last_id: Option<UintN>,
    /// The first id past the bucket's files, once it has answered. Kept, not
    /// just checked once: a file that fails to build leaves the writer's own
    /// id where it was.
    free_from: Mutex<Option<UintN>>,
}

impl BucketCheck {
    pub(crate) fn new(
        inner: Arc<dyn SealedFileSink>,
        cloud: Arc<Layer>,
        pointers: Arc<MemoryPointers>,
        last_id: Option<UintN>,
    ) -> Self {
        Self {
            inner,
            cloud,
            pointers,
            last_id,
            free_from: Mutex::new(None),
        }
    }

    async fn first_free(&self, queue: &QueueId, planned: &UintN) -> io::Result<UintN> {
        let last_file = self.cloud.last_file_id(queue).await?;
        let Some(last_file) = last_file.filter(|f| f >= planned) else {
            return Ok(planned.clone());
        };
        let (_, last) = self
            .cloud
            .get_file_range(queue, &last_file)
            .await
            .map_err(io::Error::other)?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("cloud file {last_file} has no recoverable range for queue {queue}"),
                )
            })?;
        if self.last_id.as_ref().is_none_or(|l| last > *l) {
            // Only when the pointer file was lost or written before uploads
            // were reserved; the objects are still not overwritten.
            log::error!(target: "normfs",
                "queue '{}': cloud file {last_file} holds ids up to {last}, past the {:?} \
                 recorded here; ids given out since the start repeat some of them",
                queue.short(), self.last_id);
        }
        self.pointers.mark_landed(queue, &last, &last_file).await?;
        log::info!(target: "normfs",
            "queue '{}': the bucket holds file {last_file}; writing from file {}",
            queue.short(), last_file.increment());
        Ok(last_file.increment())
    }
}

impl SealedFileSink for BucketCheck {
    fn land<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        self.inner.land(queue, file_id, file)
    }

    fn file_id<'a>(
        &'a self,
        queue: &'a QueueId,
        planned: &'a UintN,
    ) -> Pin<Box<dyn Future<Output = io::Result<UintN>> + Send + 'a>> {
        Box::pin(async move {
            let known = self.free_from.lock().unwrap().clone();
            let free_from = match known {
                Some(free_from) => free_from,
                None => {
                    let free_from = self.first_free(queue, planned).await?;
                    *self.free_from.lock().unwrap() = Some(free_from.clone());
                    free_from
                }
            };
            Ok(free_from.max(planned.clone()))
        })
    }
}
