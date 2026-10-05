use bytes::{Bytes, BytesMut};
use normfs_store::backend::{Backend, BackendError, BackendFuture, Body, End};
use normfs_types::{DataSource, QueueId};
use std::sync::Arc;
use uintn::UintN;

use crate::client::S3Client;
use crate::errors::CloudError;
use crate::paths;

/// The bucket as a [`Backend`]. A put is a PUT of the whole key and a
/// HEAD that reads its size back; S3 and its lookalikes are read-after-write
/// consistent for a new key, so the HEAD is the verification and nothing has
/// to wait for it. Appending, deleting and listing answer `Unsupported`.
pub struct S3Store {
    client: Arc<S3Client>,
    prefix: String,
}

impl S3Store {
    pub fn new(client: Arc<S3Client>, prefix: &str) -> Self {
        let prefix = if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        };
        Self { client, prefix }
    }

    pub fn client(&self) -> &Arc<S3Client> {
        &self.client
    }

    async fn verify(&self, key: &str, local: u64, status: u16) -> Result<(), BackendError> {
        if status != 200 {
            return Err(BackendError::Status(status));
        }
        let remote = self
            .client
            .head_object(key)
            .await?
            .ok_or(BackendError::Missing)?;
        if remote != local {
            return Err(BackendError::SizeMismatch { local, remote });
        }
        Ok(())
    }
}

impl From<CloudError> for BackendError {
    fn from(e: CloudError) -> Self {
        match e {
            CloudError::InvalidStatusCode(code) => BackendError::Status(code),
            CloudError::Io(e) => BackendError::Io(e),
            other => BackendError::Remote(Box::new(other)),
        }
    }
}

impl Backend for S3Store {
    fn source(&self) -> DataSource {
        DataSource::Cloud
    }

    fn key(&self, queue: &QueueId, file_id: &UintN) -> String {
        queue.to_cloud_key(&self.prefix, file_id)
    }

    fn put<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        body: Body,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let key = self.key(queue, file_id);
            let len = body.len();
            let status = match body {
                Body::Runs(runs) => {
                    // One run is sent as is; a file already built in one
                    // buffer is not copied again.
                    let data = if runs.len() == 1 {
                        runs.into_iter().next().unwrap_or_default()
                    } else {
                        let mut out = BytesMut::with_capacity(len as usize);
                        runs.iter().for_each(|r| out.extend_from_slice(r));
                        out.freeze()
                    };
                    self.client.put_object(&key, data).await?
                }
                Body::Stream { file, len } => {
                    self.client.put_object_stream(&key, file, len).await?
                }
            };
            self.verify(&key, len, status).await
        })
    }

    fn get<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Bytes>> {
        Box::pin(async move { Ok(self.client.get_object(&self.key(queue, file_id)).await?) })
    }

    fn body<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<Body>> {
        Box::pin(async move {
            let data = self.client.get_object(&self.key(queue, file_id)).await?;
            Ok(data.map(|data| Body::Runs(vec![data])))
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
            if len == 0 {
                return Ok(Some(Bytes::new()));
            }
            let key = self.key(queue, file_id);
            let end = offset + len - 1;
            Ok(self
                .client
                .get_object_range(&key, offset, Some(end))
                .await?)
        })
    }

    fn size<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<u64>> {
        Box::pin(async move { Ok(self.client.head_object(&self.key(queue, file_id)).await?) })
    }

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>> {
        Box::pin(async move {
            let prefix = queue.to_cloud_queue_path(&self.prefix);
            let found = match end {
                End::Min => paths::find_min_id(&self.client, &prefix, "store").await,
                End::Max => paths::find_max_id(&self.client, &prefix, "store").await,
            };
            match found {
                Ok(id) => Ok(Some(id)),
                Err(CloudError::NoFilesFound) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
    }
}
