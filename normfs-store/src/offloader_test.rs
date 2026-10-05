use crate::DiskUsage;
use crate::backend::{BackendError, BackendFuture, Body, End, LocalStore, StoreBackend};
use crate::layer::Layer;
use crate::offloader::QueueOffloader;
use bytes::Bytes;
use normfs_types::events::{self, SystemEvent, SystemEvents, UploadFailure};
use normfs_types::{DataSource, QueueId, QueueIdResolver};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use uintn::UintN;

/// A bucket in memory that turns the first `refuse` puts away.
#[derive(Default)]
struct Memory {
    files: Mutex<BTreeMap<(String, UintN), Bytes>>,
    refuse: Mutex<u32>,
}

impl StoreBackend for Memory {
    fn source(&self) -> DataSource {
        DataSource::Cloud
    }

    fn key(&self, queue: &QueueId, file_id: &UintN) -> String {
        format!("{queue}/{file_id}")
    }

    fn put<'a>(&'a self, q: &'a QueueId, id: &'a UintN, body: Body) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            {
                let mut refuse = self.refuse.lock().unwrap();
                if *refuse > 0 {
                    *refuse -= 1;
                    return Err(BackendError::Status(503));
                }
            }
            let data = match body {
                Body::Runs(runs) => runs.concat().into(),
                Body::Stream { mut file, .. } => {
                    let mut buf = Vec::new();
                    file.read_to_end(&mut buf).await?;
                    Bytes::from(buf)
                }
            };
            self.files
                .lock()
                .unwrap()
                .insert((q.to_string(), id.clone()), data);
            Ok(())
        })
    }

    fn get<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<Bytes>> {
        let found = self
            .files
            .lock()
            .unwrap()
            .get(&(q.to_string(), id.clone()))
            .cloned();
        Box::pin(async move { Ok(found) })
    }

    fn body<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<Body>> {
        Box::pin(async move { Ok(self.get(q, id).await?.map(|b| Body::Runs(vec![b]))) })
    }

    fn get_range<'a>(
        &'a self,
        q: &'a QueueId,
        id: &'a UintN,
        offset: u64,
        len: u64,
    ) -> BackendFuture<'a, Option<Bytes>> {
        Box::pin(async move {
            Ok(self.get(q, id).await?.map(|b| {
                let start = (offset as usize).min(b.len());
                b.slice(start..(start + len as usize).min(b.len()))
            }))
        })
    }

    fn size<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<u64>> {
        Box::pin(async move { Ok(self.get(q, id).await?.map(|b| b.len() as u64)) })
    }

    fn find<'a>(&'a self, q: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>> {
        let files = self.files.lock().unwrap();
        let mut ids = files
            .keys()
            .filter(|(k, _)| *k == q.to_string())
            .map(|(_, id)| id);
        let found = match end {
            End::Min => ids.next().cloned(),
            End::Max => ids.next_back().cloned(),
        };
        Box::pin(async move { Ok(found) })
    }
}

#[derive(Default)]
struct Recorded(Mutex<Vec<SystemEvent>>);

impl SystemEvents for Recorded {
    fn emit(&self, event: SystemEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn a_file_moves_to_the_next_layer_once_it_accepts_it() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let usage = Arc::new(DiskUsage::default());
    let local: Arc<dyn StoreBackend> = Arc::new(LocalStore::new(fs, temp.path(), false, usage));
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let file_id = UintN::from(1u64);
    let data = Bytes::from_static(b"a store file");
    local
        .put(&queue, &file_id, Body::Runs(vec![data.clone()]))
        .await
        .unwrap();

    let remote = Arc::new(Memory::default());
    *remote.refuse.lock().unwrap() = 1;
    let recorded = Arc::new(Recorded::default());
    let events: events::EventSink = recorded.clone();
    let offloader = QueueOffloader::new(
        Arc::new(Layer::new(local, None, false)),
        Arc::new(Layer::new(remote.clone(), None, true)),
        queue.clone(),
        events,
    )
    .await;

    for _ in 0..500 {
        if offloader.get_latest_offloaded_id().await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        offloader.get_latest_offloaded_id().await,
        Some(file_id.clone())
    );
    assert_eq!(remote.get(&queue, &file_id).await.unwrap(), Some(data));

    let recorded = recorded.0.lock().unwrap();
    assert!(matches!(
        recorded.first(),
        Some(SystemEvent::UploadFailed {
            failure: UploadFailure::Status(503),
            ..
        })
    ));
}
