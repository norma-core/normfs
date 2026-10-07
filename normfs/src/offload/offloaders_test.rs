use super::Offloaders;
use crate::offload::disk_monitor::{DiskMonitor, DiskMonitorConfig};
use bytes::Bytes;
use normfs_store::{local_store, Backend, Body, DiskUsage, End, Layer, Local};
use normfs_types::events;
use normfs_types::{DataSource, QueueId, QueueIdResolver};
use normfs_wal::BackendFuture;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use uintn::UintN;

/// A bucket whose puts for one queue never complete.
struct Stuck {
    queue: String,
    files: Mutex<BTreeMap<(String, UintN), Bytes>>,
}

impl Backend for Stuck {
    fn source(&self) -> DataSource {
        DataSource::Cloud
    }

    fn key(&self, queue: &QueueId, file_id: &UintN) -> String {
        format!("{queue}/{file_id}")
    }

    fn put<'a>(&'a self, q: &'a QueueId, id: &'a UintN, body: Body) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if q.to_string() == self.queue {
                std::future::pending::<()>().await;
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

    fn find<'a>(&'a self, _q: &'a QueueId, _end: End) -> BackendFuture<'a, Option<UintN>> {
        Box::pin(async move { Ok(None) })
    }
}

#[tokio::test]
async fn a_queue_stuck_on_its_next_layer_holds_up_no_other() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let local = Arc::new(local_store(fs.clone(), temp.path(), false, Arc::default()));
    let resolver = QueueIdResolver::new("inst");
    let (stuck, free) = (resolver.resolve("stuck"), resolver.resolve("free"));
    let one = UintN::from(1u64);
    for queue in [&stuck, &free] {
        local
            .put(
                queue,
                &one,
                Body::Runs(vec![Bytes::from_static(b"a store file")]),
            )
            .await
            .unwrap();
    }
    let bucket = Arc::new(Stuck {
        queue: stuck.to_string(),
        files: Mutex::default(),
    });
    let offloaders = Offloaders::new(
        Arc::new(Layer::new(local, None, false)),
        Arc::new(Layer::new(bucket.clone(), None, false)),
        Arc::new(Local::wal(fs, temp.path())),
        events::discard(),
    );
    offloaders.start(&stuck).await;
    offloaders.start(&free).await;

    tokio::time::timeout(Duration::from_secs(10), async {
        for id in 1..=2000u64 {
            offloaders.file_landed(&stuck, UintN::from(id)).await;
        }
        offloaders.file_landed(&free, one.clone()).await;
        while bucket.get(&free, &one).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the free queue's file reached the bucket");
}

#[tokio::test]
async fn eviction_keeps_a_wal_file_a_later_landing_overtook() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let local = Arc::new(local_store(fs.clone(), root, false, Arc::default()));
    // File 1 is still being migrated out of the WAL when file 2 lands.
    let wal_one = queue.to_wal_path(root, &UintN::from(1u64));
    std::fs::create_dir_all(wal_one.parent().unwrap()).unwrap();
    std::fs::write(&wal_one, vec![0u8; 100]).unwrap();
    let two = UintN::from(2u64);
    local
        .put(&queue, &two, Body::Runs(vec![Bytes::from(vec![0u8; 100])]))
        .await
        .unwrap();

    let bucket = Arc::new(Stuck {
        queue: "none".to_string(),
        files: Mutex::default(),
    });
    let offloaders = Offloaders::new(
        Arc::new(Layer::new(local, None, false)),
        Arc::new(Layer::new(bucket.clone(), None, false)),
        Arc::new(Local::wal(fs.clone(), root)),
        events::discard(),
    );
    let offloader = offloaders.start(&queue).await;
    offloaders.file_landed(&queue, two.clone()).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while bucket.get(&queue, &two).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("file 2 reached the bucket");

    let monitor = DiskMonitor::new(
        fs,
        root,
        None,
        Arc::new(DiskUsage::default()),
        events::discard(),
    )
    .await
    .unwrap();
    let config = DiskMonitorConfig {
        max_size: 50,
        check_interval: Duration::from_secs(60),
        wal_settings: normfs_wal::WalSettings {
            max_file_size: 10,
            ..Default::default()
        },
        offload: true,
    };
    monitor
        .add_queue(&queue, config, Some(offloader))
        .await
        .unwrap();
    assert!(
        wal_one.exists(),
        "file 1 is in neither the store nor the bucket"
    );
}

#[tokio::test]
async fn empty_wal_files_below_moved_files_do_not_stop_eviction() {
    let temp = tempfile::TempDir::new().unwrap();
    let root = temp.path();
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("inst").resolve("cam");
    let local = Arc::new(local_store(fs.clone(), root, false, Arc::default()));
    for id in [1u64, 4] {
        local
            .put(
                &queue,
                &UintN::from(id),
                Body::Runs(vec![Bytes::from(vec![0u8; 100])]),
            )
            .await
            .unwrap();
    }
    // As recovery leaves them when it reuses the latest of several empty files.
    for id in [2u64, 3] {
        let path = queue.to_wal_path(root, &UintN::from(id));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"").unwrap();
    }

    let bucket = Arc::new(Stuck {
        queue: "none".to_string(),
        files: Mutex::default(),
    });
    let offloaders = Offloaders::new(
        Arc::new(Layer::new(local, None, false)),
        Arc::new(Layer::new(bucket.clone(), None, false)),
        Arc::new(Local::wal(fs.clone(), root)),
        events::discard(),
    );
    let offloader = offloaders.start(&queue).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while offloader.get_latest_offloaded_id().await != Some(UintN::from(4u64)) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the bound passed the empty WAL files");

    let monitor = DiskMonitor::new(
        fs,
        root,
        None,
        Arc::new(DiskUsage::default()),
        events::discard(),
    )
    .await
    .unwrap();
    let config = DiskMonitorConfig {
        max_size: 50,
        check_interval: Duration::from_secs(60),
        wal_settings: normfs_wal::WalSettings {
            max_file_size: 10,
            ..Default::default()
        },
        offload: true,
    };
    monitor
        .add_queue(&queue, config, Some(offloader))
        .await
        .unwrap();
    assert!(!queue.to_store_path(root, &UintN::from(4u64)).exists());
}
