use crate::backend::{AppendTarget, Appended, Backend, BackendFuture, Body, Fill, Local, Reader};
use crate::{PackSlot, WalHeader, WalSettings, WalStore};
use bytes::Bytes;
use normfs_types::{DataSource, End, QueueId, QueueIdResolver};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use uintn::UintN;

/// A backend that counts appends and hands everything else to the local one.
struct Counting {
    inner: Local,
    appends: Arc<AtomicU64>,
}

struct CountingTarget {
    inner: Arc<dyn AppendTarget>,
    appends: Arc<AtomicU64>,
}

impl AppendTarget for CountingTarget {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn append(&self, at: u64, runs: Vec<Bytes>, sync: bool) -> BackendFuture<'_, Appended> {
        self.appends.fetch_add(1, Ordering::Relaxed);
        self.inner.append(at, runs, sync)
    }

    fn restore(&self, at: u64) -> BackendFuture<'_, ()> {
        self.inner.restore(at)
    }

    fn size(&self) -> BackendFuture<'_, u64> {
        self.inner.size()
    }
}

impl Backend for Counting {
    fn source(&self) -> DataSource {
        self.inner.source()
    }

    fn key(&self, queue: &QueueId, file_id: &UintN) -> String {
        self.inner.key(queue, file_id)
    }

    fn put<'a>(&'a self, q: &'a QueueId, id: &'a UintN, body: Body) -> BackendFuture<'a, ()> {
        self.inner.put(q, id, body)
    }

    fn get<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<Bytes>> {
        self.inner.get(q, id)
    }

    fn body<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<Body>> {
        self.inner.body(q, id)
    }

    fn get_range<'a>(
        &'a self,
        q: &'a QueueId,
        id: &'a UintN,
        offset: u64,
        len: u64,
    ) -> BackendFuture<'a, Option<Bytes>> {
        self.inner.get_range(q, id, offset, len)
    }

    fn size<'a>(&'a self, q: &'a QueueId, id: &'a UintN) -> BackendFuture<'a, Option<u64>> {
        self.inner.size(q, id)
    }

    fn find<'a>(&'a self, queue: &'a QueueId, end: End) -> BackendFuture<'a, Option<UintN>> {
        self.inner.find(queue, end)
    }

    fn list<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, Vec<UintN>> {
        self.inner.list(queue)
    }

    fn delete<'a>(&'a self, queue: &'a QueueId, file_id: &'a UintN) -> BackendFuture<'a, ()> {
        self.inner.delete(queue, file_id)
    }

    fn clear<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, ()> {
        self.inner.clear(queue)
    }

    fn prepare<'a>(&'a self, queue: &'a QueueId) -> BackendFuture<'a, ()> {
        self.inner.prepare(queue)
    }

    fn create<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        header: Bytes,
        sync: bool,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        Box::pin(async move {
            let inner = self.inner.create(queue, file_id, header, sync).await?;
            let target: Arc<dyn AppendTarget> = Arc::new(CountingTarget {
                inner,
                appends: self.appends.clone(),
            });
            Ok(target)
        })
    }

    fn reopen<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Arc<dyn AppendTarget>> {
        self.inner.reopen(queue, file_id)
    }

    fn open_read<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
    ) -> BackendFuture<'a, Option<(Reader, u64)>> {
        self.inner.open_read(queue, file_id)
    }

    fn read_into<'a>(
        &'a self,
        queue: &'a QueueId,
        file_id: &'a UintN,
        slot: PackSlot,
        cap: usize,
    ) -> BackendFuture<'a, (PackSlot, Fill)> {
        self.inner.read_into(queue, file_id, slot, cap)
    }
}

#[tokio::test]
async fn a_wal_store_writes_batches_through_its_backend() {
    let dir = tempfile::tempdir().unwrap();
    let fs = normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap();
    let appends = Arc::new(AtomicU64::new(0));
    let backend = Arc::new(Counting {
        inner: Local::wal(fs, dir.path()),
        appends: appends.clone(),
    });
    let (written_tx, _) = mpsc::unbounded_channel();
    let (complete_tx, _) = mpsc::unbounded_channel();
    let store = WalStore::with_backend(backend, written_tx, complete_tx);

    let queue = QueueIdResolver::new("inst").resolve("cam");
    let file_id = UintN::from(1u64);
    let settings = WalSettings {
        write_interval: std::time::Duration::from_millis(50),
        ..Default::default()
    };
    store
        .start_writer(&queue, &file_id, WalHeader::default(), settings, None)
        .await
        .unwrap();
    for id in 0..1000u64 {
        store
            .enqueue(
                &queue,
                UintN::from(id),
                Bytes::from(id.to_le_bytes().to_vec()),
            )
            .unwrap();
    }
    store.close().await.unwrap();

    let appended = appends.load(Ordering::Relaxed);
    assert!(
        appended > 0 && appended < 100,
        "{appended} appends for 1000 records"
    );

    let (tx, mut rx) = mpsc::channel(1001);
    store
        .read_wal_range(
            &queue,
            &file_id,
            &UintN::zero(),
            &None,
            1,
            &tx,
            DataSource::DiskWal,
        )
        .await
        .unwrap();
    drop(tx);
    let mut read = 0u64;
    while let Some(entry) = rx.recv().await {
        assert_eq!(entry.data, Bytes::from(read.to_le_bytes().to_vec()));
        read += 1;
    }
    assert_eq!(read, 1000);
}
