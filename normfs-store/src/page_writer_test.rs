use crate::header::{CompressionType, EncryptionType};
use crate::page_writer::{PageStoreWriter, PageWriterSettings};
use crate::sink::SealedFileSink;
use crate::store_file::SealedFile;
use crate::{PersistStore, StoreWriteConfig};
use normfs_crypto::CryptoContext;
use normfs_types::{QueueId, QueueIdResolver};
use normfs_wal::{AnyWalHeader, PagePool, WalHeader, WalStore};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use uintn::UintN;

const PAGE_SIZE: usize = 64;
const RECORD: [u8; 16] = [0xC3; 16];

struct Fixture {
    _dir: tempfile::TempDir,
    store: Arc<PersistStore>,
    crypto: Arc<CryptoContext>,
    queue: QueueId,
    pool: Arc<PagePool>,
    written_rx: mpsc::UnboundedReceiver<(QueueId, UintN)>,
}

fn fixture(pages: usize) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let crypto = Arc::new(CryptoContext::open(dir.path()).unwrap());
    let (wal_tx, _) = mpsc::unbounded_channel();
    let (done_tx, _) = mpsc::unbounded_channel();
    let wal = Arc::new(WalStore::new(dir.path(), wal_tx, done_tx));
    let (written_tx, written_rx) = mpsc::unbounded_channel();
    let store = Arc::new(PersistStore::new(
        dir.path(),
        StoreWriteConfig {
            num_workers: 1,
            verify_signatures: true,
        },
        crypto.clone(),
        wal,
        written_tx,
    ));
    let queue = QueueIdResolver::new(crypto.instance_id_hex()).resolve("pages");
    Fixture {
        _dir: dir,
        store,
        crypto,
        queue,
        pool: Arc::new(PagePool::new(pages, PAGE_SIZE, 0)),
        written_rx,
    }
}

fn settings(close_max_attempts: u32) -> PageWriterSettings {
    PageWriterSettings {
        compression: CompressionType::Zstd,
        encryption: EncryptionType::Aes,
        retry_delay: Duration::from_millis(1),
        close_max_attempts,
    }
}

/// Lands only when the test hands it a permit, and remembers what it landed.
struct GatedSink {
    permits: Semaphore,
    landed: Mutex<Vec<(UintN, Option<UintN>)>>,
}

impl GatedSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            permits: Semaphore::new(0),
            landed: Mutex::new(Vec::new()),
        })
    }

    fn landed(&self) -> Vec<(UintN, Option<UintN>)> {
        self.landed.lock().unwrap().clone()
    }
}

impl SealedFileSink for GatedSink {
    fn land<'a>(
        &'a self,
        _queue: &'a QueueId,
        file_id: &'a UintN,
        file: &'a SealedFile,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            match self.permits.try_acquire() {
                Ok(permit) => permit.forget(),
                Err(_) => return Err(std::io::Error::other("sink held")),
            }
            self.landed
                .lock()
                .unwrap()
                .push((file_id.clone(), file.last_entry_id()));
            Ok(())
        })
    }
}

fn start(f: &Fixture, sink: Arc<dyn SealedFileSink>, close_max_attempts: u32) -> PageStoreWriter {
    PageStoreWriter::start(
        &f.queue,
        &UintN::one(),
        WalHeader::default(),
        settings(close_max_attempts),
        f.pool.clone(),
        sink,
        f.crypto.clone(),
        f.store.written_sender_for_tests(),
    )
}

async fn settle() {
    tokio::time::sleep(Duration::from_millis(30)).await;
}

async fn next_id(rx: &mut mpsc::UnboundedReceiver<(QueueId, UintN)>) -> UintN {
    tokio::time::timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("announced within a second")
        .expect("channel open")
        .1
}

#[tokio::test]
async fn nothing_is_reported_durable_before_the_sink_returns() {
    let mut f = fixture(2);
    let sink = GatedSink::new();
    let _writer = start(&f, sink.clone(), 1);

    // Two records fill page 0; the third opens page 1 and closes file 0.
    for i in 0..3u64 {
        f.pool.place(i, &RECORD).await.unwrap();
    }
    settle().await;
    assert!(
        sink.landed().is_empty(),
        "the sink was never given a permit"
    );
    assert_eq!(f.pool.durable_before(), 0);
    assert!(
        f.written_rx.try_recv().is_err(),
        "no ack before the file is safe"
    );

    sink.permits.add_permits(1);
    settle().await;
    assert_eq!(sink.landed(), vec![(UintN::one(), Some(UintN::from(1u64)))]);
    assert_eq!(f.pool.durable_before(), 2);
    assert_eq!(next_id(&mut f.written_rx).await, UintN::from(1u64));
}

#[tokio::test]
async fn a_sink_that_does_not_land_is_back_pressure_not_loss() {
    let mut f = fixture(2);
    let sink = GatedSink::new();
    let _writer = start(&f, sink.clone(), 1);

    for i in 0..4u64 {
        f.pool.place(i, &RECORD).await.unwrap();
    }
    settle().await;
    // Both pages hold records nobody has landed, so the fifth has nowhere to go.
    assert!(f.pool.try_place_now(4, &RECORD).unwrap().is_none());

    sink.permits.add_permits(1);
    settle().await;
    assert!(f.pool.try_place_now(4, &RECORD).unwrap().is_some());
    assert_eq!(next_id(&mut f.written_rx).await, UintN::from(1u64));
}

#[tokio::test]
async fn close_finishes_while_a_reader_holds_landed_records() {
    let f = fixture(4);
    let sink = GatedSink::new();
    sink.permits.add_permits(2);
    let writer = start(&f, sink.clone(), 1);
    for id in 0..3 {
        f.pool.place(id, &RECORD).await.unwrap();
    }
    let records = f.pool.pin_range(0, 1);
    assert_eq!(records.len(), 2);
    assert!(writer.clone().close().await);
    assert_eq!(sink.landed().len(), 2);
    drop(records);
    assert!(writer.close().await);
    assert!(f.pool.is_fully_durable());
}

#[tokio::test]
async fn close_reports_a_file_that_did_not_land_and_keeps_trying() {
    let f = fixture(2);
    let sink = GatedSink::new();
    let writer = start(&f, sink.clone(), 3);

    f.pool.place(0, &RECORD).await.unwrap();
    assert!(!writer.close().await, "three attempts, no permit");
    assert!(!f.pool.is_fully_durable());

    sink.permits.add_permits(1);
    settle().await;
    assert_eq!(sink.landed().len(), 1);
    assert!(f.pool.is_fully_durable(), "the background retry landed it");
}

/// Reads a store file back through the same code every reader uses, and
/// returns `(num_entries_before, records)`.
async fn read_back(f: &Fixture, file_id: u64) -> (u64, Vec<Vec<u8>>) {
    let file_id = UintN::from(file_id);
    let bytes = f
        .store
        .get_store_bytes(&f.queue, &file_id)
        .await
        .unwrap()
        .expect("store file exists");
    let wal = f
        .store
        .extract_wal_bytes(&f.queue, &file_id, bytes, true)
        .unwrap();
    let (header, mut at) = AnyWalHeader::from_bytes(&wal).unwrap();
    let mut records = Vec::new();
    while at < wal.len() {
        let (entry, used) = normfs_wal::WalEntryV1::from_bytes(&wal[at..]).unwrap();
        records.push(entry.record.to_vec());
        at += used;
    }
    let before: u64 = header.num_entries_before().to_string().parse().unwrap();
    (before, records)
}

#[tokio::test]
async fn full_pages_flushes_and_close_each_land_one_store_file() {
    let mut f = fixture(4);
    let (_, wal_done_rx) = mpsc::unbounded_channel();
    let mut store_done_rx = f.store.start_writers(wal_done_rx).await;
    let sink = f.store.local_sink(true);
    let writer = start(&f, sink, 1);

    // File 1: page 0 fills, the third record opens page 1.
    for i in 0..3u64 {
        f.pool.place(i, &RECORD).await.unwrap();
    }
    settle().await;
    assert_eq!(next_id(&mut f.written_rx).await, UintN::from(1u64));
    assert_eq!(next_id(&mut store_done_rx).await, UintN::from(1u64));
    assert_eq!(read_back(&f, 1).await, (0, vec![RECORD.to_vec(); 2]));

    // File 2: the flush takes the open page's single record.
    assert!(writer.flush().await);
    assert_eq!(read_back(&f, 2).await, (2, vec![RECORD.to_vec()]));
    assert_eq!(
        f.store
            .get_file_range(&f.queue, &UintN::from(2u64))
            .await
            .unwrap(),
        Some((UintN::from(2u64), UintN::from(2u64)))
    );
    assert!(writer.flush().await, "a flush with nothing owed is a no-op");
    assert!(
        f.store
            .get_store_bytes(&f.queue, &UintN::from(3u64))
            .await
            .unwrap()
            .is_none()
    );

    // File 3: the close takes what came after the flush, on the same page.
    f.pool.place(3, &RECORD).await.unwrap();
    assert!(writer.close().await);
    assert_eq!(read_back(&f, 3).await, (3, vec![RECORD.to_vec()]));
    assert!(f.pool.is_fully_durable());
    assert!(!f.pool.has_drainer());
    assert!(
        !f.queue.to_wal_dir(f._dir.path()).exists(),
        "no .wal was ever written"
    );
}
