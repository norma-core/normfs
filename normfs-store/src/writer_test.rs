use crate::{Packer, PersistStore, StoreWriteConfig};
use bytes::BytesMut;
use normfs_crypto::CryptoContext;
use normfs_types::{CompressionType, EncryptionType, QueueId, QueueIdResolver};
use normfs_wal::{
    AnyWalHeader, WAL_HEADER_V1_MAX_SIZE, WalEntryV1, WalFile, WalHeader, WalHeaderV1, WalStore,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uintn::UintN;

const BEFORE: u64 = 10;

struct Fixture {
    _dir: tempfile::TempDir,
    store: PersistStore,
    queue: QueueId,
}

fn fixture(packer: Option<Arc<Packer>>) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let crypto = Arc::new(CryptoContext::open(dir.path()).unwrap());
    let (wal_tx, _) = mpsc::unbounded_channel();
    let (done_tx, _) = mpsc::unbounded_channel();
    let wal = Arc::new(WalStore::new(dir.path(), wal_tx, done_tx));
    let (written_tx, _) = mpsc::unbounded_channel();
    let mut store = PersistStore::new(
        dir.path(),
        StoreWriteConfig {
            num_workers: 1,
            verify_signatures: true,
        },
        crypto.clone(),
        wal,
        normfs_fs::Fs::new(normfs_fs::FsConfig::default()).unwrap(),
        written_tx,
    );
    if let Some(packer) = packer {
        store = store.with_wal_packer(packer);
    }
    let queue = QueueIdResolver::new(crypto.instance_id_hex()).resolve("wal");
    Fixture {
        _dir: dir,
        store,
        queue,
    }
}

fn records() -> Vec<Vec<u8>> {
    (0..5u8).map(|i| vec![i; 100 + i as usize]).collect()
}

/// A V1 WAL file of [`records`], ending in a record cut short by a crash.
fn wal_file() -> Vec<u8> {
    let header = WalHeader {
        num_entries_before: UintN::from(BEFORE),
        ..WalHeader::default()
    };
    let mut out = BytesMut::new();
    WalHeaderV1::from_v0(&header)
        .unwrap()
        .write_to_bytes(&mut out)
        .unwrap();
    for record in records() {
        WalEntryV1::new(&record).write_to_bytes(&mut out).unwrap();
    }
    let mut torn = BytesMut::new();
    WalEntryV1::new(&[0xEE; 64])
        .write_to_bytes(&mut torn)
        .unwrap();
    out.extend_from_slice(&torn[..20]);
    out.to_vec()
}

/// Migrates file 1 to the store and reads back what landed.
async fn migrate(f: &Fixture, file: &[u8]) -> (u64, Vec<Vec<u8>>) {
    let file_id = UintN::one();
    let path = f.queue.to_wal_path(f._dir.path(), &file_id);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, file).unwrap();

    let (tx, rx) = mpsc::unbounded_channel();
    let mut done = f.store.start_writers(rx).await;
    tx.send(WalFile {
        queue_id: f.queue.clone(),
        file_id: file_id.clone(),
        encryption_type: EncryptionType::Aes,
        compression_type: CompressionType::Zstd,
    })
    .unwrap();
    let (queue, landed) = tokio::time::timeout(Duration::from_secs(10), done.recv())
        .await
        .expect("landed within ten seconds")
        .unwrap();
    assert_eq!((queue, landed), (f.queue.clone(), file_id.clone()));

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
    assert_eq!(
        &wal[..],
        file,
        "the store file holds the WAL file as it was"
    );

    let (header, mut at) = AnyWalHeader::from_bytes(&wal).unwrap();
    let mut got = Vec::new();
    while let Ok((entry, used)) = WalEntryV1::from_bytes(&wal[at..]) {
        got.push(entry.record.to_vec());
        at += used;
    }
    let before = header.num_entries_before().to_u64().unwrap();
    (before, got)
}

fn packer(input_cap: usize) -> Arc<Packer> {
    Arc::new(Packer::new(1, input_cap).unwrap())
}

#[tokio::test]
async fn a_wal_file_is_packed_in_a_slot_and_gives_it_back() {
    let file = wal_file();
    let packer = packer(WAL_HEADER_V1_MAX_SIZE + file.len());
    let f = fixture(Some(packer.clone()));
    assert_eq!(migrate(&f, &file).await, (BEFORE, records()));
    tokio::time::timeout(Duration::from_secs(1), packer.take())
        .await
        .expect("the slot is free once the file has landed");
}

#[tokio::test]
async fn a_wal_file_larger_than_a_slot_is_read_whole() {
    let file = wal_file();
    let f = fixture(Some(packer(file.len() / 2)));
    assert_eq!(migrate(&f, &file).await, (BEFORE, records()));
}

#[tokio::test]
async fn without_slots_a_wal_file_is_read_whole() {
    let file = wal_file();
    let f = fixture(None);
    assert_eq!(migrate(&f, &file).await, (BEFORE, records()));
}
