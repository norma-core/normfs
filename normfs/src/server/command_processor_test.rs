use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use bytes::Bytes;
use normfs_types::{DataSource, ReadEntry};
use tokio::sync::{mpsc, Semaphore};
use uintn::UintN;

use super::command_processor::{encoded_with, PEAK};
use crate::proto::Encoding;

fn record(i: u64) -> Bytes {
    let mut out = Vec::new();
    for pid in 0..2000 {
        out.extend(format!("{i} pid={pid} user=root status=sleeping\n").bytes());
    }
    Bytes::from(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_compressed_reads_share_one_cap_and_keep_order() {
    const READS: u64 = 30;
    const ENTRIES: u64 = 20;
    const SLOTS: usize = 3;
    let slots = Arc::new(Semaphore::new(SLOTS));
    PEAK.store(0, SeqCst);

    let mut reads = Vec::new();
    for _ in 0..READS {
        let (tx, rx) = mpsc::channel(2048);
        for i in 0..ENTRIES {
            let entry = ReadEntry::new(UintN::from(i), record(i), DataSource::Memory);
            tx.send(entry).await.unwrap();
        }
        drop(tx);
        reads.push(encoded_with(rx, true, slots.clone()));
    }
    for mut out in reads {
        for i in 0..ENTRIES {
            let (entry, (data, encoding)) = out.recv().await.unwrap();
            assert_eq!(entry.id, UintN::from(i));
            assert_eq!(encoding, Encoding::EncZstd);
            assert_eq!(normfs_store::zstd_decompress(&data).unwrap(), record(i));
        }
    }
    let peak = PEAK.load(SeqCst);
    assert!(
        peak <= SLOTS,
        "{peak} compressions ran at once, the cap is {SLOTS}"
    );
}

#[tokio::test]
async fn a_read_waiting_for_a_slot_stops_once_its_client_is_gone() {
    let slots = Arc::new(Semaphore::new(0));
    let (tx, rx) = mpsc::channel(16);
    tx.send(ReadEntry::new(
        UintN::from(0u64),
        record(0),
        DataSource::Memory,
    ))
    .await
    .unwrap();
    let out = encoded_with(rx, true, slots);
    drop(out);
    tokio::time::timeout(std::time::Duration::from_secs(2), tx.closed())
        .await
        .expect("the read kept waiting for a slot after its client went");
}
