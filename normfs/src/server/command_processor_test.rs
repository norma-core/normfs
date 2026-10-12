use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use bytes::Bytes;
use normfs_types::{DataSource, ReadEntry};
use tokio::sync::{mpsc, Semaphore};
use uintn::UintN;

use super::command_processor::{encoded_with, worth_compressing, MAX_WIRE_ENTRY, PEAK};
use crate::proto::Encoding;
use crate::WireCompression;

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
        reads.push(encoded_with(
            rx,
            Some(WireCompression::default()),
            slots.clone(),
        ));
    }
    for mut out in reads {
        for i in 0..ENTRIES {
            let (entry, (data, encoding, size)) = out.recv().await.unwrap();
            assert_eq!(entry.id, UintN::from(i));
            assert_eq!(encoding, Encoding::EncZstd);
            assert_eq!(normfs_store::zstd_decompress(&data).unwrap(), record(i));
            assert_eq!(size as usize, record(i).len());
        }
    }
    let peak = PEAK.load(SeqCst);
    assert!(
        peak <= SLOTS,
        "{peak} compressions ran at once, the cap is {SLOTS}"
    );
}

#[test]
fn an_entry_above_the_client_limit_goes_raw() {
    let wire = WireCompression::default();
    assert!(worth_compressing(MAX_WIRE_ENTRY, &wire));
    assert!(!worth_compressing(MAX_WIRE_ENTRY + 1, &wire));
    assert!(!worth_compressing(wire.min_size - 1, &wire));
}
