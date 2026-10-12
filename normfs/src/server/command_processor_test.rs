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
            let data = match encoding {
                Encoding::EncZstd => Bytes::from(normfs_store::zstd_decompress(&data).unwrap()),
                Encoding::EncNone => data,
            };
            assert_eq!(data, record(i));
        }
    }
    let peak = PEAK.load(SeqCst);
    assert!(
        peak <= SLOTS,
        "{peak} compressions ran at once, the cap is {SLOTS}"
    );
}

#[tokio::test]
async fn a_follow_keeps_draining_while_every_slot_is_taken() {
    const ENTRIES: u64 = 2100;
    let slots = Arc::new(Semaphore::new(0));
    let (tx, rx) = mpsc::channel(2048);
    let mut out = encoded_with(rx, true, slots);
    let reader = tokio::spawn(async move {
        let mut raw = 0;
        while let Some((_, (_, encoding))) = out.recv().await {
            assert_eq!(encoding, Encoding::EncNone);
            raw += 1;
        }
        raw
    });
    for i in 0..ENTRIES {
        let entry = ReadEntry::new(
            UintN::from(i),
            Bytes::from(vec![7u8; 2048]),
            DataSource::Memory,
        );
        tx.try_send(entry)
            .unwrap_or_else(|_| panic!("the follow's channel filled at entry {i}"));
        tokio::task::yield_now().await;
    }
    drop(tx);
    assert_eq!(reader.await.unwrap(), ENTRIES);
}
