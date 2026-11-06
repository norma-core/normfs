use std::future::{poll_fn, Future};
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use normfs_wal::{WalEntryV1, WalHeaderV1};
use tokio::sync::mpsc;
use uintn::UintN;

use super::super::cold::Cold;
use super::*;
use crate::{NormFS, NormFsSettings};

#[tokio::test]
async fn a_reader_arriving_during_a_pause_gets_the_cold_slot_promptly() {
    let temp = tempfile::TempDir::new().unwrap();
    let settings = NormFsSettings {
        system_queue: false,
        max_memory_usage: 1024 * 1024,
        cold_read_files: 1,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let fsm = fs.reader_fsm.clone();
    let queue = fs.resolve("paused");
    let mut content = BytesMut::new();
    WalHeaderV1::new(8, 8, 0)
        .unwrap()
        .write_to_bytes(&mut content)
        .unwrap();
    for _ in 0..5 {
        WalEntryV1::new(b"record")
            .write_to_bytes(&mut content)
            .unwrap();
    }
    let content = content.freeze();
    let loaded = fsm
        .cold
        .get((queue.clone(), UintN::from(1u64), false), true, || {
            let content = content.clone();
            async move { Ok::<_, Error>(Some(content)) }
        })
        .await
        .unwrap();
    let Cold::Found(held) = loaded else {
        panic!("file did not load")
    };
    let (tx, rx) = mpsc::channel(1);
    let ctx = ReadContext::new(
        queue,
        UintN::from(1u64),
        UintN::from(1u64),
        2,
        Some(UintN::from(5u64)),
        tx,
    );
    let mut paused = Box::pin(fsm.handle_parse_wal_bytes(ctx, held, DataSource::DiskStore, None));
    poll_fn(|cx| {
        assert!(paused.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(rx.len(), 1);
    let started = Instant::now();
    let waiter = fsm.cold.get(
        (fs.resolve("other"), UintN::from(1u64), false),
        true,
        || async { Ok::<_, Error>(Some(Bytes::from_static(b"other file"))) },
    );
    let result = tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(3), waiter) => result,
        state = &mut paused => panic!("paused reader unexpectedly resumed: {state:?}"),
    };
    let elapsed = started.elapsed();
    assert!(
        matches!(result, Ok(Ok(Cold::Found(_)))),
        "waiting load never finished"
    );
    eprintln!("late cold-slot waiter took {elapsed:?}");
    drop(paused);
    drop(rx);
    fs.close().await.unwrap();
    assert!(
        elapsed < Duration::from_millis(200),
        "late waiter blocked for {elapsed:?}"
    );
}
