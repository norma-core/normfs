use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition, UintN};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[tokio::test]
async fn concurrent_append_at_handoff_does_not_replay_disk_history() {
    const N: usize = 30_000;

    let dir = tempfile::TempDir::new().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.max_memory_usage = 16 * 1024 * 1024;
    settings.system_queue = false;
    settings.wal_settings.max_file_size = 8 * 1024 * 1024;
    settings.wal_settings.write_interval = Duration::from_millis(1);

    let fs = Arc::new(
        NormFS::new(dir.path().to_path_buf(), settings)
            .await
            .unwrap(),
    );
    let queue = fs.resolve("handoff-overflow");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    for i in 0..N as u64 {
        fs.enqueue(&queue, Bytes::from(i.to_le_bytes().to_vec()))
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (tx, mut rx) = mpsc::channel(N);

    let writer_fs = fs.clone();
    let writer_queue = queue.clone();
    let writer_tx = tx.clone();
    let writer = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(3);
        while writer_tx.capacity() == N {
            assert!(Instant::now() < deadline, "history send never started");
            std::thread::yield_now();
        }

        writer_fs
            .try_enqueue(
                &writer_queue,
                Bytes::from((N as u64).to_le_bytes().to_vec()),
            )
            .unwrap();
    });

    let read = fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 0, 1, tx);
    tokio::pin!(read);

    let mut done = false;
    tokio::select! {
        biased;
        result = &mut read => {
            assert!(result.unwrap());
            done = true;
        },
        _ = std::future::ready(()) => {},
    }
    writer.join().unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        for expected in 0..=N as u64 {
            let entry = loop {
                tokio::select! {
                    biased;
                    result = &mut read, if !done => {
                        assert!(result.unwrap());
                        done = true;
                    },
                    entry = rx.recv() => {
                        let Some(entry) = entry else {
                            assert_eq!(expected, N as u64, "history ended early");
                            return;
                        };
                        break entry;
                    },
                }
            };

            assert_eq!(
                entry.id,
                UintN::from(expected),
                "after {expected} entries, source={:?}",
                entry.source,
            );
        }
    })
    .await
    .unwrap();

    drop(rx);
    fs.close().await.unwrap();
}
