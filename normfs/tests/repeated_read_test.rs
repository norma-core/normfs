use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::mpsc;
use uintn::UintN;

/// Test scenario: Repeatedly reading the last entry with backward direction
/// while another thread is continuously publishing new entries
/// This reproduces the bug where read_full_negative() didn't respect limit,
/// causing it to send multiple entries to a small channel and deadlock
#[tokio::test]
async fn test_repeated_backward_read_with_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    let settings = NormFsSettings::default();
    let fs = Arc::new(NormFS::new(path.clone(), settings).await.unwrap());

    let queue_id = fs.resolve("test-queue");
    fs.ensure_queue_exists_for_write(&queue_id).await.unwrap();

    // Write initial entries
    for i in 0..100 {
        let data = Bytes::from(format!("entry-{}", i));
        fs.enqueue(&queue_id, data).unwrap();
    }

    // Spawn background task that continuously writes (simulating st3215 driver)
    let fs_writer = Arc::clone(&fs);
    let stop_writing = Arc::new(AtomicBool::new(false));
    let stop_writing_clone = Arc::clone(&stop_writing);
    let queue_id_writer = queue_id.clone();

    let writer_task = tokio::spawn(async move {
        let mut counter = 100;
        while !stop_writing_clone.load(Ordering::Relaxed) {
            let data = Bytes::from(format!("bg-entry-{}", counter));
            fs_writer.enqueue(&queue_id_writer, data).unwrap();
            counter += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        println!("Writer task stopped after {} entries", counter);
    });

    // Give writer some time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now simulate motors-mirroring loop: repeated reads every 20ms
    for iteration in 0..10 {
        println!("=== Iteration {} ===", iteration);

        let offset = UintN::from(1u64);
        let (tx, mut rx) = mpsc::channel(1);
        let read_future = fs.read(
            &queue_id,
            ReadPosition::ShiftFromTail(offset.clone()),
            1,
            1,
            tx,
        );

        let result = tokio::time::timeout(Duration::from_secs(2), read_future).await;
        assert!(
            result.is_ok(),
            "Read timed out at iteration {} - BUG REPRODUCED",
            iteration
        );
        assert!(
            result.unwrap().is_ok(),
            "Read failed at iteration {}",
            iteration
        );

        if let Some(entry) = rx.recv().await {
            println!("Iteration {}: read id={}", iteration, entry.id);
        }

        // Wait 20ms like motors-mirroring
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Stop writer and wait for it
    stop_writing.store(true, Ordering::Relaxed);
    writer_task.await.unwrap();

    fs.close().await.unwrap();
}

/// Simpler test: repeated reads in a loop without concurrent background writes
#[tokio::test]
async fn test_repeated_backward_read_loop() {
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().to_path_buf();

    let settings = NormFsSettings::default();
    let fs = NormFS::new(path.clone(), settings).await.unwrap();

    let queue_id = fs.resolve("test-queue");
    fs.ensure_queue_exists_for_write(&queue_id).await.unwrap();

    // Write initial entries
    for i in 0..50 {
        let data = Bytes::from(format!("entry-{}", i));
        fs.enqueue(&queue_id, data).unwrap();
    }

    // Simulate motors-mirroring pattern: repeated reads every 20ms
    let offset = UintN::from(1u64);
    for iteration in 0..10 {
        println!("=== Iteration {} ===", iteration);

        let (tx, mut rx) = mpsc::channel(1);
        let read_future = fs.read(
            &queue_id,
            ReadPosition::ShiftFromTail(offset.clone()),
            1,
            1,
            tx,
        );

        let result = tokio::time::timeout(Duration::from_secs(2), read_future).await;
        assert!(result.is_ok(), "Read timed out at iteration {}", iteration);
        assert!(
            result.unwrap().is_ok(),
            "Read failed at iteration {}",
            iteration
        );

        if let Some(entry) = rx.recv().await {
            println!("Iteration {}: read id={}", iteration, entry.id);
        }

        // Write new entry
        let data = Bytes::from(format!("entry-{}", 50 + iteration));
        fs.enqueue(&queue_id, data).unwrap();

        // Wait 20ms like motors-mirroring
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    fs.close().await.unwrap();
}
