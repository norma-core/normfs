use bytes::Bytes;
use normfs::{NormFS, ReadPosition};
use tempfile::TempDir;
use uintn::UintN;

#[tokio::test]
async fn test_step_read_with_prefetch() {
    // Initialize logging
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = TempDir::new().unwrap();
    let temp_path = temp_dir.path().to_path_buf();

    println!("Creating NormFS with data directory: {:?}", temp_path);

    let queue_id = "test_step_queue";

    // Write 10 batches of 10 entries each (100 entries total)
    // Close and re-open between batches to force new files
    println!("Writing 10 batches of 10 entries each...");
    for batch in 0..10 {
        let normfs = NormFS::new(temp_path.clone(), Default::default())
            .await
            .unwrap();

        // Start or open the queue
        let queue_id_obj = normfs.resolve(queue_id);
        normfs
            .ensure_queue_exists_for_write(&queue_id_obj)
            .await
            .unwrap();

        for i in 0..10 {
            let entry_num = batch * 10 + i;
            let data = format!("entry_{}", entry_num);
            normfs.enqueue(&queue_id_obj, Bytes::from(data)).unwrap();
        }
        println!(
            "Wrote batch {} (entries {} to {})",
            batch,
            batch * 10,
            batch * 10 + 9
        );

        // Close to flush and switch files
        let _ = normfs.close().await;
    }

    println!("Re-opening NormFS for reading...");
    let normfs = NormFS::new(temp_path.clone(), Default::default())
        .await
        .unwrap();

    // Start the queue for reading
    let queue_id_obj = normfs.resolve(queue_id);
    normfs
        .ensure_queue_exists_for_write(&queue_id_obj)
        .await
        .unwrap();

    println!("Queue opened for reading");

    // Read with step=20 (should skip many files)
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    println!("Starting read with step=20, expecting entries: 0, 20, 40, 60");
    let mut read_result = tokio::spawn({
        async move {
            normfs
                .read(
                    &queue_id_obj,
                    ReadPosition::Absolute(UintN::zero()),
                    4,
                    20,
                    tx,
                )
                .await
        }
    });

    let mut entries_read = 0;
    let mut expected_id = UintN::zero();

    loop {
        tokio::select! {
            Some(entry) = rx.recv() => {
                println!("Read entry: id={}, data={}", entry.id, String::from_utf8_lossy(&entry.data));

                assert_eq!(entry.id, expected_id, "Expected ID: {}, got: {}", expected_id, entry.id);

                let expected_data = format!("entry_{}", expected_id);
                assert_eq!(
                    String::from_utf8_lossy(&entry.data),
                    expected_data,
                    "Expected data: {}, got: {}",
                    expected_data,
                    String::from_utf8_lossy(&entry.data)
                );

                entries_read += 1;
                expected_id = expected_id.add(&UintN::from(20u64));
            }
            result = &mut read_result => {
                println!("Read task completed");
                match result {
                    Ok(Ok(subscribed)) => println!("Read completed, subscribed={}", subscribed),
                    Ok(Err(e)) => println!("Read failed with error: {:?}", e),
                    Err(e) => println!("Read task panicked: {:?}", e),
                }
                break;
            }
        }
    }

    // Drain any remaining entries in channel
    while let Ok(entry) = rx.try_recv() {
        println!(
            "Draining entry: id={}, data={}",
            entry.id,
            String::from_utf8_lossy(&entry.data)
        );
        entries_read += 1;
    }

    println!("Read {} entries with step=20", entries_read);
    assert_eq!(entries_read, 4, "Should have read 4 entries with step=20");

    println!("Test completed successfully!");
}
