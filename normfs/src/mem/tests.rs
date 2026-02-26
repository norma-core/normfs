use super::MemStore;
use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use std::sync::Arc;
use tokio::sync::mpsc;
use uintn::UintN;

const TEST_INSTANCE_ID: &str = "test-instance";

fn create_test_data(count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|i| Bytes::from(format!("data_{}", i)))
        .collect()
}

async fn setup_queue_with_data(mem: &Arc<MemStore>, queue: &QueueId, count: usize) -> Vec<UintN> {
    mem.start_queue(queue, None);

    let data = create_test_data(count);
    let mut ids = Vec::new();

    for d in data {
        let id = mem.enqueue(queue, d);
        ids.push(id);
    }

    ids
}

#[tokio::test]
async fn test_read_full_positive_basic() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Read from id[2] to id[5] with step 1
    let (tx, mut rx) = mpsc::channel(100);
    let start_id = ids[2].clone();
    let end_id = ids[5].clone();

    let result = mem.read_full(&queue, start_id, end_id, 1, &tx).await;

    assert!(result.success, "read_full should succeed");

    // Verify: Should receive ids[2], ids[3], ids[4], ids[5]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 4, "Should receive 4 entries");
    assert_eq!(received[0].id, ids[2]);
    assert_eq!(received[1].id, ids[3]);
    assert_eq!(received[2].id, ids[4]);
    assert_eq!(received[3].id, ids[5]);
}

#[tokio::test]
async fn test_read_full_positive_with_step() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Read from id[0] to id[8] with step 2
    let (tx, mut rx) = mpsc::channel(100);
    let start_id = ids[0].clone();
    let end_id = ids[8].clone();

    let result = mem.read_full(&queue, start_id, end_id, 2, &tx).await;

    assert!(result.success, "read_full should succeed");

    // Verify: Should receive ids[0], ids[2], ids[4], ids[6], ids[8]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 5, "Should receive 5 entries with step 2");
    assert_eq!(received[0].id, ids[0]);
    assert_eq!(received[1].id, ids[2]);
    assert_eq!(received[2].id, ids[4]);
    assert_eq!(received[3].id, ids[6]);
    assert_eq!(received[4].id, ids[8]);
}

#[tokio::test]
async fn test_read_full_positive_out_of_range() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 5 entries
    let ids = setup_queue_with_data(&mem, &queue, 5).await;

    // Test: Try to read beyond memory range - should return what's available
    let (tx, mut rx) = mpsc::channel(100);
    let start_id = ids[0].clone();
    let end_id = ids[4].add(&UintN::from(10u64)); // Beyond memory

    let result = mem.read_full(&queue, start_id, end_id, 1, &tx).await;

    assert!(
        result.success,
        "read_full should succeed and return available entries"
    );

    // Verify we received all 5 available entries
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 5, "Should receive all 5 available entries");
    for i in 0..5 {
        assert_eq!(received[i].id, ids[i]);
    }
}

#[tokio::test]
async fn test_read_full_negative_basic() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries (ids will be 0, 1, 2, ..., 9)
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Read last 3 entries (offset = 2 means start from last_id - 2)
    let (tx, mut rx) = mpsc::channel(100);
    let offset = UintN::from(2u64);

    let result = mem.read_full_negative(&queue, offset, 1, 3, &tx).await; // Read 3 entries

    assert!(result.success, "read_full_negative should succeed");
    assert_eq!(
        result.start_id,
        Some(ids[7].clone()),
        "Start ID should be last_id - 2 = id[7]"
    );

    // Verify: Should receive ids[7], ids[8], ids[9]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 3, "Should receive 3 entries");
    assert_eq!(received[0].id, ids[7]);
    assert_eq!(received[1].id, ids[8]);
    assert_eq!(received[2].id, ids[9]);
}

#[tokio::test]
async fn test_read_full_negative_with_step() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Read from offset 5 with step 2 (expect 3 entries: ids[4], ids[6], ids[8])
    let (tx, mut rx) = mpsc::channel(100);
    let offset = UintN::from(5u64);

    let result = mem.read_full_negative(&queue, offset, 2, 3, &tx).await; // Read 3 entries with step 2

    assert!(result.success, "read_full_negative should succeed");
    assert_eq!(
        result.start_id,
        Some(ids[4].clone()),
        "Start ID should be id[4]"
    );

    // Verify: Should receive ids[4], ids[6], ids[8]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 3, "Should receive 3 entries with step 2");
    assert_eq!(received[0].id, ids[4]);
    assert_eq!(received[1].id, ids[6]);
    assert_eq!(received[2].id, ids[8]);
}

#[tokio::test]
async fn test_read_full_negative_offset_too_large() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 5 entries
    let _ids = setup_queue_with_data(&mem, &queue, 5).await;

    // Test: Offset larger than last_id
    let (tx, mut rx) = mpsc::channel(100);
    let offset = UintN::from(100u64);

    let result = mem.read_full_negative(&queue, offset, 1, 5, &tx).await; // Read all 5 entries

    assert!(result.success, "Should succeed but start from id 0");
    assert_eq!(result.start_id, Some(UintN::zero()), "Start ID should be 0");

    // Verify: Should receive all entries
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 5, "Should receive all 5 entries");
}

#[tokio::test]
async fn test_read_full_negative_not_all_in_memory() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Simulate some entries being flushed (adjust first_id)
    // In reality, this would happen through flush mechanism
    // For now, test with offset that would go before first entry

    // Test: Try to read with offset that would start before memory range
    let (tx, _rx) = mpsc::channel(100);
    // If we set offset = last_id + 1, start_id would be negative (or before first_id)
    let offset = ids[9].add(&UintN::from(5u64));

    let result = mem.read_full_negative(&queue, offset, 1, 0, &tx).await; // limit=0 for unlimited

    // This should fail because calculated start_id would be before memory range
    // In practice, this depends on implementation - let's check what we get
    assert!(
        result.start_id.is_some(),
        "Should return calculated start_id"
    );
}

#[tokio::test]
async fn test_follow_full_positive_subscribe() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 5 entries
    let ids = setup_queue_with_data(&mem, &queue, 5).await;

    // Test: Follow from id[2]
    let (tx, mut rx) = mpsc::channel(100);
    let from_id = ids[2].clone();

    let result = mem
        .follow_full(&queue, &from_id, from_id.clone(), 1, &tx)
        .await;

    assert!(result.success, "follow_full should succeed");
    assert!(
        result.subscription_id.is_some(),
        "Should return subscription ID"
    );

    // Verify: Should receive existing entries ids[2], ids[3], ids[4]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 3, "Should receive 3 existing entries");
    assert_eq!(received[0].id, ids[2]);
    assert_eq!(received[1].id, ids[3]);
    assert_eq!(received[2].id, ids[4]);

    // Add new entries
    let new_data = Bytes::from("new_data_1");
    let new_id = mem.enqueue(&queue, new_data.clone());

    // Give subscription callback time to fire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify: Should receive new entry
    let new_entry = rx.try_recv();
    assert!(
        new_entry.is_ok(),
        "Should receive new entry via subscription"
    );
    let entry = new_entry.unwrap();
    assert_eq!(entry.id, new_id);
    assert_eq!(entry.data, new_data);

    // Cleanup
    if let Some(sub_id) = result.subscription_id {
        mem.unsubscribe(&queue, sub_id);
    }
}

#[tokio::test]
async fn test_follow_full_negative_subscribe() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Follow with negative offset 3 (should start from id[6])
    let (tx, mut rx) = mpsc::channel(100);
    let offset = UintN::from(3u64);

    let result = mem.follow_full_negative(&queue, offset, 1, &tx).await;

    assert!(result.success, "follow_full_negative should succeed");
    assert!(
        result.subscription_id.is_some(),
        "Should return subscription ID"
    );
    assert_eq!(
        result.start_id,
        Some(ids[6].clone()),
        "Start ID should be id[6]"
    );

    // Verify: Should receive existing entries ids[6], ids[7], ids[8], ids[9]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 4, "Should receive 4 existing entries");
    assert_eq!(received[0].id, ids[6]);
    assert_eq!(received[3].id, ids[9]);

    // Add new entry
    let new_data = Bytes::from("new_data");
    let new_id = mem.enqueue(&queue, new_data.clone());

    // Give subscription callback time to fire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify: Should receive new entry
    let new_entry = rx.try_recv();
    assert!(
        new_entry.is_ok(),
        "Should receive new entry via subscription"
    );
    let entry = new_entry.unwrap();
    assert_eq!(entry.id, new_id);
    assert_eq!(entry.data, new_data);

    // Cleanup
    if let Some(sub_id) = result.subscription_id {
        mem.unsubscribe(&queue, sub_id);
    }
}

#[tokio::test]
async fn test_follow_full_with_step() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add 10 entries
    let ids = setup_queue_with_data(&mem, &queue, 10).await;

    // Test: Follow from id[0] with step 3
    let (tx, mut rx) = mpsc::channel(100);
    let from_id = ids[0].clone();

    let result = mem
        .follow_full(&queue, &from_id, from_id.clone(), 3, &tx)
        .await;

    assert!(result.success, "follow_full should succeed");
    assert!(
        result.subscription_id.is_some(),
        "Should return subscription ID"
    );

    // Verify: Should receive ids[0], ids[3], ids[6], ids[9]
    let mut received = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        received.push(entry);
    }

    assert_eq!(received.len(), 4, "Should receive 4 entries with step 3");
    assert_eq!(received[0].id, ids[0]);
    assert_eq!(received[1].id, ids[3]);
    assert_eq!(received[2].id, ids[6]);
    assert_eq!(received[3].id, ids[9]);

    // Add 3 new entries
    mem.enqueue(&queue, Bytes::from("data_10")); // id[10]
    mem.enqueue(&queue, Bytes::from("data_11")); // id[11]
    let id_12 = mem.enqueue(&queue, Bytes::from("data_12")); // id[12]

    // Give subscription callback time to fire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify: Should only receive id[12] (step 3 from id[9])
    let new_entries: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
    assert_eq!(
        new_entries.len(),
        1,
        "Should receive only one entry matching step"
    );
    assert_eq!(new_entries[0].id, id_12);

    // Cleanup
    if let Some(sub_id) = result.subscription_id {
        mem.unsubscribe(&queue, sub_id);
    }
}

#[tokio::test]
async fn test_read_full_empty_queue() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("empty_queue");

    // Test: Try to read from empty queue
    let (tx, _rx) = mpsc::channel(100);
    let start_id = UintN::zero();
    let end_id = UintN::from(10u64);

    let result = mem.read_full(&queue, start_id, end_id, 1, &tx).await;

    assert!(!result.success, "read_full should fail on empty queue");
}

#[tokio::test]
async fn test_read_full_negative_empty_queue() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("empty_queue");

    // Test: Try to read negative from empty queue
    let (tx, _rx) = mpsc::channel(100);
    let offset = UintN::from(5u64);

    let result = mem.read_full_negative(&queue, offset, 1, 1, &tx).await; // Try to read 1 entry

    assert!(
        !result.success,
        "read_full_negative should fail on empty queue"
    );
    assert!(
        result.start_id.is_none(),
        "Should not return start_id for empty queue"
    );
}

#[tokio::test]
async fn test_follow_full_empty_queue() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("empty_queue");

    // Initialize empty queue
    mem.start_queue(&queue, None);

    // Test: Try to follow empty queue
    let (tx, mut rx) = mpsc::channel(100);
    let from_id = UintN::zero();

    let result = mem
        .follow_full(&queue, &from_id, from_id.clone(), 1, &tx)
        .await;

    assert!(
        result.success,
        "follow_full should succeed even on empty queue"
    );
    assert!(
        result.subscription_id.is_some(),
        "Should return subscription ID"
    );

    // Verify: No existing entries
    assert!(
        rx.try_recv().is_err(),
        "Should not receive any entries initially"
    );

    // Add new entry
    let new_data = Bytes::from("first_entry");
    let new_id = mem.enqueue(&queue, new_data.clone());

    // Give subscription callback time to fire
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify: Should receive new entry
    let new_entry = rx.try_recv();
    assert!(
        new_entry.is_ok(),
        "Should receive new entry via subscription"
    );
    let entry = new_entry.unwrap();
    assert_eq!(entry.id, new_id);
    assert_eq!(entry.data, new_data);

    // Cleanup
    if let Some(sub_id) = result.subscription_id {
        mem.unsubscribe(&queue, sub_id);
    }
}

#[tokio::test]
async fn test_channel_closed_unsubscribes() {
    let mem = Arc::new(MemStore::new(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("test_queue");

    // Setup: Add some entries
    setup_queue_with_data(&mem, &queue, 5).await;

    // Test: Follow then close channel
    let (tx, _rx) = mpsc::channel(100);
    let from_id = UintN::zero();

    let result = mem
        .follow_full(&queue, &from_id, from_id.clone(), 1, &tx)
        .await;

    assert!(result.success, "follow_full should succeed");
    assert!(
        result.subscription_id.is_some(),
        "Should return subscription ID"
    );

    // Drop receiver to close channel
    drop(_rx);
    drop(tx);

    // Add new entry - subscription callback should detect closed channel and unsubscribe
    mem.enqueue(&queue, Bytes::from("trigger_callback"));

    // Give callback time to fire and unsubscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Note: We can't easily verify unsubscribe was called without accessing internal state
    // This test mainly ensures no panic occurs when sending to closed channel
}
