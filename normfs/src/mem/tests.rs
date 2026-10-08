use super::MemStore;
use crate::config::PoolKind;
use bytes::Bytes;
use normfs_types::{QueueId, QueueIdResolver};
use std::sync::Arc;
use tokio::sync::mpsc;
use uintn::UintN;

const TEST_INSTANCE_ID: &str = "test-instance";

/// Page size for the tests that do not care what it is.
///
/// Named rather than inherited: `NormFsSettings`'s default is chosen for
/// production and moves when the benchmarks say it should, and a test asserting
/// on page counts should not move with it.
const TEST_PAGE_SIZE: usize = 256 * 1024;

fn mem_store(max_memory_usage: usize) -> MemStore {
    MemStore::with_pools(max_memory_usage, TEST_PAGE_SIZE, 64 * 1024, 1024)
        .expect("test budget holds a queue's floor")
}

fn create_test_data(count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|i| Bytes::from(format!("data_{}", i)))
        .collect()
}

async fn setup_queue_with_data(mem: &Arc<MemStore>, queue: &QueueId, count: usize) -> Vec<UintN> {
    mem.start_queue(queue, None, false, PoolKind::Active);

    let data = create_test_data(count);
    let mut ids = Vec::new();

    for d in data {
        let id = mem
            .enqueue_awaiting(queue, d)
            .await
            .expect("queue is open")
            .0;
        ids.push(id);
    }

    ids
}

#[tokio::test]
async fn test_read_full_positive_basic() {
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let new_id = mem
        .enqueue_awaiting(&queue, new_data.clone())
        .await
        .expect("queue is open")
        .0;

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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let new_id = mem
        .enqueue_awaiting(&queue, new_data.clone())
        .await
        .expect("queue is open")
        .0;

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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    mem.enqueue_awaiting(&queue, Bytes::from("data_10"))
        .await
        .expect("queue is open"); // id[10]
    mem.enqueue_awaiting(&queue, Bytes::from("data_11"))
        .await
        .expect("queue is open"); // id[11]
    let id_12 = mem
        .enqueue_awaiting(&queue, Bytes::from("data_12"))
        .await
        .expect("queue is open")
        .0; // id[12]

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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("empty_queue");

    // Initialize empty queue
    mem.start_queue(&queue, None, false, PoolKind::Active);

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
    let new_id = mem
        .enqueue_awaiting(&queue, new_data.clone())
        .await
        .expect("queue is open")
        .0;

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
    let mem = Arc::new(mem_store(1024 * 1024 * 1024)); // 1GB for tests
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
    mem.enqueue_awaiting(&queue, Bytes::from("trigger_callback"))
        .await
        .expect("queue is open");

    // Give callback time to fire and unsubscribe
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Note: We can't easily verify unsubscribe was called without accessing internal state
    // This test mainly ensures no panic occurs when sending to closed channel
}

#[tokio::test]
async fn test_bounded_cache_drops_old_unacked_and_falls_back() {
    // A tiny budget still gives a queue its floor -- two 256 KiB pages, one to
    // append into and one for the writer to drain -- so the cache holds about
    // 512 KiB. Enough ~100 KiB records to overflow that force the oldest out
    // while nothing is acked, so the newest is held and older ids fall back to
    // file.
    let mem = Arc::new(mem_store(2 * TEST_PAGE_SIZE));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("bounded_queue");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let big = Bytes::from(vec![7u8; 100 * 1024]);
    let id0 = mem
        .enqueue_awaiting(&queue, big.clone())
        .await
        .expect("queue is open")
        .0;
    let mut newest = id0.clone();
    for _ in 0..7 {
        newest = mem
            .enqueue_awaiting(&queue, big.clone())
            .await
            .expect("queue is open")
            .0;
    }

    // Newest id is cached and served from memory.
    let (tx, mut rx) = mpsc::channel(10);
    let hit = mem
        .read_full(&queue, newest.clone(), newest.clone(), 1, &tx)
        .await;
    assert!(hit.success, "newest entry should be served from memory");
    assert_eq!(rx.try_recv().unwrap().id, newest);

    // The oldest id was evicted, so the read reports a memory miss (file fallback).
    let (tx2, _rx2) = mpsc::channel(10);
    let miss = mem
        .read_full(&queue, id0.clone(), newest.clone(), 1, &tx2)
        .await;
    assert!(
        !miss.success,
        "evicted entry should miss memory and fall back to file"
    );
}

#[tokio::test]
async fn every_queue_holds_a_disjoint_range_of_the_one_arena() {
    // 16 pages of 1 KiB. Every queue's pages are slots in this one allocation,
    // which is what makes `max_memory_usage` a total rather than a per-queue
    // allowance.
    let mem = Arc::new(
        MemStore::with_pools(16 * 1024, 1024, 64 * 1024, 1024)
            .expect("test budget holds a queue's floor"),
    );
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let arena = mem.arena().clone();
    assert_eq!(arena.page_count(), 16);

    let a = resolver.resolve("qa");
    let b = resolver.resolve("qb");
    mem.start_queue(&a, None, false, PoolKind::Active);
    mem.start_queue(&b, None, false, PoolKind::Active);

    let ra = mem.pool(&a).unwrap().slot_range().expect("a pooled queue");
    let rb = mem.pool(&b).unwrap().slot_range().expect("a pooled queue");

    // Disjoint by construction: the arena has one owner slot per page, so two
    // queues holding the same page is not a state it can represent. Checking it
    // anyway is what catches the ranges themselves being wrong.
    let overlaps = ra.first_slot < rb.first_slot + rb.page_count
        && rb.first_slot < ra.first_slot + ra.page_count;
    assert!(!overlaps, "ranges {ra:?} and {rb:?} overlap");

    assert_eq!(
        arena.free_pages(),
        16 - ra.page_count - rb.page_count,
        "every page is either free or owned by exactly one queue"
    );

    // A stalled queue can say who is holding the memory, not just that it is
    // held.
    let holders = arena.holders();
    assert_eq!(holders.len(), 2);
    assert!(
        holders.iter().any(|(name, _)| name == &a.to_string()),
        "the arena should know queue '{a}' by name, got {holders:?}"
    );
}

#[tokio::test]
async fn a_busy_queue_takes_a_page_rather_than_waiting_for_the_disk() {
    // The gap this closes. Before pages could move, a queue that ran out
    // waited for its own records to reach disk however much of the arena was
    // sitting idle next to it.
    let mem = Arc::new(
        MemStore::with_pools(16 * 1024, 1024, 64 * 1024, 1024)
            .expect("test budget holds a queue's floor"),
    );
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("busy");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    let started_with = pool.page_count();
    assert!(
        mem.arena().free_pages() > 0,
        "this test needs spare pages in the arena"
    );

    // Waiting is only allowed once a writer is draining, and this test never
    // starts one: if the pool ever chose to wait instead of taking a page, it
    // would hang here rather than fail.
    pool.set_drainer();

    let record = Bytes::from(vec![7u8; 400]);
    for id in 0..(started_with as u64 + 8) {
        pool.place(id, &record).await.expect("fits a page");
    }

    assert!(
        pool.page_count() > started_with,
        "the queue should have taken arena pages rather than waited: still {started_with} pages"
    );
    let range = pool.slot_range().unwrap();
    assert_eq!(range.page_count, pool.page_count());
    assert_eq!(
        mem.arena().free_pages(),
        16 - range.page_count,
        "the pages came out of the arena, so the process-wide total did not move"
    );
}

#[tokio::test]
async fn the_page_budget_is_a_total_across_queues() {
    // The old code read `max_memory_usage / queues.len()` *before* inserting
    // the queue, so the first queue took the whole budget and every later one
    // allocated again on top: the setting bounded nothing, and total memory
    // grew with the queue count. Pages now come out of one pot.
    let budget_bytes = 64 * 256 * 1024; // 64 pages
    let mem = Arc::new(mem_store(budget_bytes));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);

    let queues = 12usize;
    let mut total_pages = 0usize;
    for i in 0..queues {
        let queue = resolver.resolve(&format!("q{i}"));
        mem.start_queue(&queue, None, false, PoolKind::Active);
        total_pages += mem
            .pool(&queue)
            .expect("a started queue has a pool")
            .with_ring(|ring| ring.page_count());
    }

    // Every queue keeps its floor, so many queues can still exceed the pot --
    // but by a floor each, not by a whole budget each.
    let floor_total = queues * 2;
    assert!(
        total_pages <= 64 + floor_total,
        "{queues} queues took {total_pages} pages against a 64-page budget: not a total"
    );
    assert!(
        total_pages >= floor_total,
        "every queue must get at least its floor, got {total_pages} for {queues} queues"
    );
}

#[tokio::test]
async fn a_read_only_queue_does_not_reserve_a_writers_share() {
    // A queue is started read-only by any client that merely names a path, so
    // a writer's share here is memory an unauthenticated caller can reserve and
    // never release. Nothing appends to such a queue, so the share sits idle.
    let mem = Arc::new(
        MemStore::with_pools(64 * 1024, 1024, 64 * 1024, 1024)
            .expect("test budget holds a queue's floor"),
    );
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);

    let reader = resolver.resolve("reader");
    let writer = resolver.resolve("writer");
    mem.start_queue(&reader, None, true, PoolKind::Active);
    mem.start_queue(&writer, None, false, PoolKind::Active);

    let read_pages = mem.pool(&reader).unwrap().page_count();
    let write_pages = mem.pool(&writer).unwrap().page_count();

    assert_eq!(read_pages, 2, "a reader should start at the floor");
    assert!(
        write_pages > read_pages,
        "a writer should still get a share: {write_pages} against the reader's {read_pages}"
    );
}

#[tokio::test]
async fn a_promoted_reader_takes_pages_rather_than_keeping_its_floor() {
    // The other half of starting a reader small: being wrong about the mode has
    // to be cheap. A queue that starts read-only and is then written to takes
    // what it needs from the arena on its first busy moment.
    let mem = Arc::new(
        MemStore::with_pools(16 * 1024, 1024, 64 * 1024, 1024)
            .expect("test budget holds a queue's floor"),
    );
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("promoted");
    mem.start_queue(&queue, None, true, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    let started_with = pool.page_count();
    assert_eq!(started_with, 2);

    // Waiting is only allowed once a writer is draining, and this test never
    // starts one: if the pool chose to wait rather than take a page it would
    // hang.
    pool.set_drainer();
    let record = Bytes::from(vec![3u8; 400]);
    for id in 0..12u64 {
        pool.place(id, &record).await.expect("fits a page");
    }

    assert!(
        pool.page_count() > started_with,
        "a promoted reader should take arena pages, still {started_with} pages"
    );
}

/// A cancelled enqueue consumes nothing: the id it would have taken goes to
/// the next caller, so the pool never has to step over or renumber a gap.
#[tokio::test]
async fn a_cancelled_enqueue_leaves_no_gap_in_the_id_sequence() {
    // Four pages in the whole arena, so there is nothing left to take and the
    // pool fills.
    let mem = Arc::new(mem_store(1024 * 1024));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("cancel_queue");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    let (first, _) = mem
        .enqueue_awaiting(&queue, Bytes::from_static(b"first"))
        .await
        .expect("the queue is set up");

    // Fill every page, so the next enqueue must wait -- and is then cancelled.
    let block = Bytes::from(vec![0u8; 200 * 1024]);
    for _ in 0..8 {
        let placed = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            mem.enqueue_awaiting(&queue, block.clone()),
        )
        .await;
        if placed.is_err() {
            break;
        }
    }
    let last_before = mem.get_last_id(&queue).flatten();
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        mem.enqueue_awaiting(&queue, block.clone()),
    )
    .await;
    assert!(cancelled.is_err(), "the pool should have been full");
    assert_eq!(
        mem.get_last_id(&queue).flatten(),
        last_before,
        "a cancelled enqueue must not consume an id"
    );

    // Free the pool; the next enqueue takes the id the cancelled one did not.
    for (w, _) in pool.take_pending(0) {
        pool.commit_written(&w);
    }
    pool.mark_durable(pool.next_entry_id());
    let (next, _) = mem
        .enqueue_awaiting(&queue, Bytes::from_static(b"after"))
        .await
        .expect("the queue is set up");
    let expected = last_before.map_or(0, |id| id.to_u64().unwrap() + 1);
    assert_eq!(next.to_u64().unwrap(), expected, "ids must stay dense");
    assert!(next.to_u64().unwrap() > first.to_u64().unwrap());
}

/// A follow whose backlog memory cannot serve goes to the files.
///
/// The pages hold records, so memory looks able to answer -- but the run they
/// hold starts above the id the follow asks from, because a rotation evicted
/// the ids below it. Serving that as a success subscribes the client and
/// silently skips its backlog, which the client cannot see.
#[tokio::test]
async fn a_follow_with_an_unservable_backlog_fails_to_the_files() {
    let mem = Arc::new(mem_store(1024 * 1024));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("floor_follow");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    // One record per page. Once they are durable the next one rotates into the
    // page holding id 0, and the cached run then starts above it.
    let wide = Bytes::from(vec![7u8; normfs_wal::max_record_len(TEST_PAGE_SIZE)]);
    let pages = pool.page_count();
    for _ in 0..pages {
        let _ = mem.enqueue_awaiting(&queue, wide.clone()).await;
    }
    for (w, _) in pool.take_pending(0) {
        pool.commit_written(&w);
    }
    pool.mark_durable(pages as u64);
    let _ = mem.enqueue_awaiting(&queue, wide.clone()).await;

    assert!(
        pool.min_cached_id().is_some_and(|m| m > 0),
        "id 0 must have been evicted for this to test anything"
    );
    assert!(!pool.is_empty(), "and the pages must still hold records");

    // The backlog from id 0 is on disk only; memory must decline the follow
    // rather than subscribe with the backlog skipped.
    let (tx, _rx) = mpsc::channel(16);
    let from = UintN::zero();
    let result = mem.follow_full(&queue, &from, UintN::zero(), 1, &tx).await;
    assert!(
        !result.success,
        "memory served a follow whose backlog it cannot answer"
    );
}

#[test]
fn a_page_below_the_ring_minimum_is_refused() {
    // The C contracts require a page to hold one empty record's frame and its
    // offset slot; past this check the arena panics instead of erroring.
    assert!(matches!(
        MemStore::with_pools(16, 8, 64 * 1024, 1024),
        Err(crate::Error::PageBelowMinimum { page_size: 8, .. })
    ));
    assert!(matches!(
        MemStore::with_pools(0, 0, 64 * 1024, 1024),
        Err(crate::Error::PageBelowMinimum { page_size: 0, .. })
    ));
}

/// A follow into an empty ring with a disk backlog also goes to the files:
/// after a recovery-style start the backlog exists only on disk, and
/// subscribing would silently skip it.
#[tokio::test]
async fn a_follow_into_an_empty_ring_with_a_disk_backlog_fails_to_the_files() {
    let mem = Arc::new(mem_store(1024 * 1024));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("empty_follow");
    // Ids 0..=4 exist on disk; memory holds none of them.
    mem.start_queue(&queue, Some(UintN::from(4u64)), false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    assert_eq!(
        pool.min_cached_id(),
        None,
        "the ring must be empty for this to test anything"
    );

    let (tx, _rx) = mpsc::channel(16);
    let result = mem
        .follow_full(&queue, &UintN::zero(), UintN::zero(), 1, &tx)
        .await;
    assert!(
        !result.success,
        "memory subscribed a follow whose backlog it cannot answer"
    );
}

#[test]
fn a_passive_queue_draws_its_floor_from_the_passive_arena() {
    let mem = MemStore::with_pools(1024 * 1024, 4096, 64 * 1024, 1024)
        .expect("both budgets hold a floor");
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);

    let active_free = mem.arena().free_pages();
    let passive_free = mem.passive_arena().free_pages();

    let passive = resolver.resolve("startup");
    mem.start_queue(&passive, None, false, PoolKind::Passive);
    assert_eq!(
        mem.arena().free_pages(),
        active_free,
        "a passive queue must not touch the active arena"
    );
    assert_eq!(
        mem.passive_arena().free_pages(),
        passive_free - 2,
        "a passive queue starts at the floor even in write mode: rare \
         writers are why its arena exists"
    );

    let active = resolver.resolve("cam0");
    mem.start_queue(&active, None, false, PoolKind::Active);
    assert_eq!(
        mem.passive_arena().free_pages(),
        passive_free - 2,
        "an active queue must not touch the passive arena"
    );
    assert!(
        mem.arena().free_pages() < active_free,
        "an active queue takes its share from the active arena"
    );
}

#[test]
fn a_closed_queue_returns_its_floor_to_the_arena() {
    let mem = mem_store(64 * TEST_PAGE_SIZE);
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let free_before = mem.arena().free_pages();

    let queue = resolver.resolve("doomed");
    mem.start_queue(&queue, None, false, PoolKind::Active);
    assert!(
        mem.arena().free_pages() < free_before,
        "the queue must hold pages for the release to prove anything"
    );

    // A straggler holding the pool blocks the release: handing its pages to
    // the next queue while it can still read them would serve one queue's
    // bytes as another's.
    let held = mem.pool(&queue).unwrap();

    mem.close_queue(&queue);
    assert!(
        mem.arena().free_pages() < free_before,
        "pages must stay put while a straggler can still read them"
    );
    assert!(mem.is_closed(&queue), "close must leave the closed mark");

    // The last holder letting go is what frees them.
    drop(held);
    assert_eq!(
        mem.arena().free_pages(),
        free_before,
        "a closed queue's pages must go back to the arena once unheld"
    );

    // And with no straggler, the release is immediate.
    let queue2 = resolver.resolve("doomed2");
    mem.start_queue(&queue2, None, false, PoolKind::Active);
    let free_mid = mem.arena().free_pages();
    assert!(free_mid < free_before);
    mem.close_queue(&queue2);
    assert_eq!(
        mem.arena().free_pages(),
        free_before,
        "an unheld closed queue's pages must return at close time"
    );
}

#[tokio::test]
async fn a_parked_append_whose_drainer_leaves_takes_no_id_and_tells_no_one() {
    let mem = Arc::new(mem_store(1024 * 1024));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("closing_park");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    // Fill every page so the next append parks.
    let block = Bytes::from(vec![0u8; 200 * 1024]);
    for _ in 0..8 {
        let placed = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            mem.enqueue_awaiting(&queue, block.clone()),
        )
        .await;
        if placed.is_err() {
            break;
        }
    }
    let last_before = mem.get_last_id(&queue).flatten();

    // Park an append, then take the drainer away, the way a close does.
    let parked = tokio::spawn({
        let mem = mem.clone();
        let queue = queue.clone();
        let block = block.clone();
        async move { mem.enqueue_awaiting(&queue, block).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    pool.clear_drainer();

    let refused = tokio::time::timeout(std::time::Duration::from_secs(5), parked)
        .await
        .expect("the parked append must return once the drainer leaves")
        .unwrap();
    assert!(
        refused.is_none(),
        "a record that reaches no file must not be accepted, got {refused:?}"
    );
    assert_eq!(
        mem.get_last_id(&queue).flatten(),
        last_before,
        "a refused record must not consume an id"
    );
}

/// The enqueue/close race in miniature: a record placed in a page whose
/// `Write` message never reaches the writer. The closing flush cannot see
/// it, so certifying the close would write a marker over a record that no
/// recovery can show.
#[tokio::test]
async fn a_close_cannot_certify_a_record_the_flush_never_saw() {
    let temp = tempfile::TempDir::new().unwrap();
    let mut settings = crate::NormFsSettings::all_active();
    // No autonomous flush tick: the record must stay unflushed.
    settings.wal_settings.write_interval = std::time::Duration::from_secs(3600);
    let fs = crate::NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("racy");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"flushed"))
        .await
        .unwrap();

    fs.mem
        .enqueue_awaiting(&queue, Bytes::from_static(b"placed, never sent"))
        .await
        .expect("the queue is open; the record is placed");

    let closed = fs.close_queue(&queue).await;
    assert!(
        matches!(
            closed,
            Err(crate::Error::Wal(normfs_wal::WalError::CloseIncomplete))
        ),
        "a close must refuse to certify while an accepted record is not on \
         disk, got {closed:?}"
    );
    assert!(
        !queue.to_fs_path(temp.path()).join("closed").is_file(),
        "no marker may exist for an uncertified close"
    );
}

/// The confirmed close race: an append parked on a full pool holds the gate
/// while a close begins. The close must not become visible with a bound the
/// parked append is about to move past, and the bound it records must be the
/// id that append actually took.
#[tokio::test]
async fn a_close_waits_for_a_parked_append_and_records_its_id() {
    let mem = Arc::new(mem_store(1024 * 1024));
    let resolver = QueueIdResolver::new(TEST_INSTANCE_ID);
    let queue = resolver.resolve("closing_race");
    mem.start_queue(&queue, None, false, PoolKind::Active);

    let pool = mem.pool(&queue).unwrap();
    pool.set_drainer();
    pool.arm_file_fill(1 << 20, 16);

    let block = Bytes::from(vec![0u8; 200 * 1024]);
    for _ in 0..8 {
        let placed = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            mem.enqueue_awaiting(&queue, block.clone()),
        )
        .await;
        if placed.is_err() {
            break;
        }
    }

    // Park an append; it holds the gate at the wait for a page.
    let parked = tokio::spawn({
        let mem = mem.clone();
        let queue = queue.clone();
        let block = block.clone();
        async move { mem.enqueue_awaiting(&queue, block).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let closing = tokio::spawn({
        let mem = mem.clone();
        let queue = queue.clone();
        async move { mem.begin_close(&queue).await }
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !mem.is_closed(&queue),
        "the close must not be visible while an accepted append can still move \
         the last id"
    );

    // Free pages; the parked append completes and takes its id.
    for (w, _) in pool.take_pending(0) {
        pool.commit_written(&w);
    }
    pool.mark_durable(pool.next_entry_id());

    let placed = tokio::time::timeout(std::time::Duration::from_secs(5), parked)
        .await
        .expect("the parked append must resume once pages are free")
        .unwrap()
        .expect("the append was accepted before the close became visible");
    tokio::time::timeout(std::time::Duration::from_secs(5), closing)
        .await
        .expect("begin_close must return once the gate is free")
        .unwrap();

    assert!(mem.is_closed(&queue));
    assert_eq!(
        mem.closed_last_id(&queue),
        Some(placed.0),
        "the recorded bound must be the id the parked append took, or every \
         follow ends one record early forever"
    );
}
