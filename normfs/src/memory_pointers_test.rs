use std::future::Future;
use std::task::{Context, Waker};

use normfs_fs::{Fs, FsConfig};
use normfs_types::QueueIdResolver;
use uintn::UintN;

use crate::memory_pointers::{MemoryPointers, RESERVE_AHEAD};

#[tokio::test]
async fn cancelled_flush_keeps_serialization_until_the_snapshot_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    pointers.advance(&queue, &UintN::from(7u64), None).unwrap();
    let (release, blocked) = std::sync::mpsc::channel();
    let mut blocker = Box::pin(fs.run_blocking(move || {
        blocked.recv().unwrap();
        Ok(())
    }));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(blocker.as_mut().poll(&mut cx).is_pending());
    let mut first = Box::pin(pointers.flush_if_dirty());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    drop(first);
    let mut second = Box::pin(pointers.flush_if_dirty());
    assert!(second.as_mut().poll(&mut cx).is_pending());
    pointers.advance(&queue, &UintN::from(8u64), None).unwrap();
    release.send(()).unwrap();
    blocker.await.unwrap();
    second.await.unwrap();
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(recovered.last_id(&queue), Some(UintN::from(8u64)));
}

#[tokio::test]
async fn failed_flush_retains_dirty_state_for_retry() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    pointers.advance(&queue, &UintN::from(9u64), None).unwrap();
    let obstruction = dir.path().join(".memory_pointers.tmp");
    std::fs::create_dir(&obstruction).unwrap();
    assert!(pointers.flush_if_dirty().await.is_err());
    std::fs::remove_dir(obstruction).unwrap();
    pointers.flush_if_dirty().await.unwrap();
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(recovered.last_id(&queue), Some(UintN::from(9u64)));
}

#[tokio::test]
async fn a_reserve_is_written_once_per_reserve_and_survives_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    let ids = 3 * RESERVE_AHEAD;
    for last in (3..ids).step_by(4) {
        pointers.reserve(&queue, &UintN::from(last)).await.unwrap();
        let file = UintN::from(last / 4 + 1);
        pointers
            .mark_landed(&queue, &UintN::from(last), &file)
            .await
            .unwrap();
    }
    assert_eq!(pointers.publishes(), 3);

    drop(pointers);
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(recovered.last_id(&queue).unwrap() >= UintN::from(ids - 1));
}

#[tokio::test]
async fn records_older_versions_kept_for_memory_queues_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let resolver = QueueIdResolver::new("instance");
    let (memory, cloud) = (resolver.resolve("memory"), resolver.resolve("cloud"));
    let lines = format!("{}\t5\n{}\t7\t2\n", memory.as_str(), cloud.as_str());
    let path = dir.path().join(".memory_pointers");

    std::fs::write(&path, format!("# normfs memory-only pointers v1\n{lines}")).unwrap();
    let old = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    assert_eq!(old.last_id(&memory), None);
    assert_eq!(old.last_id(&cloud), Some(UintN::from(7u64)));

    // A reserve written since then has no file either, and stays.
    std::fs::write(&path, format!("# normfs pointers v1\n{lines}")).unwrap();
    let new = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(new.last_id(&memory), Some(UintN::from(5u64)));
}

#[tokio::test]
async fn a_reserve_whose_write_failed_fails_again_until_written() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    let obstruction = dir.path().join(".memory_pointers.tmp");
    std::fs::create_dir(&obstruction).unwrap();

    assert!(pointers.reserve(&queue, &UintN::from(10u64)).await.is_err());
    assert!(
        pointers.reserve(&queue, &UintN::from(10u64)).await.is_err(),
        "a retry reported the reserve written while the file still cannot be"
    );

    std::fs::remove_dir(obstruction).unwrap();
    pointers.reserve(&queue, &UintN::from(10u64)).await.unwrap();
    drop(pointers);
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(recovered.used_id(&queue).unwrap() >= UintN::from(10u64));
}

#[tokio::test]
async fn entry_zero_landed_on_a_recorded_queue_survives_a_crash() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    pointers.record(&queue).await.unwrap();

    pointers.reserve(&queue, &UintN::zero()).await.unwrap();
    pointers
        .mark_landed(&queue, &UintN::zero(), &UintN::one())
        .await
        .unwrap();

    drop(pointers);
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(
        recovered.used_id(&queue).is_some(),
        "entry 0 landed, but the restart sees a queue that used no id"
    );
}

#[tokio::test]
async fn a_reserve_after_a_close_lowered_it_is_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let resolver = QueueIdResolver::new("instance");
    let (queue, other) = (resolver.resolve("queue"), resolver.resolve("other"));

    pointers
        .reserve(&queue, &UintN::from(100u64))
        .await
        .unwrap();
    pointers
        .mark_landed(&queue, &UintN::from(100u64), &UintN::one())
        .await
        .unwrap();
    pointers.settle_landed_queue(&queue);

    pointers
        .reserve(&queue, &UintN::from(150u64))
        .await
        .unwrap();
    pointers.reserve(&other, &UintN::from(5u64)).await.unwrap();

    drop(pointers);
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(recovered.used_id(&queue).unwrap() >= UintN::from(150u64));
}

#[tokio::test]
async fn a_record_whose_write_failed_is_written_on_retry() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    let queue = QueueIdResolver::new("instance").resolve("queue");
    let obstruction = dir.path().join(".memory_pointers.tmp");
    std::fs::create_dir(&obstruction).unwrap();
    assert!(pointers.record(&queue).await.is_err());
    assert!(pointers.record(&queue).await.is_err());

    std::fs::remove_dir(obstruction).unwrap();
    pointers.record(&queue).await.unwrap();
    drop(pointers);
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(recovered.last_id(&queue), Some(UintN::zero()));
}
