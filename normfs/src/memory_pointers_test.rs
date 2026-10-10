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

#[tokio::test]
async fn lines_0_4_2_kept_for_memory_queues_stay_marks() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let resolver = QueueIdResolver::new("instance");
    let (memory, cloud) = (resolver.resolve("memory"), resolver.resolve("cloud"));
    let old = format!(
        "# normfs memory-only pointers v1\n{}\t5\n{}\t7\t2\n",
        memory.as_str(),
        cloud.as_str()
    );
    std::fs::write(dir.path().join(".memory_pointers"), old).unwrap();

    let pointers = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(pointers.used_id(&memory), Some(UintN::from(5u64)));
    assert_eq!(pointers.last_landed(&memory), None);
    assert!(pointers.is_settled(&memory), "a memory line names no file");
    assert_eq!(
        pointers.last_landed(&cloud),
        Some((UintN::from(7u64), UintN::from(2u64)))
    );
    assert!(!pointers.is_settled(&cloud));
}

#[tokio::test]
async fn a_marked_entry_zero_is_used_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let resolver = QueueIdResolver::new("instance");
    let (fresh, recorded) = (resolver.resolve("fresh"), resolver.resolve("recorded"));
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    pointers.record(&recorded).await.unwrap();
    pointers.mark(&fresh, &UintN::zero()).unwrap();
    pointers.mark(&recorded, &UintN::zero()).unwrap();
    pointers.flush_all().await.unwrap();
    drop(pointers);

    let pointers = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(pointers.used_id(&fresh), Some(UintN::zero()));
    assert_eq!(pointers.used_id(&recorded), Some(UintN::zero()));
    assert!(pointers.is_settled(&recorded));
}

#[tokio::test]
async fn ids_go_on_across_memory_and_cloud_lives_in_one_file() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("instance").resolve("switched");
    let reopen = || MemoryPointers::open(fs.clone(), dir.path());

    let memory = reopen().await.unwrap();
    memory.mark(&queue, &UintN::from(4u64)).unwrap();
    memory.flush_all().await.unwrap();
    drop(memory);

    let cloud = reopen().await.unwrap();
    assert_eq!(cloud.used_id(&queue), Some(UintN::from(4u64)));
    cloud.reserve(&queue, &UintN::from(9u64)).await.unwrap();
    cloud
        .mark_landed(&queue, &UintN::from(9u64), &UintN::from(1u64))
        .await
        .unwrap();
    cloud.settle_landed();
    cloud.flush_all().await.unwrap();
    drop(cloud);

    let memory = reopen().await.unwrap();
    assert_eq!(memory.used_id(&queue), Some(UintN::from(9u64)));
    memory.mark(&queue, &UintN::from(12u64)).unwrap();
    memory.flush_all().await.unwrap();
    drop(memory);

    let local = reopen().await.unwrap();
    assert_eq!(local.used_id(&queue), Some(UintN::from(12u64)));
    assert_eq!(
        local.last_landed(&queue).map(|(_, file)| file),
        Some(UintN::from(1u64))
    );
    assert!(
        local.is_settled(&queue),
        "a clean cloud life then a memory one"
    );
    assert!(!dir.path().join(".memory_ids").exists());
}

#[tokio::test]
async fn a_reserve_inside_a_memory_mark_writes_a_cloud_line() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("instance").resolve("switched");
    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    pointers.mark(&queue, &UintN::from(9u64)).unwrap();
    pointers.flush_all().await.unwrap();
    pointers.reserve(&queue, &UintN::from(5u64)).await.unwrap();
    drop(pointers);

    let crashed = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(
        !crashed.is_settled(&queue),
        "an upload may have landed, so the bucket has to be asked"
    );
}

#[tokio::test]
async fn a_clean_close_does_not_lower_a_line_below_a_memory_mark() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("instance").resolve("switched");
    std::fs::write(
        dir.path().join(".memory_pointers"),
        format!("# normfs pointers v1\n{}\t400\n", queue.as_str()),
    )
    .unwrap();

    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    pointers
        .reserve(&queue, &UintN::from(100u64))
        .await
        .unwrap();
    pointers
        .mark_landed(&queue, &UintN::from(100u64), &UintN::from(1u64))
        .await
        .unwrap();
    pointers.settle_landed();
    pointers.flush_all().await.unwrap();
    drop(pointers);

    let reopened = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(reopened.used_id(&queue), Some(UintN::from(400u64)));
}

#[tokio::test]
async fn a_reserve_after_a_memory_life_of_entry_zero_never_writes_a_bare_record() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Fs::new(FsConfig::default()).unwrap();
    let queue = QueueIdResolver::new("instance").resolve("switched");
    std::fs::write(
        dir.path().join(".memory_pointers"),
        format!("# normfs pointers v1\n{}\t0\n", queue.as_str()),
    )
    .unwrap();

    let pointers = MemoryPointers::open(fs.clone(), dir.path()).await.unwrap();
    pointers.reserve(&queue, &UintN::zero()).await.unwrap();
    let line = std::fs::read_to_string(dir.path().join(".memory_pointers")).unwrap();
    assert!(
        !line.contains(&format!("{}\t0\t0", queue.as_str())),
        "{line}"
    );
    drop(pointers);
    let crashed = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert!(crashed.used_id(&queue).is_some());
}
