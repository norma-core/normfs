use std::future::Future;
use std::task::{Context, Waker};

use normfs_fs::{Fs, FsConfig};
use normfs_types::QueueIdResolver;
use uintn::UintN;

use crate::memory_pointers::MemoryPointers;

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
    pointers.mark(&queue, &UintN::from(7u64)).unwrap();
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
    pointers.mark(&queue, &UintN::from(8u64)).unwrap();
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
    pointers.mark(&queue, &UintN::from(9u64)).unwrap();
    let obstruction = dir.path().join(".memory_pointers.tmp");
    std::fs::create_dir(&obstruction).unwrap();
    assert!(pointers.flush_if_dirty().await.is_err());
    std::fs::remove_dir(obstruction).unwrap();
    pointers.flush_if_dirty().await.unwrap();
    let recovered = MemoryPointers::open(fs, dir.path()).await.unwrap();
    assert_eq!(recovered.last_id(&queue), Some(UintN::from(9u64)));
}
