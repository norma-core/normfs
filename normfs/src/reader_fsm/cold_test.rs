use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use normfs_types::QueueIdResolver;
use uintn::UintN;

use super::cold::{Cold, ColdFiles, FileKey};

fn key(file: u64) -> FileKey {
    let queue = QueueIdResolver::new("instance").resolve("queue");
    (queue, UintN::from(file), false)
}

#[derive(Default)]
struct Loads {
    started: AtomicUsize,
    live: AtomicUsize,
    most_live: AtomicUsize,
}

impl Loads {
    async fn load(&self) -> Result<Option<Bytes>, ()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_live.fetch_max(live, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.live.fetch_sub(1, Ordering::SeqCst);
        Ok(Some(Bytes::from(vec![7u8; 1024])))
    }
}

fn found(cold: Result<Cold, ()>) -> Bytes {
    match cold {
        Ok(Cold::Found(bytes)) => bytes,
        _ => panic!("the file was not found"),
    }
}

#[tokio::test]
async fn concurrent_reads_of_one_file_share_a_load() {
    let cold = Arc::new(ColdFiles::new(2));
    let loads = Arc::new(Loads::default());
    let reads: Vec<_> = (0..8)
        .map(|_| {
            let (cold, loads) = (cold.clone(), loads.clone());
            tokio::spawn(async move { found(cold.get(key(1), true, || loads.load()).await) })
        })
        .collect();
    for read in reads {
        assert_eq!(read.await.unwrap().len(), 1024);
    }
    assert_eq!(loads.started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn decoded_files_never_exceed_the_slots() {
    let cold = Arc::new(ColdFiles::new(2));
    let loads = Arc::new(Loads::default());
    let reads: Vec<_> = (0..6)
        .map(|file| {
            let (cold, loads) = (cold.clone(), loads.clone());
            tokio::spawn(async move {
                let bytes = found(cold.get(key(file), true, || loads.load()).await);
                tokio::time::sleep(Duration::from_millis(30)).await;
                drop(bytes);
            })
        })
        .collect();
    for read in reads {
        read.await.unwrap();
    }
    assert_eq!(loads.started.load(Ordering::SeqCst), 6);
    assert_eq!(loads.most_live.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_held_file_keeps_its_slot() {
    let cold = ColdFiles::new(1);
    let loads = Loads::default();
    let held = found(cold.get(key(1), true, || loads.load()).await);
    assert!(matches!(
        cold.get(key(2), false, || loads.load()).await,
        Ok(Cold::Busy)
    ));
    drop(held);
    let next = tokio::time::timeout(
        Duration::from_secs(1),
        cold.get(key(2), true, || loads.load()),
    )
    .await
    .expect("the kept file gives up its slot to a waiting load");
    found(next);
}

#[tokio::test]
async fn a_recent_file_is_not_loaded_again() {
    let cold = ColdFiles::new(2);
    let loads = Loads::default();
    drop(found(cold.get(key(1), true, || loads.load()).await));
    drop(found(cold.get(key(1), true, || loads.load()).await));
    assert_eq!(loads.started.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failed_load_is_retried_by_the_next_read() {
    let cold = ColdFiles::new(1);
    let failed: Result<Cold, ()> = cold.get(key(1), true, || async { Err(()) }).await;
    assert!(failed.is_err());
    let loads = Loads::default();
    found(cold.get(key(1), true, || loads.load()).await);
}

#[tokio::test]
async fn a_cancelled_load_leaves_no_entry_behind() {
    let cold = Arc::new(ColdFiles::new(100));
    let reads: Vec<_> = (0..100)
        .map(|file| {
            let cold = cold.clone();
            tokio::spawn(async move {
                let _ = cold
                    .get(key(file), true, || {
                        std::future::pending::<Result<Option<Bytes>, ()>>()
                    })
                    .await;
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(20)).await;
    for read in &reads {
        read.abort();
    }
    for read in reads {
        let _ = read.await;
    }
    assert_eq!(cold.loading(), 0);
}

#[tokio::test]
async fn a_cancelled_slot_wait_does_not_stop_keeping_recent_files() {
    let cold = Arc::new(ColdFiles::new(1));
    let loads = Arc::new(Loads::default());
    let held = found(cold.get(key(1), true, || loads.load()).await);
    let waiter = {
        let (cold, loads) = (cold.clone(), loads.clone());
        tokio::spawn(async move {
            let _ = cold.get(key(2), true, || loads.load()).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    waiter.abort();
    let _ = waiter.await;
    drop(held);

    let before = loads.started.load(Ordering::SeqCst);
    drop(found(cold.get(key(3), true, || loads.load()).await));
    drop(found(cold.get(key(3), true, || loads.load()).await));
    assert_eq!(loads.started.load(Ordering::SeqCst) - before, 1);
}

#[tokio::test]
async fn a_read_takes_over_the_load_a_cancelled_read_started() {
    let cold = Arc::new(ColdFiles::new(2));
    let leader = {
        let cold = cold.clone();
        tokio::spawn(async move {
            let _ = cold
                .get(key(1), true, || {
                    std::future::pending::<Result<Option<Bytes>, ()>>()
                })
                .await;
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let loads = Arc::new(Loads::default());
    let follower = {
        let (cold, loads) = (cold.clone(), loads.clone());
        tokio::spawn(async move { found(cold.get(key(1), true, || loads.load()).await) })
    };
    tokio::time::sleep(Duration::from_millis(5)).await;
    leader.abort();
    let _ = leader.await;
    assert_eq!(follower.await.unwrap().len(), 1024);
    assert_eq!(loads.started.load(Ordering::SeqCst), 1);
    assert_eq!(cold.loading(), 0);
}
