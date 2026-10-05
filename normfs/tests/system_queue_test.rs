//! NormFS records its own work in `normfs/system`: what reached the store or
//! the bucket, what failed, what was deleted. Nothing else may write there.

use std::time::Duration;

use bytes::Bytes;
use normfs::proto::system::{self as pb, EventType};
use normfs::{Error, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition, SYSTEM_QUEUE};
use prost::Message;
use tokio::sync::mpsc;
use uintn::UintN;

fn settings() -> NormFsSettings {
    NormFsSettings {
        queue_settings: QueueSettings::default().with_default_persist(Persist::STORE),
        ..NormFsSettings::default()
    }
}

async fn recorded(fs: &NormFS) -> Vec<pb::Event> {
    let queue = fs.resolve(SYSTEM_QUEUE);
    let Ok(last) = fs.get_last_id(&queue) else {
        return Vec::new();
    };
    let count = last.to_u64().unwrap() + 1;
    let (tx, mut rx) = mpsc::channel(count as usize + 1);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), count, 1, tx)
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Ok(entry) = rx.try_recv() {
        out.push(pb::Event::decode(entry.data).unwrap());
    }
    out
}

async fn wait_for(fs: &NormFS, done: impl Fn(&[pb::Event]) -> bool) -> Vec<pb::Event> {
    for _ in 0..500 {
        let events = recorded(fs).await;
        if done(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("expected events never arrived: {:?}", recorded(fs).await);
}

fn stored(events: &[pb::Event], queue: &str) -> Vec<pb::File> {
    events
        .iter()
        .filter(|e| e.r#type() == EventType::EtFileStored && e.queue == queue)
        .filter_map(|e| e.file.clone())
        .collect()
}

#[tokio::test]
async fn a_stored_file_is_recorded_with_its_entries() {
    let dir = tempfile::tempdir().unwrap();
    let fs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .unwrap();
    let cam = fs.resolve("cam");
    fs.ensure_queue_exists_for_write(&cam).await.unwrap();
    for i in 0..10u8 {
        fs.enqueue(&cam, Bytes::from(vec![i; 100])).await.unwrap();
    }
    fs.flush_queue(&cam).await.unwrap();

    let events = wait_for(&fs, |events| !stored(events, cam.as_str()).is_empty()).await;
    assert!(events
        .iter()
        .any(|e| e.r#type() == EventType::EtQueueStarted && e.queue == cam.as_str() && e.store));
    let file = &stored(&events, cam.as_str())[0];
    assert_eq!(file.num_entries, 10);
    let first = &file.first_id.as_ref().unwrap().raw;
    assert_eq!(
        UintN::read_value_from_slice(first, first.len()).unwrap(),
        UintN::zero()
    );
    assert!(file.raw_bytes > 1000);
    assert_eq!(file.content_signature.len(), 64);
    assert!(events.iter().all(|e| e.dropped_before == 0));

    let on_disk = std::fs::read(cam.to_store_path(dir.path(), &UintN::from(1u64))).unwrap();
    assert_eq!(file.file_bytes, on_disk.len() as u64);
    // The signature block's content signature: after version, type,
    // header signature and type.
    assert_eq!(file.content_signature.as_ref(), &on_disk[88..152]);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn only_normfs_writes_the_system_queue() {
    let dir = tempfile::tempdir().unwrap();
    let fs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .unwrap();
    let system = fs.resolve(SYSTEM_QUEUE);
    assert!(matches!(
        fs.enqueue(&system, Bytes::from_static(b"x")).await,
        Err(Error::ReservedQueue)
    ));
    assert!(matches!(
        fs.try_enqueue(&system, Bytes::from_static(b"x")),
        Err(Error::ReservedQueue)
    ));
    assert!(matches!(
        fs.enqueue_batch(&system, vec![Bytes::from_static(b"x")])
            .await,
        Err(Error::ReservedQueue)
    ));
    assert!(matches!(
        fs.close_queue(&system).await,
        Err(Error::ReservedQueue)
    ));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn the_record_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let closed = |e: &pb::Event| e.r#type() == EventType::EtQueueClosed;
    {
        let fs = NormFS::new(dir.path().to_path_buf(), settings())
            .await
            .unwrap();
        let cam = fs.resolve("cam");
        fs.ensure_queue_exists_for_write(&cam).await.unwrap();
        fs.ensure_queue_exists_for_write(&cam).await.unwrap();
        fs.enqueue(&cam, Bytes::from_static(b"one")).await.unwrap();
        fs.close_queue(&cam).await.unwrap();
        wait_for(&fs, |events| events.iter().any(closed)).await;
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(dir.path().to_path_buf(), settings())
        .await
        .unwrap();
    let cam = fs.resolve("cam");
    fs.ensure_queue_exists_for_write(&cam).await.unwrap();
    fs.enqueue(&cam, Bytes::from_static(b"two")).await.unwrap();
    let events = wait_for(&fs, |events| {
        events
            .iter()
            .skip_while(|e| !closed(e))
            .any(|e| e.r#type() == EventType::EtQueueStarted)
    })
    .await;
    assert!(events
        .iter()
        .all(|e| e.stamp.as_ref().is_some_and(|s| s.app_start_id > 0
            && s.local_stamp_ns > 0
            && s.monotonic_stamp_ns > 0)));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn an_instance_without_a_disk_keeps_its_record_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    let fs = NormFS::new(dir.path().to_path_buf(), NormFsSettings::memory_only())
        .await
        .unwrap();
    let cam = fs.resolve("cam");
    fs.ensure_queue_exists_for_write(&cam).await.unwrap();
    fs.enqueue(&cam, Bytes::from_static(b"x")).await.unwrap();
    wait_for(&fs, |events| {
        events
            .iter()
            .any(|e| e.r#type() == EventType::EtQueueStarted)
    })
    .await;
    fs.close().await.unwrap();
    let system = fs.resolve(SYSTEM_QUEUE);
    assert!(!system.to_store_dir(dir.path()).exists());
}

#[tokio::test]
async fn turned_off_it_records_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let fs = NormFS::new(
        dir.path().to_path_buf(),
        NormFsSettings {
            system_queue: false,
            ..settings()
        },
    )
    .await
    .unwrap();
    let cam = fs.resolve("cam");
    fs.ensure_queue_exists_for_write(&cam).await.unwrap();
    fs.enqueue(&cam, Bytes::from_static(b"x")).await.unwrap();
    fs.flush_queue(&cam).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(recorded(&fs).await.is_empty());
    fs.close().await.unwrap();
}
