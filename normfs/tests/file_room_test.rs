//! `file_room` tells a queue's writer, before it writes, whether a record of
//! a given width would be the first of a file.

use std::path::{Path, PathBuf};

use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, Persist, QueueId, QueueSettings};

const PAGE: usize = 4096;
const RECORD_LEN: usize = 1000;

async fn open(root: &Path, persist: Persist) -> NormFS {
    let settings = NormFsSettings {
        mem_page_size: PAGE,
        max_memory_usage: 64 * 1024,
        queue_settings: QueueSettings::all_active().with_default_persist(persist),
        ..NormFsSettings::all_active()
    };
    NormFS::new(root.to_path_buf(), settings).await.unwrap()
}

async fn write(fs: &NormFS, queue: &QueueId, len: usize) {
    fs.enqueue(queue, Bytes::from(vec![0x5A; len]))
        .await
        .unwrap();
}

fn store_files(root: &Path, queue: &QueueId) -> usize {
    let mut stack: Vec<PathBuf> = vec![queue.to_store_dir(root)];
    let mut n = 0;
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|ext| ext == "store") {
                n += 1;
            }
        }
    }
    n
}

/// Three records, then one `extra` bytes wider than the room left, then a
/// flush; returns the store files that made.
async fn files_after_tail(root: &Path, extra: usize) -> usize {
    let fs = open(root, Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    for _ in 0..3 {
        write(&fs, &queue, RECORD_LEN).await;
    }
    let room = fs.file_room(&queue).unwrap();
    let len = room.room().unwrap() + extra;
    assert_eq!(room.starts_file(len), extra > 0);
    write(&fs, &queue, len).await;
    fs.flush_queue(&queue).await.unwrap();
    let files = store_files(root, &queue);
    fs.close().await.unwrap();
    files
}

#[tokio::test]
async fn the_room_left_is_exact_to_the_byte() {
    let fits = tempfile::TempDir::new().unwrap();
    assert_eq!(files_after_tail(fits.path(), 0).await, 1);
    let spills = tempfile::TempDir::new().unwrap();
    assert_eq!(files_after_tail(spills.path(), 1).await, 2);
}

#[tokio::test]
async fn a_file_starts_on_open_on_a_full_page_and_after_a_flush() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let mut starts = Vec::new();
    for id in 0..10 {
        if fs.file_room(&queue).unwrap().starts_file(RECORD_LEN) {
            starts.push(id);
        }
        write(&fs, &queue, RECORD_LEN).await;
    }
    assert_eq!(starts, [0, 4, 8]);

    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(fs.file_room(&queue).unwrap().room(), None);
    write(&fs, &queue, RECORD_LEN).await;
    assert!(!fs.file_room(&queue).unwrap().starts_file(RECORD_LEN));
    fs.close().await.unwrap();

    let fs = open(temp.path(), Persist::STORE).await;
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    assert_eq!(fs.file_room(&queue).unwrap().room(), None);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_queue_counts_pages() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::MEMORY).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let mut starts = Vec::new();
    for id in 0..10 {
        if fs.file_room(&queue).unwrap().starts_file(RECORD_LEN) {
            starts.push(id);
        }
        write(&fs, &queue, RECORD_LEN).await;
    }
    assert_eq!(starts, [0, 4, 8]);
    assert!(matches!(
        fs.file_room(&fs.resolve("nowhere")),
        Err(Error::QueueNotFound)
    ));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_wal_file_runs_across_pages() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::WAL_STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let mut starts = Vec::new();
    for id in 0..10 {
        if fs.file_room(&queue).unwrap().starts_file(RECORD_LEN) {
            starts.push(id);
        }
        write(&fs, &queue, RECORD_LEN).await;
    }
    assert_eq!(starts, [0]);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_flush_between_two_looks_is_seen() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;

    let before = fs.file_room(&queue).unwrap();
    write(&fs, &queue, RECORD_LEN).await;
    let joined = fs.file_room(&queue).unwrap();
    assert!(before.same_file(&joined));

    fs.flush_queue(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;
    assert!(!joined.same_file(&fs.file_room(&queue).unwrap()));
    fs.close().await.unwrap();
}

fn record(len: usize) -> Bytes {
    Bytes::from(vec![0x5A; len])
}

#[tokio::test]
async fn an_append_into_a_sealed_file_is_refused_and_takes_no_id() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;

    let room = fs.file_room(&queue).unwrap();
    fs.flush_queue(&queue).await.unwrap();
    assert!(matches!(
        fs.try_enqueue_in(&queue, &room, record(RECORD_LEN)),
        Err(Error::NotInFile)
    ));
    assert_eq!(fs.get_last_id(&queue).unwrap().to_u64().unwrap(), 0);

    let id = fs.try_enqueue(&queue, record(RECORD_LEN)).unwrap();
    assert_eq!(id.to_u64().unwrap(), 1);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn an_append_joins_the_file_it_saw_while_it_fits() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;

    let room = fs.file_room(&queue).unwrap();
    fs.try_enqueue_in(&queue, &room, record(RECORD_LEN))
        .unwrap();
    let room = fs.file_room(&queue).unwrap();
    let wider = room.room().unwrap() + 1;
    assert!(matches!(
        fs.try_enqueue_in(&queue, &room, record(wider)),
        Err(Error::NotInFile)
    ));
    fs.try_enqueue_in(&queue, &room, record(room.room().unwrap()))
        .unwrap();

    fs.flush_queue(&queue).await.unwrap();
    assert_eq!(store_files(temp.path(), &queue), 1);
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_queue_refuses_an_append_past_its_page() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::MEMORY).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;

    let room = fs.file_room(&queue).unwrap();
    fs.try_enqueue_in(&queue, &room, record(RECORD_LEN))
        .unwrap();
    for _ in 0..2 {
        write(&fs, &queue, RECORD_LEN).await;
    }
    assert!(matches!(
        fs.try_enqueue_in(&queue, &room, record(RECORD_LEN)),
        Err(Error::NotInFile)
    ));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_look_from_before_a_reopen_matches_no_later_file() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = open(temp.path(), Persist::STORE).await;
    let queue = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;
    let stale = fs.file_room(&queue).unwrap();

    fs.close_queue(&queue).await.unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    write(&fs, &queue, RECORD_LEN).await;

    assert!(!stale.same_file(&fs.file_room(&queue).unwrap()));
    assert!(matches!(
        fs.try_enqueue_in(&queue, &stale, record(RECORD_LEN)),
        Err(Error::NotInFile)
    ));
    fs.close().await.unwrap();
}
