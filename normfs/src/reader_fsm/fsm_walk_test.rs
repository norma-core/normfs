use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use uintn::UintN;

use crate::reader_fsm::{ReadContext, ReaderFSM, ReaderState};
use crate::{NormFS, NormFsSettings};

async fn begins_at(fsm: &ReaderFSM, queue: &normfs_types::QueueId, file: u64) -> UintN {
    let file = UintN::from(file);
    if let Ok(before) = fsm.wal.get_entries_before(queue, &file).await {
        return before;
    }
    fsm.store
        .get_entries_before(queue, &file)
        .await
        .unwrap()
        .expect("file is in the WAL or the store")
}

// A partial read of a file the writer was still finishing leaves the walk at
// ReadNextFile with next_id inside the current file; the next file begins
// past it.
#[tokio::test]
async fn next_file_past_next_id_goes_back_to_the_current_file() {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp = tempfile::TempDir::new().unwrap();
    let mut settings = NormFsSettings::all_active();
    settings.max_memory_usage = 512 * 1024;
    settings.mem_page_size = 64 * 1024;
    settings.wal_settings.max_file_size = 64 * 1024;
    settings.wal_settings.write_buffer_size = 64 * 1024;
    settings.wal_settings.write_interval = Duration::from_millis(10);
    settings.wal_settings.enable_fsync = false;
    let fs = NormFS::new(temp.path().to_path_buf(), settings)
        .await
        .unwrap();
    let queue = fs.resolve("walk");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    for i in 0..2000u64 {
        let mut v = vec![0u8; 1024];
        v[..8].copy_from_slice(&i.to_le_bytes());
        fs.enqueue(&queue, Bytes::from(v)).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let fsm = fs.reader_fsm.clone();
    let start = begins_at(&fsm, &queue, 1).await;
    let next = begins_at(&fsm, &queue, 2).await;
    let owed = start.add(&UintN::from(5u64));
    assert!(owed < next, "file 1 holds the owed id");

    let (tx, mut rx) = mpsc::channel(4096);
    let mut ctx = ReadContext::new(queue.clone(), UintN::from(1u64), owed.clone(), 1, None, tx);
    ctx.cut_short = Some(UintN::from(1u64));
    let mut state = ReaderState::ReadNextFile {
        current_file: UintN::from(1u64),
        ctx,
        prefetch_handle: None,
    };
    while !state.is_terminal() {
        state = fsm.transition(state, 1).await.unwrap();
    }
    assert!(
        !matches!(state, ReaderState::Failed(_)),
        "the walk failed: {state:?}"
    );
    let mut want = owed.clone();
    while want < next {
        let got = rx.recv().await.unwrap();
        assert_eq!(got.id, want, "the walk skipped {}..{}", want, got.id);
        want = want.increment();
    }
    drop(rx);
    fs.close().await.unwrap();
}
