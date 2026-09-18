use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition};
use tempfile::TempDir;
use uintn::UintN;

async fn last_n(fs: &NormFS, q: &normfs::QueueId, n: u64) -> usize {
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    if fs
        .read(q, ReadPosition::ShiftFromTail(UintN::from(n - 1)), n, 1, tx)
        .await
        .is_err()
    {
        return 0;
    }
    let mut got = 0;
    while rx.recv().await.is_some() {
        got += 1;
    }
    got
}

#[tokio::test]
async fn memory_only_keeps_a_sliding_tail_window() {
    let temp_dir = TempDir::new().unwrap();
    let settings = NormFsSettings {
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::MEMORY),
        max_memory_usage: 64 * 1024,
        mem_page_size: 4096,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(temp_dir.path().to_path_buf(), settings)
        .await
        .unwrap();
    let q = fs.resolve("rover/events");
    fs.ensure_queue_exists_for_write(&q).await.unwrap();

    let record = Bytes::from(vec![7u8; 1000]);
    for i in 0u64..400 {
        fs.enqueue(&q, record.clone()).await.unwrap();
        let want = (i + 1).min(8);
        let got = last_n(&fs, &q, want).await;
        assert_eq!(
            got as u64, want,
            "after entry {i} only {got} of the last {want} records are readable"
        );
    }

    fs.close().await.unwrap();
}
