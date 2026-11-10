use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn only_records_that_exist_and_landed_are_durable() {
    let dir = TempDir::new().unwrap();
    let fs = NormFS::new(dir.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("mail/inbox");
    assert!(!fs.is_durable_through(&queue, 0));

    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"one"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !fs.is_durable_through(&queue, 0) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!fs.is_durable_through(&queue, 1));
    assert!(!fs.is_durable_through(&queue, 1_000_000));
    fs.close().await.unwrap();
}
