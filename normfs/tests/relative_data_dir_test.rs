//! Alone in its binary: it changes the working directory.

use std::path::Path;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};

#[tokio::test]
async fn a_relative_data_directory_not_made_yet_opens() {
    let cwd = tempfile::TempDir::new().unwrap();
    std::env::set_current_dir(cwd.path()).unwrap();
    let fs = NormFS::new(
        Path::new("rel/data").to_path_buf(),
        NormFsSettings::all_active(),
    )
    .await
    .unwrap();
    let queue = fs.resolve("q");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"record"))
        .await
        .unwrap();
    fs.close().await.unwrap();

    let made = std::fs::read_dir(cwd.path().join("rel/data"))
        .unwrap()
        .count();
    assert!(made > 0);

    let fs = NormFS::new(Path::new("").to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("q");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"record"))
        .await
        .unwrap();
    fs.close().await.unwrap();
    assert!(std::fs::read_dir(cwd.path()).unwrap().count() > 1);
    std::env::set_current_dir(std::env::temp_dir()).unwrap();
}
