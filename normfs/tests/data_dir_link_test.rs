#![cfg(unix)]

use std::path::Path;
use std::time::Duration;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use tokio::sync::mpsc;
use tokio::time::timeout;
use uintn::UintN;

fn files_under(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let path = e.unwrap().path();
            if path.is_dir() {
                files_under(&path)
            } else {
                1
            }
        })
        .sum()
}

#[tokio::test]
async fn a_queue_behind_a_link_writes_nothing_outside_the_data_directory() {
    let data = tempfile::TempDir::new().unwrap();
    let outside = tempfile::TempDir::new().unwrap();
    std::os::unix::fs::symlink(outside.path(), data.path().join("redirect")).unwrap();
    let fs = NormFS::new(data.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("/redirect/new");

    let started = fs.ensure_queue_exists_for_write(&queue).await;
    if started.is_ok() {
        let _ = fs.enqueue(&queue, Bytes::from_static(b"escaped")).await;
    }
    let _ = fs.close().await;

    assert!(started.is_err(), "a start through a link must fail");
    assert_eq!(files_under(outside.path()), 0);
}

#[tokio::test]
async fn a_queue_moved_behind_a_link_is_not_read_through_it() {
    let data = tempfile::TempDir::new().unwrap();
    let outside = tempfile::TempDir::new().unwrap();
    {
        let fs = NormFS::new(data.path().to_path_buf(), NormFsSettings::all_active())
            .await
            .unwrap();
        let queue = fs.resolve("/moved/q");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        fs.enqueue(&queue, Bytes::from_static(b"record"))
            .await
            .unwrap();
        fs.close().await.unwrap();
    }
    std::fs::rename(data.path().join("moved"), outside.path().join("moved")).unwrap();
    std::os::unix::fs::symlink(outside.path().join("moved"), data.path().join("moved")).unwrap();

    let fs = NormFS::new(data.path().to_path_buf(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("/moved/q");
    let (tx, mut rx) = mpsc::channel(4);
    let read = match fs.ensure_queue_exists_for_read(&queue).await {
        Ok(()) => fs
            .read(&queue, ReadPosition::Absolute(UintN::zero()), 1, 1, tx)
            .await
            .map(|_| ()),
        Err(e) => Err(e),
    };
    let got = timeout(Duration::from_secs(2), rx.recv()).await;
    let _ = fs.close().await;

    assert!(
        !matches!(got, Ok(Some(_))),
        "a record behind a link must not be read, read result {read:?}"
    );
}

#[tokio::test]
async fn a_data_directory_that_is_a_link_opens_and_writes() {
    let real = tempfile::TempDir::new().unwrap();
    let parent = tempfile::TempDir::new().unwrap();
    let data = parent.path().join("data");
    std::os::unix::fs::symlink(real.path(), &data).unwrap();
    let fs = NormFS::new(data, NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("q");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"record"))
        .await
        .unwrap();
    fs.close().await.unwrap();

    assert!(files_under(real.path()) > 0);
}

#[tokio::test]
async fn a_queue_name_climbing_out_creates_nothing_above_the_data_directory() {
    let parent = tempfile::TempDir::new().unwrap();
    let data = parent.path().join("data");
    let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
        .await
        .unwrap();
    let queue = fs.resolve("/../escape");
    let started = fs.ensure_queue_exists_for_write(&queue).await;
    let _ = fs.close().await;

    assert!(started.is_err());
    assert!(!parent.path().join("escape").exists());
}

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

    assert!(files_under(&cwd.path().join("rel/data")) > 0);
}
