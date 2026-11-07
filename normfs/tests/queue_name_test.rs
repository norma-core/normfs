use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, ReadPosition};
use uintn::UintN;

#[tokio::test]
async fn a_queue_name_that_leaves_the_data_directory_is_refused() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
        .await
        .unwrap();

    for name in [
        "../../escaped",
        "/../escaped",
        "a/./b",
        "a/..",
        "./a",
        "a\u{0}b",
    ] {
        let queue = fs.resolve(name);
        assert!(
            matches!(
                fs.ensure_queue_exists_for_write(&queue).await,
                Err(Error::BadQueueName(_))
            ),
            "write {name}"
        );
        assert!(
            matches!(
                fs.ensure_queue_exists_for_read(&queue).await,
                Err(Error::BadQueueName(_))
            ),
            "read {name}"
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let read = fs.read(&queue, ReadPosition::ShiftFromTail(UintN::zero()), 1, 1, tx);
        assert!(
            matches!(read.await, Err(Error::BadQueueName(_))),
            "direct read {name}"
        );
        assert!(
            matches!(fs.close_queue(&queue).await, Err(Error::BadQueueName(_))),
            "close {name}"
        );
    }
    assert!(!root.path().join("escaped").exists());

    for name in ["plain", "a/b.c", "..a", "v1.2/x"] {
        let queue = fs.resolve(name);
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        fs.enqueue(&queue, Bytes::from_static(b"x")).await.unwrap();
    }
    fs.close().await.unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn a_windows_drive_or_unc_queue_name_is_refused() {
    let root = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(root.path().join("data"), NormFsSettings::all_active())
        .await
        .unwrap();

    for name in [
        "/C:/escaped",
        "C:escaped",
        "/\\\\server\\share\\q",
        "a\\..\\..\\b",
        "a/... ",
        "a./b",
    ] {
        let queue = fs.resolve(name);
        assert!(
            matches!(
                fs.ensure_queue_exists_for_write(&queue).await,
                Err(Error::BadQueueName(_))
            ),
            "write {name}"
        );
    }
    fs.close().await.unwrap();
}
