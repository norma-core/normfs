use bytes::Bytes;
use normfs::{Error, NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition};
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
        "parent/wal/001.wal",
        "parent/store",
        "parent/closed",
        "parent/Closed",
        "a/WAL/b",
        "/tmp/x",
        "/TMP/x",
        "//tmp/x",
        "parent//wal",
        "//.memory_pointers",
        "/.memory_pointers",
        "/",
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

    for name in ["plain", "a/b.c", "..a", "v1.2/x", "wallet/x", "tmp/x", ".a"] {
        let queue = fs.resolve(name);
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        fs.enqueue(&queue, Bytes::from_static(b"x")).await.unwrap();
    }
    fs.close().await.unwrap();
}

#[tokio::test]
async fn separator_variants_are_one_queue() {
    let root = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(root.path().join("data"), NormFsSettings::all_active())
        .await
        .unwrap();

    let queue = fs.resolve("a/b");
    for name in ["a//b", "a/b/", "a///b//"] {
        assert_eq!(fs.resolve(name), queue, "{name}");
    }
    assert_eq!(fs.resolve("//x//y/"), fs.resolve("/x/y"));

    let system = fs.resolve("normfs//system/");
    assert!(matches!(
        fs.enqueue(&system, Bytes::from_static(b"x")).await,
        Err(Error::ReservedQueue)
    ));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn another_spelling_of_an_existing_queue_is_refused() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
        .await
        .unwrap();

    let queue = fs.resolve("cam/Front");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"x")).await.unwrap();
    let dir = queue.to_fs_path(&data);
    if !folds(&data) {
        fs.close().await.unwrap();
        return;
    }

    for name in ["cam/front", "CAM/Front"] {
        let other = fs.resolve(name);
        assert!(
            matches!(
                fs.ensure_queue_exists_for_write(&other).await,
                Err(Error::BadQueueName(_))
            ),
            "write {name}"
        );
        assert!(
            matches!(
                fs.ensure_queue_exists_for_read(&other).await,
                Err(Error::BadQueueName(_))
            ),
            "read {name}"
        );
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let read = fs.read(&other, ReadPosition::ShiftFromTail(UintN::zero()), 1, 1, tx);
        assert!(
            matches!(read.await, Err(Error::BadQueueName(_))),
            "direct read {name}"
        );
        assert!(
            matches!(fs.close_queue(&other).await, Err(Error::BadQueueName(_))),
            "close {name}"
        );
    }
    assert!(
        !dir.join("closed").exists(),
        "a refused close must not close cam/Front"
    );

    fs.close_queue(&queue).await.unwrap();
    assert!(
        matches!(
            fs.ensure_queue_exists_for_write(&fs.resolve("cam/front"))
                .await,
            Err(Error::BadQueueName(_))
        ),
        "reopen through another spelling"
    );
    assert!(
        dir.join("closed").exists(),
        "a refused reopen must leave the marker"
    );
    fs.close().await.unwrap();
}

fn folds(data: &std::path::Path) -> bool {
    let probe = data.join("Fold-Probe");
    std::fs::create_dir_all(&probe).unwrap();
    let folds = data.join("fold-probe").exists();
    std::fs::remove_dir(&probe).unwrap();
    folds
}

#[tokio::test]
async fn two_first_writes_under_two_spellings_leave_one_queue() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
        .await
        .unwrap();
    if !folds(&data) {
        fs.close().await.unwrap();
        return;
    }

    let (upper, lower) = (fs.resolve("cam/Front"), fs.resolve("cam/front"));
    let (a, b) = tokio::join!(
        fs.ensure_queue_exists_for_write(&upper),
        fs.ensure_queue_exists_for_write(&lower)
    );
    assert!(
        a.is_ok() != b.is_ok(),
        "exactly one spelling may start: {a:?} {b:?}"
    );
    assert!(matches!(a.err().or(b.err()), Some(Error::BadQueueName(_))));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_queue_is_held_to_its_spelling_too() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let settings = NormFsSettings {
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::MEMORY),
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(data.clone(), settings).await.unwrap();
    if !folds(&data) {
        fs.close().await.unwrap();
        return;
    }

    let queue = fs.resolve("mem/Q");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"x")).await.unwrap();
    let other = fs.resolve("mem/q");
    assert!(matches!(
        fs.ensure_queue_exists_for_write(&other).await,
        Err(Error::BadQueueName(_))
    ));
    assert!(matches!(
        fs.close_queue(&other).await,
        Err(Error::BadQueueName(_))
    ));
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
