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
        "q/\u{17f}tore",
        "q/clo\u{17f}ed",
        "q/\u{fb06}ore",
        "q/\u{fb05}ore",
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
    // The second pair is composed and decomposed é, which the start lock does not fold.
    for (one, two) in [
        ("cam/Front", "cam/front"),
        ("cam/caf\u{e9}", "cam/cafe\u{301}"),
    ] {
        let root = tempfile::TempDir::new().unwrap();
        let data = root.path().join("data");
        let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
            .await
            .unwrap();
        let (one, two) = (fs.resolve(one), fs.resolve(two));
        let probe = one.to_fs_path(&data);
        std::fs::create_dir_all(&probe).unwrap();
        let folds = two.to_fs_path(&data).exists();
        std::fs::remove_dir(&probe).unwrap();
        if !folds {
            fs.close().await.unwrap();
            continue;
        }

        let (a, b) = tokio::join!(
            fs.ensure_queue_exists_for_write(&one),
            fs.ensure_queue_exists_for_write(&two)
        );
        assert!(
            a.is_ok() != b.is_ok(),
            "exactly one spelling may start: {a:?} {b:?}"
        );
        assert!(matches!(a.err().or(b.err()), Some(Error::BadQueueName(_))));
        fs.close().await.unwrap();
    }
}

#[tokio::test]
async fn closing_two_spellings_started_for_read_marks_one_directory() {
    // The second pair is composed and decomposed é, which the start lock does not fold.
    for (one, two) in [
        ("cam/Front", "cam/front"),
        ("cam/caf\u{e9}", "cam/cafe\u{301}"),
    ] {
        let root = tempfile::TempDir::new().unwrap();
        let data = root.path().join("data");
        let fs = NormFS::new(data.clone(), NormFsSettings::all_active())
            .await
            .unwrap();
        let (one, two) = (fs.resolve(one), fs.resolve(two));
        let probe = one.to_fs_path(&data);
        std::fs::create_dir_all(&probe).unwrap();
        let folds = two.to_fs_path(&data).exists();
        std::fs::remove_dir(&probe).unwrap();
        if !folds {
            fs.close().await.unwrap();
            continue;
        }

        // Started for read, neither has a directory yet, so both are let in.
        fs.ensure_queue_exists_for_read(&one).await.unwrap();
        fs.ensure_queue_exists_for_read(&two).await.unwrap();

        let (a, b) = tokio::join!(fs.close_queue(&one), fs.close_queue(&two));
        assert!(
            a.is_ok() != b.is_ok(),
            "exactly one close may mark: {a:?} {b:?}"
        );
        let names: Vec<_> = std::fs::read_dir(probe.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
        fs.close().await.unwrap();
    }
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
    // Started for read first, then written: the write is what fixes the spelling.
    let first_read = fs.resolve("mem/R");
    fs.ensure_queue_exists_for_read(&first_read).await.unwrap();
    fs.ensure_queue_exists_for_write(&first_read).await.unwrap();
    assert!(matches!(
        fs.ensure_queue_exists_for_write(&fs.resolve("mem/r")).await,
        Err(Error::BadQueueName(_))
    ));
    // Started for read, then another spelling writes first and owns the directory.
    let read_first = fs.resolve("mem/S");
    fs.ensure_queue_exists_for_read(&read_first).await.unwrap();
    fs.ensure_queue_exists_for_write(&fs.resolve("mem/s"))
        .await
        .unwrap();
    assert!(matches!(
        fs.ensure_queue_exists_for_write(&read_first).await,
        Err(Error::BadQueueName(_))
    ));
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

#[tokio::test]
async fn normfs_own_names_cannot_be_taken_under_another_spelling() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let quiet = || NormFsSettings {
        system_queue: false,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(data.clone(), quiet()).await.unwrap();
    if !folds(&data) {
        fs.close().await.unwrap();
        return;
    }

    let upper = format!("/{}/x", fs.get_instance_id().to_uppercase());
    for name in [upper.as_str(), "normfs/System", "NORMFS/system"] {
        assert!(
            matches!(
                fs.ensure_queue_exists_for_write(&fs.resolve(name)).await,
                Err(Error::BadQueueName(_))
            ),
            "{name}"
        );
    }
    fs.ensure_queue_exists_for_write(&fs.resolve("y"))
        .await
        .unwrap();
    fs.close().await.unwrap();

    let fs = NormFS::new(data, NormFsSettings::all_active())
        .await
        .unwrap();
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_directory_normfs_made_is_not_a_queue_to_close() {
    let root = tempfile::TempDir::new().unwrap();
    let settings = NormFsSettings {
        system_queue: false,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(root.path().join("data"), settings)
        .await
        .unwrap();
    assert!(matches!(
        fs.close_queue(&fs.resolve("normfs")).await,
        Err(Error::QueueNotFound)
    ));
    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_memory_queue_reopened_and_restarted_can_still_be_closed() {
    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let settings = || NormFsSettings {
        queue_settings: QueueSettings::all_active().with_default_persist(Persist::MEMORY),
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(data.clone(), settings()).await.unwrap();
    let queue = fs.resolve("m");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.enqueue(&queue, Bytes::from_static(b"x")).await.unwrap();
    fs.close_queue(&queue).await.unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close().await.unwrap();

    let fs = NormFS::new(data, settings()).await.unwrap();
    fs.close_queue(&fs.resolve("m")).await.unwrap();
    fs.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_read_only_data_directory_still_opens() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempfile::TempDir::new().unwrap();
    let data = root.path().join("data");
    let quiet = || NormFsSettings {
        system_queue: false,
        ..NormFsSettings::all_active()
    };
    let fs = NormFS::new(data.clone(), quiet()).await.unwrap();
    let instance = data.join(fs.get_instance_id());
    fs.close().await.unwrap();
    std::fs::remove_dir_all(instance.join("normfs")).unwrap();
    std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o500)).unwrap();

    let opened = NormFS::new(data, quiet()).await;
    std::fs::set_permissions(&instance, std::fs::Permissions::from_mode(0o700)).unwrap();
    opened.unwrap().close().await.unwrap();
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
