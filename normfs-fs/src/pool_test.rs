use std::fs;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;

use crate::{AppendOutcome, Fs, FsConfig, PublishSpec, Runs, Scan, ScanResult, TmpMode};

fn fs() -> Fs {
    Fs::new(FsConfig {
        threads: 2,
        ..FsConfig::default()
    })
    .unwrap()
}

fn runs(parts: &[&[u8]]) -> Runs {
    Runs(parts.iter().map(|p| Bytes::copy_from_slice(p)).collect())
}

#[tokio::test]
async fn publish_lands_the_bytes_and_leaves_no_temp() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("a.tmp");
    let dst = dir.path().join("a.store");
    let report = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst: dst.clone(),
                runs: runs(&[b"head", b"-", b"body"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.old_len, None);
    assert_eq!(report.new_len, 9);
    assert_eq!(fs::read(&dst).unwrap(), b"head-body");
    assert!(!tmp.exists());

    let report = fs
        .publish(
            PublishSpec {
                tmp,
                dst: dst.clone(),
                runs: runs(&[b"v2"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(report.old_len, Some(9));
    assert_eq!(fs::read(&dst).unwrap(), b"v2");
}

#[tokio::test]
async fn publish_failure_reports_errno_and_cleans_up() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("b.tmp");
    let dst = dir.path().join("missing").join("b.store");
    let err = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst,
                runs: runs(&[b"x"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(err).kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(!tmp.exists());
}

#[tokio::test]
async fn accounting_runs_on_the_executor_even_when_the_future_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let (tx, rx) = mpsc::channel::<(Option<u64>, u64, String)>();
    let then = Box::new(move |r: &crate::PublishReport| {
        let name = std::thread::current().name().unwrap_or("").to_string();
        let _ = tx.send((r.old_len, r.new_len, name));
    });
    let fut = fs.publish(
        PublishSpec {
            tmp: dir.path().join("c.tmp"),
            dst: dir.path().join("c.store"),
            runs: runs(&[b"abc"]),
            tmp_mode: TmpMode::Excl,
            sync: true,
        },
        Some(then),
    );
    let mut fut = Box::pin(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let _ = std::future::Future::poll(fut.as_mut(), &mut cx);
    drop(fut);

    let (old, new, thread) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(old, None);
    assert_eq!(new, 3);
    assert!(thread.starts_with("normfs-fs-"), "ran on {thread}");
    assert_eq!(fs::read(dir.path().join("c.store")).unwrap(), b"abc");
}

#[tokio::test]
async fn append_commits_whole_batches_and_cuts_back_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let path = dir.path().join("q.wal");
    let file = Arc::new(
        fs.create_durable(&path, runs(&[b"HDR"]), TmpMode::Excl, true)
            .await
            .unwrap(),
    );
    assert_eq!(fs::read(&path).unwrap(), b"HDR");

    let out = fs
        .append_sync(file.clone(), &path, 3, runs(&[b"one", b"two"]), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwo");

    crate::fault::fail_flushes(&path, 1);
    let out = fs
        .append_sync(file.clone(), &path, 9, runs(&[b"three"]), true)
        .await
        .unwrap();
    match out {
        AppendOutcome::Failed { err, restored } => {
            assert!(restored);
            assert_eq!(err.raw_os_error(), Some(libc::EIO));
        }
        AppendOutcome::Committed => panic!("committed through an injected failure"),
    }
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwo");

    let out = fs
        .append_sync(file.clone(), &path, 9, runs(&[b"three"]), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
    assert_eq!(fs::read(&path).unwrap(), b"HDRonetwothree");

    let out = fs
        .append_sync(file, &path, 14, Runs::default(), true)
        .await
        .unwrap();
    assert!(matches!(out, AppendOutcome::Committed));
}

#[tokio::test]
async fn restore_cuts_the_file_back() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let path = dir.path().join("r.wal");
    fs::write(&path, b"0123456789").unwrap();
    let file = Arc::new(fs::OpenOptions::new().write(true).open(&path).unwrap());
    fs.restore(file, &path, 4).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"0123");
}

#[tokio::test]
async fn markers_come_and_go_durably() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let marker = dir.path().join("closed");
    let f = fs
        .create_durable(&marker, Runs::default(), TmpMode::Trunc, true)
        .await
        .unwrap();
    drop(f);
    assert!(marker.is_file());
    assert!(fs.remove_durable(&marker, true).await.unwrap());
    assert!(!marker.exists());
    assert!(!fs.remove_durable(&marker, false).await.unwrap());

    let err = fs
        .create_durable(
            &dir.path().join("no").join("dir"),
            Runs::default(),
            TmpMode::Excl,
            true,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(err).kind(),
        std::io::ErrorKind::NotFound
    );
}

#[tokio::test]
async fn reads_stats_and_scans_run_off_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let wal = dir.path().join("wal");
    fs.mkdir_all(&wal.join("abc")).await.unwrap();
    fs::write(wal.join("abc").join("def.wal"), b"x").unwrap();
    fs::write(wal.join("abc").join("001.wal"), b"yy").unwrap();

    assert_eq!(
        fs.read_whole(&wal.join("abc").join("001.wal"))
            .await
            .unwrap(),
        Bytes::from_static(b"yy")
    );
    assert!(fs.stat(&wal.join("abc")).await.unwrap().unwrap().is_dir());
    assert!(fs.stat(&wal.join("nope")).await.unwrap().is_none());

    let max = fs.scan_ids(&wal, "wal", Scan::Max).await.unwrap();
    let min = fs.scan_ids(&wal, "wal", Scan::Min).await.unwrap();
    assert_eq!(max, ScanResult::One(uintn::UintN::from(0xabcdefu64)));
    assert_eq!(min, ScanResult::One(uintn::UintN::from(0xabc001u64)));
    assert_eq!(
        fs.scan_ids(&dir.path().join("none"), "wal", Scan::Max)
            .await
            .unwrap(),
        ScanResult::None
    );

    fs.unlink(&wal.join("abc").join("001.wal")).await.unwrap();
    assert!(fs.unlink(&wal.join("abc").join("001.wal")).await.is_err());
}

#[tokio::test]
async fn exclusive_publish_preserves_an_occupied_temp() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("occupied");
    fs::write(&tmp, b"owned by somebody else").unwrap();
    let error = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst: dir.path().join("target"),
                runs: runs(&[b"new"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(error).kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(fs::read(tmp).unwrap(), b"owned by somebody else");
}

#[tokio::test]
async fn publish_rejects_equal_paths_without_truncating() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let dst = dir.path().join("target");
    fs::write(&dst, b"old").unwrap();
    for tmp in [dst.clone(), dir.path().join(".").join("target")] {
        let error = fs
            .publish(
                PublishSpec {
                    tmp,
                    dst: dst.clone(),
                    runs: runs(&[b"new"]),
                    tmp_mode: TmpMode::Trunc,
                    sync: true,
                },
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(
            std::io::Error::from(error).kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(&dst).unwrap(), b"old");
    }
}

#[tokio::test]
async fn workers_survive_panicking_closures_and_accounting() {
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        fs.run_blocking(|| -> std::io::Result<()> { panic!("job") })
            .await,
        Err(crate::FsError::JobPanicked)
    ));
    let dir = tempfile::tempdir().unwrap();
    let result = fs
        .publish(
            PublishSpec {
                tmp: dir.path().join("tmp"),
                dst: dir.path().join("dst"),
                runs: runs(&[b"committed"]),
                tmp_mode: TmpMode::Excl,
                sync: true,
            },
            Some(Box::new(|_| panic!("accounting"))),
        )
        .await;
    assert!(result.is_ok());
    assert_eq!(
        fs.read_whole(&dir.path().join("dst")).await.unwrap(),
        "committed"
    );
}

#[tokio::test]
async fn bounded_admission_waits_and_cancellation_does_not_submit() {
    use std::future::Future;
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let slots = fs.slots.available_permits();
    let (release, blocked) = mpsc::channel();
    let mut first = Box::pin(fs.run_blocking(move || {
        blocked.recv().unwrap();
        Ok(())
    }));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(first.as_mut().poll(&mut cx).is_pending());
    let mut queued = Vec::new();
    for _ in 1..slots {
        let mut f = Box::pin(fs.run_blocking(|| Ok(())));
        assert!(f.as_mut().poll(&mut cx).is_pending());
        queued.push(f);
    }
    assert_eq!(fs.slots.available_permits(), 0);
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = ran.clone();
    let mut waiting = Box::pin(fs.run_blocking(move || {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(waiting);
    release.send(()).unwrap();
    first.await.unwrap();
    for f in queued {
        f.await.unwrap();
    }
    fs.run_blocking(|| Ok(())).await.unwrap();
    assert!(!ran.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn publish_rejects_a_dangling_temp_symlink_to_the_destination() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("link");
    let dst = dir.path().join("destination");
    std::os::unix::fs::symlink(&dst, &tmp).unwrap();
    let fs = fs();
    let error = fs
        .publish(
            PublishSpec {
                tmp: tmp.clone(),
                dst: dst.clone(),
                runs: runs(&[b"data"]),
                tmp_mode: TmpMode::Trunc,
                sync: true,
            },
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(
        std::io::Error::from(error).raw_os_error(),
        Some(libc::ELOOP)
    );
    assert!(tmp.is_symlink());
    assert!(!dst.exists());
}

#[tokio::test]
async fn cached_inode_append_submits_exactly_one_job() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Counting {
        inner: Arc<dyn crate::executor::Executor>,
        submissions: Arc<AtomicUsize>,
    }
    impl crate::executor::Executor for Counting {
        fn submit(&self, job: crate::executor::Job) -> Result<(), crate::FsError> {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            self.inner.submit(job)
        }
        fn name(&self) -> &'static str {
            self.inner.name()
        }
    }
    let mut fs = fs();
    let submissions = Arc::new(AtomicUsize::new(0));
    fs.exec = Arc::new(Counting {
        inner: fs.exec.clone(),
        submissions: submissions.clone(),
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    let (file, inode) = fs
        .create_durable_with_inode(&path, runs(&[b"header"]), TmpMode::Excl, true)
        .await
        .unwrap();
    let file = Arc::new(file);
    use std::os::unix::fs::MetadataExt;
    assert_eq!(inode, fs.metadata(file.clone()).await.unwrap().ino());
    submissions.store(0, Ordering::SeqCst);
    assert!(matches!(
        fs.append_sync_with_inode(file, inode, &path, 6, runs(&[b"batch"]), true)
            .await
            .unwrap(),
        AppendOutcome::Committed
    ));
    assert_eq!(submissions.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(&path).unwrap(), b"headerbatch");
}
