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
async fn publish_replaces_a_file_a_reader_holds_open() {
    use std::io::Read;

    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let tmp = dir.path().join("a.tmp");
    let dst = dir.path().join("a.store");
    let spec = |body: &[u8]| PublishSpec {
        tmp: tmp.clone(),
        dst: dst.clone(),
        runs: runs(&[body]),
        tmp_mode: TmpMode::Excl,
        sync: true,
    };
    fs.publish(spec(b"old"), None).await.unwrap();
    let mut reader = fs::File::open(&dst).unwrap();

    fs.publish(spec(b"new"), None).await.unwrap();
    assert_eq!(fs::read(&dst).unwrap(), b"new");
    let mut held = Vec::new();
    reader.read_to_end(&mut held).unwrap();
    assert_eq!(held, b"old");
}

#[tokio::test]
async fn publish_lands_below_a_path_past_the_windows_limit() {
    let dir = tempfile::tempdir().unwrap();
    let fs = fs();
    let mut deep = dir.path().to_path_buf();
    for _ in 0..6 {
        deep.push("d".repeat(50));
    }
    fs::create_dir_all(&deep).unwrap();
    let dst = deep.join("a.store");
    assert!(dst.as_os_str().len() > 300);
    fs.publish(
        PublishSpec {
            tmp: deep.join("a.tmp"),
            dst: dst.clone(),
            runs: runs(&[b"deep"]),
            tmp_mode: TmpMode::Excl,
            sync: true,
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(fs::read(&dst).unwrap(), b"deep");
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
            assert_eq!(err.raw_os_error(), Some(crate::os::EIO));
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

#[cfg(unix)]
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
    assert_eq!(inode, crate::os::inode(&file).unwrap());
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

#[cfg(windows)]
#[tokio::test]
async fn publish_refuses_a_temp_symlink_and_leaves_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let tmp = dir.path().join("link");
    let target = dir.path().join("target");
    let dst = dir.path().join("destination");
    std::fs::write(&target, b"keep").unwrap();
    // Creating a symlink needs the privilege or Developer Mode; CI runners have it.
    if let Err(e) = std::os::windows::fs::symlink_file(&target, &tmp) {
        assert_eq!(e.raw_os_error(), Some(1314), "{e}");
        return;
    }
    let error = fs()
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
    assert_eq!(std::io::Error::from(error).raw_os_error(), Some(1921));
    assert_eq!(std::fs::read(&target).unwrap(), b"keep");
    assert!(!dst.exists());
}

#[cfg(windows)]
#[tokio::test]
async fn remove_frees_the_name_while_a_reader_holds_the_file() {
    use std::os::windows::fs::OpenOptionsExt;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held");
    std::fs::write(&path, b"old").unwrap();
    let reader = std::fs::OpenOptions::new()
        .read(true)
        .share_mode(7)
        .open(&path)
        .unwrap();
    fs().remove_durable(&path, true).await.unwrap();
    assert!(!path.exists());
    std::fs::write(&path, b"new").unwrap();
    drop(reader);
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
}

#[cfg(unix)]
#[tokio::test]
async fn remove_dir_all_takes_a_link_and_not_what_it_points_at() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir(&real).unwrap();
    std::fs::write(real.join("kept"), b"x").unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let rooted = Fs::new(FsConfig {
        root: Some(dir.path().to_path_buf()),
        ..FsConfig::default()
    })
    .unwrap();

    fs().remove_dir_all(&link).await.unwrap();
    assert!(real.join("kept").exists());
    assert!(std::fs::symlink_metadata(&link).is_err());

    std::os::unix::fs::symlink(&real, &link).unwrap();
    rooted.remove_dir_all(&link).await.unwrap();
    assert!(real.join("kept").exists());
    assert!(std::fs::symlink_metadata(&link).is_err());
}

/// A subdirectory swapped for a link to `outside` while the removal runs.
#[cfg(unix)]
async fn swap_during_remove_dir_all(rooted: bool) -> usize {
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut lost = 0;
    for _ in 0..200 {
        let base = tempfile::tempdir().unwrap();
        let outside = base.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("keep"), b"x").unwrap();
        let top = base.path().join("top");
        let sub = top.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        for i in 0..50 {
            std::fs::write(sub.join(format!("f{i}")), b"").unwrap();
        }
        std::fs::write(sub.join("keep"), b"y").unwrap();
        let fs = Fs::new(FsConfig {
            root: rooted.then(|| base.path().to_path_buf()),
            ..FsConfig::default()
        })
        .unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let swapper = {
            let (stop, sub, outside) = (stop.clone(), sub.clone(), outside.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if std::fs::rename(&sub, sub.with_file_name("moved")).is_ok() {
                        let _ = std::os::unix::fs::symlink(&outside, &sub);
                        return;
                    }
                }
            })
        };
        let _ = fs.remove_dir_all(&top).await;
        stop.store(true, Ordering::Relaxed);
        swapper.join().unwrap();
        if !outside.join("keep").exists() {
            lost += 1;
        }
    }
    lost
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn remove_dir_all_deletes_nothing_through_a_directory_swapped_for_a_link() {
    assert_eq!(swap_during_remove_dir_all(false).await, 0);
    assert_eq!(swap_during_remove_dir_all(true).await, 0);
}

#[tokio::test]
async fn remove_dir_all_refuses_a_file() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    std::fs::write(&file, b"x").unwrap();
    assert!(fs().remove_dir_all(&file).await.is_err());
    assert!(file.exists());
}
