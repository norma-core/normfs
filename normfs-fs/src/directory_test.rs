use std::fs;
use std::future::Future;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::{Path, PathBuf};

use crate::{Fs, FsConfig};
#[cfg(unix)]
use crate::{PublishSpec, TmpMode};

#[cfg(unix)]
struct RestorePermissions(PathBuf);

#[cfg(unix)]
impl Drop for RestorePermissions {
    fn drop(&mut self) {
        fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700)).unwrap();
    }
}

#[cfg(unix)]
fn permissions(path: &Path, mode: u32) -> RestorePermissions {
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    RestorePermissions(path.to_owned())
}

fn fs() -> Fs {
    Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap()
}

// chmod of an ancestor is how these reach the failure paths.
#[cfg(unix)]
#[tokio::test]
async fn publication_below_a_search_only_ancestor() {
    let root = tempfile::tempdir().unwrap();
    let base = root.path().join("service");
    fs::create_dir(&base).unwrap();
    let _restore = permissions(root.path(), 0o111);
    assert_eq!(
        fs::File::open(root.path()).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let fs = fs();
    let leaf = base.join("queue/wal");
    fs.mkdir_all(&leaf).await.unwrap();
    fs.mkdir_all(&leaf).await.unwrap();
    let dst = leaf.join("target");
    fs.publish(
        PublishSpec {
            tmp: leaf.join("tmp"),
            dst: dst.clone(),
            runs: crate::Runs(vec![bytes::Bytes::from_static(b"committed")]),
            tmp_mode: TmpMode::Excl,
            sync: true,
        },
        None,
    )
    .await
    .unwrap();
    assert_eq!(fs.read_whole(&dst).await.unwrap(), b"committed"[..]);
}

#[cfg(unix)]
#[tokio::test]
async fn a_failed_parent_sync_is_retried_across_fs_instances() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("new");
    let restore = permissions(root.path(), 0o300);
    assert_eq!(
        fs::File::open(root.path()).unwrap_err().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    let first = fs();
    assert!(first.mkdir_all(&leaf).await.is_err());
    assert!(leaf.is_dir());
    let second = fs();
    assert!(second.mkdir_all(&leaf).await.is_err());
    drop(restore);
    second.mkdir_all(&leaf).await.unwrap();
    let _restore = permissions(root.path(), 0o111);
    second.mkdir_all(&leaf).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn a_removal_after_a_failed_parent_sync_restarts_creation() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("new");
    let fs = fs();
    {
        let _restore = permissions(root.path(), 0o300);
        assert!(fs.mkdir_all(&leaf).await.is_err());
    }
    assert!(leaf.is_dir());
    fs.remove_dir_all(&leaf).await.unwrap();
    fs.mkdir_all(&leaf).await.unwrap();
    assert!(leaf.is_dir());
}

#[tokio::test]
async fn concurrent_creation_shares_completion_for_the_same_directory() {
    let root = tempfile::tempdir().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    let fs = Fs::new(FsConfig {
        threads: 4,
        ..Default::default()
    })
    .unwrap();
    for i in 0..48 {
        let fs = fs.clone();
        let path = root.path().join(format!("shared/queue-{}/wal", i % 12));
        tasks.spawn(async move {
            fs.mkdir_all(&path).await.unwrap();
            fs.mkdir_all(&path).await.unwrap();
            assert!(path.is_dir());
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
}

#[tokio::test]
async fn a_creation_overlapping_a_removal_is_not_cached() {
    let root = tempfile::tempdir().unwrap();
    let leaf = root.path().join("queue/wal");
    let fs = fs();
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);

    let (release, hold) = std::sync::mpsc::channel::<()>();
    let mut blocker = Box::pin(fs.run_blocking(move || {
        hold.recv().unwrap();
        Ok(())
    }));
    assert!(blocker.as_mut().poll(&mut cx).is_pending());

    let mut create = Box::pin(fs.mkdir_all(&leaf));
    assert!(create.as_mut().poll(&mut cx).is_pending());
    let mut remove = Box::pin(fs.remove_dir_all(&leaf));
    assert!(remove.as_mut().poll(&mut cx).is_pending());

    release.send(()).unwrap();
    blocker.await.unwrap();
    create.await.unwrap();
    remove.await.unwrap();
    assert!(!leaf.exists());

    fs.mkdir_all(&leaf).await.unwrap();
    assert!(leaf.is_dir());
}

#[tokio::test]
async fn only_files_with_the_extension_are_removed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("tmp");
    fs::create_dir_all(dir.join("nested.tmp")).unwrap();
    fs::write(dir.join("a.tmp"), b"").unwrap();
    fs::write(dir.join("b.tmp"), b"").unwrap();
    fs::write(dir.join("keep.store"), b"").unwrap();
    let fs = fs();

    assert_eq!(fs.remove_files_with_ext(&dir, "tmp").await.unwrap(), 2);
    assert!(!dir.join("a.tmp").exists());
    assert!(dir.join("keep.store").exists());
    assert!(dir.join("nested.tmp").is_dir());
    assert_eq!(
        fs.remove_files_with_ext(&root.path().join("missing"), "tmp")
            .await
            .unwrap(),
        0
    );
}

#[cfg(unix)]
#[tokio::test]
async fn a_link_with_the_extension_is_removed_and_not_its_target() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("tmp");
    fs::create_dir_all(&dir).unwrap();
    fs::write(root.path().join("target"), b"kept").unwrap();
    std::os::unix::fs::symlink(root.path().join("target"), dir.join("a.tmp")).unwrap();
    std::os::unix::fs::symlink(root.path().join("gone"), dir.join("b.tmp")).unwrap();
    let fs = Fs::new(crate::FsConfig {
        root: Some(root.path().to_path_buf()),
        ..crate::FsConfig::default()
    })
    .unwrap();

    assert_eq!(fs.remove_files_with_ext(&dir, "tmp").await.unwrap(), 2);
    assert!(fs::symlink_metadata(dir.join("a.tmp")).is_err());
    assert_eq!(fs::read(root.path().join("target")).unwrap(), b"kept");
}

#[cfg(unix)]
#[tokio::test]
async fn without_a_data_directory_only_files_with_the_extension_go() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("tmp");
    fs::create_dir_all(root.path().join("target")).unwrap();
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("a.tmp"), b"").unwrap();
    std::os::unix::fs::symlink(root.path().join("target"), dir.join("b.tmp")).unwrap();
    std::os::unix::fs::symlink(root.path().join("gone"), dir.join("c.tmp")).unwrap();

    assert_eq!(fs().remove_files_with_ext(&dir, "tmp").await.unwrap(), 1);
    assert!(fs::symlink_metadata(dir.join("b.tmp")).is_ok());
    assert!(fs::symlink_metadata(dir.join("c.tmp")).is_ok());
}

#[cfg(unix)]
#[tokio::test]
async fn without_a_data_directory_a_trailing_slash_is_kept() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("target")).unwrap();
    let link = root.path().join("ln");
    std::os::unix::fs::symlink(root.path().join("target"), &link).unwrap();
    let slashed = PathBuf::from(format!("{}/", link.display()));

    assert!(fs().unlink(&slashed).await.is_err());
    assert!(fs::symlink_metadata(&link).is_ok());
}

#[test]
fn an_empty_data_directory_is_refused() {
    let made = Fs::new(crate::FsConfig {
        root: Some(std::path::PathBuf::new()),
        ..crate::FsConfig::default()
    });

    assert_eq!(
        made.err().map(|e| e.kind()),
        Some(std::io::ErrorKind::InvalidInput)
    );
}
