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

#[tokio::test]
async fn exact_names_refuse_only_another_spelling_of_what_is_there() {
    let temp = tempfile::tempdir().unwrap();
    let fs = fs();
    fs::create_dir_all(temp.path().join("a").join("b")).unwrap();

    assert!(fs.exact_names(temp.path(), "a/b").await.unwrap());
    assert!(fs.exact_names(temp.path(), "a/b/c/d").await.unwrap());
    assert!(fs.exact_names(temp.path(), "x/y").await.unwrap());
    assert!(fs.exact_names(temp.path(), "").await.unwrap());
    // Absent where names are exact, the same directory where the filesystem folds case.
    let folds = temp.path().join("A").exists();
    assert_eq!(fs.exact_names(temp.path(), "A/b").await.unwrap(), !folds);
    assert_eq!(fs.exact_names(temp.path(), "a/B/c").await.unwrap(), !folds);

    fs::create_dir(temp.path().join("\u{e9}")).unwrap();
    let decomposed = "e\u{301}";
    let folds = temp.path().join(decomposed).exists();
    assert_eq!(
        fs.exact_names(temp.path(), decomposed).await.unwrap(),
        !folds
    );
    assert!(fs.exact_names(temp.path(), "\u{e9}").await.unwrap());
}

#[cfg(windows)]
#[tokio::test]
async fn exact_names_refuse_a_short_name() {
    let temp = tempfile::tempdir().unwrap();
    let long = temp.path().join("inference-states");
    fs::create_dir(&long).unwrap();
    // cmd prints the 8.3 alias, or the long name where the volume makes none.
    let out = std::process::Command::new("cmd")
        .args([
            "/c",
            &format!("for %I in ({}) do @echo %~snxI", long.display()),
        ])
        .output()
        .unwrap();
    let short = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if short.is_empty() || short == "inference-states" {
        return;
    }
    assert!(temp.path().join(&short).exists());
    assert!(!fs().exact_names(temp.path(), &short).await.unwrap());
    assert!(
        fs().exact_names(temp.path(), "inference-states")
            .await
            .unwrap()
    );
}
