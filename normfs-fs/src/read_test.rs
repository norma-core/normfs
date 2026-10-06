use std::fs;
use std::sync::mpsc;

use crate::{Fs, FsConfig};

#[tokio::test]
async fn pool_reader_keeps_bytes_when_a_read_is_cancelled_or_resized() {
    use std::future::Future;
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, b"0123456789").unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut reader = fs.open_read(&path).await.unwrap();
    let (release, blocked) = mpsc::channel();
    let mut blocker = Box::pin(fs.run_blocking(move || {
        blocked.recv().unwrap();
        Ok(())
    }));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(blocker.as_mut().poll(&mut cx).is_pending());
    let mut large = [0; 10];
    let mut read = Box::pin(reader.read(&mut large));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    drop(read);
    release.send(()).unwrap();
    blocker.await.unwrap();
    let mut small = [0; 3];
    reader.read_exact(&mut small).await.unwrap();
    assert_eq!(&small, b"012");
    let mut rest = Vec::new();
    reader.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest, b"3456789");
    reader.seek(std::io::SeekFrom::End(-2)).await.unwrap();
    rest.clear();
    reader.read_to_end(&mut rest).await.unwrap();
    assert_eq!(rest, b"89");
}

#[tokio::test]
async fn sequential_reads_and_seek_reuse_the_allocation() {
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, vec![0x5a; 3 * 1024 * 1024]).unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut reader = fs.open_read(&path).await.unwrap();
    let mut block = vec![0; 1024 * 1024];
    reader.read_exact(&mut block).await.unwrap();
    let allocation = reader.buffer.as_ptr();
    for _ in 0..2 {
        reader.read_exact(&mut block).await.unwrap();
        assert_eq!(reader.buffer.as_ptr(), allocation);
        assert!(block.iter().all(|&b| b == 0x5a));
    }
    reader.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    reader.read_exact(&mut block[..4096]).await.unwrap();
    assert_eq!(reader.buffer.as_ptr(), allocation);
}

fn counting(fs: &mut Fs) -> std::sync::Arc<std::sync::atomic::AtomicUsize> {
    use std::sync::Arc;
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
    let submissions = Arc::new(AtomicUsize::new(0));
    fs.exec = Arc::new(Counting {
        inner: fs.exec.clone(),
        submissions: submissions.clone(),
    });
    submissions
}

#[tokio::test]
async fn a_header_probe_fills_the_smallest_window() {
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, vec![1; 2 * 1024 * 1024]).unwrap();
    let fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut reader = fs.open_read(&path).await.unwrap();
    let mut probe = [0; 128];
    reader.read_exact(&mut probe).await.unwrap();
    assert_eq!(reader.buffer.len(), 64 * 1024);
}

#[tokio::test]
async fn small_sequential_reads_share_a_growing_window() {
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let size = 3 * 1024 * 1024;
    fs::write(&path, (0..size).map(|i| i as u8).collect::<Vec<_>>()).unwrap();
    let mut fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let submissions = counting(&mut fs);
    let mut reader = fs.open_read(&path).await.unwrap();
    submissions.store(0, Ordering::SeqCst);
    let mut piece = vec![0; 32 * 1024];
    for i in 0..size / piece.len() {
        // The benchmark's pattern: an explicit seek to where the reader already is.
        reader
            .seek(std::io::SeekFrom::Start((i * piece.len()) as u64))
            .await
            .unwrap();
        reader.read_exact(&mut piece).await.unwrap();
        assert_eq!(piece[0], (i * piece.len()) as u8);
    }
    // 64K, 128K, 256K, 512K, then 1 MiB windows: seven fills for 3 MiB.
    assert!(submissions.load(Ordering::SeqCst) <= 7);
    assert_eq!(reader.buffer.len(), 1024 * 1024);
}

#[tokio::test]
async fn a_seek_inside_the_window_costs_no_read() {
    use std::sync::atomic::Ordering;
    use tokio::io::AsyncReadExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    fs::write(&path, (0..200_000u32).map(|i| i as u8).collect::<Vec<_>>()).unwrap();
    let mut fs = Fs::new(FsConfig {
        threads: 1,
        ..Default::default()
    })
    .unwrap();
    let submissions = counting(&mut fs);
    let mut reader = fs.open_read(&path).await.unwrap();
    let mut head = [0; 16];
    reader.read_exact(&mut head).await.unwrap();
    let after_first = submissions.load(Ordering::SeqCst);
    reader.seek(std::io::SeekFrom::Start(1000)).await.unwrap();
    let mut piece = [0; 8];
    reader.read_exact(&mut piece).await.unwrap();
    assert_eq!(piece[0], 1000u32 as u8);
    reader.seek(std::io::SeekFrom::Current(-500)).await.unwrap();
    reader.read_exact(&mut piece).await.unwrap();
    assert_eq!(piece[0], 508u32 as u8);
    assert_eq!(submissions.load(Ordering::SeqCst), after_first);
    reader
        .seek(std::io::SeekFrom::Start(150_000))
        .await
        .unwrap();
    reader.read_exact(&mut piece).await.unwrap();
    assert_eq!(piece[0], 150_000u32 as u8);
    assert_eq!(submissions.load(Ordering::SeqCst), after_first + 1);
}
