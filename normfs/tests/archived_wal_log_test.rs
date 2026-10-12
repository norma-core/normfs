//! A WAL file archived to the store is the ordinary lifecycle, so restarting
//! and reading a queue over such files must not warn about each one.

use std::sync::Mutex;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings, ReadPosition};
use tokio::sync::mpsc;

struct Capture(Mutex<Vec<String>>);

static WARNINGS: Capture = Capture(Mutex::new(Vec::new()));

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Warn
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

#[tokio::test]
async fn reading_over_archived_wal_files_does_not_warn() {
    log::set_logger(&WARNINGS).unwrap();
    log::set_max_level(log::LevelFilter::Warn);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;
    settings.wal_settings.max_file_size = 4096;

    let payload = |i: u32| Bytes::from(vec![(i % 251) as u8; 1500]);
    {
        let fs = NormFS::new(path.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("archived");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        for i in 0..200 {
            fs.enqueue(&queue, payload(i)).await.unwrap();
        }
        fs.close().await.unwrap();
    }

    WARNINGS.0.lock().unwrap().clear();
    let fs = NormFS::new(path, settings).await.unwrap();
    let queue = fs.resolve("archived");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    let (tx, mut rx) = mpsc::channel(256);
    fs.read(&queue, ReadPosition::Absolute(0u64.into()), 200, 1, tx)
        .await
        .unwrap();
    let mut read = 0;
    while let Ok(Some(_)) =
        tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv()).await
    {
        read += 1;
    }
    fs.close().await.unwrap();

    assert_eq!(read, 200, "the read did not find every record");
    let misses: Vec<_> = WARNINGS
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|w| w.contains("WAL reader: file") && w.contains("not found"))
        .cloned()
        .collect();
    assert!(misses.is_empty(), "archived WAL files warned: {misses:?}");
}
