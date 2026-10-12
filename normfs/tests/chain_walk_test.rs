//! File ids start at 1. The id-chain check at recovery walks down from the
//! file it resumes after and must stop there, not ask the reader for file 0.

use std::sync::Mutex;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};

struct Capture(Mutex<Vec<String>>);

static WARNINGS: Capture = Capture(Mutex::new(Vec::new()));

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Debug
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

#[tokio::test]
async fn recovery_does_not_ask_for_file_zero() {
    log::set_logger(&WARNINGS).unwrap();
    // Debug: a probe of a missing WAL file is logged at debug.
    log::set_max_level(log::LevelFilter::Debug);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;

    let instance_id = {
        let fs = NormFS::new(path.clone(), settings.clone()).await.unwrap();
        let queue = fs.resolve("restarted");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        for i in 0u8..8 {
            fs.enqueue(&queue, Bytes::from(vec![i; 64])).await.unwrap();
        }
        let id = fs.get_instance_id().to_string();
        fs.close().await.unwrap();
        id
    };

    let fs = NormFS::new(path, settings).await.unwrap();
    assert_eq!(fs.get_instance_id(), instance_id);
    fs.ensure_queue_exists_for_write(&fs.resolve("restarted"))
        .await
        .unwrap();

    let warnings = WARNINGS.0.lock().unwrap().clone();
    let file_zero: Vec<_> = warnings
        .iter()
        .filter(|w| w.contains("file 0 not found"))
        .collect();
    assert!(
        file_zero.is_empty(),
        "recovery probed a file that never exists: {file_zero:?}"
    );
}
