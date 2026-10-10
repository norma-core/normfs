//! A second process that recovers a queue while the first still writes it
//! issues ids the first has already used. Recovery must call that an overlap,
//! not a gap with its bounds reversed.

use std::path::Path;
use std::sync::Mutex;

use bytes::Bytes;
use normfs::{NormFS, NormFsSettings};
use normfs_types::QueueIdResolver;
use uintn::UintN;

struct Capture(Mutex<Vec<String>>);

static ERRORS: Capture = Capture(Mutex::new(Vec::new()));

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::Level::Error
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            self.0.lock().unwrap().push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

async fn session(path: &Path, settings: &NormFsSettings, from: u8, count: u8) -> String {
    let fs = NormFS::new(path.to_path_buf(), settings.clone())
        .await
        .unwrap();
    let queue = fs.resolve("shared");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    for i in from..from + count {
        fs.enqueue(&queue, Bytes::from(vec![i; 64])).await.unwrap();
    }
    let instance_id = fs.get_instance_id().to_string();
    fs.close().await.unwrap();
    instance_id
}

// Built from two copies of one folder rather than two live instances, whose
// interleaving would decide which files collide.
#[tokio::test]
async fn recovery_reports_ids_two_files_hold() {
    log::set_logger(&ERRORS).unwrap();
    log::set_max_level(log::LevelFilter::Error);

    let dir = tempfile::tempdir().unwrap();
    let first = dir.path().join("first");
    let second = dir.path().join("second");
    let mut settings = NormFsSettings::all_active();
    settings.mem_page_size = 256 * 1024;
    settings.system_queue = false;

    // File 1 holds 0..=9 in both copies. The first then writes 10..=14 and
    // 15..=19 as files 2 and 3; the second 10..=11, 12 and 13..=17 as files 2
    // to 4, so its file 4 starts below the first's file 3 and ends inside it.
    let instance_id = session(&first, &settings, 0, 10).await;
    copy_dir(&first, &second);
    session(&first, &settings, 10, 5).await;
    session(&first, &settings, 15, 5).await;
    session(&second, &settings, 10, 2).await;
    session(&second, &settings, 12, 1).await;
    session(&second, &settings, 13, 5).await;

    let queue = QueueIdResolver::new(&instance_id).resolve("shared");
    let file = UintN::from(4u64);
    let mut moved = 0;
    for (from, to) in [
        (
            queue.to_wal_path(&second, &file),
            queue.to_wal_path(&first, &file),
        ),
        (
            queue.to_store_path(&second, &file),
            queue.to_store_path(&first, &file),
        ),
    ] {
        if from.exists() {
            std::fs::create_dir_all(to.parent().unwrap()).unwrap();
            std::fs::copy(from, to).unwrap();
            moved += 1;
        }
    }
    assert!(moved > 0, "the second copy wrote no file 4");

    ERRORS.0.lock().unwrap().clear();
    let fs = NormFS::new(first, settings).await.unwrap();
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();
    fs.close().await.unwrap();

    let errors = ERRORS.0.lock().unwrap().clone();
    assert!(
        errors
            .iter()
            .any(|e| e.contains("ids 15..=17 are in two files")),
        "the overlap was not reported as one: {errors:?}"
    );
    assert!(
        !errors.iter().any(|e| e.contains("reach no file")),
        "the overlap was reported as a gap: {errors:?}"
    );
}
