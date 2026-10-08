#![allow(dead_code)]

use normfs::{NormFS, NormFsSettings, Persist, QueueSettings};
use std::sync::Arc;
use tempfile::TempDir;

pub async fn memory_fs(dir: &TempDir, queues: QueueSettings) -> Arc<NormFS> {
    let settings = NormFsSettings {
        queue_settings: queues.with_default_persist(Persist::MEMORY),
        ..NormFsSettings::default()
    };
    Arc::new(
        NormFS::new(dir.path().to_path_buf(), settings)
            .await
            .unwrap(),
    )
}
