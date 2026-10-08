#![allow(dead_code)]

use normfs::{NormFS, NormFsSettings, Persist, QueueSettings, ReadPosition, UintN};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::sync::mpsc;

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

/// The first eight bytes of each of the first `n` entries, as a u64.
pub async fn entries(fs: &NormFS, queue: &str, n: usize) -> Vec<u64> {
    let (tx, mut rx) = mpsc::channel(n + 1);
    fs.read(
        &fs.resolve(queue),
        ReadPosition::Absolute(UintN::zero()),
        n as u64,
        1,
        tx,
    )
    .await
    .unwrap();
    let mut out = Vec::new();
    while let Some(entry) = rx.recv().await {
        out.push(u64::from_le_bytes(entry.data[..8].try_into().unwrap()));
    }
    out
}
