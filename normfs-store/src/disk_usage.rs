use normfs_fs::{Fs, PublishReport, PublishSpec};
use normfs_types::QueueId;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

/// Publications share the gate through rename and accounting; rescans and
/// evictions take it exclusively to avoid counting a publication twice.
#[derive(Default)]
pub struct QueueBytes {
    pub(crate) gate: Arc<RwLock<()>>,
    bytes: AtomicU64,
}

impl QueueBytes {
    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Acquire)
    }

    /// Excludes publications for the guard's lifetime.
    pub async fn exclusive(self: Arc<Self>) -> Exclusive {
        let guard = self.gate.clone().write_owned().await;
        Exclusive {
            bytes: self,
            _guard: guard,
        }
    }
}

/// A rescan or eviction in progress; the only way to overwrite the count.
pub struct Exclusive {
    bytes: Arc<QueueBytes>,
    _guard: OwnedRwLockWriteGuard<()>,
}

impl Exclusive {
    pub fn get(&self) -> u64 {
        self.bytes.bytes()
    }

    pub fn set(&self, value: u64) {
        self.bytes.bytes.store(value, Ordering::Release);
    }

    pub fn sub(&self, value: u64) {
        self.bytes
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |b| {
                Some(b.saturating_sub(value))
            })
            .ok();
    }
}

#[derive(Default)]
pub struct DiskUsage {
    queues: Mutex<HashMap<QueueId, Arc<QueueBytes>>>,
}

impl DiskUsage {
    pub fn queue(&self, queue: &QueueId) -> Arc<QueueBytes> {
        self.queues
            .lock()
            .unwrap()
            .entry(queue.clone())
            .or_default()
            .clone()
    }

    pub async fn publish(&self, fs: &Fs, queue: &QueueId, spec: PublishSpec) -> io::Result<()> {
        let tracked = self.queue(queue);
        let shared = tracked.gate.clone().read_owned().await;
        fs.publish(
            spec,
            Some(Box::new(move |report: &PublishReport| {
                tracked
                    .bytes
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |b| {
                        Some(
                            b.saturating_sub(report.old_len.unwrap_or(0))
                                .saturating_add(report.new_len),
                        )
                    })
                    .ok();
                drop(shared);
            })),
        )
        .await?;
        Ok(())
    }
}
