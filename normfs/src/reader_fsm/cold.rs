use bytes::Bytes;
use normfs_types::QueueId;
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use uintn::UintN;

/// Long enough for a page of neighbouring reads to land in the same file.
const KEEP_RECENT: Duration = Duration::from_secs(10);

const SLOW_SLOT: Duration = Duration::from_secs(10);

/// A file and whether it came from the cloud rather than the local store.
pub(crate) type FileKey = (QueueId, UintN, bool);

pub(crate) enum Cold {
    Found(Bytes),
    Missing,
    /// No slot was free and the caller would not wait for one.
    Busy,
}

#[derive(Clone)]
enum Shared {
    Found(Bytes),
    Missing,
    /// Whoever was waiting on this load makes its own.
    Failed,
}

struct Held {
    bytes: Bytes,
    _permit: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for Held {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Default)]
struct Recent {
    generation: u64,
    file: Option<(FileKey, Bytes)>,
}

/// Decoded store and cloud files. Each holds a slot for as long as any slice
/// of it lives, so no more than `slots` are in memory at once; reads of the
/// same file share one load.
pub(crate) struct ColdFiles {
    slots: Arc<Semaphore>,
    loading: Mutex<BTreeMap<FileKey, Arc<OnceCell<Shared>>>>,
    recent: Arc<Mutex<Recent>>,
    waiting: AtomicUsize,
}

impl ColdFiles {
    pub(crate) fn new(slots: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(slots.max(1))),
            loading: Mutex::new(BTreeMap::new()),
            recent: Arc::new(Mutex::new(Recent::default())),
            waiting: AtomicUsize::new(0),
        }
    }

    /// The file under `key`, loaded by `load` unless a concurrent or recent
    /// read already has it. Without `wait` a load finding no free slot is
    /// `Busy`.
    pub(crate) async fn get<F, Fut, E>(&self, key: FileKey, wait: bool, load: F) -> Result<Cold, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, E>>,
    {
        loop {
            if let Some(bytes) = self.recent(&key) {
                return Ok(Cold::Found(bytes));
            }

            let cell = self
                .loading
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_default()
                .clone();
            let mut own = None;
            let (own_ref, key_ref, load_ref) = (&mut own, &key, &load);
            let shared = cell
                .get_or_init(move || self.lead(key_ref, wait, load_ref, own_ref))
                .await
                .clone();
            {
                let mut loading = self.loading.lock().unwrap();
                if loading.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                    loading.remove(&key);
                }
            }

            if let Some(own) = own {
                return own;
            }
            match shared {
                Shared::Found(bytes) => return Ok(Cold::Found(bytes)),
                Shared::Missing => return Ok(Cold::Missing),
                Shared::Failed => {}
            }
        }
    }

    async fn lead<F, Fut, E>(
        &self,
        key: &FileKey,
        wait: bool,
        load: &F,
        own: &mut Option<Result<Cold, E>>,
    ) -> Shared
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Option<Bytes>, E>>,
    {
        let Some(permit) = self.slot(wait).await else {
            *own = Some(Ok(Cold::Busy));
            return Shared::Failed;
        };
        match load().await {
            Ok(Some(bytes)) => {
                let bytes = Bytes::from_owner(Held {
                    bytes,
                    _permit: permit,
                });
                self.remember(key, &bytes);
                Shared::Found(bytes)
            }
            Ok(None) => Shared::Missing,
            Err(e) => {
                *own = Some(Err(e));
                Shared::Failed
            }
        }
    }

    async fn slot(&self, wait: bool) -> Option<OwnedSemaphorePermit> {
        if let Ok(permit) = self.slots.clone().try_acquire_owned() {
            return Some(permit);
        }
        if !wait {
            return None;
        }
        self.waiting.fetch_add(1, Ordering::SeqCst);
        // The kept file may be what holds the slot.
        self.recent.lock().unwrap().file = None;
        let permit = match tokio::time::timeout(SLOW_SLOT, self.slots.clone().acquire_owned()).await
        {
            Ok(permit) => permit,
            Err(_) => {
                log::warn!(target: "normfs-reader-fsm",
                    "A cold read has waited {:?} for a load slot; the reads holding them are not \
                     finishing their files", SLOW_SLOT);
                self.slots.clone().acquire_owned().await
            }
        };
        self.waiting.fetch_sub(1, Ordering::SeqCst);
        Some(permit.expect("the semaphore is never closed"))
    }

    fn recent(&self, key: &FileKey) -> Option<Bytes> {
        match &self.recent.lock().unwrap().file {
            Some((k, bytes)) if k == key => Some(bytes.clone()),
            _ => None,
        }
    }

    fn remember(&self, key: &FileKey, bytes: &Bytes) {
        if self.waiting.load(Ordering::SeqCst) > 0 {
            return;
        }
        let generation = {
            let mut recent = self.recent.lock().unwrap();
            recent.generation += 1;
            recent.file = Some((key.clone(), bytes.clone()));
            recent.generation
        };
        let recent = Arc::downgrade(&self.recent);
        tokio::spawn(async move {
            tokio::time::sleep(KEEP_RECENT).await;
            if let Some(recent) = recent.upgrade() {
                let mut recent = recent.lock().unwrap();
                if recent.generation == generation {
                    recent.file = None;
                }
            }
        });
    }
}
