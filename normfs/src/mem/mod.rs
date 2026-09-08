use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::mpsc::Sender;

use crate::config::PoolKind;
use bytes::Bytes;
use normfs_types::{DataSource, QueueId, ReadEntry, SubscriberCallback};
use normfs_wal::{AppendOutcome, PagePool, Placement, WalArena};
use uintn::UintN;

// Geometry of the in-memory paged store. Every record fits a page -- one that
// does not is refused before it is given an id -- and the ring caps a queue's
// cache at MEM_MAX_PAGES pages.
const MEM_MAX_PAGES: usize = 64;

/// The fewest pages a queue can work with: one to append into, and one the
/// file writer is draining. With a single page an appender waits for its own
/// page to reach disk before it can continue, which serialises the queue
/// against the disk rather than merely bounding it.
const MEM_MIN_PAGES_PER_QUEUE: usize = 2;

/// How many pages a queue starting now should ask the arena for.
///
/// `max_memory_usage` is a total, and the arena is what makes it one: every
/// queue's pages are slots in one allocation, so the sum cannot exceed the
/// setting however many queues appear. It used to be divided by the queue count
/// at the moment each queue was created and never revisited --
/// `max_memory_usage / queues.len().max(1)`, read *before* the insert -- so the
/// first queue kept the whole budget for ever and every queue that arrived
/// later added its own allocation on top. The total was not bounded by the
/// setting at all: it overshot by roughly the number of queues.
///
/// Nothing counts what has been handed out. The arena's owner array already
/// knows, one slot per page, and it stays right as pages move; a counter beside
/// it would be a second registry that only drifts -- the old one never
/// decremented, so a queue giving pages back was invisible to it.
///
/// This number is a starting point rather than a verdict. A queue that runs out
/// takes a free slot above its range instead of waiting for the disk, and gives
/// pages back once its records are durable, so a busy queue can borrow from an
/// idle one without the process-wide total moving.
fn pages_for_new_queue(free: usize, total: usize) -> usize {
    // A quarter of the arena per queue, so the first few get a useful allowance
    // and later ones still find something left. Any fixed fraction is a guess
    // without knowing the queue count up front; migration is what makes the
    // guess survivable.
    let share = (total / 4).clamp(MEM_MIN_PAGES_PER_QUEUE, MEM_MAX_PAGES);
    share.min(free).max(MEM_MIN_PAGES_PER_QUEUE)
}

// Entry ids are sequential counters that fit u64 for any real queue; the
// fallback is defensive only (cache_append saturates on it, never wraps).
fn id_to_u64(id: &UintN) -> u64 {
    debug_assert!(id.to_u64().is_ok(), "WAL entry id {} does not fit u64", id);
    id.to_u64().unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub enum TryEnqueue {
    Placed(UintN, Placement),
    Full,
    Closed,
}

/// Result of a memory read operation
#[derive(Debug)]
pub struct MemReadResult {
    /// Whether the read was fully satisfied by memory
    pub success: bool,
    /// For negative lookups: the resolved start_id (offset from end)
    pub start_id: Option<UintN>,
    /// For follow/subscribe operations: the subscription ID
    pub subscription_id: Option<usize>,
}

impl MemReadResult {
    pub fn fail() -> Self {
        Self {
            success: false,
            start_id: None,
            subscription_id: None,
        }
    }
}

pub struct MemStore {
    queues: RwLock<HashMap<QueueId, Arc<MemQueue>>>,
    /// One arena of pages for every queue, so `max_memory_usage` is a total
    /// rather than a per-queue allowance that each new queue re-derives -- and
    /// so a busy queue can take a page an idle one has released.
    arena: Arc<WalArena>,
    /// A second, small-paged arena for queues that write rarely: their
    /// permanent 2-page floor costs two small pages instead of two big ones.
    passive_arena: Arc<WalArena>,
    /// Ring ids, which is what the arena's owner array records. Distinct per
    /// queue and never reused while a queue is alive. Shared across both
    /// arenas so a ring id names one queue no matter where its pages live.
    next_ring_id: AtomicU64,
    /// Queues closed for good, mapped to the last id each had at close
    /// (`None` when only the durable marker is known). Recorded here because
    /// close removes the queue from the map above, and a later follow still
    /// needs to know where the sequence ended.
    closed: RwLock<HashMap<QueueId, Option<UintN>>>,
    /// Whether a full cache page may be forgotten to admit new records. Only
    /// set when nothing ever drains these pools to disk (memory-only mode):
    /// eviction moves the durability watermark, which must stay fsync-backed
    /// anywhere a writer can attach.
    cache_evicts: std::sync::atomic::AtomicBool,
}

struct Inner {
    // The paged store, allocated on first enqueue. It holds a contiguous suffix
    // of recent entries; older acked entries are reclaimed and served from file.
    pool: Option<Arc<PagePool>>,
    // The last enqueued id. It persists beyond the cache, so it is tracked
    // separately from the ring.
    last_id: Option<UintN>,
    last_acked_id: Option<UintN>,
}

struct MemQueue {
    inner: RwLock<Inner>,
    /// Set under the append gate when the queue closes, so it cannot flip
    /// mid-batch: a close waits its turn before refusing what follows.
    closed: std::sync::atomic::AtomicBool,
    /// The pages this queue started with. It can hold more or fewer now --
    /// the pool's own `page_count` is the live figure.
    pages: usize,
    page_size: usize,
    evicts: bool,
    /// Held across an append so the pool sees ids in order.
    append_gate: tokio::sync::Mutex<()>,
    subscribers: Mutex<HashMap<usize, SubscriberCallback>>,
    next_subscriber_id: Mutex<usize>,
}

impl MemQueue {
    /// Takes `pages` slots from the shared arena, or the floor if that is all
    /// there is room for.
    ///
    /// A queue below the floor cannot drain -- an appender would wait for its
    /// own page to reach disk, serialising the queue against the disk rather
    /// than merely bounding it -- and refusing to start the queue would turn a
    /// memory setting into an availability limit. So when the arena has no room
    /// at all, the queue gets a private allocation of its floor and says so:
    /// overshooting by a floor's worth is the lesser fault. Those pages cannot
    /// migrate, which is the other half of the cost.
    fn new(
        last_id: Option<UintN>,
        arena: &Arc<WalArena>,
        ring_id: u64,
        pages: usize,
        label: &QueueId,
        evicts: bool,
    ) -> Self {
        let page_size = arena.page_size();
        let first_entry_id = last_id
            .as_ref()
            .map_or(0, |id| id_to_u64(id).wrapping_add(1));
        let want = pages.clamp(MEM_MIN_PAGES_PER_QUEUE, MEM_MAX_PAGES);

        let reserved = arena
            .reserve(want, ring_id, first_entry_id)
            .or_else(|| arena.reserve(MEM_MIN_PAGES_PER_QUEUE, ring_id, first_entry_id));

        let pool = match reserved {
            Some(range) => {
                arena.set_label(ring_id, label.to_string());
                Arc::new(PagePool::new_in(
                    arena,
                    range,
                    ring_id,
                    first_entry_id,
                    MEM_MIN_PAGES_PER_QUEUE,
                ))
            }
            None => {
                log::warn!(
                    target: "normfs-mem",
                    "arena exhausted: giving queue '{}' a private floor of {} pages ({} KiB) \
                     beyond the configured total, because a queue below the floor cannot drain. \
                     These pages cannot be traded with other queues.",
                    label,
                    MEM_MIN_PAGES_PER_QUEUE,
                    MEM_MIN_PAGES_PER_QUEUE * page_size / 1024,
                );
                Arc::new(PagePool::new(
                    MEM_MIN_PAGES_PER_QUEUE,
                    page_size,
                    first_entry_id,
                ))
            }
        };

        MemQueue {
            pages: pool.page_count(),
            closed: std::sync::atomic::AtomicBool::new(false),
            inner: RwLock::new(Inner {
                pool: Some(pool),
                last_id,
                last_acked_id: None,
            }),
            page_size,
            evicts,
            append_gate: tokio::sync::Mutex::new(()),
            subscribers: Mutex::new(HashMap::new()),
            next_subscriber_id: Mutex::new(0),
        }
    }

    // Caches `data` under `id_u64` in the pre-allocated pool.
    //
    // The pool is the id authority: it hands out ids in sequence from where it
    // was seeded, and `last_id` mirrors them. They can only disagree if a
    // record went to the file without being cached, and the pool is re-seeded
    // then so its contents stay a contiguous id suffix.
    //
    // `Full` still declines to cache here rather than waiting. Waiting belongs
    // on the async path (`PagePool::append`), which is what `NormFS::enqueue`
    // will use; this synchronous one cannot block a runtime thread.
    fn cache_append(&self, inner: &mut Inner, id_u64: u64, data: &[u8]) {
        let pages = self.pages;
        let page_size = self.page_size;
        let pool = inner
            .pool
            .get_or_insert_with(|| Arc::new(PagePool::new(pages, page_size, id_u64)));

        // Once a file writer is draining this pool, whatever is in a page is
        // going to be written to the file from that page. A caller on this path
        // has no way to say so -- it returns no `Placement` -- so the writer
        // would buffer the bytes as well and the record would reach the file
        // twice. Declining to cache costs a memory hit; caching here would cost
        // a duplicated record, so this is the safe direction.
        if pool.has_drainer() {
            log::debug!(
                target: "normfs-mem",
                "not caching entry {id_u64} on the synchronous path: the WAL writer is taking \
                 its bytes from these pages, and only the awaiting path can say so"
            );
            return;
        }

        if pool.next_entry_id() != id_u64 {
            pool.reseed(id_u64);
        }

        // Forgetting the oldest page beats forgetting everything, but only
        // where no writer can ever attach; the durable pre-writer window
        // keeps the reseed below. No page is taken from the arena here:
        // pages taken on this path are never given back.
        let first_try = if self.evicts {
            pool.try_append_evicting(data)
        } else {
            pool.try_append(data)
        };
        match first_try {
            AppendOutcome::Cached(_) => {}
            AppendOutcome::Full => {
                // Start the cache again at this record and keep it, so what is
                // held stays the newest contiguous run of ids rather than an
                // arbitrary older one.
                pool.reseed(id_u64);
                if !matches!(pool.try_append(data), AppendOutcome::Cached(_)) {
                    // saturating: a wrapping add on id_to_u64's MAX fallback
                    // would resume caching at id 0, colliding with real ids.
                    pool.reseed(id_u64.saturating_add(1));
                }
            }
            AppendOutcome::TooLarge => {
                pool.reseed(id_u64.saturating_add(1));
            }
        }
    }

    /// Enqueues, waiting for a page rather than dropping the cache when the
    /// pool is full. The synchronous [`MemQueue::enqueue`] stays for callers
    /// that cannot await.
    ///
    /// Returns the id and the [`Placement`] the WAL writer must carry out. The
    /// rotation decision is made here, and not in the writer, because this is
    /// the last point that runs *before* the record's bytes enter a page: by
    /// the time the writer sees the entry, the bytes are already placed, and a
    /// rotation decided then would flush them into the file they were meant to
    /// come after.
    pub async fn enqueue_awaiting(&self, data: Bytes) -> Option<(UintN, Placement)> {
        // Held across the append and the charge together, so the pool sees ids
        // in order: it follows the caller's sequence rather than owning it.
        let _gate = self.append_gate.lock().await;
        self.enqueue_gated(data).await
    }

    /// Enqueues a batch, each record placed exactly as a single
    /// [`MemQueue::enqueue_awaiting`] would place it.
    ///
    /// The gate is taken once for the whole batch, so the ids a batch returns
    /// are contiguous — another enqueue cannot interleave into the middle of
    /// one.
    pub async fn enqueue_batch_awaiting(
        &self,
        entries: Vec<Bytes>,
    ) -> Option<Vec<(UintN, Placement)>> {
        let _gate = self.append_gate.lock().await;
        let mut out = Vec::with_capacity(entries.len());
        for data in entries {
            match self.enqueue_gated(data).await {
                Some(placed) => out.push(placed),
                // The writer left mid-batch. The placed prefix keeps its
                // ids; the WAL send reports the failure.
                None if !out.is_empty() => break,
                None => return None,
            }
        }
        Some(out)
    }

    /// The body of an enqueue, with the append gate already held.
    async fn enqueue_gated(&self, data: Bytes) -> Option<(UintN, Placement)> {
        // Under the gate, so a refusal is final: the close that set this
        // waited for every in-flight append.
        if self.closed.load(Ordering::Relaxed) {
            return None;
        }
        // The id is not committed until the record is placed. This future can
        // be dropped at the await below, and a cancelled enqueue must consume
        // nothing: an id taken with no record behind it is a gap the pool
        // would eventually re-seed over, discarding records it still holds.
        // The append gate is held, so no other enqueue takes the id first.
        let (id, pool) = {
            let inner = self.inner.read().unwrap();
            let id = inner
                .last_id
                .as_ref()
                .map_or(UintN::zero(), |id| id.increment());
            (id, inner.pool.clone())
        };

        // Waiting is only safe once the WAL writer drains this pool: the wait
        // ends when a flush reports pages durable, and nothing reports that
        // until the writer is started with Some(pool). Until then this uses the
        // non-blocking cache behaviour, or a full pool would hang the caller
        // forever with nothing able to free it.
        let mut placement = Placement::legacy();
        let mut cache = false;
        if let Some(pool) = pool {
            if pool.has_drainer() {
                placement = match pool.place(id_to_u64(&id), &data).await {
                    Ok(placed) => placed,
                    Err(normfs_wal::PoolError::NoDrainer) => {
                        // The writer left while this waited for a page. The
                        // record reaches no file, so it must not take an id
                        // or reach a subscriber.
                        log::warn!(
                            target: "normfs-mem",
                            "entry {id} arrived while the queue was closing; it is refused"
                        );
                        return None;
                    }
                    Err(e) => {
                        // Unreachable through `NormFS::enqueue`, which refuses
                        // a record no page can hold before it takes an id --
                        // with the same arithmetic the pool uses, so the two
                        // cannot disagree. Reaching this arm means that guard
                        // was bypassed.
                        log::error!(
                            target: "normfs-mem",
                            "entry {id} of {} bytes was accepted and cannot be written ({e:?}): \
                             every later entry in its file will read back under the wrong id",
                            data.len(),
                        );
                        debug_assert!(false, "an unframeable record reached the pool");
                        Placement::legacy()
                    }
                };
            } else {
                cache = true;
            }
        }

        self.commit(&id, &data, cache);
        Some((id, placement))
    }

    /// Called once the record is placed and with the append gate still held,
    /// so the id the pool saw and the id the queue reports are the same.
    fn commit(&self, id: &UintN, data: &Bytes, cache: bool) {
        let subscribers_data = {
            let mut inner = self.inner.write().unwrap();
            inner.last_id = Some(id.clone());
            if cache {
                self.cache_append(&mut inner, id_to_u64(id), data);
            }
            if self.subscribers.lock().unwrap().is_empty() {
                None
            } else {
                Some(data.clone())
            }
        };

        log::debug!(target: "normfs-mem", "Enqueued entry - ID: {}, Data size: {} bytes", id, data.len());

        if let Some(data) = subscribers_data {
            self.notify_subscribers(&[(id.clone(), data)]);
        }
    }

    /// `try_lock` on the gate, because an enqueue parked on a full pool holds
    /// it. It is also what makes a write from inside a subscriber callback
    /// safe: the notify runs under the gate, so a callback writing back into
    /// its own queue is refused rather than deadlocked.
    pub fn try_enqueue(&self, data: Bytes) -> TryEnqueue {
        let Ok(_gate) = self.append_gate.try_lock() else {
            return TryEnqueue::Full;
        };
        if self.closed.load(Ordering::Relaxed) {
            return TryEnqueue::Closed;
        }

        // As on the awaiting path: no id is taken unless the record is placed.
        let (id, pool) = {
            let inner = self.inner.read().unwrap();
            let id = inner
                .last_id
                .as_ref()
                .map_or(UintN::zero(), |id| id.increment());
            (id, inner.pool.clone())
        };

        let mut placement = Placement::legacy();
        let mut cache = false;
        if let Some(pool) = pool {
            if pool.has_drainer() {
                match pool.try_place_now(id_to_u64(&id), &data) {
                    Ok(Some(placed)) => placement = placed,
                    Ok(None) => return TryEnqueue::Full,
                    Err(e) => {
                        log::error!(
                            target: "normfs-mem",
                            "entry {id} of {} bytes was accepted and cannot be written ({e:?}): \
                             every later entry in its file will read back under the wrong id",
                            data.len(),
                        );
                        debug_assert!(false, "an unframeable record reached the pool");
                    }
                }
            } else {
                cache = true;
            }
        }

        self.commit(&id, &data, cache);
        TryEnqueue::Placed(id, placement)
    }

    pub fn get_last_id(&self) -> Option<UintN> {
        self.inner.read().unwrap().last_id.clone()
    }

    pub fn ack(&self, id: &UintN) {
        let mut inner = self.inner.write().unwrap();
        if inner.last_acked_id.as_ref().is_none_or(|last| id > last) {
            log::debug!(target: "normfs-mem", "Acknowledging entry - ID: {}", id);
            inner.last_acked_id = Some(id.clone());
            // Deliberately does not free pages. A page may only be reused
            // once its records are on disk, and that is reported by the WAL
            // writer through PagePool::mark_durable. Letting a consumer ack
            // advance the same watermark would hand a page back to be
            // overwritten while its records were still only in memory.
        }
    }

    pub async fn read_full(
        &self,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let entries_to_send: Vec<(UintN, Bytes)> = {
            let inner = self.inner.read().unwrap();

            // Check queue's last_id first - if start_id is beyond it, we're done
            let mem_last_id = match &inner.last_id {
                Some(id) => id,
                None => {
                    // Queue exists but is empty (no entries ever enqueued)
                    // Return success with 0 entries
                    return MemReadResult {
                        success: true,
                        start_id: None,
                        subscription_id: None,
                    };
                }
            };

            // If requesting entries beyond what exists, complete with no entries
            if start_id > *mem_last_id {
                return MemReadResult {
                    success: true,
                    start_id: None,
                    subscription_id: None,
                };
            }

            // Check if entries are actually loaded in memory
            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => return MemReadResult::fail(), // Not in memory, read from files
            };

            let mem_start_id = match ring.min_cached_id() {
                Some(m) => UintN::from(m),
                None => return MemReadResult::fail(),
            };

            // If start_id is before what's in memory, need to read from files
            if start_id < mem_start_id {
                return MemReadResult::fail();
            }

            let mut current_id = start_id.clone();
            let mut results = Vec::new();

            for (id_u64, data) in ring.pin_range(id_to_u64(&start_id), id_to_u64(&end_id)) {
                let id = UintN::from(id_u64);
                if id > end_id {
                    break;
                }

                while current_id < id {
                    current_id = current_id.step_by(step);
                    if current_id > end_id {
                        break;
                    }
                }

                if current_id == id {
                    results.push((id.clone(), data));
                    current_id = current_id.step_by(step);
                }
            }

            // The floor can rise between the min_cached check and the pin --
            // a page leaving the ring -- and the run then starts above
            // `start_id`. A truncated range must go to the files, not out as
            // a success.
            if ring
                .min_cached_id()
                .is_none_or(|m| UintN::from(m) > start_id)
            {
                return MemReadResult::fail();
            }

            results
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                break;
            }
        }

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: None,
        }
    }

    pub async fn read_full_negative(
        &self,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Collect entries to send while holding the lock
        let (entries_to_send, start_id) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            // Calculate start_id = last_id - offset (before first_id check,
            // so we can return it even when memory has no entries yet)
            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => {
                    // Nothing in memory yet; return start_id for file fallback.
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            let mem_start_id = match ring.min_cached_id() {
                Some(m) => UintN::from(m),
                None => {
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            if start_id < mem_start_id {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
            let mut current_id = start_id.clone();
            let mut entries = Vec::new();
            let mut count = 0u64;

            for (id_u64, data) in ring.pin_range(id_to_u64(&start_id), last) {
                if limit > 0 && count >= limit {
                    break;
                }
                let id = UintN::from(id_u64);
                while current_id < id {
                    current_id = current_id.step_by(step);
                }
                if current_id == id {
                    entries.push((id.clone(), data));
                    current_id = current_id.step_by(step);
                    count += 1;
                }
            }

            // See read_full: the floor can rise between the check and the pin.
            if ring
                .min_cached_id()
                .is_none_or(|m| UintN::from(m) > start_id)
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            (entries, start_id)
        };

        if entries_to_send.is_empty() {
            return MemReadResult {
                success: false,
                start_id: Some(start_id),
                subscription_id: None,
            };
        }

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: None,
        }
    }

    pub async fn follow_full(
        self: &Arc<Self>,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Read all existing entries from start_id onwards
        let (entries_to_send, last_sent_id) = {
            let inner = self.inner.read().unwrap();

            match &inner.pool {
                Some(ring) if !ring.is_empty() => {
                    // A backlog exists when the start is at or below the last
                    // id; serving it from memory needs the cached run to reach
                    // down to the start. `None` is not "no lower bound": since
                    // the cache floor it can mean "nothing servable at all",
                    // and the backlog is then on disk.
                    let backlog = inner.last_id.as_ref().is_some_and(|last| start_id <= *last);
                    let covered = |m: Option<u64>| m.is_some_and(|m| start_id >= UintN::from(m));
                    if backlog && !covered(ring.min_cached_id()) {
                        return MemReadResult::fail();
                    }
                    let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
                    let mut current_id = start_id.clone();
                    let mut entries = Vec::new();
                    for (id_u64, data) in ring.pin_range(id_to_u64(&start_id), last) {
                        let id = UintN::from(id_u64);
                        while current_id < id {
                            current_id = current_id.step_by(step);
                        }
                        if current_id == id {
                            entries.push((id.clone(), data));
                            current_id = current_id.step_by(step);
                        }
                    }
                    // The floor can rise between the check and the pin -- a
                    // page leaving the ring -- and the run above starts past
                    // the ids this follow owes. Re-checked now that the
                    // survivors are pinned: a truncated backlog must go to
                    // the files, not out as a success.
                    if backlog && !covered(ring.min_cached_id()) {
                        return MemReadResult::fail();
                    }
                    let last_id = entries.last().map(|(id, _)| id.clone());
                    (entries, last_id)
                }
                _ => {
                    // An empty ring holds none of the backlog either: after a
                    // recovery-style start the ids below `last_id` exist only
                    // on disk, and subscribing here would hand the client the
                    // future while silently skipping its past.
                    let backlog = inner.last_id.as_ref().is_some_and(|last| start_id <= *last);
                    if backlog {
                        return MemReadResult::fail();
                    }
                    (Vec::new(), None)
                }
            }
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult::fail();
            }
        }

        let target_chan_clone = target_chan.clone();
        let from_id_clone = from_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&from_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: None,
            subscription_id: Some(subscription_id),
        }
    }

    pub async fn follow_full_negative(
        self: &Arc<Self>,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        // Special case: offset 0 means just subscribe from now on, no existing entries
        if offset == UintN::zero() {
            let (last_id, from_id) = {
                let inner = self.inner.read().unwrap();
                let last = inner.last_id.clone();
                let from = last
                    .as_ref()
                    .map(|id| id.add(&UintN::from(1u64)))
                    .unwrap_or(UintN::zero());
                (last, from)
            };

            let target_chan_clone = target_chan.clone();
            let last_id_clone = last_id.clone();
            let from_id_clone = from_id.clone();

            let mut next_id = self.next_subscriber_id.lock().unwrap();
            let subscription_id = *next_id;
            *next_id += 1;
            drop(next_id);

            let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
                for (id, data) in entries {
                    if let Some(ref last) = last_id_clone {
                        if id <= last {
                            continue;
                        }
                    }

                    if id.in_step(&from_id_clone, step)
                        && target_chan_clone
                            .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                            .is_err()
                    {
                        return false;
                    }
                }
                true
            });

            let mut subscribers = self.subscribers.lock().unwrap();
            subscribers.insert(subscription_id, callback);

            log::debug!(target: "normfs-mem", "Started follow_full_negative with offset 0 (subscribe only), subscription {}, from_id: {}",
                subscription_id, from_id);

            return MemReadResult {
                success: true,
                start_id: Some(from_id),
                subscription_id: Some(subscription_id),
            };
        }

        // Calculate start_id from last_id
        let (start_id, last_sent_id, entries_to_send) = {
            let inner = self.inner.read().unwrap();

            let last_id = if let Some(id) = &inner.last_id {
                id
            } else {
                // Queue exists but is empty (no entries ever enqueued)
                // Return success with 0 entries and start_id = 0
                return MemReadResult {
                    success: true,
                    start_id: Some(UintN::zero()),
                    subscription_id: None,
                };
            };

            let start_id = if offset > *last_id {
                UintN::zero()
            } else {
                last_id.sub(&offset).unwrap_or(UintN::zero())
            };

            let ring = match &inner.pool {
                Some(ring) if !ring.is_empty() => ring,
                _ => {
                    return MemReadResult {
                        success: false,
                        start_id: Some(start_id),
                        subscription_id: None,
                    };
                }
            };

            // See follow_full: `None` can mean "nothing servable", and the
            // floor can rise between this check and the pin below, so both are
            // needed for a backlog the caller expects in full.
            let backlog = inner.last_id.as_ref().is_some_and(|last| start_id <= *last);
            let covered = |m: Option<u64>| m.is_some_and(|m| start_id >= UintN::from(m));
            if backlog && !covered(ring.min_cached_id()) {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }

            let last = inner.last_id.as_ref().map(id_to_u64).unwrap_or(u64::MAX);
            let mut current_id = start_id.clone();
            let mut entries = Vec::new();

            for (id_u64, data) in ring.pin_range(id_to_u64(&start_id), last) {
                let id = UintN::from(id_u64);
                while current_id < id {
                    current_id = current_id.step_by(step);
                }
                if current_id == id {
                    entries.push((id.clone(), data));
                    current_id = current_id.step_by(step);
                }
            }

            if backlog && !covered(ring.min_cached_id()) {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
            let last_sent = entries.last().map(|(id, _)| id.clone());
            (start_id, last_sent, entries)
        };

        for (id, data) in entries_to_send {
            if target_chan
                .send(ReadEntry::new(id, data, DataSource::Memory))
                .await
                .is_err()
            {
                return MemReadResult {
                    success: false,
                    start_id: Some(start_id),
                    subscription_id: None,
                };
            }
        }

        let target_chan_clone = target_chan.clone();
        let start_id_clone = start_id.clone();
        let last_sent_id_clone = last_sent_id.clone();

        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        drop(next_id);

        let callback = Box::new(move |entries: &[(UintN, Bytes)]| -> bool {
            for (id, data) in entries {
                if let Some(ref last_id) = last_sent_id_clone {
                    if id <= last_id {
                        continue;
                    }
                }

                if id.in_step(&start_id_clone, step)
                    && target_chan_clone
                        .try_send(ReadEntry::new(id.clone(), data.clone(), DataSource::Memory))
                        .is_err()
                {
                    return false;
                }
            }
            true
        });

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscription_id, callback);

        log::debug!(target: "normfs-mem", "Started follow_full_negative with subscription {} from last_sent_id: {:?}",
            subscription_id, last_sent_id);

        MemReadResult {
            success: true,
            start_id: Some(start_id),
            subscription_id: Some(subscription_id),
        }
    }

    fn notify_subscribers(&self, entries: &[(UintN, Bytes)]) {
        log::debug!(target: "normfs-mem", "Notifying {} subscribers about {} new entries",
            self.subscribers.lock().unwrap().len(),
            entries.len());

        let to_remove = {
            let subscribers = self.subscribers.lock().unwrap();
            let mut to_remove = Vec::new();

            for (id, callback) in subscribers.iter() {
                if !callback(entries) {
                    to_remove.push(*id);
                }
            }

            to_remove
        }; // Lock released here

        // Clean up failed subscriptions without holding lock
        for id in to_remove {
            self.unsubscribe(id);
        }
    }

    pub fn subscribe(&self, callback: SubscriberCallback) -> usize {
        let mut next_id = self.next_subscriber_id.lock().unwrap();
        let subscriber_id = *next_id;
        *next_id += 1;

        let mut subscribers = self.subscribers.lock().unwrap();
        subscribers.insert(subscriber_id, callback);

        log::debug!(target: "normfs-mem", "Added subscription {}", subscriber_id);
        subscriber_id
    }

    pub fn unsubscribe(&self, subscriber_id: usize) {
        let mut subscribers = self.subscribers.lock().unwrap();
        if subscribers.remove(&subscriber_id).is_some() {
            log::debug!(target: "normfs-mem", "Removed subscription {}", subscriber_id);
        }
    }
}

impl MemStore {
    /// Allocates the arena: `max_memory_usage / page_size` pages, shared by
    /// every queue.
    ///
    /// A WAL file ends on a page boundary, so `page_size` is also the
    /// granularity at which a file tracks `max_file_size`: crossing the
    /// threshold seals the active page, and the tail of that page goes unused.
    /// A test that wants many small files wants a small page here.
    ///
    /// A budget too small for [`MEM_MIN_PAGES_PER_QUEUE`] pages is an error
    /// rather than a floor. Rounding it up is how `max_memory_usage` stops
    /// meaning what it says -- at a 4 MiB page a 1 MiB budget would silently
    /// allocate 8 MiB -- and the pool exists to make that number true.
    pub fn with_pools(
        max_memory_usage: usize,
        page_size: usize,
        passive_memory_usage: usize,
        passive_page_size: usize,
    ) -> Result<Self, crate::Error> {
        let arena = Self::arena_for(max_memory_usage, page_size)?;
        let passive_arena = Self::arena_for(passive_memory_usage, passive_page_size)?;
        Ok(MemStore {
            queues: RwLock::new(HashMap::new()),
            arena,
            passive_arena,
            next_ring_id: AtomicU64::new(0),
            closed: RwLock::new(HashMap::new()),
            cache_evicts: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn evict_cache_on_full(&self) {
        self.cache_evicts.store(true, Ordering::Relaxed);
    }

    fn arena_for(max_memory_usage: usize, page_size: usize) -> Result<Arc<WalArena>, crate::Error> {
        if page_size < normfs_wal::MIN_PAGE_SIZE {
            return Err(crate::Error::PageBelowMinimum {
                page_size,
                minimum: normfs_wal::MIN_PAGE_SIZE,
            });
        }
        let needed = MEM_MIN_PAGES_PER_QUEUE.saturating_mul(page_size);
        let pages = max_memory_usage / page_size.max(1);
        if pages < MEM_MIN_PAGES_PER_QUEUE {
            return Err(crate::Error::MemoryBelowFloor {
                max_memory_usage,
                page_size,
                needed,
            });
        }
        Ok(Arc::new(WalArena::new(pages, page_size)))
    }

    /// The shared page arena, for tests that assert on who holds what.
    #[cfg(test)]
    pub fn arena(&self) -> &Arc<WalArena> {
        &self.arena
    }

    #[cfg(test)]
    pub fn passive_arena(&self) -> &Arc<WalArena> {
        &self.passive_arena
    }

    /// The queue's page pool, so the WAL writer can put those same pages on
    /// disk instead of copying their contents into a buffer of its own.
    pub fn pool(&self, queue: &QueueId) -> Option<Arc<PagePool>> {
        let queues = self.queues.read().unwrap();
        let q = queues.get(queue)?;
        let inner = q.inner.read().unwrap();
        inner.pool.clone()
    }

    /// Starts a queue, taking its pages from the shared arena.
    ///
    /// A read-only queue starts at the floor rather than at a writer's share.
    /// Nothing appends to it, so the share would sit idle for the life of the
    /// process — and a queue is started read-only by any client that merely
    /// *names* a path, which is what made the share an unauthenticated way to
    /// reserve 16 MiB. Being wrong about it is cheap now: the range takes more
    /// of the arena on demand, so a queue promoted to writing takes what it
    /// needs on its first busy moment instead of holding it in advance.
    pub fn start_queue(
        &self,
        queue: &QueueId,
        last_id: Option<UintN>,
        readonly: bool,
        pool: PoolKind,
    ) {
        let mut queues = self.queues.write().unwrap();
        if !queues.contains_key(queue) {
            let arena = match pool {
                PoolKind::Active => &self.arena,
                PoolKind::Passive => &self.passive_arena,
            };
            // Pages come out of the one arena, so the total is bounded by the
            // setting however many queues start. It used to be the setting
            // divided by the queue count at this moment, which bounded nothing.
            //
            // A passive queue starts at the floor even for writing; the
            // range still takes more on demand if one bursts.
            let want = if readonly || pool == PoolKind::Passive {
                MEM_MIN_PAGES_PER_QUEUE
            } else {
                pages_for_new_queue(arena.free_pages(), arena.page_count())
            };
            let ring_id = self.next_ring_id.fetch_add(1, Ordering::Relaxed);
            let new_queue = Arc::new(MemQueue::new(
                last_id.clone(),
                arena,
                ring_id,
                want,
                queue,
                self.cache_evicts.load(Ordering::Relaxed),
            ));
            log::debug!(target: "normfs-mem",
                "Starting queue '{}' with last_id: {:?}, {} pages ({} KiB) from the {:?} arena \
                 ({} of {} still free)",
                queue, last_id, new_queue.pages, new_queue.pages * arena.page_size() / 1024,
                pool, arena.free_pages(), arena.page_count());
            queues.insert(queue.clone(), new_queue);
        }
    }

    pub fn mark_closed(&self, queue: &QueueId) {
        self.closed
            .write()
            .unwrap()
            .entry(queue.clone())
            .or_insert(None);
    }

    pub fn is_closed(&self, queue: &QueueId) -> bool {
        self.closed.read().unwrap().contains_key(queue)
    }

    /// The last id a closed queue ever assigned; `None` means it never
    /// wrote.
    pub fn closed_last_id(&self, queue: &QueueId) -> Option<UintN> {
        let recorded = self.closed.read().unwrap().get(queue).cloned().flatten();
        recorded.or_else(|| self.get_last_id(queue).flatten())
    }

    /// Stops the write side. Waits for the in-flight append, reads the
    /// final id under the same gate, and only then makes the close visible:
    /// any other order records a bound an accepted append can still move,
    /// and a follow told "complete" at N misses a durable N+1 forever.
    pub async fn begin_close(&self, queue: &QueueId) {
        let mem_queue = {
            let queues = self.queues.read().unwrap();
            queues.get(queue).cloned()
        };
        let last = match mem_queue {
            Some(q) => {
                let _gate = q.append_gate.lock().await;
                q.closed.store(true, Ordering::Relaxed);
                q.inner.read().unwrap().last_id.clone()
            }
            None => None,
        };
        let mut closed = self.closed.write().unwrap();
        let entry = closed.entry(queue.clone()).or_insert(None);
        if entry.is_none() {
            *entry = last;
        }
    }

    /// Whether everything the queue accepted is on disk; the close marker
    /// must not be written while this is false.
    pub fn is_fully_durable(&self, queue: &QueueId) -> bool {
        let pool = {
            let queues = self.queues.read().unwrap();
            queues
                .get(queue)
                .and_then(|q| q.inner.read().unwrap().pool.clone())
        };
        pool.is_none_or(|p| p.is_fully_durable())
    }

    /// Ends a closed queue's life in memory: subscriber streams end and the
    /// pages go back to their arena.
    pub fn close_queue(&self, queue: &QueueId) {
        // A recorded id is never clobbered: a second close must not erase
        // what the first one knew.
        self.mark_closed(queue);
        let removed = self.queues.write().unwrap().remove(queue);
        if let Some(q) = removed {
            let pool = q.inner.write().unwrap().pool.take();
            if let Some(pool) = pool {
                // Only when nothing else holds the pool: released slots go
                // to the next queue, and a straggler would read pages the
                // new owner is writing. A straggler's own drop releases them.
                if let Some(pool) = Arc::into_inner(pool) {
                    pool.release_to_arena();
                }
            }
        }
    }

    /// `None` when the queue is not in the map (never started, or closed):
    /// the record took no id, so it needs no place in the sequence.
    pub async fn enqueue_awaiting(
        &self,
        queue: &QueueId,
        data: Bytes,
    ) -> Option<(UintN, Placement)> {
        let mem_queue = {
            let queues = self.queues.read().unwrap();
            queues.get(queue).cloned()
        };
        mem_queue?.enqueue_awaiting(data).await
    }

    pub fn try_enqueue(&self, queue: &QueueId, data: Bytes) -> Option<TryEnqueue> {
        let mem_queue = {
            let queues = self.queues.read().unwrap();
            queues.get(queue).cloned()
        };
        Some(mem_queue?.try_enqueue(data))
    }

    pub async fn enqueue_batch_awaiting(
        &self,
        queue: &QueueId,
        entries: Vec<Bytes>,
    ) -> Option<Vec<(UintN, Placement)>> {
        let mem_queue = {
            let queues = self.queues.read().unwrap();
            queues.get(queue).cloned()
        };
        mem_queue?.enqueue_batch_awaiting(entries).await
    }

    pub fn get_last_id(&self, queue: &QueueId) -> Option<Option<UintN>> {
        let queues = self.queues.read().unwrap();
        queues.get(queue).map(|q| q.get_last_id())
    }

    pub fn ack(&self, queue: &QueueId, id: &UintN) {
        log::debug!(target: "normfs-mem", "Acknowledging entry in queue '{}' - Entry ID: {}", queue, id);
        let queues = self.queues.read().unwrap();
        if let Some(mem_queue) = queues.get(queue) {
            mem_queue.ack(id);
        }
    }

    pub async fn read_full(
        &self,
        queue: &QueueId,
        start_id: UintN,
        end_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading from queue '{}' - Start ID: {}, End ID: {}",
            queue, start_id, end_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full(start_id, end_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read from queue '{}' completed - Success: {}", queue, result.success);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full(
        &self,
        queue: &QueueId,
        from_id: &UintN,
        start_id: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full from queue '{}' - Start ID: {}",
            queue, start_id);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full(from_id, start_id, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full from queue '{}' - Success: {}, Subscription ID: {:?}",
                queue, result.success, result.subscription_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full", queue);
            MemReadResult::fail()
        }
    }

    pub async fn read_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        limit: u64,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Reading negative from queue '{}' - Offset: {}, Limit: {}",
            queue, offset, limit);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .read_full_negative(offset, step, limit, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Read negative from queue '{}' completed - Success: {}, Start ID: {:?}",
                queue, result.success, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for read_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub async fn follow_full_negative(
        &self,
        queue: &QueueId,
        offset: UintN,
        step: usize,
        target_chan: &Sender<ReadEntry>,
    ) -> MemReadResult {
        log::debug!(target: "normfs-mem", "Starting follow_full_negative from queue '{}' - Offset: {}",
            queue, offset);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let result = mem_queue
                .follow_full_negative(offset, step, target_chan)
                .await;
            log::debug!(target: "normfs-mem", "Follow_full_negative from queue '{}' - Success: {}, Subscription ID: {:?}, Start ID: {:?}",
                queue, result.success, result.subscription_id, result.start_id);
            result
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for follow_full_negative", queue);
            MemReadResult::fail()
        }
    }

    pub fn subscribe(&self, queue: &QueueId, callback: SubscriberCallback) -> Option<usize> {
        log::debug!(target: "normfs-mem", "Subscribing to queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            let subscriber_id = mem_queue.subscribe(callback);
            Some(subscriber_id)
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for subscription", queue);
            None
        }
    }

    pub fn unsubscribe(&self, queue: &QueueId, subscriber_id: usize) {
        log::debug!(target: "normfs-mem", "Unsubscribing from queue '{}'", queue);

        let mem_queue = self.queues.read().unwrap().get(queue).cloned();

        if let Some(mem_queue) = mem_queue {
            mem_queue.unsubscribe(subscriber_id);
        } else {
            log::warn!(target: "normfs-mem", "Queue '{}' not found for unsubscription", queue);
        }
    }
}

#[cfg(test)]
mod tests;
