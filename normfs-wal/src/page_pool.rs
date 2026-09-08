//! A fixed pool of WAL pages that writers wait on.
//!
//! The pool owns one [`WalRing`] — every byte it will ever use is allocated
//! once, at construction — and hands out entry ids as records are appended into
//! pages. When no page can be reclaimed, an appending task **waits** rather than
//! failing or discarding: the pages still hold records that are not yet on disk,
//! and dropping them is the one thing this store must never do.
//!
//! The waiting lives here and not in C. The C ring reports `NEEDS_ROTATE` and
//! `find_reusable` reports "none", and it never blocks, never allocates and
//! never learns what a file is — which is what keeps it portable down to a
//! microcontroller later.
//!
//! ## Why a page can be reused
//!
//! Reuse is governed entirely by the proven C predicate
//! `normfs_wal_page_is_reusable(p, m)`:
//!
//! ```text
//! p->pin_count == 0 && (p->count == 0 || p->last_entry_id < m)
//! ```
//!
//! and `normfs_wal_ring_rotate_to` requires it. The two conjuncts are two
//! different claims, and this module is what gives each its meaning:
//!
//! * `pin_count` — a reader or a stream is still looking at these bytes.
//! * `min_essential_id` — **the bytes are on disk**. [`PagePool::mark_durable`]
//!   is the only thing that advances it, and its caller may only call it after
//!   an `fsync` has returned.
//!
//! Together those give the property the store exists for: a record that
//! `append` accepted is never overwritten in memory until it has been written
//! *and* synced. The C side proves the "never overwritten" half; this module is
//! responsible for never advancing the watermark on anything less than a
//! completed sync.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Notify;

use crate::wal_arena::{SlotRange, WalArena};
use crate::wal_ring_v1::{AppendOutcome, WalRing};

/// Keeps a page alive for as long as a reader is looking at bytes on it.
///
/// The payload handed out by [`PagePool::pin_range`] is not a copy — it points
/// into the page the record was written into, which is the same memory the WAL
/// file writer takes its bytes from. Nothing else keeps that memory still, so
/// this does: the guard holds a pin for its lifetime and drops it afterwards,
/// including when the read fails or the stream is dropped part-way.
///
/// ## Why the borrow is sound
///
/// A page is reused in exactly one place, `normfs_wal_ring_rotate_to`, and its
/// contract requires `normfs_wal_page_is_reusable`, whose first conjunct is
/// `pin_count == 0`. So a pinned page cannot be reset, and the bytes below
/// `used_bytes` cannot change: appends only ever move that cursor forward, into
/// space this slice does not cover. The arena is allocated once and never
/// reallocated, and the `Arc` keeps the pool — and with it the arena — alive
/// for at least as long as the guard.
///
/// That is what the pin conjunct was always for. Until now nothing pinned
/// anything, so it read as a precaution against a caller that did not exist;
/// with reads borrowing from pages it carries its intended meaning.
///
/// ## Provenance
///
/// `ptr` comes from [`WalRing::record_ptr`], which derives it from the arena
/// base the C ring writes through. It is deliberately not taken from a `&[u8]`
/// over the page: that would give the pointer a temporary reference's
/// provenance while C keeps writing the same allocation through an older
/// pointer, which under Stacked Borrows entitles an append to invalidate a tag
/// this guard is still holding — a page it may not touch, argued from a rule
/// that does not care. Deriving from the base the writes already use settles
/// it structurally instead.
pub struct PageGuard {
    pool: Arc<PagePool>,
    page_index: usize,
    ptr: *const u8,
    len: usize,
}

// The pointer refers to a page buffer owned by the pool, which this guard holds
// an `Arc` to and a pin on, so it stays valid and unwritten for the guard's
// lifetime. Nothing here is mutated, so sharing across threads is safe.
unsafe impl Send for PageGuard {}
unsafe impl Sync for PageGuard {}

impl AsRef<[u8]> for PageGuard {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: as argued on the type -- the pin holds the page against
        // reuse, and the buffer outlives this guard.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for PageGuard {
    fn drop(&mut self) {
        self.pool.unpin_page(self.page_index);
    }
}

impl std::fmt::Debug for PageGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageGuard")
            .field("page_index", &self.page_index)
            .field("len", &self.len)
            .finish()
    }
}

/// How long a task may wait for a page before the pool starts reporting who is
/// holding things up. Waiting itself is not an error — a full pool means the
/// disk is behind, and back-pressure is the correct response — but an
/// indefinite wait with no explanation is not debuggable.
const STALL_WARN_AFTER: Duration = Duration::from_secs(5);

/// How often a pool that is still out of pages repeats itself.
///
/// The wait loop re-arms every [`STALL_WARN_AFTER`] and every queue has its own
/// pool, so reporting on each pass buries the first line — the one that says
/// what is wrong.
const STALL_REPORT_EVERY: Duration = Duration::from_secs(30);

/// What the pool remembers between wait passes.
#[derive(Default)]
struct Stall {
    /// When the current stall began, or `None` if the pool is not stalled.
    since: Option<Instant>,
    /// Appends that went to the wait path since the stall began.
    waits: u64,
    /// When the last line was emitted, so the repeat is paced.
    reported: Option<Instant>,
}

/// The share of the pool reads may hold pinned at once, as a divisor: pages
/// beyond `page_count / PIN_SHARE_DIVISOR` are copied out rather than borrowed.
///
/// A pin lasts as long as the payload, and a payload lives until the client has
/// been sent it — through a 2048-deep read channel and a bounded response
/// channel, and released only when the response is encoded onto the wire. So a
/// client that stops reading its socket holds pages for as long as it likes.
///
/// The reserve used to be a single page. That is enough to prove the pool never
/// deadlocks, and it was proved: an append always has somewhere to go. It is
/// not enough to keep it *working*. With one page to append into, a queue
/// advances one page per flush — 256 KiB per interval against otherwise
/// unbounded throughput — and the cache degrades to nothing but the stalled
/// reader's pages, so every other reader starts missing memory too. Half the
/// pool is the smallest share that leaves the writer a working set rather than
/// a single page.
const PIN_SHARE_DIVISOR: usize = 2;

/// Pages a read may never pin, whatever the share works out to, so the writer
/// always has one to append into.
const PIN_RESERVE: usize = 1;

/// Payloads one `pin_range` may hand out borrowed rather than copied.
///
/// The page share bounds how much of the *pool* one reader can hold; this
/// bounds how much of it a single call can claim in one go. `read_full`
/// materialises the whole requested range before sending any of it, so without
/// this one large read takes its share of the pool in a single step and holds
/// it for the length of the send.
const PIN_PAYLOADS_PER_READ: usize = 4096;

/// How far ahead of the pool the caller's id may run before re-seeding.
///
/// The two can only disagree if something allocated an id for this queue
/// without holding its append gate. Stepping keeps every record already held;
/// re-seeding throws them away, so it is the fallback and the gap that reaches
/// it has to be one no bookkeeping slip explains.
const MAX_STEPPABLE_GAP: u64 = 1024;

/// Why an append could not be satisfied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolError {
    /// The record does not fit a page, framing included. Waiting would never
    /// help; only a larger `mem_page_size` would.
    ///
    /// Reaching this is a bug rather than a condition to handle: `check_framable`
    /// refuses such a record *before* it is given an id. Once an id has been
    /// taken there is no good answer left — V1 derives entry ids from position,
    /// so a record that takes an id and then reaches no file does not go
    /// missing on its own, it renumbers every record after it.
    TooLarge,
    /// The drainer went away while this append was waiting for a page. Only a
    /// flush can end the wait and a flush needs a drainer, so this wait would
    /// never end: the queue is closing, and the caller must give up.
    NoDrainer,
}

/// What the enqueue side decided about a record, for the writer to carry out.
///
/// The writer used to decide rotation itself, from `AckFileWriter::can_add`.
/// It cannot any more: the bytes are in a page before the writer sees the
/// entry, so by the time it decided, the record that triggers the rotation had
/// already been flushed into the file it was supposed to start *after*. The
/// decision therefore moves to the one place that runs before the bytes enter a
/// page, and reaches the writer as an instruction rather than a hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Placement {
    /// The record is already in a page, so its bytes reach the file from there
    /// and must not be buffered again.
    pub in_pool: bool,
    /// The record's length. A pooled record rides the writer channel without
    /// its payload — the pool already holds the bytes — and this is what the
    /// writer sizes a rotation's header from instead.
    pub record_len: usize,
    pub rotate: RotateHint,
    /// Which file this record was charged to, counted from zero.
    ///
    /// A tripwire, not a control input: `check_epoch` compares it against the
    /// writer's own count so a disagreement is greppable rather than silent.
    /// What a flush asks the pool with is the writer's own `file_epoch`.
    pub epoch: u64,
}

impl Placement {
    /// The placement for a caller that has no pool: the writer decides
    /// rotation exactly as it always did.
    pub fn legacy() -> Self {
        Placement::default()
    }
}

/// Whether the writer must rotate before this record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RotateHint {
    /// No decision was made for this record; the writer makes its own, from
    /// its own accounting. This is the path `WalStore::enqueue` takes, and it
    /// behaves exactly as it did before pages existed.
    #[default]
    WriterDecides,
    /// Rotate, then write. This record opened a page, so the file it closes
    /// ends where that page's predecessor ended.
    Before,
    /// Do not rotate, whatever the writer's own accounting says.
    None,
}

/// How full the currently-open WAL file is.
///
/// This lives here rather than in `AckFileWriter` because an `AckFileWriter` is
/// per-file and dies on rotation, while the accounting has to run on the enqueue
/// path. It is the *only* fill accounting on the pooled path:
/// `AckFileWriter::current_size` is not maintained for pooled records, so the
/// two can never drift.
///
/// `max` is a threshold rather than a cap. A file ends at the first page to open
/// after it is crossed, so a file overshoots by at most the tail of one page —
/// and a `max_file_size` below the page size buys nothing.
struct FileFill {
    used: u64,
    max: u64,
    header_len: u64,
    /// Whether this file has been charged an entry yet. Both paths consult it,
    /// so a file takes its first record however large it is: without it a
    /// record bigger than `max` would rotate forever into empty files, and a
    /// file whose max is below its own header would be closed holding nothing.
    has_written: bool,
    epoch: u64,
}

/// A run of bytes the file writer has not written yet: bytes `from..to` of
/// page `page`.
///
/// One kind, because there is one place a record can be. A record wider than a
/// page is refused before it is given an id — see `check_framable` — so the
/// pool never holds bytes off-page, and there is no second road to the file to
/// order this one against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingWrite {
    pub page: usize,
    pub from: usize,
    pub to: usize,
    /// The first entry id covered. Runs are handed out in this order.
    pub first_entry_id: u64,
    /// The last entry id covered, so the caller knows what its `fsync` makes
    /// durable.
    pub last_entry_id: u64,
}

/// Owned copies, not borrows: the pages are recycled the moment
/// [`PagePool::take_stranded`] returns.
#[derive(Debug)]
pub struct Stranded {
    pub runs: Vec<(PendingWrite, Bytes)>,
    pub first_entry_id: u64,
    pub last_entry_id: u64,
}

struct Inner {
    ring: WalRing,
    /// Per page: how many of its bytes the file writer has taken. A page is
    /// appended to while it is being written out, so this is a cursor rather
    /// than a flag — the writer takes the run that appeared since last time.
    written: Vec<usize>,
    /// Fill of the open file, once a writer has armed it.
    fill: Option<FileFill>,
    /// Per page: which file the bytes currently on it belong to.
    ///
    /// This is what keeps a flush inside its own file. The enqueue side runs
    /// ahead of the writer over an unbounded channel, so by the time a writer
    /// flushes, later pages may already hold records for files that are not
    /// open yet. A writer takes only the pages stamped with its own epoch, so
    /// several rotations can be outstanding at once and each file still gets
    /// exactly its own records.
    page_epoch: Vec<u64>,
    /// Bytes appended and not yet reported written. An estimate: it drives the
    /// flush hint, not a decision.
    unwritten: usize,
    /// Ids below this are no longer served from memory even if a page still
    /// holds them.
    ///
    /// Raised when a page is evicted by rotation or release, which is what can
    /// put a gap in what memory can answer. Reads are a contiguous run or they
    /// are wrong -- a range that silently omits the id in the middle is worse
    /// than one that says "read it from the file".
    cache_floor: u64,
    /// The highest id the file writer has taken responsibility for, in order.
    /// `None` until it has taken any. Nothing above it may be written.
    handed_through: Option<u64>,
    /// Per file whose closing flush did not land: the first id a retry still
    /// owes it. The bytes are the retry's; this is only the debt, and it is
    /// what stops a close certifying that everything accepted is on disk.
    stranded: BTreeMap<u64, u64>,
}

/// The offset table is only defined below `count`, which
/// `normfs_wal_page_offset` asserts, so the walk stops there.
fn first_id_at(inner: &Inner, k: usize, from: usize) -> Option<u64> {
    let first = inner.ring.page_first_entry_id(k)?;
    let count = inner.ring.page_len(k);
    for i in 0..count {
        if inner.ring.page_entry_offset(k, i) >= from {
            return Some(first + i as u64);
        }
    }
    None
}

impl Inner {
    /// Drops the per-page cursors, for a pool that has fallen out of step with
    /// the id sequence.
    ///
    /// Only `reinit` gets here, and only after it has already renumbered the
    /// pages, so the bytes are gone whatever this does. What this adds is that
    /// the cursors go with them, rather than pointing into a page whose
    /// contents now answer to different ids.
    fn forget_pages(&mut self) {
        self.written.iter_mut().for_each(|w| *w = 0);
        self.unwritten = 0;
    }

    /// Folds what rotation or release just evicted into the cache floor, so a
    /// read can never be served across the gap an eviction leaves.
    fn absorb_eviction(&mut self) {
        if let Some(last) = self.ring.take_evicted() {
            self.cache_floor = self.cache_floor.max(last.saturating_add(1));
        }
    }

    /// The lowest id memory can answer for, or `None` when it can answer for
    /// nothing.
    fn min_cached(&self) -> Option<u64> {
        let lowest = self.ring.min_cached_id()?;
        let floor = lowest.max(self.cache_floor);
        (floor < self.ring.next_entry_id()).then_some(floor)
    }
}

/// The offset-table slot every entry on a page costs beside its own bytes.
///
/// `normfs_wal_page_append` proves NO_SPACE holds exactly when
/// `used_bytes + 4 * count + entry_size + 4 > cap`, so on an empty page an
/// entry fits iff `entry_size + 4 <= cap`.
const PAGE_ENTRY_SLOT: usize = 4;

/// The smallest page the ring accepts: one empty record's frame plus its
/// offset slot. The C contracts require exactly this, so a smaller page is a
/// violated precondition, not a degenerate configuration.
pub const MIN_PAGE_SIZE: usize = crate::wal_entry_v1::WAL_ENTRY_V1_MIN_SIZE + PAGE_ENTRY_SLOT;

/// The largest record a page of `page_size` bytes will accept.
///
/// The one place this arithmetic lives. The enqueue guard refuses a record
/// before it is given an id and the pool refuses it after, and if those two
/// disagreed by so much as a byte the guard would pass records the pool then
/// has to turn away -- which is the one case with no good answer left, because
/// the id has already been taken and V1 numbers entries by position.
pub fn max_record_len(page_size: usize) -> usize {
    // Down from the largest record that could conceivably fit. The frame costs
    // between 5 and 9 bytes, so this settles within a handful of steps.
    let mut n = page_size.saturating_sub(PAGE_ENTRY_SLOT);
    while n > 0 && !fits_page(n, page_size) {
        n -= 1;
    }
    n
}

/// Whether a record of `record_len` bytes fits an empty page of `page_size`.
fn fits_page(record_len: usize, page_size: usize) -> bool {
    encoded_len_of(record_len)
        .and_then(|e| usize::try_from(e).ok())
        .and_then(|e| e.checked_add(PAGE_ENTRY_SLOT))
        .is_some_and(|need| need <= page_size)
}

/// What a record of this length costs the file: framing and CRC, not payload.
///
/// `None` for a record wider than the V1 frame, which is a record no file can
/// take. Callers decide what that means for them.
pub(crate) fn encoded_len_of(record_len: usize) -> Option<u64> {
    u32::try_from(record_len)
        .ok()
        .map(|n| crate::wal_entry_v1::encoded_len(n) as u64)
}

/// Charges a record that landed on a page, and stamps that page with the file
/// it now belongs to.
///
/// A file ends where a page ends. The threshold alone does not rotate: the
/// record that crosses it stays where it is, and the file runs on to the end of
/// the page it is on. So a page's records belong to one file by construction —
/// which is what a flush needs, because a page's bytes go to the file whole and
/// there is no boundary inside one to split at.
///
/// The cost is that `max_file_size` is a threshold rather than a cap: a file
/// overshoots it by at most the tail of one page.
fn charge_paged(
    inner: &mut Inner,
    entry_len: u64,
    record_len: usize,
    opened_page: bool,
) -> Placement {
    let active = inner.ring.active_page();
    let Some(fill) = inner.fill.as_mut() else {
        // Not armed: no writer is taking pages from this pool yet, so there is
        // no file to fill and nothing to decide. Nothing has stamped a page
        // either, so they are all still at the zero this would write.
        return Placement {
            in_pool: true,
            record_len,
            rotate: RotateHint::None,
            epoch: 0,
        };
    };

    // `has_written` guards the first record of a file: it opens a page by
    // definition, and without this a file whose max is below its own header
    // would be closed empty, leaving two files claiming the same
    // num_entries_before.
    let rotate = if opened_page && fill.has_written && fill.used >= fill.max {
        fill.epoch += 1;
        fill.used = fill.header_len.saturating_add(entry_len);
        RotateHint::Before
    } else {
        fill.used = fill.used.saturating_add(entry_len);
        RotateHint::None
    };
    fill.has_written = true;
    let epoch = fill.epoch;
    inner.page_epoch[active] = epoch;

    Placement {
        in_pool: true,
        record_len,
        rotate,
        epoch,
    }
}

/// Appends under the pool lock, keeping the file writer's cursor honest.
///
/// A rotation resets a page's bytes, and with them the cursor into that page.
/// Rotation is detected by `next_page_id`, which `normfs_wal_ring_rotate_to`
/// increments and nothing else touches. Comparing the active index instead
/// misses the ring rotating into the page that was already active, which is
/// legal when that page is empty or fully durable, and would leave a stale
/// cursor that silently swallowed the new page's first bytes.
///
/// Returns whether this append started a fresh page, which is where a file is
/// allowed to end.
fn append_locked(inner: &mut Inner, record: &[u8]) -> (AppendOutcome, bool) {
    let out = append_locked_inner(inner, record);
    inner.absorb_eviction();
    out
}

fn append_locked_inner(inner: &mut Inner, record: &[u8]) -> (AppendOutcome, bool) {
    let before_page_id = inner.ring.next_page_id();
    let outcome = inner.ring.append(record);
    let rotated = inner.ring.next_page_id() != before_page_id;
    if rotated {
        let active = inner.ring.active_page();
        inner.written[active] = 0;
    }
    // Counted here rather than at the caller, because this is the one place
    // bytes enter a page: `try_append` reaches it too, and a count kept only on
    // the `place` path would tell an idle writer there was nothing to flush.
    if matches!(outcome, AppendOutcome::Cached(_))
        && let Some(entry_len) = encoded_len_of(record.len())
    {
        inner.unwritten = inner.unwritten.saturating_add(entry_len as usize);
    }
    (outcome, rotated)
}

pub struct PagePool {
    inner: Mutex<Inner>,
    /// Set once a file writer is draining this pool. Until then nothing can
    /// report pages durable, so nothing may wait for one to be freed.
    drainer: std::sync::atomic::AtomicBool,
    /// Woken when [`PagePool::mark_durable`] advances the watermark, which is
    /// the only event that can turn a full pool into a pool with room.
    space: Notify,
    /// Poked at `flush_watermark` unwritten bytes. Otherwise the writer's
    /// interval tick is the only thing on this path that can start a flush.
    flush: Mutex<Option<Arc<Notify>>>,
    /// Fixed at construction, from the range the queue started with. A pool
    /// that has since taken more pages flushes a little earlier than half
    /// full, which is the harmless direction for a hint.
    flush_watermark: usize,
    /// The shared arena this pool's pages come from, if any. A pool built by
    /// [`PagePool::new`] owns its pages and can neither take one from the
    /// arena nor give one back.
    arena: Option<Arc<WalArena>>,
    /// The fewest pages this pool will release down to. One to append into and one
    /// the writer is draining; below that an appender waits for its own page to
    /// reach disk, which serialises the queue against the disk rather than
    /// merely bounding it.
    floor: usize,
    /// Never held across `inner`: it is taken, a decision is read out of it,
    /// and released before anything else is locked.
    stall: Mutex<Stall>,
}

impl PagePool {
    /// Allocates `page_count` pages of `page_size` bytes. This is the only
    /// allocation the pool ever performs.
    ///
    /// The pages are this pool's for life. See [`PagePool::new_in`] for a pool
    /// that borrows a slot range from the process-wide arena and can trade
    /// pages with other queues.
    pub fn new(page_count: usize, page_size: usize, first_entry_id: u64) -> Self {
        // Nothing below the first id was ever accepted, so the watermark
        // starts there: a recovered idle queue is durable, not busy.
        let mut ring = WalRing::new(page_count, page_size, first_entry_id);
        ring.set_essential(first_entry_id);
        PagePool {
            inner: Mutex::new(Inner {
                ring,
                written: vec![0; page_count],
                fill: None,
                page_epoch: vec![0; page_count],
                unwritten: 0,
                cache_floor: 0,
                handed_through: None,
                stranded: BTreeMap::new(),
            }),
            space: Notify::new(),
            drainer: std::sync::atomic::AtomicBool::new(false),
            flush: Mutex::new(None),
            flush_watermark: (page_count * page_size / 2).max(page_size),
            arena: None,
            floor: page_count,
            stall: Mutex::new(Stall::default()),
        }
    }

    pub fn set_flush_signal(&self, flush: Arc<Notify>) {
        *self.flush.lock().unwrap() = Some(flush);
    }

    fn signal_flush(&self) {
        if let Some(flush) = self.flush.lock().unwrap().as_ref() {
            flush.notify_one();
        }
    }

    /// Borrows the slot range `arena` has reserved for `ring_id`.
    ///
    /// Unlike [`PagePool::new`] this allocates nothing: the bytes already
    /// exist, and what the pool holds is a claim on part of them. The claim can
    /// move — see [`PagePool::place`], which takes a free slot rather than
    /// waiting for one of its own pages to reach disk, and
    /// [`PagePool::mark_durable`], which hands the top page back once it is
    /// safe to.
    pub fn new_in(
        arena: &Arc<WalArena>,
        range: SlotRange,
        ring_id: u64,
        first_entry_id: u64,
        floor: usize,
    ) -> Self {
        let ring = WalRing::in_arena(arena, range, ring_id, first_entry_id);
        let page_count = ring.page_count();
        let page_size = ring.page_size();
        PagePool {
            inner: Mutex::new(Inner {
                written: vec![0; page_count],
                page_epoch: vec![0; page_count],
                ring,
                fill: None,
                unwritten: 0,
                cache_floor: 0,
                handed_through: None,
                stranded: BTreeMap::new(),
            }),
            space: Notify::new(),
            drainer: std::sync::atomic::AtomicBool::new(false),
            flush: Mutex::new(None),
            flush_watermark: (page_count * page_size / 2).max(page_size),
            arena: Some(Arc::clone(arena)),
            floor: floor.max(1),
            stall: Mutex::new(Stall::default()),
        }
    }

    /// Takes the free arena slot above this pool's range, if there is one.
    ///
    /// The per-page bookkeeping widens with it. `written` starts at zero because
    /// the page is empty, and `page_epoch` at zero because nothing has been
    /// charged to it yet — the append that lands on it stamps it with the file
    /// it belongs to, exactly as `charge_paged` does for every other page.
    fn try_retain_page(&self) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.ring.retain_page() {
            return false;
        }
        let pages = inner.ring.page_count();
        inner.written.resize(pages, 0);
        inner.page_epoch.resize(pages, 0);
        log::debug!(
            target: "normfs-wal",
            "page pool took arena slot {}, now {pages} pages",
            inner.ring.slot_range().map_or(0, |r| r.first_slot + pages - 1),
        );
        true
    }

    pub fn try_append_evicting(&self, record: &[u8]) -> AppendOutcome {
        let mut inner = self.inner.lock().unwrap();
        let (first_try, _) = append_locked(&mut inner, record);
        if !matches!(first_try, AppendOutcome::Full) || self.has_drainer() {
            return first_try;
        }
        let active = inner.ring.active_page();
        // A pinned oldest page is passed over: the next-oldest goes instead,
        // and its span is condemned too. That loses one extra page of window,
        // where the reseed alternative loses all of them. Scan and retry sit
        // under one lock, so the page condemned is the page the retry takes.
        let mut oldest: Option<(u64, u64)> = None;
        for k in 0..inner.ring.page_count() {
            if k == active || inner.ring.page_pin_count(k) > 0 {
                continue;
            }
            let (Some(first), Some(last)) = (
                inner.ring.page_first_entry_id(k),
                inner.ring.page_last_entry_id(k),
            ) else {
                continue;
            };
            if oldest.is_none_or(|(f, _)| first < f) {
                oldest = Some((first, last));
            }
        }
        let Some((_, last)) = oldest else {
            return AppendOutcome::Full;
        };
        let past = last.saturating_add(1);
        if past <= inner.ring.min_essential_id() {
            return AppendOutcome::Full;
        }
        inner.ring.set_essential(past);
        append_locked(&mut inner, record).0
    }

    /// The arena this pool's pages come from, if it shares one.
    pub fn arena(&self) -> Option<&Arc<WalArena>> {
        self.arena.as_ref()
    }

    /// How many pages this pool currently holds.
    pub fn page_count(&self) -> usize {
        self.inner.lock().unwrap().ring.page_count()
    }

    /// This pool's slot range in the shared arena, if it has one.
    pub fn slot_range(&self) -> Option<SlotRange> {
        self.inner.lock().unwrap().ring.slot_range()
    }

    /// Hands every page back to the arena, for a queue that is shutting down.
    ///
    /// Does nothing unless every page is reusable: `give_back` requires it, so
    /// a range still holding records that are not on disk stays put.
    pub fn release_to_arena(&self) {
        let Some(arena) = self.arena.as_ref() else {
            return;
        };
        // One lock: the range is read and released under the same guard, so it
        // cannot change width between the two.
        let inner = self.inner.lock().unwrap();
        let Some(range) = inner.ring.slot_range() else {
            return;
        };
        let essential = inner.ring.min_essential_id();
        let all_reusable =
            (0..inner.ring.page_count()).all(|k| inner.ring.page_is_reusable(k, essential));
        if !all_reusable {
            log::debug!(
                target: "normfs-wal",
                "not releasing {} pages to the arena: some still hold records that are not on disk",
                range.page_count,
            );
            return;
        }
        arena.release(range, inner.ring.ring_id(), essential);
        arena.forget_label(inner.ring.ring_id());
    }

    /// Whether every record in every page is on disk: what a close needs to
    /// know before it certifies anything.
    pub fn is_fully_durable(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        // The one case the pages cannot answer for: `take_stranded` advanced
        // their cursors, so they look durable while a file is missing its tail.
        if !inner.stranded.is_empty() {
            return false;
        }
        let essential = inner.ring.min_essential_id();
        (0..inner.ring.page_count()).all(|k| inner.ring.page_is_reusable(k, essential))
    }

    /// Whether a file writer is taking pages from this pool, and therefore
    /// whether waiting for a page can ever end.
    pub fn has_drainer(&self) -> bool {
        self.drainer.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Marks that a file writer has taken responsibility for draining this
    /// pool. Callers may wait for a page only after this.
    pub fn set_drainer(&self) {
        self.drainer
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Gives up that responsibility. Waiting for a page nothing drains is a
    /// wait nothing can end.
    pub fn clear_drainer(&self) {
        self.drainer
            .store(false, std::sync::atomic::Ordering::Release);
        self.space.notify_waiters();
    }

    /// Takes over the rotation decision from the writer.
    ///
    /// Called once when the WAL writer starts, beside
    /// [`PagePool::set_drainer`]. Until it is called, [`PagePool::place`]
    /// declines to decide and the writer keeps deciding for itself, which is
    /// what the unpooled path relies on.
    ///
    /// `header_len` should be the *widest* a V1 header can be, not the current
    /// one's encoded length: `WalHeader::resize` can widen the id and data size
    /// fields when a file rotates, and the enqueue side cannot know the next
    /// header's exact size without duplicating that logic. Over-charging by a
    /// few bytes rotates fractionally early, which is the safe direction
    /// against a cap.
    pub fn arm_file_fill(&self, max_file_size: u64, header_len: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.fill = Some(FileFill {
            used: header_len,
            max: max_file_size,
            header_len,
            has_written: false,
            epoch: 0,
        });

        // Whatever the pages already hold is not this writer's to write. A
        // queue upgraded from readonly to write keeps its `MemQueue`, and so
        // its pool: those records were cached by reads, or written by the
        // previous writer, and either way this one has no entry for them in
        // its file and no index to place them at. Writing them would append
        // records the header does not account for, which V1's positional ids
        // turn into every later entry reading back under the wrong one.
        //
        // So the cursor starts at what is there: this writer emits only the
        // records it is told about. `handed_through` is what enforces that --
        // cleared here, it holds every flush back until this writer has taken
        // an entry of its own, and the cursors keep the pages it inherited out
        // of that entry's file.
        for k in 0..inner.ring.page_count() {
            inner.written[k] = inner.ring.page_bytes(k).len();
            inner.page_epoch[k] = 0;
        }
        inner.handed_through = None;
    }

    /// Bytes charged to the open file so far, including its header. For tests.
    pub fn fill_used(&self) -> Option<u64> {
        self.inner.lock().unwrap().fill.as_ref().map(|f| f.used)
    }

    /// The file the next record will be charged to, counted from zero.
    pub fn epoch(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .fill
            .as_ref()
            .map_or(0, |f| f.epoch)
    }

    /// Appends without waiting. `Full` means every page is either pinned by a
    /// reader or still holds records that are not on disk.
    pub fn try_append(&self, record: &[u8]) -> AppendOutcome {
        let mut inner = self.inner.lock().unwrap();
        append_locked(&mut inner, record).0
    }

    /// Appends the record that must land on `expected_id`, waiting until a
    /// page can be reclaimed.
    ///
    /// The caller owns the id sequence; the pool follows it. They can only
    /// disagree after a record the pool refused, and re-seeding to catch up
    /// discards pages, so callers must serialise their appends.
    pub async fn place(&self, expected_id: u64, record: &[u8]) -> Result<Placement, PoolError> {
        // Unwrap-free: a record too wide to frame never reaches `charge_paged`,
        // because `append_locked` reports `TooLarge` for it first.
        let entry_len = encoded_len_of(record.len()).unwrap_or(u64::MAX);
        match self.try_place(expected_id, record, entry_len) {
            Ok(Some(placed)) => return Ok(placed),
            Ok(None) => {}
            Err(e) => return Err(e),
        }

        // Registered before the retry re-tests the pool: `notify_waiters`
        // leaves no permit, and `enable()` is what joins the waiter list.
        let had_drainer = self.has_drainer();
        loop {
            let woken = self.space.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();

            match self.try_place(expected_id, record, entry_len) {
                Ok(Some(placed)) => {
                    self.note_resumed(expected_id);
                    return Ok(placed);
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }

            // Only a wait the close orphaned: a pool that never had a
            // drainer may still be fed durability by hand, and its waits are
            // the caller's own. Checked between arming the waiter and parking
            // on it, so a `clear_drainer` cannot slip through the gap -- it
            // flips the flag before it notifies, and the notify wakes this
            // waiter.
            if had_drainer && !self.has_drainer() {
                return Err(PoolError::NoDrainer);
            }

            // `Full` means every page this queue holds is pinned or still holds
            // records that are not on disk. Before waiting for the disk, take
            // the free slot above the range if there is one: it costs nothing
            // anybody else is using, and the process-wide total does not move
            // -- the page came from the arena, and some other queue released
            // it. Nothing is allocated here or anywhere below this line; when
            // no slot is free the queue waits for its own pages instead.
            //
            // A ring can take the slot directly above it and no other, so this
            // fails on a fragmented arena even when free pages exist. That is
            // the price of a contiguous range, and waiting is the correct
            // fallback rather than an error.
            if self.try_retain_page() {
                continue;
            }

            // Only a flush can end this wait, so ask for one rather than
            // waiting for the writer's next tick. Without this an appender
            // waiting on a full pool pays a whole flush interval per page.
            self.signal_flush();

            self.note_wait();
            if tokio::time::timeout(STALL_WARN_AFTER, woken).await.is_err() {
                let _ = self.report_stall();
            }
        }
    }

    /// `Ok(None)` means the pool cannot take the record yet.
    /// [`PagePool::place`] without the wait: `Ok(None)` where that would park.
    pub fn try_place_now(
        &self,
        expected_id: u64,
        record: &[u8],
    ) -> Result<Option<Placement>, PoolError> {
        let entry_len = encoded_len_of(record.len()).unwrap_or(u64::MAX);
        if let Some(placed) = self.try_place(expected_id, record, entry_len)? {
            return Ok(Some(placed));
        }
        if self.try_retain_page()
            && let Some(placed) = self.try_place(expected_id, record, entry_len)?
        {
            return Ok(Some(placed));
        }
        self.signal_flush();
        Ok(None)
    }

    fn try_place(
        &self,
        expected_id: u64,
        record: &[u8],
        entry_len: u64,
    ) -> Result<Option<Placement>, PoolError> {
        let over_watermark;
        let placed = {
            let mut inner = self.inner.lock().unwrap();
            if inner.ring.next_entry_id() != expected_id {
                // Behind, and by a small enough margin to walk: step the
                // sequence over the ids that went past without a record, which
                // keeps everything already held. Re-seeding is the fallback,
                // not the first move, because it discards -- and a discarded
                // record here is one that was accepted and never written.
                let behind = expected_id.checked_sub(inner.ring.next_entry_id());
                match behind.filter(|&n| n > 0 && n <= MAX_STEPPABLE_GAP) {
                    Some(n) => {
                        for _ in 0..n {
                            if !inner.ring.skip_entry() {
                                return Ok(None);
                            }
                            inner.absorb_eviction();
                            let active = inner.ring.active_page();
                            inner.written[active] = 0;
                        }
                        log::warn!(
                            target: "normfs-wal",
                            "page pool stepped over {n} id(s) nothing appended: something \
                             took ids for this queue without holding its append gate"
                        );
                    }
                    None => {
                        // Renumbering discards everything held, so it is only
                        // for a pool with nothing to lose: everything durable
                        // and every held record written. Anything less waits
                        // for the disk instead -- a record accepted and not yet
                        // written must never be discarded to catch up.
                        let nothing_to_lose = inner.ring.is_empty()
                            || inner.ring.min_essential_id() >= inner.ring.next_entry_id();
                        if !nothing_to_lose || !inner.ring.reinit(expected_id) {
                            return Ok(None);
                        }
                        inner.forget_pages();
                    }
                }
            }
            let (outcome, opened_page) = append_locked(&mut inner, record);
            match outcome {
                AppendOutcome::Cached(_) => {
                    over_watermark = inner.unwritten >= self.flush_watermark;
                    charge_paged(&mut inner, entry_len, record.len(), opened_page)
                }
                // The pre-id guard let a record through that no page can
                // hold. There is no id-preserving way to take it now, and
                // taking it any other way renumbers everything after it.
                AppendOutcome::TooLarge => return Err(PoolError::TooLarge),
                AppendOutcome::Full => return Ok(None),
            }
        };
        if over_watermark {
            self.signal_flush();
        }
        Ok(Some(placed))
    }

    /// Records that an append went to the wait path.
    ///
    /// One queue's appends are serialised, so this is one wait per record: a
    /// queue a thousand records behind would otherwise report a thousand times
    /// about a single condition.
    pub(crate) fn note_wait(&self) {
        let mut stall = self.stall.lock().unwrap();
        if stall.since.is_none() {
            stall.since = Some(Instant::now());
            stall.waits = 0;
            stall.reported = None;
        }
        stall.waits = stall.waits.saturating_add(1);
    }

    /// Whether the pool is mid-stall, and how many appends have waited in it.
    /// The tests rule on this; nothing else reads it.
    #[cfg(test)]
    pub(crate) fn stall_waits(&self) -> Option<u64> {
        let stall = self.stall.lock().unwrap();
        stall.since.map(|_| stall.waits)
    }

    /// Closes out a stall, and only if its start was reported. Without this the
    /// log shows a queue going quiet without saying whether it recovered.
    pub(crate) fn note_resumed(&self, expected_id: u64) {
        let summary = {
            let mut stall = self.stall.lock().unwrap();
            match (stall.since.take(), stall.reported.take()) {
                (Some(since), Some(_)) => {
                    let waits = std::mem::take(&mut stall.waits);
                    Some((since.elapsed(), waits))
                }
                _ => {
                    stall.waits = 0;
                    None
                }
            }
        };
        match summary {
            Some((held, waits)) => log::warn!(
                target: "normfs-wal",
                "WAL pages available again after {:.1}s, {waits} append(s) had to wait; \
                 resumed at entry {expected_id}",
                held.as_secs_f64(),
            ),
            None => {
                log::debug!(target: "normfs-wal", "page pool: resumed at entry {expected_id}")
            }
        }
    }

    /// Reports a stall at most once every [`STALL_REPORT_EVERY`], carrying the
    /// count of appends that waited since the last line. Returns whether it
    /// emitted, which is what the tests rule on.
    pub(crate) fn report_stall(&self) -> bool {
        let due = {
            let mut stall = self.stall.lock().unwrap();
            let first = stall.reported.is_none();
            let elapsed = stall.reported.map(|at| at.elapsed());
            if first || elapsed.is_some_and(|e| e >= STALL_REPORT_EVERY) {
                stall.reported = Some(Instant::now());
                let held = stall.since.map(|at| at.elapsed()).unwrap_or_default();
                Some((held, stall.waits))
            } else {
                None
            }
        };
        let Some((held, waits)) = due else {
            return false;
        };
        self.warn_stalled(held, waits);
        true
    }

    /// Reports which page is holding the pool up.
    ///
    /// What ran out is the first thing the line has to settle, because the two
    /// answers need opposite actions: a moving durable watermark means the
    /// queue has too few pages, a stuck one means more pages would only
    /// postpone this.
    fn warn_stalled(&self, held: Duration, waits: u64) {
        let inner = self.inner.lock().unwrap();
        let essential = inner.ring.min_essential_id();
        let pinned = (0..inner.ring.page_count())
            .filter(|&k| inner.ring.page_pin_count(k) > 0)
            .count();
        let oldest = (0..inner.ring.page_count())
            .filter_map(|k| inner.ring.page_last_entry_id(k).map(|last| (k, last)))
            .min_by_key(|&(_, last)| last);
        match oldest {
            Some((k, last)) => log::warn!(
                target: "normfs-wal",
                "out of WAL pages for {:.0}s, {waits} append(s) waiting: all {} pages of \
                 this queue are in use — {pinned} pinned by readers, durable through \
                 entry {essential}, oldest page {k} ends at entry {last} (pins {}). \
                 This is the page budget, not disk throughput: if `durable through` is \
                 rising, the queue needs more pages (max_memory_usage); if it is not, \
                 the writer is stuck and pages will not come back.",
                held.as_secs_f64(),
                inner.ring.page_count(),
                inner.ring.page_pin_count(k),
            ),
            None => log::warn!(
                target: "normfs-wal",
                "out of WAL pages for {:.0}s but every page is empty — this is a bug",
                held.as_secs_f64(),
            ),
        }

        // With one arena for the whole process, "the pool is full" is only half
        // the story: this queue could not take a page because the slot above it
        // is held, and what a reader wants to know is by whom. Naming the holders
        // turns a stall into something to act on rather than guess at.
        if let Some(arena) = self.arena.as_ref() {
            let holders = arena.holders();
            let free = arena.free_pages();
            log::warn!(
                target: "normfs-wal",
                "arena: {free} of {} pages free, held by {} — a queue can only take \
                 the slot directly above its own range, so free pages elsewhere in \
                 the arena do not reach it",
                arena.page_count(),
                if holders.is_empty() {
                    "nobody".to_string()
                } else {
                    holders
                        .iter()
                        .map(|(name, pages)| format!("{name} ({pages})"))
                        .collect::<Vec<_>>()
                        .join(", ")
                },
            );
        }
    }

    /// Byte runs that have been appended but not yet handed to the file writer,
    /// **in id order** — and never past the end of the file that is open, nor
    /// past the last entry the writer has actually taken responsibility for.
    ///
    /// This is the only road to the file, which is what keeps one id order over
    /// everything on it: two roads cannot be ordered against each other, and V1
    /// turns a swapped pair into every later entry answering to the wrong id.
    ///
    /// The runs come out sorted by id, not by page index. A ring rotates into
    /// whichever page is reusable, so index order is not id order and the walk
    /// below cannot stand in for the sort.
    ///
    /// The bytes are copied out under the lock rather than borrowed: the writer
    /// awaits I/O, and holding the pool locked across that would block every
    /// appender for the duration of a disk write.
    ///
    /// This does not mark the runs as taken — call
    /// [`PagePool::commit_written`] once a run is actually on disk, or an
    /// uncommitted run just comes back here. Advancing the cursor at take time
    /// would drop the bytes of any write that then failed.
    ///
    /// ## Why `epoch`
    ///
    /// A flush must not write bytes belonging to the next file into this one.
    /// That is the whole of trap 3: the pool is filled at enqueue time and the
    /// file is closed later, so an unfiltered close would drain records that the
    /// rotation had already assigned to the next file, and the reader — which
    /// derives V1 ids positionally — would be skewed by exactly that many
    /// entries.
    ///
    /// Each writer passes the file it is writing, and gets the pages stamped
    /// with it. No page carries two epochs, because a file only ever ends where
    /// a page ends, so this is a filter rather than a cut: nothing has to be
    /// split, and there is no straddling case left to detect.
    ///
    /// A run of a closed file is skipped and logged rather than taken.
    /// Absorbing it into this file would put its records under this file's
    /// `num_entries_before`, renumbering every record after them; a gap is at
    /// least visible to a reader, where a silent renumbering is not.
    ///
    /// ## Why the handover bound
    ///
    /// A record's bytes are in its page from the moment `place` returns, which
    /// is before the writer has been told the entry exists — the two are joined
    /// by an unbounded channel and `NormFS::enqueue` releases the append gate
    /// between them, so entries reach the writer out of order and wait in its
    /// `OrderedBuffer`. Without a bound, a flush landing in that window writes
    /// the later record first.
    ///
    /// `note_handed_over` is the writer saying "this id is mine now", in the id
    /// order its buffer restores. Nothing above that id is taken, and a page is
    /// cut at the offset of the first entry beyond it — which the page's own
    /// offset table gives exactly, so the cut is between two entries and never
    /// inside one.
    pub fn take_pending(&self, epoch: u64) -> Vec<(PendingWrite, Bytes)> {
        let inner = self.inner.lock().unwrap();
        let count = inner.ring.page_count();
        let bound = inner.handed_through;
        let mut out: Vec<(PendingWrite, Bytes)> = Vec::new();

        for k in 0..count {
            let used = inner.ring.page_bytes(k).len();
            let from = inner.written[k];
            let entries = inner.ring.page_len(k);
            if from >= used || entries == 0 {
                continue;
            }
            let (Some(first_entry_id), Some(last_entry_id)) = (
                inner.ring.page_first_entry_id(k),
                inner.ring.page_last_entry_id(k),
            ) else {
                continue;
            };
            if inner.page_epoch[k] > epoch {
                continue;
            }

            // Cut at the handover bound. `to` is where the first entry the
            // writer has not claimed begins; the whole page when it has claimed
            // them all.
            let Some(bound) = bound else { continue };
            if first_entry_id > bound {
                continue;
            }
            let (to, last_entry_id) = if last_entry_id <= bound {
                (used, last_entry_id)
            } else {
                let next = (bound - first_entry_id + 1) as u32;
                (inner.ring.page_entry_offset(k, next), bound)
            };
            if from >= to {
                continue;
            }

            if inner.page_epoch[k] < epoch {
                // Skipped, not absorbed: this file's header does not account
                // for them, so writing them here renumbers every later entry.
                // A gap is at least visible to a reader.
                log::error!(
                    target: "normfs-wal",
                    "page {k} still holds unwritten entries ..={last_entry_id} for file {}, \
                     which is already closed: its last flush did not complete and these \
                     records reach no file",
                    inner.page_epoch[k],
                );
                continue;
            }
            out.push((
                PendingWrite {
                    page: k,
                    from,
                    to,
                    first_entry_id,
                    last_entry_id,
                },
                Bytes::copy_from_slice(&inner.ring.page_bytes(k)[from..to]),
            ));
        }

        // By id, not by the index the walk visited. The ids on any one page are
        // a dense run and no two pages' runs overlap, so comparing first ids is
        // a total order rather than an approximation of one.
        out.sort_by_key(|(w, _)| w.first_entry_id);
        out
    }

    /// Lifts out what file `epoch`'s closing flush did not land, so a retry can
    /// finish that file while the queue writes the next one.
    ///
    /// ## Why this copies, and commits
    ///
    /// Leaving the bytes in their pages fails either way. Holding the pages
    /// means holding `min_essential_id` at the hole, and that is one scalar for
    /// the whole ring: every later page becomes unreclaimable, `place` parks,
    /// and `begin_close` deadlocks behind the append gate that parked appender
    /// holds. Not holding them means `arm_file_fill` — which runs on every
    /// writer start, including the restart after an incomplete close — resets
    /// every cursor, and the retry finds nothing and calls the file done.
    pub fn take_stranded(&self, epoch: u64) -> Option<Stranded> {
        let mut inner = self.inner.lock().unwrap();
        let bound = inner.handed_through?;
        let count = inner.ring.page_count();
        let mut runs: Vec<(PendingWrite, Bytes)> = Vec::new();

        for k in 0..count {
            // The page that triggered the rotation is stamped with the next
            // epoch and holds that file's first records, not this one's.
            if inner.page_epoch[k] != epoch {
                continue;
            }
            let used = inner.ring.page_bytes(k).len();
            let from = inner.written[k];
            if from >= used || inner.ring.page_len(k) == 0 {
                continue;
            }
            let (Some(first_entry_id), Some(last_entry_id)) = (
                inner.ring.page_first_entry_id(k),
                inner.ring.page_last_entry_id(k),
            ) else {
                continue;
            };
            // Never above the handover bound, for `take_pending`'s reason: a
            // record the writer was never told about has no place in this file.
            // `close` runs with such records parked in the reorder buffer, so
            // this is reachable rather than theoretical.
            if first_entry_id > bound {
                continue;
            }
            let (to, last_entry_id) = if last_entry_id <= bound {
                (used, last_entry_id)
            } else {
                let next = (bound - first_entry_id + 1) as u32;
                (inner.ring.page_entry_offset(k, next), bound)
            };
            if from >= to {
                continue;
            }
            let Some(first_owed) = first_id_at(&inner, k, from) else {
                continue;
            };
            runs.push((
                PendingWrite {
                    page: k,
                    from,
                    to,
                    first_entry_id: first_owed,
                    last_entry_id,
                },
                Bytes::copy_from_slice(&inner.ring.page_bytes(k)[from..to]),
            ));
        }

        if runs.is_empty() {
            return None;
        }
        runs.sort_by_key(|(w, _)| w.first_entry_id);

        for (write, _) in &runs {
            let PendingWrite { page, to, .. } = *write;
            if to > inner.written[page] {
                let taken = to - inner.written[page];
                inner.written[page] = to;
                inner.unwritten = inner.unwritten.saturating_sub(taken);
            }
        }

        let first_entry_id = runs[0].0.first_entry_id;
        let last_entry_id = runs[runs.len() - 1].0.last_entry_id;
        inner.stranded.insert(epoch, first_entry_id);

        Some(Stranded {
            runs,
            first_entry_id,
            last_entry_id,
        })
    }

    pub fn clear_stranded(&self, epoch: u64) {
        self.inner.lock().unwrap().stranded.remove(&epoch);
    }

    pub fn has_stranded(&self) -> bool {
        !self.inner.lock().unwrap().stranded.is_empty()
    }

    /// The first id this queue may not report durable.
    pub fn stranded_from(&self) -> Option<u64> {
        self.inner.lock().unwrap().stranded.values().min().copied()
    }

    /// Marks a run from [`PagePool::take_pending`] as written. Only moves the
    /// cursor forward.
    pub fn commit_written(&self, write: &PendingWrite) {
        let mut inner = self.inner.lock().unwrap();
        let PendingWrite { page, to, .. } = *write;
        if to > inner.written[page] {
            let taken = to - inner.written[page];
            inner.written[page] = to;
            inner.unwritten = inner.unwritten.saturating_sub(taken);
        }
    }

    /// The writer has taken responsibility for every entry up to `entry_id`, in
    /// id order. Nothing above it may be written yet — see the handover bound
    /// on [`PagePool::take_pending`].
    pub fn note_handed_over(&self, entry_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.handed_through = Some(match inner.handed_through {
            Some(prev) => prev.max(entry_id),
            None => entry_id,
        });
    }

    /// Records that every entry below `first_non_durable_id` is on disk, and
    /// wakes whoever is waiting for a page.
    ///
    /// **Only call this after an `fsync` covering those entries has returned.**
    /// Everything the pool promises rests on that: the C ring will hand a page
    /// to be overwritten as soon as this watermark passes its last entry, and it
    /// is right to do so exactly when the bytes are safe.
    pub fn mark_durable(&self, first_non_durable_id: u64) {
        {
            let mut inner = self.inner.lock().unwrap();
            if first_non_durable_id <= inner.ring.min_essential_id() {
                return;
            }
            inner.ring.set_essential(first_non_durable_id);

            // Advancing the watermark is the only event that can make a page
            // reusable, so it is also the only moment a queue can discover it
            // is holding more of the arena than it needs. Give the top pages
            // back while they are safe to give, down to the floor.
            //
            // `release_page` refuses the active page, refuses to go below the
            // floor, and checks `normfs_wal_page_reusable` -- which is the
            // predicate `normfs_wal_ring_release_page` requires, not a
            // restatement of it. So a page carrying records that are not yet on
            // disk, or that a reader is holding, cannot leave this queue.
            let mut released = 0usize;
            while inner.ring.release_page(self.floor) {
                released += 1;
            }
            // A released page can hold ids from the middle of the cached run;
            // the floor rises past them so memory never serves across the gap.
            inner.absorb_eviction();
            if released > 0 {
                let pages = inner.ring.page_count();
                inner.written.resize(pages, 0);
                inner.page_epoch.resize(pages, 0);
                log::debug!(
                    target: "normfs-wal",
                    "page pool returned {released} page(s) to the arena, now {pages} pages",
                );
            }
        }
        self.space.notify_waiters();
    }

    /// Whether the writer still owes the file work.
    ///
    /// The timer asks this before taking the pool lock and walking every page:
    /// a queue written to once a week is idle for the whole of that week, and
    /// the tick is what puts its one record on disk, so the tick has to be
    /// cheap rather than rare.
    ///
    /// "Not yet durable" rather than "not yet written", which is two scalars
    /// and covers a case the narrower question does not: bytes that reached the
    /// file but whose fsync failed are written, so a not-yet-written test would
    /// report nothing to do and no later tick would ever retry the sync. The
    /// watermark is what actually says the work is finished, because it is what
    /// only moves once a sync has returned.
    pub fn has_pending(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.ring.min_essential_id() < inner.ring.next_entry_id()
    }

    /// The id the next appended record will get.
    pub fn next_entry_id(&self) -> u64 {
        self.inner.lock().unwrap().ring.next_entry_id()
    }

    /// Everything below this id is on disk.
    pub fn durable_before(&self) -> u64 {
        self.inner.lock().unwrap().ring.min_essential_id()
    }

    /// Runs `f` against the ring under the pool lock, for reads.
    ///
    /// The delegating accessors below cover what the store needs; this is for
    /// tests that assert on page-level state — which page holds which ids, how
    /// many pages a queue was given — where adding a delegate each time would
    /// widen the API for no caller.
    pub fn with_ring<T>(&self, f: impl FnOnce(&WalRing) -> T) -> T {
        f(&self.inner.lock().unwrap().ring)
    }

    /// Whether the pool holds no records.
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.ring.is_empty()
    }

    /// The lowest id still held in memory, or `None` when nothing is.
    pub fn min_cached_id(&self) -> Option<u64> {
        self.inner.lock().unwrap().min_cached()
    }

    /// Every held record with id in `[start, end]`, in id order.
    pub fn collect_range(&self, start: u64, end: u64) -> Vec<(u64, Vec<u8>)> {
        let inner = self.inner.lock().unwrap();
        let start = start.max(inner.cache_floor);
        // The floor can pass the whole request.
        if start > end {
            return Vec::new();
        }
        inner.ring.collect_range(start, end)
    }

    /// Every held record with id in `[start, end]`, in id order, **borrowed
    /// from the pages rather than copied out of them**.
    ///
    /// Each payload is a [`Bytes`] over the page the record was written into,
    /// and holds a pin on that page until it is dropped. See [`PageGuard`] for
    /// why that is sound.
    pub fn pin_range(self: &Arc<Self>, start: u64, end: u64) -> Vec<(u64, Bytes)> {
        let mut found: Vec<(u64, usize, *const u8, usize)> = Vec::new();
        let mut copied: Vec<(u64, Bytes)> = Vec::new();
        {
            let inner = self.inner.lock().unwrap();
            let start = start.max(inner.cache_floor);
            // The floor can pass the whole request.
            if start > end {
                return Vec::new();
            }
            let count = inner.ring.page_count();

            let pinned_now = (0..count)
                .filter(|&k| inner.ring.page_pin_count(k) > 0)
                .count();
            // Past this budget records are copied, not borrowed: a pin lasts as
            // long as the payload, and a pinned page is one no append can use.
            let share = (count / PIN_SHARE_DIVISOR).min(count.saturating_sub(PIN_RESERVE));
            let mut budget = share.saturating_sub(pinned_now);
            let active = inner.ring.active_page();
            let mut taking: Vec<bool> = vec![false; count];

            for k in 0..count {
                let n = inner.ring.page_len(k);
                let Some(first) = inner.ring.page_first_entry_id(k) else {
                    continue;
                };
                // Cheap page-level skip on the id span before touching entries.
                let last = first + u64::from(n) - 1;
                if last < start || first > end {
                    continue;
                }
                if !taking[k] && inner.ring.page_pin_count(k) == 0 {
                    // Never the page being appended into. Pinning it stops the
                    // rotation that would free it, and it is the one page the
                    // writer cannot do without.
                    if budget == 0 || k == active {
                        for i in 0..n {
                            let id = first + u64::from(i);
                            if id < start || id > end {
                                continue;
                            }
                            if let Some(rec) = inner.ring.record(k, i) {
                                copied.push((id, Bytes::copy_from_slice(rec)));
                            }
                        }
                        continue;
                    }
                    budget -= 1;
                }
                taking[k] = true;
                for i in 0..n {
                    let id = first + u64::from(i);
                    if id < start || id > end {
                        continue;
                    }
                    // `record_ptr` rather than `record(..).as_ptr()`: this
                    // pointer outlives the call, so it has to carry the arena's
                    // provenance rather than a temporary slice's. See
                    // `WalRing::record_ptr_at`.
                    let Some((ptr, len)) = inner.ring.record_ptr(k, i) else {
                        continue;
                    };
                    if found.len() >= PIN_PAYLOADS_PER_READ {
                        // SAFETY: copied out before the lock is released, and
                        // the page cannot be reused while the lock is held.
                        copied.push((
                            id,
                            Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, len) }),
                        ));
                    } else {
                        found.push((id, k, ptr, len));
                    }
                }
            }
            // One pin per payload handed out, so each guard's drop balances
            // exactly one of them.
            let mut inner = inner;
            for &(_, k, _, _) in &found {
                inner.ring.pin(k);
            }
        }
        // The lock is released before any guard exists. A `PageGuard` unpins on
        // drop, which takes this same lock, so one dropped while it were held
        // would deadlock against itself.

        let mut out: Vec<(u64, Bytes)> = found
            .into_iter()
            .map(|(id, page_index, ptr, len)| {
                let guard = PageGuard {
                    pool: Arc::clone(self),
                    page_index,
                    ptr,
                    len,
                };
                (id, Bytes::from_owner(guard))
            })
            .collect();
        out.extend(copied);
        out.sort_by_key(|(id, _)| *id);
        out
    }

    /// Releases one pin taken by [`PagePool::pin_range`]. Called from
    /// [`PageGuard::drop`], and from nowhere else: an unbalanced unpin wraps
    /// the count and pins the page for good.
    fn unpin_page(&self, page_index: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.ring.unpin(page_index);
        drop(inner);
        // A page whose last reader has gone may now be reclaimable, and an
        // appender may be waiting for exactly that.
        self.space.notify_waiters();
    }

    /// Restarts the pool empty, numbering from `first_entry_id`.
    ///
    /// This drops whatever the pages held, so it is only for resynchronising a
    /// pool that has fallen out of step with the id sequence — never for making
    /// room. Making room is what waiting is for.
    /// False when every page is pinned, in which case nothing was reseeded.
    pub fn reseed(&self, first_entry_id: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.ring.reinit(first_entry_id) {
            return false;
        }
        inner.written.iter_mut().for_each(|w| *w = 0);
        inner.unwritten = 0;
        true
    }
}
