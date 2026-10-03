//! One arena of WAL pages, shared by every queue in the process.
//!
//! Rust allocates the arena, the page-descriptor array and the owner array
//! once, at startup, and hands all three to the verified C pool; C never
//! allocates. Page `k` is the arena slice at `k * page_size`, which is what
//! makes distinct pages provably disjoint without a quantifier over pairs.
//!
//! ## What the owner array is for
//!
//! `owner[k]` is either [`POOL_FREE`] or the id of the ring holding page `k`.
//! One slot per page, so "two rings hold the same page" is not a state this can
//! represent — cross-queue exclusivity is structural rather than argued. The C
//! side proves that ownership only moves through
//! `normfs_wal_pool_take`/`give_back`, and that `give_back` requires
//! `normfs_wal_page_is_reusable`: a page carrying records that are not yet on
//! disk, or that a reader still holds, cannot reach another queue.
//!
//! ## Slot ranges, and why they are contiguous
//!
//! A ring holds a contiguous range `[first_slot, first_slot + page_count)`. Not
//! an arbitrary set of slots, and not for convenience: an arbitrary slot array
//! would need "no two slots name the same page", a quantifier over pairs, which
//! is the shape that would not discharge anywhere until the arena removed it. A
//! range needs none — page `k` is slot `first_slot + k`, and disjointness
//! follows from the layout.
//!
//! The price is that a ring can take only the slot directly above it, so a
//! fragmented arena can hold free pages a given queue cannot reach. That is why
//! [`WalArena::reserve`] places a new range at the *start of the largest free
//! gap* rather than packing ranges back to back: packed, every queue but the
//! last would start with no room to expand. It is a heuristic — a range cannot be
//! moved once it holds records, so placement is decided with no knowledge of
//! how many queues will arrive.

use std::collections::HashMap;
use std::os::raw::c_int;
use std::sync::{Mutex, MutexGuard};

use crate::wal_ring_v1::{CWalPage, normfs_wal_page_init};

/// The owner value of a page no ring holds. Mirrors `NORMFS_WAL_POOL_FREE`.
pub const POOL_FREE: u64 = 0xFFFF_FFFF_FFFF_FFFF;

#[repr(C)]
pub(crate) struct CWalPool {
    pub(crate) pages: *mut CWalPage,
    pub(crate) arena: *mut u8,
    pub(crate) owner: *mut u64,
    pub(crate) page_count: usize,
    pub(crate) page_size: usize,
}

#[repr(C)]
struct CPoolTakeResult {
    index: usize,
    found: c_int,
}

unsafe extern "C" {
    fn normfs_wal_pool_init(
        pool: *mut CWalPool,
        pages: *mut CWalPage,
        arena: *mut u8,
        owner: *mut u64,
        page_count: usize,
        page_size: usize,
    );
    fn normfs_wal_pool_find_free(pool: *mut CWalPool, ring_id: u64) -> CPoolTakeResult;
    fn normfs_wal_pool_take(pool: *mut CWalPool, index: usize, ring_id: u64);
    fn normfs_wal_pool_give_back(
        pool: *mut CWalPool,
        index: usize,
        ring_id: u64,
        min_essential_id: u64,
    );
}

/// A contiguous run of pool slots handed to one ring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    pub first_slot: usize,
    pub page_count: usize,
}

/// Names for the rings holding slots, so a stalled pool can say *which* queue
/// is holding it up rather than only that something is.
#[derive(Default)]
pub(crate) struct Labels(HashMap<u64, String>);

/// The process-wide page arena.
///
/// Every `Vec` here is allocated once and never resized, so the raw pointers
/// the C pool and every ring hold into them stay valid for the arena's life.
/// Nothing takes a `&mut` to any of them after construction — all mutation goes
/// through the raw pointers, to disjoint elements, which is what makes rings on
/// different threads sound.
pub struct WalArena {
    _arena: Vec<u8>,
    _pages: Vec<CWalPage>,
    _owner: Vec<u64>,
    pool: Box<CWalPool>,
    page_size: usize,
    page_count: usize,
    /// Held across a check-and-take, so two rings cannot both decide the same
    /// free slot is theirs. Also guards the label map.
    ///
    /// Lock order: a `PagePool`'s own lock is taken *before* this one, never
    /// after.
    slots: Mutex<Labels>,
}

impl WalArena {
    /// Allocates `page_count` pages of `page_size` bytes, all free.
    pub fn new(page_count: usize, page_size: usize) -> Self {
        assert!(page_count >= 1, "arena needs at least one page");
        assert!(
            page_size >= 9,
            "page must hold the smallest entry plus its offset"
        );

        // One allocation for the bytes, never reallocated. Page k starts at
        // k * page_size, which is what makes the pages provably disjoint
        // without a pairwise separation axiom.
        let mut arena: Vec<u8> = vec![0u8; page_count * page_size];
        let base = arena.as_mut_ptr();

        let mut pages: Vec<CWalPage> = Vec::with_capacity(page_count);
        for k in 0..page_count {
            let mut page: CWalPage = unsafe { std::mem::zeroed() };
            unsafe {
                normfs_wal_page_init(&mut page, base.add(k * page_size), page_size, k as u64, 0);
            }
            pages.push(page);
        }

        // The pool contract requires the caller to mark the pages free: Rust
        // allocates this array anyway, so it fills it as it allocates, and C
        // does not have to write two disjoint regions and then re-establish a
        // quantified property across both.
        let mut owner: Vec<u64> = vec![POOL_FREE; page_count];

        let mut pool: Box<CWalPool> = Box::new(unsafe { std::mem::zeroed() });
        unsafe {
            normfs_wal_pool_init(
                pool.as_mut(),
                pages.as_mut_ptr(),
                base,
                owner.as_mut_ptr(),
                page_count,
                page_size,
            );
        }

        WalArena {
            _arena: arena,
            _pages: pages,
            _owner: owner,
            pool,
            page_size,
            page_count,
            slots: Mutex::new(Labels::default()),
        }
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn page_count(&self) -> usize {
        self.page_count
    }

    /// The C pool, for the ring bindings. Shared access only: the pool struct
    /// itself is written once, at construction.
    pub(crate) fn pool_ptr(&self) -> *mut CWalPool {
        &*self.pool as *const CWalPool as *mut CWalPool
    }

    /// Takes the slot lock. Held across a check-and-take so two rings cannot
    /// both conclude the same free slot is theirs.
    pub(crate) fn lock_slots(&self) -> MutexGuard<'_, Labels> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Who holds slot `k`, or [`POOL_FREE`].
    ///
    /// Takes the slot lock: every write to the owner array happens under it,
    /// and an unlocked read beside those writes is a data race, not a stale
    /// answer. Callers already holding the lock use [`WalArena::owner_at`].
    pub fn owner_of(&self, slot: usize) -> u64 {
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        self.owner_at(slot)
    }

    /// Gives one slot back. The caller holds the slot lock, owns the slot, and
    /// has checked `normfs_wal_page_reusable` -- this is the ring-drop path,
    /// where the range is released page by page rather than whole.
    pub(crate) fn give_back_slot(&self, slot: usize, ring_id: u64, min_essential_id: u64) {
        unsafe { normfs_wal_pool_give_back(self.pool_ptr(), slot, ring_id, min_essential_id) };
    }

    /// As [`WalArena::owner_of`], for callers already under the slot lock.
    pub(crate) fn owner_at(&self, slot: usize) -> u64 {
        assert!(slot < self.page_count, "slot {slot} is outside the arena");
        unsafe { *self.pool.owner.add(slot) }
    }

    /// The descriptor for slot `k`. Raw, because rings write their own pages
    /// through it concurrently with other rings writing theirs.
    pub(crate) fn page_ptr(&self, slot: usize) -> *mut CWalPage {
        unsafe { self.pool.pages.add(slot) }
    }

    /// The arena byte at which slot `k` begins.
    pub(crate) fn slot_base(&self, slot: usize) -> *mut u8 {
        unsafe { self.pool.arena.add(slot * self.page_size) }
    }

    /// Reserves `pages` slots for `ring_id`, seeding each page's ids from
    /// `first_entry_id`. Returns the range, or `None` if no gap is that wide.
    ///
    /// The range goes in the *largest* free gap, and where in that gap is the
    /// whole of the placement policy. A ring can take the slot directly above
    /// it, so free slots are only reachable by the range immediately below
    /// them: pack ranges back to back and every queue but the newest is walled
    /// in from the moment the next one starts.
    ///
    /// So the spare room in the gap is split. Half stays below the new range,
    /// as headroom for whichever range ends where the gap begins; the new range
    /// takes the rest above it. Nothing is reserved below when the gap starts
    /// at a slot nobody is under — the first queue in an empty arena keeps the
    /// whole thing above it.
    ///
    /// It is still a heuristic, and it has to be: a range cannot move once it
    /// holds records, so where to put one is decided with no knowledge of how
    /// many queues will arrive.
    pub fn reserve(&self, pages: usize, ring_id: u64, first_entry_id: u64) -> Option<SlotRange> {
        assert!(ring_id != POOL_FREE, "a ring cannot be called FREE");
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());

        let gap_start = self.largest_gap()?;
        let (_, gap_len) = self.gap_at(gap_start);
        if gap_len < pages {
            return None;
        }

        // Only leave headroom below if there is a range there to use it.
        let has_neighbour_below = gap_start > 0 && self.owner_at(gap_start - 1) != POOL_FREE;
        let spare = gap_len - pages;
        let below = if has_neighbour_below { spare / 2 } else { 0 };
        let first_slot = gap_start + below;

        for k in 0..pages {
            let slot = first_slot + k;
            // The pool's contract wants a page that is well-formed and empty
            // before anyone appends to it; re-initialising here is also what
            // stops a slot handed on from another queue answering this ring's
            // seeks with the previous holder's records.
            unsafe {
                normfs_wal_page_init(
                    self.page_ptr(slot),
                    self.slot_base(slot),
                    self.page_size,
                    slot as u64,
                    first_entry_id,
                );
                normfs_wal_pool_take(self.pool_ptr(), slot, ring_id);
            }
        }

        Some(SlotRange {
            first_slot,
            page_count: pages,
        })
    }

    /// Hands a whole range back, for a queue that is shutting down.
    ///
    /// `min_essential_id` is the caller's durable watermark: `give_back`
    /// requires every page to be reusable under it, so a range still holding
    /// records that are not on disk cannot be released.
    pub fn release(&self, range: SlotRange, ring_id: u64, min_essential_id: u64) {
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        for k in 0..range.page_count {
            unsafe {
                normfs_wal_pool_give_back(
                    self.pool_ptr(),
                    range.first_slot + k,
                    ring_id,
                    min_essential_id,
                );
            }
        }
    }

    /// Any free slot, by the proven C scan. Used only for diagnostics — a ring
    /// can take the slot above it and no other.
    pub fn any_free_slot(&self) -> Option<usize> {
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let r = unsafe { normfs_wal_pool_find_free(self.pool_ptr(), 1) };
        (r.found != 0).then_some(r.index)
    }

    /// Takes any free slot for `owner`, by the proven C scan. For holders of
    /// single slots, which never grow into a neighbour.
    pub(crate) fn take_any(&self, owner: u64) -> Option<usize> {
        assert!(owner != POOL_FREE, "a holder cannot be called FREE");
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let r = unsafe { normfs_wal_pool_find_free(self.pool_ptr(), owner) };
        if r.found == 0 {
            return None;
        }
        unsafe { normfs_wal_pool_take(self.pool_ptr(), r.index, owner) };
        Some(r.index)
    }

    /// Gives back a slot taken with [`WalArena::take_any`]. Its page never held
    /// a record, so it is reusable under any watermark.
    pub(crate) fn give_back_any(&self, slot: usize, owner: u64) {
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            self.owner_at(slot),
            owner,
            "slot {slot} is not held by {owner}"
        );
        unsafe { normfs_wal_pool_give_back(self.pool_ptr(), slot, owner, 0) };
    }

    /// How many slots no ring holds.
    pub fn free_pages(&self) -> usize {
        let _slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        (0..self.page_count)
            .filter(|&k| self.owner_at(k) == POOL_FREE)
            .count()
    }

    /// Records a human name for a ring, for [`WalArena::holders`].
    pub fn set_label(&self, ring_id: u64, label: impl Into<String>) {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots.0.insert(ring_id, label.into());
    }

    pub fn forget_label(&self, ring_id: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        slots.0.remove(&ring_id);
    }

    /// Who is holding the arena, as `(label, pages held)`, most pages first.
    ///
    /// This is what turns "the pool is full" into something actionable: a
    /// stalled queue can name the queue that is actually holding the memory.
    pub fn holders(&self) -> Vec<(String, usize)> {
        let slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        let mut held: HashMap<u64, usize> = HashMap::new();
        for k in 0..self.page_count {
            let owner = self.owner_at(k);
            if owner != POOL_FREE {
                *held.entry(owner).or_default() += 1;
            }
        }
        let mut out: Vec<(String, usize)> = held
            .into_iter()
            .map(|(id, pages)| {
                let name = slots
                    .0
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| format!("ring {id}"));
                (name, pages)
            })
            .collect();
        out.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        out
    }

    /// The start of the widest run of free slots.
    fn largest_gap(&self) -> Option<usize> {
        let mut best: Option<(usize, usize)> = None;
        let mut k = 0usize;
        while k < self.page_count {
            if self.owner_at(k) != POOL_FREE {
                k += 1;
                continue;
            }
            let (start, len) = self.gap_at(k);
            if best.is_none_or(|(_, best_len)| len > best_len) {
                best = Some((start, len));
            }
            k = start + len;
        }
        best.map(|(start, _)| start)
    }

    /// The free run containing `slot`, as `(start, length)`. `slot` must be
    /// free.
    fn gap_at(&self, slot: usize) -> (usize, usize) {
        let mut end = slot;
        while end < self.page_count && self.owner_at(end) == POOL_FREE {
            end += 1;
        }
        (slot, end - slot)
    }
}

// The raw pointers refer to this value's own allocations, which are never
// resized. Rings on different threads write through them, but only ever to
// pages the owner array says are theirs, and the C pool proves that ownership
// is exclusive — one slot per page, so two rings holding one page is not a
// representable state. The slot lock covers the one moment ownership moves.
unsafe impl Send for WalArena {}
unsafe impl Sync for WalArena {}
