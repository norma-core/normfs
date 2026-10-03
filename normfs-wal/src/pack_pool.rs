//! Buffers for turning a file into its store form and sending it on.
//!
//! Packing a file needs room for its WAL bytes, the compressed and encrypted
//! body, and the compressor's own tables. Allocated per file, that memory
//! grows with the number of queues sealing at once; here it is a fixed number
//! of slots allocated at startup, and a file waits for a slot the way an
//! append waits for a page.
//!
//! The slots are a [`WalArena`], so ownership goes through the same verified
//! C pool as the write buffer: a slot is held by at most one file, and only
//! a slot nobody holds is handed out.

use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::Notify;

use crate::wal_arena::WalArena;

/// The owner id the arena records for a slot in use. Rings number from 1
/// upward, and pack slots live in their own arena, so it never meets one.
const PACK_OWNER: u64 = u64::MAX - 1;

pub struct PackPool {
    arena: WalArena,
    space: Notify,
}

impl PackPool {
    pub fn new(slots: usize, slot_size: usize) -> Arc<Self> {
        Arc::new(PackPool {
            arena: WalArena::new(slots, slot_size),
            space: Notify::new(),
        })
    }

    pub fn slots(&self) -> usize {
        self.arena.page_count()
    }

    pub fn slot_size(&self) -> usize {
        self.arena.page_size()
    }

    pub fn try_take(self: &Arc<Self>) -> Option<PackSlot> {
        let index = self.arena.take_any(PACK_OWNER)?;
        Some(PackSlot {
            pool: Arc::clone(self),
            index,
        })
    }

    /// Waits for a free slot.
    pub async fn take(self: &Arc<Self>) -> PackSlot {
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(slot) = self.try_take() {
                return slot;
            }
            notified.await;
        }
    }

    fn give_back(&self, index: usize) {
        self.arena.give_back_any(index, PACK_OWNER);
        self.space.notify_one();
    }
}

/// One slot, held until dropped. [`PackSlot::freeze`] turns it into shared
/// bytes that hold it instead.
pub struct PackSlot {
    pool: Arc<PackPool>,
    index: usize,
}

impl PackSlot {
    /// Which slot this is: per-slot state kept beside the pool is indexed by it.
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn buf(&mut self) -> &mut [u8] {
        // SAFETY: the arena owner array names this guard's holder for the slot,
        // and only the guard reaches its bytes.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.pool.arena.slot_base(self.index),
                self.pool.slot_size(),
            )
        }
    }

    /// The whole slot as read-only bytes; slices of them share the slot, and
    /// it goes back to the pool when the last one is dropped.
    pub fn freeze(self) -> Bytes {
        Bytes::from_owner(Frozen(self))
    }
}

impl Drop for PackSlot {
    fn drop(&mut self) {
        self.pool.give_back(self.index);
    }
}

struct Frozen(PackSlot);

impl AsRef<[u8]> for Frozen {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: as for `buf`; nothing writes the slot once it is frozen.
        unsafe {
            std::slice::from_raw_parts(
                self.0.pool.arena.slot_base(self.0.index),
                self.0.pool.slot_size(),
            )
        }
    }
}

#[cfg(test)]
#[path = "pack_pool_test.rs"]
mod tests;
