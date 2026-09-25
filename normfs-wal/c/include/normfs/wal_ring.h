#ifndef NORMFS_WAL_RING_H
#define NORMFS_WAL_RING_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/wal_page.h"
#include "normfs/wal_pool.h"

/*
 * A ring of WAL pages. Rust allocates one arena of page_count * page_size
 * bytes, initialises each page descriptor (normfs_wal_page_init) over its own
 * slice of it, and hands both to normfs_wal_ring_init; C never allocates.
 *
 * The ring exposes primitives that each touch a single page, so their frames
 * stay precise; Rust sequences them:
 *
 *   r = try_append(record)
 *   if r.status == NEEDS_ROTATE:
 *       f = find_reusable()
 *       if !f.found: buffer is full, flush/wait
 *       rotate_to(f.index)
 *       r = try_append(record)     // an empty page always has room
 *
 * Entry ids run sequentially across pages; the id of the next entry is
 * next_entry_id. Page k is the arena slice at k * page_size, so distinct pages
 * are disjoint by arithmetic and each keeps its own page_wf independently.
 */

enum normfs_wal_ring_status {
	NORMFS_WAL_RING_OK = 0,
	NORMFS_WAL_RING_NEEDS_ROTATE = 1,  /* active page is full */
	NORMFS_WAL_RING_ERR_TOO_LARGE = 2  /* record does not fit an empty page */
};

struct normfs_wal_ring {
	/*
	 * The pool this ring's pages come from, and where in it they start.
	 * NULL for a ring that owns its pages outright, which is how the
	 * standalone tests build one.
	 *
	 * A ring holds the contiguous slot range [first_slot, first_slot +
	 * page_count). `pages` and `arena` below are that range's base
	 * addresses, cached so every existing operation indexes exactly as it
	 * did before pages were shared -- retaining and releasing a page move
	 * the top of the range, which leaves both bases untouched.
	 *
	 * Contiguous rather than an arbitrary set of slots on purpose. A slot
	 * array would need "no two slots name the same page", which is a
	 * quantifier over pairs -- the shape that would not discharge before the
	 * arena removed it. A range needs none: page k is slot first_slot + k,
	 * and disjointness follows from the pool's own layout. The cost is that
	 * a queue can retain only the slot directly above it, so a fragmented
	 * arena can hold free pages a given queue cannot reach.
	 */
	struct normfs_wal_pool *pool;
	size_t first_slot;
	struct normfs_wal_page *pages;
	uint8_t *arena;
	size_t page_count;
	size_t page_size;
	size_t active;
	uint64_t next_entry_id;
	uint64_t next_page_id;
	uint64_t min_essential_id;
};

struct normfs_wal_ring_append_result {
	uint64_t entry_id;
	size_t page_index;
	int status;
};

struct normfs_wal_ring_reusable_result {
	size_t index;
	int found;
};

struct normfs_wal_ring_seek_result {
	size_t page_index;
	uint32_t index;
	int found;
};

/*@ axiomatic NormfsWalRing {
      predicate normfs_wal_ring_pages_wf{L}(struct normfs_wal_ring *r) =
        \forall integer k; 0 <= k < r->page_count ==>
          normfs_wal_page_wf(&r->pages[k]);

      // Each page's cap matches page_size (so an empty page fits any record
      // that fits page_size), and the active page continues the id run.
      predicate normfs_wal_ring_scalar_wf{L}(struct normfs_wal_ring *r) =
        r->page_count >= 1 &&
        r->page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4 &&
        r->page_size <= 0xFFFFFFFF &&
        r->active < r->page_count &&
        (\forall integer k; 0 <= k < r->page_count ==>
           r->pages[k].cap == r->page_size) &&
        r->pages[r->active].first_entry_id + (integer)r->pages[r->active].count
          == r->next_entry_id;

      // The pages are slices of one allocation, in index order.
      //
      // This is what makes the pages provably disjoint. Stated the other way --
      // one buffer per page, and a quantifier asserting that no two of them
      // overlap -- disjointness is an axiom the caller supplies, and every
      // function that writes anything has to carry it across itself. That is a
      // \forall over *pairs* of pages, and the automatic provers do not
      // transport it: it is the one goal rotate_to could not discharge. As
      // slices of a single block it is arithmetic instead, and it is derived
      // where it is needed rather than assumed everywhere.
      predicate normfs_wal_ring_layout{L}(struct normfs_wal_ring *r) =
        \valid(r->arena + (0 .. r->page_count * r->page_size - 1)) &&
        (\forall integer k; 0 <= k < r->page_count ==>
           r->pages[k].buf == r->arena + k * r->page_size);

      // The ring, the descriptor array and the arena are three disjoint
      // regions. No quantifier: every term is a base pointer or a scalar, and
      // no function assigns any of them, so this survives a mutation by frame.
      predicate normfs_wal_ring_sep{L}(struct normfs_wal_ring *r) =
        \separated(r, r->pages + (0 .. r->page_count - 1)) &&
        \separated(r, r->arena + (0 .. r->page_count * r->page_size - 1)) &&
        \separated(r->pages + (0 .. r->page_count - 1),
                   r->arena + (0 .. r->page_count * r->page_size - 1));

      // This ring's pages are the pool's slots [first_slot, +page_count), and
      // the pool says they belong to this ring. Only retain_page and
      // release_page need this; every other operation works off the cached
      // bases and does not care where they came from.
      predicate normfs_wal_ring_in_pool{L}(struct normfs_wal_ring *r,
                                           uint64_t ring_id) =
        \valid(r->pool) &&
        normfs_wal_pool_wf(r->pool) &&
        r->page_size == r->pool->page_size &&
        r->first_slot + r->page_count <= r->pool->page_count &&
        r->pages == r->pool->pages + r->first_slot &&
        r->arena == r->pool->arena + r->first_slot * r->page_size &&
        // The ring is its own object, disjoint from the pool and from all three
        // of the regions the pool owns.
        //
        // The owner clause is what stops a take or a release -- each writes one
        // owner slot -- landing on the ring's own fields as far as WP knows;
        // without it even "next_entry_id is unchanged" fails.
        //
        // The arena clause is for a different reason, and it is the one the
        // ring's own separation cannot supply. normfs_wal_ring_sep says the
        // ring is disjoint from `page_count * page_size` bytes -- exactly the
        // range the ring holds *now*. Retaining a page needs it of a range
        // one page wider, and no hypothesis about the old range says anything
        // about the page above it. Stated over the whole pool arena it is
        // fixed, so retain_page weakens it rather than re-deriving it.
        \separated(r, r->pool) &&
        \separated(r, r->pool->owner + (0 .. r->pool->page_count - 1)) &&
        \separated(r, r->pool->pages + (0 .. r->pool->page_count - 1)) &&
        \separated(r, r->pool->arena +
                       (0 .. r->pool->page_count * r->pool->page_size - 1)) &&
        (\forall integer k; 0 <= k < r->page_count ==>
           r->pool->owner[r->first_slot + k] == ring_id);

      predicate normfs_wal_ring_wf{L}(struct normfs_wal_ring *r) =
        \valid(r) &&
        \valid(r->pages + (0 .. r->page_count - 1)) &&
        normfs_wal_ring_scalar_wf(r) &&
        normfs_wal_ring_layout(r) &&
        normfs_wal_ring_sep(r) &&
        normfs_wal_ring_pages_wf(r);
    }
*/

/*
 * Durability
 * ==========
 *
 * The property this ring exists to guarantee: **a record the ring accepted is
 * not overwritten in memory before it is on disk.**
 *
 * The ring cannot check that by itself -- it has no idea what a file is, and
 * that is on purpose. What it does instead is refuse to reuse a page until it
 * is told the page's records are safe, and the telling is `min_essential_id`.
 * The caller's obligation, and the other half of the theorem, is that
 * `normfs_wal_ring_set_essential(r, m)` is called only once every entry with id
 * `< m` has been written and synced. In this tree that call has exactly one
 * source: the WAL file writer, after `fsync` returns.
 *
 * Given that obligation, the C side of the theorem is already discharged, and
 * `normfs_wal_ring_reuse_is_safe` below is its statement. Reuse happens in one
 * place -- `normfs_wal_ring_rotate_to` -- and its `requires` is exactly this
 * predicate. So there is no path through this module that overwrites a page
 * holding a record below the durable watermark, and none that overwrites a page
 * a reader is holding.
 *
 * The two conjuncts answer two different questions, and both must hold:
 *
 *   pin_count == 0        nobody is reading these bytes right now
 *   last_entry_id < m     these bytes are on disk
 *
 * Dropping either one loses records: without the first, a stream reads a page
 * out from under itself; without the second, an accepted record is gone before
 * it was ever written.
 */
/*
 * Order
 * =====
 *
 * The other property this ring exists to give: **records reach the file in the
 * order they were accepted.**
 *
 * V1 stores no entry id. A reader derives one from position -- the file
 * header's num_entries_before plus the entry's index within the file -- so the
 * byte order in a file *is* the id order, and two records swapped there are not
 * one record out of place but every entry after them answering to the wrong id,
 * permanently and without an error anywhere.
 *
 * The ring is not what writes the file, so it cannot own that property alone.
 * What it gives is the two halves that make it derivable:
 *
 *   within a page   normfs_wal_page_offsets_wf: entry i begins strictly before
 *                   entry i + 1, so a flush can hand a run of a page's bytes to
 *                   the file without looking inside it.
 *
 *   across pages    rotate_to ensures pages[index].first_entry_id ==
 *                   \old(next_entry_id), and skip_entry ensures the same of the
 *                   active page. next_entry_id only ever moves forward, so a
 *                   page opened later starts above every id already placed, and
 *                   the ids on any one page are a dense run.
 *
 * Together those say the pages carry a total order on ids that comparing their
 * first ids recovers -- which is what lets the Rust side merge the pages, and
 * the records too large to be on one, into a single ordered stream.
 *
 * The other half of the theorem is the caller's, as with durability: the file
 * writer must hand those runs to the file in the order it is given them, and
 * must not write a record before the entry has reached it. In this tree that is
 * PagePool::take_pending and its handover bound.
 */
/*@ axiomatic NormfsWalRingDurability {
      // Everything this page holds has been reported durable. An empty page
      // trivially qualifies -- it holds nothing to lose.
      predicate normfs_wal_page_all_durable{L}(struct normfs_wal_page *p,
                                               integer m) =
        p->count == 0 || p->last_entry_id < m;

      // Page k may be handed back to be written over.
      predicate normfs_wal_ring_reuse_is_safe{L}(struct normfs_wal_ring *r,
                                                 integer k) =
        0 <= k < r->page_count &&
        r->pages[k].pin_count == 0 &&
        normfs_wal_page_all_durable(&r->pages[k], r->min_essential_id);
    }
*/

/*@ requires \valid(ring);
    requires page_count >= 1;
    requires page_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE + 4;
    requires page_size <= 0xFFFFFFFF;
    requires \valid(pages + (0 .. page_count - 1));
    requires \forall integer k; 0 <= k < page_count ==>
               normfs_wal_page_wf(&pages[k]) && pages[k].cap == page_size &&
               pages[k].count == 0;
    requires pages[0].first_entry_id == first_entry_id;
    // The caller lays the pages out over one allocation. What used to be a
    // quantifier over every pair of page buffers is now this one equation.
    requires page_count * page_size <= 0xFFFFFFFFFFFFFFFF;
    requires \valid(arena + (0 .. page_count * page_size - 1));
    requires \forall integer k; 0 <= k < page_count ==>
               pages[k].buf == arena + k * page_size;
    requires \separated(ring, pages + (0 .. page_count - 1));
    requires \separated(ring, arena + (0 .. page_count * page_size - 1));
    requires \separated(pages + (0 .. page_count - 1),
                        arena + (0 .. page_count * page_size - 1));
    assigns ring->pool, ring->first_slot, ring->pages, ring->arena,
            ring->page_count, ring->page_size, ring->active,
            ring->next_entry_id, ring->next_page_id, ring->min_essential_id;
    ensures normfs_wal_ring_wf(ring);
    ensures ring->active == 0 && ring->next_entry_id == first_entry_id;
*/
void normfs_wal_ring_init(struct normfs_wal_ring *ring,
    struct normfs_wal_page *pages, uint8_t *arena, size_t page_count,
    size_t page_size, uint64_t first_entry_id);

/*
 * Migration
 * =========
 *
 * A queue retains the free slot above its range, or releases its top slot for
 * another queue to retain. Between them, a busy queue can take over memory an
 * idle one has released without the process-wide total ever moving.
 *
 * Neither function allocates, and neither can: the arena, the page-descriptor
 * array and the owner array are allocated once by the caller and handed to
 * `normfs_wal_pool_init`, and nothing here holds an allocator. All these two
 * do is move one slot's entry in the owner array and adjust page_count, both
 * bounded by pool->page_count. A queue that needs a page when no free slot
 * sits above its range does not get one -- it waits for its own pages to
 * become reusable. That is why they are not named grow/shrink: what moves is
 * ownership of memory that already exists, never its amount.
 *
 * Releasing a page carries the durability theorem, exactly as rotate_to does:
 * a page whose records are not yet on disk, or that a reader still holds,
 * cannot leave the queue that accepted them. Retaining one needs no such
 * clause -- a free page belongs to nobody and holds nothing anyone is owed.
 */

/*@ requires normfs_wal_ring_wf(ring);
    requires normfs_wal_ring_in_pool(ring, ring_id);
    requires ring_id != NORMFS_WAL_POOL_FREE;
    requires ring->first_slot + ring->page_count < ring->pool->page_count;
    requires ring->pool->owner[ring->first_slot + ring->page_count] ==
               NORMFS_WAL_POOL_FREE;
    // The page being taken is pages[page_count], not
    // pages[first_slot + page_count]: ring->pages is already based at
    // first_slot, so adding it again names a different page -- and for a queue
    // that does not start at slot 0, one belonging to another queue. That was
    // a real bug here, invisible to any single-queue test because on queue 0
    // the two indices coincide. The proof is what found it.
    requires normfs_wal_page_wf(&ring->pages[ring->page_count]);
    requires ring->pages[ring->page_count].cap == ring->page_size;
    // Not needed for well-formedness, but needed for correctness: seek scans
    // every page in range, so a slot still holding the last holder's records
    // would answer this ring's seeks with a foreign payload under an id of its
    // own. The caller resets the page as it takes the slot.
    requires ring->pages[ring->page_count].count == 0;
    requires ring->pages[ring->page_count].buf ==
               ring->arena + ring->page_count * ring->page_size;
    assigns ring->page_count,
            ring->pool->owner[ring->first_slot + ring->page_count];
    // normfs_wal_ring_wf, one conjunct at a time. As a single clause an open
    // goal says only "well-formedness was not re-established"; naming the parts
    // costs nothing and says which part.
    ensures \valid(ring);
    ensures \valid(ring->pages + (0 .. ring->page_count - 1));
    ensures normfs_wal_ring_scalar_wf(ring);
    ensures normfs_wal_ring_layout(ring);
    ensures normfs_wal_ring_sep(ring);
    ensures normfs_wal_ring_pages_wf(ring);
    ensures normfs_wal_ring_in_pool(ring, ring_id);
    ensures ring->page_count == \old(ring->page_count) + 1;
    ensures ring->active == \old(ring->active);
    ensures ring->next_entry_id == \old(ring->next_entry_id);
*/
void normfs_wal_ring_retain_page(struct normfs_wal_ring *ring, uint64_t ring_id);

/*@ requires normfs_wal_ring_wf(ring);
    requires normfs_wal_ring_in_pool(ring, ring_id);
    requires ring_id != NORMFS_WAL_POOL_FREE;
    // Never below one page, and never the page being appended to.
    requires ring->page_count > 1;
    requires ring->active < ring->page_count - 1;
    // The durability theorem at the migration boundary: a page carrying
    // records that are not yet on disk, or that a reader is holding, does not
    // leave the queue that accepted them.
    requires normfs_wal_page_is_reusable(&ring->pages[ring->page_count - 1],
                                         ring->min_essential_id);
    assigns ring->page_count,
            ring->pool->owner[ring->first_slot + ring->page_count - 1];
    ensures \valid(ring);
    ensures \valid(ring->pages + (0 .. ring->page_count - 1));
    ensures normfs_wal_ring_scalar_wf(ring);
    ensures normfs_wal_ring_layout(ring);
    ensures normfs_wal_ring_sep(ring);
    ensures normfs_wal_ring_pages_wf(ring);
    ensures normfs_wal_ring_in_pool(ring, ring_id);
    ensures ring->page_count == \old(ring->page_count) - 1;
    ensures ring->active == \old(ring->active);
    ensures ring->next_entry_id == \old(ring->next_entry_id);
    ensures ring->pool->owner[ring->first_slot + ring->page_count] ==
              NORMFS_WAL_POOL_FREE;
*/
void normfs_wal_ring_release_page(struct normfs_wal_ring *ring, uint64_t ring_id);

/* Verified by WP. Re-establishing every other page's page_wf after this
 * writes one page's bytes is a frame under two quantifiers at once -- pages,
 * and the entries within a page -- which is why the body states the
 * disjointness at the four bytes offsets_wf actually reads and the frame's
 * conclusion as an equality between the two states. Its callee page_append is
 * proven too. */
/*@ requires normfs_wal_ring_wf(ring);
    requires ring->next_entry_id < 0xFFFFFFFFFFFFFFFF;
    requires record_size == 0 || \valid_read(record + (0 .. record_size - 1));
    requires record_size == 0 ||
             \separated(ring->pages[ring->active].buf +
                          (0 .. ring->page_size - 1),
                        record + (0 .. record_size - 1));
    requires \separated(ring, ring->pages[ring->active].buf +
                                (0 .. ring->page_size - 1));
    assigns ring->next_entry_id,
            ring->pages[ring->active].used_bytes,
            ring->pages[ring->active].count,
            ring->pages[ring->active].last_entry_id,
            ring->pages[ring->active].buf[0 .. ring->page_size - 1];
    // normfs_wal_ring_wf, one conjunct at a time, as retain_page and
    // release_page state it. As a single clause an open goal says only
    // "well-formedness was not re-established"; named, it says which part --
    // and here that matters more than elsewhere, because this is the one
    // function that writes a page's bytes.
    ensures \valid(ring);
    ensures \valid(ring->pages + (0 .. ring->page_count - 1));
    ensures normfs_wal_ring_scalar_wf(ring);
    ensures normfs_wal_ring_layout(ring);
    ensures normfs_wal_ring_sep(ring);
    ensures normfs_wal_ring_pages_wf(ring);
    ensures \result.status == NORMFS_WAL_RING_OK ||
            \result.status == NORMFS_WAL_RING_NEEDS_ROTATE ||
            \result.status == NORMFS_WAL_RING_ERR_TOO_LARGE;
    ensures \result.status == NORMFS_WAL_RING_OK ==>
            \result.entry_id == \old(ring->next_entry_id) &&
            ring->next_entry_id == \old(ring->next_entry_id) + 1 &&
            \result.page_index == ring->active;
    ensures \result.status == NORMFS_WAL_RING_ERR_TOO_LARGE ==>
            normfs_wal_entry_v1_size_logic(record_size) + 4 > ring->page_size;
    ensures \result.status != NORMFS_WAL_RING_OK ==>
            ring->next_entry_id == \old(ring->next_entry_id) &&
            ring->active == \old(ring->active);
*/
struct normfs_wal_ring_append_result
normfs_wal_ring_try_append(struct normfs_wal_ring *ring, const uint8_t *record,
    uint32_t record_size);

/*@ requires normfs_wal_ring_wf(ring);
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.index < ring->page_count &&
            normfs_wal_page_is_reusable(&ring->pages[\result.index],
                                        ring->min_essential_id);
*/
struct normfs_wal_ring_reusable_result
normfs_wal_ring_find_reusable(struct normfs_wal_ring *ring);

/* This is the one place a page's contents are discarded, which makes it the
 * one place the durability theorem is used -- see the `requires` and the
 * page-intactness `ensures` below.
 *
 * It is WP-verified. That took two things. Its only callee, page_reset,
 * assigns five scalar fields and no buffer bytes at all, so no page's offset
 * table is in its footprint and the frame is precise; the asserts in the body
 * are what let WP split ring_wf's quantifier over pages into the reset page
 * and the rest. And separation had to stop being quantified: while each page
 * owned an independent buffer, re-establishing ring_sep meant transporting a
 * \forall over every *pair* of buffers across the call, which the automatic
 * provers would not do. Laying the pages over one arena made that arithmetic
 * instead (see normfs_wal_ring_layout), and it was the last goal to fall. */
/*@ requires normfs_wal_ring_wf(ring);
    requires index < ring->page_count;
    // The durability theorem, at its one point of use: this is the only place
    // a page's contents are discarded, and it may not happen while a reader
    // holds the page or while it holds a record that is not yet on disk.
    requires normfs_wal_ring_reuse_is_safe(ring, index);
    requires normfs_wal_page_is_reusable(&ring->pages[index],
                                         ring->min_essential_id);
    assigns ring->active, ring->next_page_id,
            ring->pages[index].used_bytes,
            ring->pages[index].count,
            ring->pages[index].page_id,
            ring->pages[index].first_entry_id,
            ring->pages[index].last_entry_id;
    // normfs_wal_ring_wf, spelled out one conjunct at a time. The conjunction
    // is what callers consume, but as a single clause an unproved goal says
    // only "well-formedness was not re-established" -- naming the parts costs
    // nothing and says which part.
    ensures \valid(ring);
    ensures \valid(ring->pages + (0 .. ring->page_count - 1));
    ensures normfs_wal_ring_scalar_wf(ring);
    ensures normfs_wal_ring_sep(ring);
    ensures normfs_wal_ring_pages_wf(ring);
    ensures ring->active == index && ring->pages[index].count == 0;
    ensures ring->pages[index].first_entry_id == \old(ring->next_entry_id);
    ensures ring->next_entry_id == \old(ring->next_entry_id);

    // The durability theorem as an obligation rather than a decoration.
    //
    // A `requires` on an entry point is an assumption: WP proves the body
    // under it and no C caller exists to discharge it, so on its own it is a
    // comment with a syntax checker. This clause is what makes it carry
    // weight -- it says every page that was *not* reusable before the call is
    // byte-for-byte unchanged after it. At k != index that follows from the
    // frame. At k == index the conclusion is false, because that page was
    // just reset, so the only way to discharge the implication is to refute
    // its antecedent -- which is exactly normfs_wal_ring_reuse_is_safe above.
    //
    // Delete that `requires` and this goal must go red. That is the check
    // that says the theorem is doing something.
    ensures \forall integer k; 0 <= k < ring->page_count ==>
              ( \at(ring->pages[k].pin_count, Pre) != 0 ||
                ( \at(ring->pages[k].count, Pre) != 0 &&
                  \at(ring->pages[k].last_entry_id, Pre) >=
                    \at(ring->min_essential_id, Pre) ) )
              ==>
              ( ring->pages[k].used_bytes ==
                  \at(ring->pages[k].used_bytes, Pre) &&
                ring->pages[k].count == \at(ring->pages[k].count, Pre) &&
                ring->pages[k].first_entry_id ==
                  \at(ring->pages[k].first_entry_id, Pre) &&
                ring->pages[k].last_entry_id ==
                  \at(ring->pages[k].last_entry_id, Pre) );

    // Free from the frame, and states the other half: rotation never drops a
    // reader's pin.
    ensures \forall integer k; 0 <= k < ring->page_count ==>
              ring->pages[k].pin_count == \at(ring->pages[k].pin_count, Pre);
*/
void normfs_wal_ring_rotate_to(struct normfs_wal_ring *ring, size_t index);

/*@ requires normfs_wal_ring_wf(ring);
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.page_index < ring->page_count &&
            \result.index < ring->pages[\result.page_index].count &&
            ring->pages[\result.page_index].first_entry_id +
              (integer)\result.index == entry_id;
*/
struct normfs_wal_ring_seek_result
normfs_wal_ring_seek(struct normfs_wal_ring *ring, uint64_t entry_id);

/*
 * Steps the id sequence over one record without putting it on a page.
 *
 * A record whose frame is wider than a page can never be cached, but the id it
 * was given is part of the sequence all the same, and the sequence is what the
 * reader derives every entry id from. The ring is therefore told about the gap
 * rather than left to discover it: the alternative is to renumber, which means
 * discarding the pages -- and a record that has been accepted and is not yet on
 * disk cannot be discarded.
 *
 * The active page must be empty, because scalar_wf ties next_entry_id to
 * `pages[active].first_entry_id + count` and there is no way to advance the one
 * without moving the other. That is not a limitation but the point: the caller
 * rotates first, so a skipped id always falls *between* two pages and never
 * inside one. The ids a page holds therefore stay a dense run, which is what
 * lets a reader ask a page for an id by subtracting first_entry_id, and what
 * lets a writer put the pages and the skipped records into one id order by
 * comparing their first ids alone.
 */
/*@ requires normfs_wal_ring_wf(ring);
    requires ring->pages[ring->active].count == 0;
    requires ring->next_entry_id < 0xFFFFFFFFFFFFFFFF;
    assigns ring->next_entry_id, ring->pages[ring->active].first_entry_id;
    ensures normfs_wal_ring_wf(ring);
    ensures ring->next_entry_id == \old(ring->next_entry_id) + 1;
    ensures ring->pages[ring->active].first_entry_id == ring->next_entry_id;
    ensures ring->pages[ring->active].count == 0;

    // Nothing that is on a page moves. The skipped record never entered one, so
    // unlike rotate_to this needs no durability clause: there is nothing here
    // that could overwrite an accepted record.
    ensures \forall integer k; 0 <= k < ring->page_count ==>
              ring->pages[k].used_bytes == \at(ring->pages[k].used_bytes, Pre) &&
              ring->pages[k].count == \at(ring->pages[k].count, Pre) &&
              ring->pages[k].pin_count == \at(ring->pages[k].pin_count, Pre);
    ensures \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
              ring->pages[k].first_entry_id ==
                \at(ring->pages[k].first_entry_id, Pre);
*/
void normfs_wal_ring_skip_entry(struct normfs_wal_ring *ring);

/*@ requires \valid(ring);
    assigns ring->min_essential_id;
    ensures ring->min_essential_id == min_essential_id;
*/
void normfs_wal_ring_set_essential(struct normfs_wal_ring *ring,
    uint64_t min_essential_id);

#endif /* NORMFS_WAL_RING_H */
