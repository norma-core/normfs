#include "normfs/wal_ring.h"

void
normfs_wal_ring_init(struct normfs_wal_ring *ring, struct normfs_wal_page *pages,
    uint8_t *arena, size_t page_count, size_t page_size, uint64_t first_entry_id)
{
	/* Unbound: this ring owns its pages outright. Binding it to a pool is a
	 * separate step, so a ring built for a standalone test needs no pool. */
	ring->pool = NULL;
	ring->first_slot = 0u;
	ring->pages = pages;
	ring->arena = arena;
	ring->page_count = page_count;
	ring->page_size = page_size;
	ring->active = 0u;
	ring->next_entry_id = first_entry_id;
	ring->next_page_id = page_count;
	ring->min_essential_id = 0u;
}

void
normfs_wal_ring_set_essential(struct normfs_wal_ring *ring,
    uint64_t min_essential_id)
{
	ring->min_essential_id = min_essential_id;
}

void
normfs_wal_ring_retain_page(struct normfs_wal_ring *ring, uint64_t ring_id)
{
	/* The range widens by a page, so the ring's view of the arena has to
	 * stretch with it. That the bytes are there is the pool's -- the ring's
	 * arena is the pool's at first_slot, and the slot taken is inside the
	 * pool -- but the arithmetic joining the two is not automatic. The step
	 * that is not is the second: multiplying an inequality by page_size.
	 * Left implicit, the whole chain fails as one goal with nothing to say
	 * about which link broke. */
	/* in_pool and pool_wf reach the prover as opaque atoms: a separation
	 * stated inside them is not a hypothesis until something asks for it.
	 * Everything below reasons from these, not from the predicates. */
	/*@ assert unfold_ring_vs_pool_struct: \separated(ring, ring->pool); */
	/*@ assert unfold_ring_vs_pool_arena:
	      \separated(ring, ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert unfold_ring_vs_pool_pages:
	      \separated(ring, ring->pool->pages +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert unfold_ring_vs_pool_owner:
	      \separated(ring, ring->pool->owner +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert unfold_pool_pages_vs_arena:
	      \separated(ring->pool->pages + (0 .. ring->pool->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert unfold_pool_owner_vs_arena:
	      \separated(ring->pool->owner + (0 .. ring->pool->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert unfold_pool_owner_vs_pages:
	      \separated(ring->pool->owner + (0 .. ring->pool->page_count - 1),
	                 ring->pool->pages + (0 .. ring->pool->page_count - 1)); */

	/* The write at the end lands on one field of the ring, and narrowing a
	 * separation to a field is not automatic. Stated here, where the context
	 * is still small, rather than beside the write. */
	/*@ assert count_bump_vs_pool_pages:
	      \separated(&ring->page_count,
	                 ring->pool->pages + (0 .. ring->pool->page_count - 1)); */
	/*@ assert count_bump_vs_pool_arena:
	      \separated(&ring->page_count,
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert count_bump_vs_owner:
	      \separated(&ring->page_count,
	                 ring->pool->owner + (0 .. ring->pool->page_count - 1)); */

	/*@ assert slots_fit:
	      ring->first_slot + ring->page_count + 1 <= ring->pool->page_count; */
	/*@ assert bytes_fit:
	      (ring->first_slot + ring->page_count + 1) * ring->page_size <=
	        ring->pool->page_count * ring->page_size; */
	/*@ assert arena_covers_new_page:
	      \valid(ring->arena +
	             (0 .. (ring->page_count + 1) * ring->page_size - 1)); */

	/* Without these base equalities ring->pages and ring->pool->pages are
	 * unrelated pointers, and everything below reads a pool fact at a ring
	 * index. */
	/*@ assert page_size_match: ring->page_size == ring->pool->page_size; */

	/*@ assert arena_base:
	      ring->arena ==
	        ring->pool->arena + ring->first_slot * ring->page_size; */

	/*@ assert unfold_pool_arena_valid:
	      \valid(ring->pool->arena +
	             (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert ring_slice_fits:
	      ring->first_slot * ring->page_size +
	        (ring->page_count + 1) * ring->page_size <=
	          ring->pool->page_count * ring->pool->page_size; */
	/*@ assert pages_base:
	      ring->pages == ring->pool->pages + ring->first_slot; */
	/*@ assert slot_indices_in_pool:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        0 <= ring->first_slot + k < ring->pool->page_count; */

	/* Narrowing a \separated to a sub-range is the step neither prover takes;
	 * pointwise it becomes membership, which they do. The distribution is not
	 * automatic either -- bytes_fit has the sum inside the product. */
	/*@ assert bytes_fit_distributed:
	      (ring->first_slot + ring->page_count + 1) * ring->page_size ==
	        ring->first_slot * ring->page_size +
	          (ring->page_count + 1) * ring->page_size; */
	/*@ assert ring_arena_is_pool_arena:
	      \forall integer j;
	        0 <= j < (ring->page_count + 1) * ring->page_size ==>
	          ring->arena + j ==
	            ring->pool->arena + (ring->first_slot * ring->page_size + j); */
	/*@ assert ring_arena_indices_in_pool:
	      \forall integer j;
	        0 <= j < (ring->page_count + 1) * ring->page_size ==>
	          0 <= ring->first_slot * ring->page_size + j <
	            ring->pool->page_count * ring->pool->page_size; */

	normfs_wal_pool_take(ring->pool, ring->first_slot + ring->page_count,
	    ring_id);

	/* Read off pool_take's postcondition rather than carried across the
	 * write: pool_wf already says every page of the pool is well-formed, and
	 * this ring's pages are the pool's shifted by first_slot. Reproving it by
	 * frame means placing every byte of every offset table outside the slot
	 * written, under two quantifiers at once. */
	/*@ assert pages_are_pool_pages:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        &ring->pages[k] == &ring->pool->pages[ring->first_slot + k]; */
	/*@ assert old_pages_intact:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert old_pages_unmoved:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].cap == ring->page_size &&
	        ring->pages[k].buf == ring->arena + k * ring->page_size; */
	/*@ assert new_page_wf:
	      normfs_wal_page_wf(&ring->pages[ring->page_count]); */

	/* pool_take's preservation clause, at this ring's indices. */
	/*@ assert owner_below_unchanged:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pool->owner[ring->first_slot + k] ==
	          \at(ring->pool->owner[ring->first_slot + k], Pre); */
	/*@ assert owner_of_new_page:
	      ring->pool->owner[ring->first_slot + ring->page_count] == ring_id; */

	/* Monotonicity of multiplication on the left factor: nonlinear, and not
	 * derived from the inequality on its own. An instance rather than a
	 * lemma, because a -wp-fct list silently drops global lemma goals. */
	/*@ assert page_bytes_inside_range:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        (k + 1) * ring->page_size <=
	          (ring->page_count + 1) * ring->page_size; */

	/* One hypothesis of each postcondition's own shape, at the width the
	 * range is about to have. */
	/*@ assert pages_wf_at_new_width:
	      \forall integer k; 0 <= k < ring->page_count + 1 ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert caps_at_new_width:
	      \forall integer k; 0 <= k < ring->page_count + 1 ==>
	        ring->pages[k].cap == ring->page_size; */
	/*@ assert layout_at_new_width:
	      \forall integer k; 0 <= k < ring->page_count + 1 ==>
	        ring->pages[k].buf == ring->arena + k * ring->page_size; */
	/*@ assert owner_at_new_width:
	      \forall integer k; 0 <= k < ring->page_count + 1 ==>
	        ring->pool->owner[ring->first_slot + k] == ring_id; */

	/*@ assert count_bump_vs_pages:
	      \separated(&ring->page_count,
	                 ring->pages + (0 .. ring->page_count)); */
	/*@ assert count_bump_vs_arena:
	      \separated(&ring->page_count,
	                 ring->arena +
	                   (0 .. (ring->page_count + 1) * ring->page_size - 1)); */

	/*@ ghost taken: ; */
	ring->page_count = ring->page_count + 1u;

	/* The frame's conclusion as an equality between the two states, which is
	 * what the separations above feed directly; well-formedness follows by
	 * rewriting rather than by reproving. */
	/*@ assert offsets_unchanged_by_count_bump:
	      \forall integer k, i;
	        0 <= k < ring->page_count && 0 <= i < ring->pages[k].count ==>
	          normfs_wal_page_offset_logic(&ring->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&ring->pages[k], i), taken); */
	/*@ assert pool_offsets_unchanged_by_count_bump:
	      \forall integer k, i;
	        0 <= k < ring->pool->page_count &&
	        0 <= i < ring->pool->pages[k].count ==>
	          normfs_wal_page_offset_logic(&ring->pool->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&ring->pool->pages[k], i),
	                taken); */
	/*@ assert pages_unchanged_by_count_bump:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, taken) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, taken) &&
	        ring->pages[k].count == \at(ring->pages[k].count, taken) &&
	        ring->pages[k].used_bytes == \at(ring->pages[k].used_bytes, taken) &&
	        ring->pages[k].first_entry_id ==
	          \at(ring->pages[k].first_entry_id, taken) &&
	        ring->pages[k].last_entry_id ==
	          \at(ring->pages[k].last_entry_id, taken); */
	/*@ assert pages_wf_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert pool_wf_final: normfs_wal_pool_wf(ring->pool); */

	/* After the write, where the counter already reads what the
	 * postconditions ask about. */
	/*@ assert arena_valid_final:
	      \valid(ring->arena +
	             (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert layout_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == ring->arena + k * ring->page_size; */
	/*@ assert sep_ring_arena_pool_form:
	      \separated(ring,
	                 ring->pool->arena + ring->first_slot * ring->page_size +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert sep_ring_arena_final:
	      \separated(ring, ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert sep_ring_pages_final:
	      \separated(ring, ring->pages + (0 .. ring->page_count - 1)); */
	/* Narrowed one side at a time. Both at once -- the descriptor sub-range
	 * against the arena sub-range -- is two inclusions to find in one step,
	 * and that is the step neither prover takes. */
	/*@ assert sep_ring_pages_vs_pool_arena:
	      \separated(ring->pages + (0 .. ring->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/* Each descriptor on its own first: narrowing goes through against a
	 * single object and not against a range. */
	/*@ assert sep_each_page_vs_pool_arena:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        \separated(&ring->pages[k],
	                   ring->pool->arena +
	                     (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert ring_arena_subset:
	      \subset(ring->arena +
	                (0 .. ring->page_count * ring->page_size - 1),
	              ring->pool->arena +
	                (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert sep_pages_arena_final:
	      \separated(ring->pages + (0 .. ring->page_count - 1),
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */

	/* Folded back into the predicates the postconditions ask for. */
	/*@ assert sep_pred_final: normfs_wal_ring_sep(ring); */
	/*@ assert in_pool_final: normfs_wal_ring_in_pool(ring, ring_id); */
	/*@ assert owner_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pool->owner[ring->first_slot + k] == ring_id; */
	/*@ assert slots_fit_final:
	      ring->first_slot + ring->page_count <= ring->pool->page_count; */
}

void
normfs_wal_ring_release_page(struct normfs_wal_ring *ring, uint64_t ring_id)
{
	/* The mirror of retain_page, and every move below is one of the moves
	 * made there, in the opposite direction; the reasons are stated once, at
	 * retain_page. Two things are particular to this direction. The count
	 * comes down before the call, so pool_wf has to survive a write to the
	 * ring's own storage before give_back is asked for it -- that is the
	 * separation narrowed to &ring->page_count. And every predicate is
	 * rebuilt at the narrower width, where validity of the arena slice needs
	 * multiplication to be monotone in the left factor. */
	/*@ assert unfold_ring_vs_pool_struct: \separated(ring, ring->pool); */
	/*@ assert unfold_ring_vs_pool_pages:
	      \separated(ring, ring->pool->pages +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert unfold_ring_vs_pool_owner:
	      \separated(ring, ring->pool->owner +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert unfold_ring_vs_pool_arena:
	      \separated(ring, ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert unfold_pool_owner_vs_pages:
	      \separated(ring->pool->owner + (0 .. ring->pool->page_count - 1),
	                 ring->pool->pages + (0 .. ring->pool->page_count - 1)); */
	/*@ assert unfold_pool_owner_vs_arena:
	      \separated(ring->pool->owner + (0 .. ring->pool->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert unfold_pool_pages_vs_arena:
	      \separated(ring->pool->pages + (0 .. ring->pool->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert pool_wf_entry: normfs_wal_pool_wf(ring->pool); */

	/*@ assert page_size_match: ring->page_size == ring->pool->page_size; */
	/*@ assert pages_base:
	      ring->pages == ring->pool->pages + ring->first_slot; */
	/*@ assert arena_base:
	      ring->arena ==
	        ring->pool->arena + ring->first_slot * ring->page_size; */

	/*@ assert active_id_run_entry:
	      ring->pages[ring->active].first_entry_id +
	        (integer)ring->pages[ring->active].count == ring->next_entry_id; */
	/*@ assert pool_pages_entry:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        normfs_wal_page_wf(&ring->pool->pages[k]) &&
	        ring->pool->pages[k].cap == ring->pool->page_size; */
	/*@ assert pool_layout_entry:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        ring->pool->pages[k].buf ==
	          ring->pool->arena + k * ring->pool->page_size; */
	/*@ assert unfold_pool_arena_valid:
	      \valid(ring->pool->arena +
	             (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert slots_fit:
	      ring->first_slot + ring->page_count <= ring->pool->page_count; */

	/*@ assert sep_ring_arena_entry:
	      \separated(ring, ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert count_drop_vs_each_page:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        \separated(&ring->page_count, &ring->pool->pages[k]); */
	/*@ assert each_page_vs_owner:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        \separated(&ring->pool->pages[k],
	                   ring->pool->owner + (0 .. ring->pool->page_count - 1)); */
	/*@ assert count_drop_vs_pool_struct:
	      \separated(&ring->page_count, ring->pool); */
	/*@ assert count_drop_vs_pool_pages:
	      \separated(&ring->page_count,
	                 ring->pool->pages + (0 .. ring->pool->page_count - 1)); */
	/*@ assert count_drop_vs_pool_owner:
	      \separated(&ring->page_count,
	                 ring->pool->owner + (0 .. ring->pool->page_count - 1)); */
	/*@ assert count_drop_vs_pool_arena:
	      \separated(&ring->page_count,
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */

	/* The count comes down first, so from here `page_count` names the page
	 * being released and the range below it is already the range that
	 * survives. */
	/*@ ghost dropped: ; */
	ring->page_count = ring->page_count - 1u;

	/* The pool identified across the write, not merely separated from it:
	 * without this the two sides of the rewrite below are terms about two
	 * different pools, and re-establishing its well-formedness turns from a
	 * rewrite into a search. */
	/*@ assert pool_shape_after_count_drop:
	      ring->pool == \at(ring->pool, dropped) &&
	      ring->pool->pages == \at(ring->pool->pages, dropped) &&
	      ring->pool->page_count == \at(ring->pool->page_count, dropped) &&
	      ring->pool->page_size == \at(ring->pool->page_size, dropped); */
	/*@ assert pool_caps_unchanged_by_count_drop:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        ring->pool->pages[k].cap ==
	          \at(ring->pool->pages[k].cap, dropped); */
	/*@ assert pool_pages_unchanged_by_count_drop:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        normfs_wal_page_wf(&ring->pool->pages[k]) &&
	        ring->pool->pages[k].cap == ring->pool->page_size; */
	/*@ assert pool_wf_after_count_drop: normfs_wal_pool_wf(ring->pool); */

	/* The page leaving is the pool's slot first_slot + page_count. Since
	 * ring->pages is based at first_slot, the ring's index and the pool's
	 * name one descriptor, and it is the page the durability precondition
	 * spoke about. */
	/*@ assert same_page:
	      &ring->pages[ring->page_count] ==
	        &ring->pool->pages[ring->first_slot + ring->page_count]; */
	/*@ assert still_reusable:
	      normfs_wal_page_is_reusable(&ring->pages[ring->page_count],
	                                  ring->min_essential_id); */

	/*@ ghost given: ; */
	normfs_wal_pool_give_back(ring->pool, ring->first_slot + ring->page_count,
	    ring_id, ring->min_essential_id);

	/* Neither write touches the pool's own fields, but that is not the same
	 * as the prover knowing it: every range below is built from
	 * pool->page_count, and without this equality the entry separations and
	 * the final ones are stated over two different lengths and never meet. */
	/*@ assert ring_bases_unchanged:
	      ring->pages == \at(ring->pages, Pre) &&
	      ring->arena == \at(ring->arena, Pre) &&
	      ring->page_size == \at(ring->page_size, Pre) &&
	      ring->first_slot == \at(ring->first_slot, Pre); */
	/*@ assert count_dropped_by_one:
	      \at(ring->page_count, Pre) == ring->page_count + 1; */
	/*@ assert pool_shape_unchanged:
	      ring->pool == \at(ring->pool, Pre) &&
	      ring->pool->pages == \at(ring->pool->pages, Pre) &&
	      ring->pool->owner == \at(ring->pool->owner, Pre) &&
	      ring->pool->arena == \at(ring->pool->arena, Pre) &&
	      ring->pool->page_count == \at(ring->pool->page_count, Pre) &&
	      ring->pool->page_size == \at(ring->pool->page_size, Pre); */

	/* give_back writes one owner slot and says every other one is untouched,
	 * so the range that survives still reads as this ring's.
	 *
	 * Twice, and the order is the point. give_back states preservation over
	 * the pool's own index space -- a flat range of j -- while in_pool wants
	 * it over the ring's, at the shifted index first_slot + k. Asking for the
	 * shifted form directly means instantiating the callee's quantifier under
	 * a shift, which WP does not do. Stated first in the shape give_back
	 * already speaks, it is one instantiation each way. */
	/*@ assert slots_in_pool:
	      ring->first_slot + ring->page_count < ring->pool->page_count; */
	/*@ assert owners_absolute:
	      \forall integer j;
	        ring->first_slot <= j < ring->first_slot + ring->page_count ==>
	        ring->pool->owner[j] == ring_id; */
	/*@ assert owners_below_intact:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pool->owner[ring->first_slot + k] == ring_id; */

	/* Read off give_back's postcondition at this ring's indices, rather than
	 * carried across the owner write. */
	/*@ assert pages_are_pool_pages:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        &ring->pages[k] == &ring->pool->pages[ring->first_slot + k]; */
	/*@ assert pages_wf_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert caps_at_pool_field:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].cap == ring->pool->page_size; */
	/*@ assert caps_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].cap == ring->page_size; */
	/*@ assert slot_indices_in_pool:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        0 <= ring->first_slot + k < ring->pool->page_count; */
	/*@ assert pool_layout_final:
	      \forall integer k; 0 <= k < ring->pool->page_count ==>
	        ring->pool->pages[k].buf ==
	          ring->pool->arena + k * ring->pool->page_size; */
	/*@ assert shift_distributes:
	      \forall integer k; 0 <= k <= ring->page_count ==>
	        (ring->first_slot + k) * ring->page_size ==
	          ring->first_slot * ring->page_size + k * ring->page_size; */
	/*@ assert layout_pointwise:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pool->arena +
	          (ring->first_slot + k) * ring->pool->page_size ==
	            ring->arena + k * ring->page_size; */
	/*@ assert pool_page_buf_at_ring_index:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf ==
	          ring->pool->arena +
	            (ring->first_slot + k) * ring->pool->page_size; */
	/*@ assert layout_final:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == ring->arena + k * ring->page_size; */

	/* The one clause of scalar_wf that is about contents rather than shape.
	 * give_back assigns a single owner slot and the descriptor array is
	 * disjoint from the owner array, so the whole array reads as it did --
	 * as an equality between the two states, which is the shape the
	 * separation feeds directly. */
	/*@ assert active_unchanged:
	      ring->active == \at(ring->active, Pre) &&
	      ring->next_entry_id == \at(ring->next_entry_id, Pre); */
	/*@ assert active_slot_in_pool:
	      0 <= ring->first_slot + ring->active < ring->pool->page_count; */
	/*@ assert active_first_id_unchanged:
	      ring->pool->pages[ring->first_slot + ring->active].first_entry_id ==
	        \at(ring->pool->pages[ring->first_slot + ring->active].first_entry_id,
	            given); */
	/*@ assert active_page_unchanged:
	      ring->pages[ring->active].first_entry_id ==
	        \at(ring->pages[ring->active].first_entry_id, Pre) &&
	      ring->pages[ring->active].count ==
	        \at(ring->pages[ring->active].count, Pre); */

	/* Monotonicity of multiplication on the left factor: nonlinear, and the
	 * step from the arena the ring held to the one it holds now. */
	/*@ assert narrower_slice_fits:
	      ring->page_count * ring->page_size <=
	        (ring->page_count + 1) * ring->page_size; */
	/*@ assert bytes_fit:
	      (ring->first_slot + ring->page_count) * ring->page_size <=
	        ring->pool->page_count * ring->page_size; */
	/*@ assert bytes_fit_distributed:
	      (ring->first_slot + ring->page_count) * ring->page_size ==
	        ring->first_slot * ring->page_size +
	          ring->page_count * ring->page_size; */
	/*@ assert ring_slice_fits:
	      ring->first_slot * ring->page_size +
	        ring->page_count * ring->page_size <=
	          ring->pool->page_count * ring->pool->page_size; */
	/*@ assert ring_arena_is_pool_arena:
	      \forall integer j;
	        0 <= j < ring->page_count * ring->page_size ==>
	          ring->arena + j ==
	            ring->pool->arena + (ring->first_slot * ring->page_size + j); */
	/*@ assert ring_arena_indices_in_pool:
	      \forall integer j;
	        0 <= j < ring->page_count * ring->page_size ==>
	          0 <= ring->first_slot * ring->page_size + j <
	            ring->pool->page_count * ring->pool->page_size; */
	/*@ assert ring_arena_subset:
	      \subset(ring->arena +
	                (0 .. ring->page_count * ring->page_size - 1),
	              ring->pool->arena +
	                (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert arena_valid_final:
	      \valid(ring->arena +
	             (0 .. ring->page_count * ring->page_size - 1)); */

	/* Narrowed one side at a time; both at once is two inclusions to find in
	 * one step. */
	/*@ assert sep_ring_pages_final:
	      \separated(ring, ring->pages + (0 .. ring->page_count - 1)); */
	/*@ assert sep_ring_arena_final:
	      \separated(ring, ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert ring_pages_subset:
	      \subset(ring->pages + (0 .. ring->page_count - 1),
	              ring->pool->pages + (0 .. ring->pool->page_count - 1)); */
	/*@ assert sep_ring_pages_vs_pool_arena:
	      \separated(ring->pages + (0 .. ring->page_count - 1),
	                 ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert sep_each_page_vs_pool_arena:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        \separated(&ring->pages[k],
	                   ring->pool->arena +
	                     (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert sep_pages_arena_final:
	      \separated(ring->pages + (0 .. ring->page_count - 1),
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */

	/* Folded back into the predicates the postconditions ask for. */
	/*@ assert count_at_least_one: ring->page_count >= 1; */
	/*@ assert active_in_range_final: ring->active < ring->page_count; */
	/*@ assert active_id_run_final:
	      ring->pages[ring->active].first_entry_id +
	        (integer)ring->pages[ring->active].count == ring->next_entry_id; */
	/*@ assert scalar_wf_final: normfs_wal_ring_scalar_wf(ring); */
	/*@ assert layout_pred_final: normfs_wal_ring_layout(ring); */
	/*@ assert sep_pred_final: normfs_wal_ring_sep(ring); */
	/*@ assert pages_wf_pred_final: normfs_wal_ring_pages_wf(ring); */
	/*@ assert sep_ring_vs_pool_final: \separated(ring, ring->pool); */
	/*@ assert sep_ring_vs_pool_pages_final:
	      \separated(ring, ring->pool->pages +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert sep_ring_vs_pool_owner_final:
	      \separated(ring, ring->pool->owner +
	                   (0 .. ring->pool->page_count - 1)); */
	/*@ assert sep_ring_vs_pool_arena_final:
	      \separated(ring, ring->pool->arena +
	                   (0 .. ring->pool->page_count * ring->pool->page_size - 1)); */
	/*@ assert slots_fit_final:
	      ring->first_slot + ring->page_count <= ring->pool->page_count; */
	/*@ assert in_pool_final: normfs_wal_ring_in_pool(ring, ring_id); */
}

struct normfs_wal_ring_append_result
normfs_wal_ring_try_append(struct normfs_wal_ring *ring, const uint8_t *record,
    uint32_t record_size)
{
	struct normfs_wal_ring_append_result r;
	struct normfs_wal_page_append_result ap;
	size_t entry_size;

	r.entry_id = 0u;
	r.page_index = 0u;

	entry_size = normfs_wal_entry_v1_size(record_size);
	if (entry_size + 4u > ring->page_size) {
		r.status = NORMFS_WAL_RING_ERR_TOO_LARGE;
		return r;
	}

	/* Both writes -- the active page's bytes and, on the accepting path, the
	 * id counter -- happen between here and the merge below, and the frame is
	 * argued once there. Two arguments in sequence was what did not go
	 * through: the second had to carry a \forall over pages across a further
	 * write. */
	/*@ assert active_cap_is_page_size:
	      ring->pages[ring->active].cap == ring->page_size; */
	/* Monotonicity of multiplication on the left factor: nonlinear, and not
	 * derived from the inequality on its own. An instance rather than a
	 * lemma, because a -wp-fct list silently drops global lemma goals. */
	/*@ assert page_bytes_inside_range:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        (k + 1) * ring->page_size <=
	          ring->page_count * ring->page_size; */
	/* Disjoint by arithmetic rather than by an assumption over every pair:
	 * layout puts page k's buffer at the arena's k * page_size. */
	/*@ assert pages_disjoint_from_active:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        \separated(ring->pages[k].buf + (0 .. ring->page_size - 1),
	                   ring->pages[ring->active].buf +
	                     (0 .. ring->page_size - 1)); */
	/* And at the four bytes offsets_wf reads, buf + cap - 4 * (i + 1), which
	 * is what the frame has to place outside the write. Over the page-wide
	 * range instead, WP derives that under offsets_wf's own quantifier over
	 * entries -- the pair of quantifiers this function was excluded for. */
	/*@ assert other_page_table_separate:
	      \forall integer k, i;
	        0 <= k < ring->page_count && k != ring->active &&
	        0 <= i < ring->pages[k].count ==>
	          \separated(ring->pages[k].buf + ring->pages[k].cap - 4 * (i + 1)
	                       + (0 .. 3),
	                     ring->pages[ring->active].buf +
	                       (0 .. ring->page_size - 1)); */
	/*@ assert other_page_desc_separate:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        \separated(&ring->pages[k], &ring->pages[ring->active]); */
	/* The id counter is one field of the ring, and narrowing a separation to
	 * a field is not automatic. */
	/*@ assert id_bump_vs_pages:
	      \separated(&ring->next_entry_id,
	                 ring->pages + (0 .. ring->page_count - 1)); */
	/*@ assert id_bump_vs_arena:
	      \separated(&ring->next_entry_id,
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */

	ap = normfs_wal_page_append(&ring->pages[ring->active], record,
	    record_size);	/*@ ghost appended: ; */
	if (ap.status == NORMFS_WAL_PAGE_OK) {
		r.entry_id = ring->next_entry_id;
		r.page_index = ring->active;
		ring->next_entry_id = ring->next_entry_id + 1u;
		r.status = NORMFS_WAL_RING_OK;
	} else {
		r.status = NORMFS_WAL_RING_NEEDS_ROTATE;
	}

	/* The frame's conclusion as an equality between the two states, which the
	 * separations above feed directly; offsets_wf then follows by rewriting
	 * rather than by reproving. */
	/*@ assert other_page_offsets_unchanged:
	      \forall integer k, i;
	        0 <= k < ring->page_count && k != ring->active &&
	        0 <= i < ring->pages[k].count ==>
	          normfs_wal_page_offset_logic(&ring->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&ring->pages[k], i), Pre); */
	/* Splits ring_wf's \forall into the page just written and the rest, which
	 * WP does not split on its own. */
	/*@ assert other_pages_intact:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/* The active page's own comes from page_append; what is left is carrying
	 * it over the id counter, the same read footprint again. From the state
	 * just after the append, not from Pre, where the table is the one the
	 * append has not written yet. */
	/*@ assert active_page_offsets_unchanged:
	      \forall integer i; 0 <= i < ring->pages[ring->active].count ==>
	        normfs_wal_page_offset_logic(&ring->pages[ring->active], i) ==
	          \at(normfs_wal_page_offset_logic(&ring->pages[ring->active], i),
	              appended); */
	/*@ assert active_page_wf:
	      normfs_wal_page_wf(&ring->pages[ring->active]); */
	/*@ assert pages_wf_holds: normfs_wal_ring_pages_wf(ring); */
	/* No page's base or cap is assigned, so layout and sep carry across by
	 * frame once this is said once. */
	/*@ assert buffers_unmoved:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, Pre) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, Pre); */

	return r;
}

struct normfs_wal_ring_reusable_result
normfs_wal_ring_find_reusable(struct normfs_wal_ring *ring)
{
	struct normfs_wal_ring_reusable_result r;
	size_t k;

	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= k <= ring->page_count;
	    loop invariant r.found == 0;
	    loop assigns k, r;
	    loop variant ring->page_count - k;
	*/
	for (k = 0u; k < ring->page_count; k++) {
		if (normfs_wal_page_reusable(&ring->pages[k],
		    ring->min_essential_id) != 0) {
			r.index = k;
			r.found = 1;
			return r;
		}
	}

	return r;
}

void
normfs_wal_ring_rotate_to(struct normfs_wal_ring *ring, size_t index)
{
	normfs_wal_page_reset(&ring->pages[index], ring->next_page_id,
	    ring->next_entry_id);

	/* Splits the \forall over pages into the two cases WP does not separate
	 * on its own. Stated here, next to the call whose assigns clause proves
	 * it, it costs seconds; deferred to the end of the body it costs minutes.
	 * It stays a hypothesis for the rest of the body, so the whole-ring
	 * statement below follows cheaply. */
	/*@ assert other_pages_intact:
	      \forall integer k; 0 <= k < ring->page_count && k != index ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert reset_page_wf: normfs_wal_page_wf(&ring->pages[index]); */
	/*@ assert pages_wf_holds: normfs_wal_ring_pages_wf(ring); */

	/* Only scalars from here, but the two fields written are read back
	 * inside every range the postconditions speak about, so ring_sep has to
	 * be named and narrowed to them first. */
	/*@ assert scalar_writes_vs_pages:
	      \separated(&ring->active,
	                 ring->pages + (0 .. ring->page_count - 1)) &&
	      \separated(&ring->next_page_id,
	                 ring->pages + (0 .. ring->page_count - 1)); */
	/*@ assert scalar_writes_vs_arena:
	      \separated(&ring->active,
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)) &&
	      \separated(&ring->next_page_id,
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */

	/*@ ghost rotated: ; */
	ring->next_page_id = ring->next_page_id + 1u;
	ring->active = index;

	/* The frame's conclusion as an equality between the two states: what the
	 * separations above feed directly, and what page_wf is then rewritten
	 * with rather than reproved. */
	/*@ assert ring_shape_unchanged:
	      ring->pages == \at(ring->pages, rotated) &&
	      ring->arena == \at(ring->arena, rotated) &&
	      ring->page_count == \at(ring->page_count, rotated) &&
	      ring->page_size == \at(ring->page_size, rotated); */
	/*@ assert page_fields_unchanged:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, rotated) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, rotated) &&
	        ring->pages[k].count == \at(ring->pages[k].count, rotated) &&
	        ring->pages[k].used_bytes ==
	          \at(ring->pages[k].used_bytes, rotated) &&
	        ring->pages[k].first_entry_id ==
	          \at(ring->pages[k].first_entry_id, rotated) &&
	        ring->pages[k].last_entry_id ==
	          \at(ring->pages[k].last_entry_id, rotated); */
	/*@ assert offsets_unchanged:
	      \forall integer k, i;
	        0 <= k < ring->page_count && 0 <= i < ring->pages[k].count ==>
	          normfs_wal_page_offset_logic(&ring->pages[k], i) ==
	            \at(normfs_wal_page_offset_logic(&ring->pages[k], i), rotated); */
	/*@ assert pages_wf_final: normfs_wal_ring_pages_wf(ring); */

	/*@ assert buffers_unmoved:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, Pre) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, Pre); */

	/* Separation needs no hint of its own now. It is three statements about
	 * base pointers -- the ring, the descriptor array and the arena -- and
	 * rotation assigns none of them, so it survives by frame. This is what
	 * the arena bought: before it, sep quantified over every pair of page
	 * buffers, and transporting that across the call was the one goal that
	 * would not discharge. */
}

struct normfs_wal_ring_seek_result
normfs_wal_ring_seek(struct normfs_wal_ring *ring, uint64_t entry_id)
{
	struct normfs_wal_ring_seek_result r;
	size_t k;

	r.page_index = 0u;
	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= k <= ring->page_count;
	    loop invariant r.found == 0;
	    loop assigns k, r;
	    loop variant ring->page_count - k;
	*/
	for (k = 0u; k < ring->page_count; k++) {
		struct normfs_wal_page_find_result f =
		    normfs_wal_page_find(&ring->pages[k], entry_id);
		if (f.found != 0) {
			r.page_index = k;
			r.index = f.index;
			r.found = 1;
			return r;
		}
	}

	return r;
}

void
normfs_wal_ring_skip_entry(struct normfs_wal_ring *ring)
{
	/* Splits ring_wf's \forall over pages into the active page and the
	 * rest, which WP does not split on its own -- the same shape rotate_to
	 * and retain_page need, for the same reason. The rest follows from the frame:
	 * this assigns two scalars and no page's bytes at all. */
	/*@ assert other_pages_wf:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        normfs_wal_page_wf(&ring->pages[k]); */

	/* ring_sep arrives as an opaque atom, and the write below lands on one
	 * field of one descriptor, so the separations it holds have to be named
	 * and then narrowed to that field. */
	/*@ assert unfold_sep_ring_vs_pages:
	      \separated(ring, ring->pages + (0 .. ring->page_count - 1)); */
	/*@ assert unfold_sep_ring_vs_arena:
	      \separated(ring, ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert unfold_sep_pages_vs_arena:
	      \separated(ring->pages + (0 .. ring->page_count - 1),
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert active_vs_ring:
	      \separated(&ring->pages[ring->active], ring); */
	/*@ assert active_vs_arena:
	      \separated(&ring->pages[ring->active],
	                 ring->arena +
	                   (0 .. ring->page_count * ring->page_size - 1)); */
	/*@ assert active_vs_other_pages:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        \separated(&ring->pages[k], &ring->pages[ring->active]); */

	/* The active page is empty, so ids_wf's bound is the only clause the new
	 * first id has to satisfy and the offset table is not read at all. */
	/*@ assert active_empty:
	      ring->pages[ring->active].count == 0 &&
	      ring->pages[ring->active].used_bytes == 0; */

	ring->next_entry_id = ring->next_entry_id + 1u;
	ring->pages[ring->active].first_entry_id = ring->next_entry_id;

	/* Every range below is built from these, and a separation from the
	 * address written is not by itself the statement that they did not
	 * move. */
	/*@ assert ring_shape_unchanged:
	      ring->pages == \at(ring->pages, Pre) &&
	      ring->arena == \at(ring->arena, Pre) &&
	      ring->page_count == \at(ring->page_count, Pre) &&
	      ring->page_size == \at(ring->page_size, Pre) &&
	      ring->active == \at(ring->active, Pre); */
	/*@ assert pages_unchanged:
	      \forall integer k; 0 <= k < ring->page_count ==>
	        ring->pages[k].buf == \at(ring->pages[k].buf, Pre) &&
	        ring->pages[k].cap == \at(ring->pages[k].cap, Pre) &&
	        ring->pages[k].count == \at(ring->pages[k].count, Pre) &&
	        ring->pages[k].used_bytes == \at(ring->pages[k].used_bytes, Pre); */

	/*@ assert other_pages_wf_final:
	      \forall integer k; 0 <= k < ring->page_count && k != ring->active ==>
	        normfs_wal_page_wf(&ring->pages[k]); */
	/*@ assert active_page_wf_final:
	      normfs_wal_page_wf(&ring->pages[ring->active]); */
	/*@ assert pages_wf_final: normfs_wal_ring_pages_wf(ring); */
	/*@ assert scalar_wf_final: normfs_wal_ring_scalar_wf(ring); */
	/*@ assert layout_final: normfs_wal_ring_layout(ring); */
	/*@ assert sep_final: normfs_wal_ring_sep(ring); */
}
