#include "normfs/store_pack.h"

/*@ logic integer normfs_store_pack_bound_logic(integer n) =
      n + n / 256 +
      (n < NORMFS_STORE_PACK_ZSTD_BLOCK ?
         (NORMFS_STORE_PACK_ZSTD_BLOCK - n) / 2048 : 0);

    logic integer normfs_store_pack_slot_logic(integer cap) =
      cap + NORMFS_STORE_PACK_OVERHEAD + normfs_store_pack_bound_logic(cap);

    predicate normfs_store_pack_layout_ok(struct normfs_store_pack_layout l,
                                          integer slot, integer cap,
                                          integer header, integer enc) =
      l.file_at == cap &&
      l.header_at == cap + NORMFS_STORE_PACK_AUTH_SIZE &&
      l.body_at == l.header_at + header &&
      l.data_at == l.body_at + (enc ? NORMFS_STORE_PACK_NONCE_SIZE : 0) &&
      l.data_at + l.data_cap + (enc ? NORMFS_STORE_PACK_TAG_SIZE : 0) == slot &&
      (l.encrypted != 0) == (enc != 0);

*/

/*@ requires n <= NORMFS_STORE_PACK_INPUT_MAX;
    assigns \nothing;
    ensures \result == normfs_store_pack_bound_logic(n);
    ensures n <= \result;
*/
size_t
normfs_store_pack_compress_bound(size_t n)
{
	size_t tail = 0;

	if (n < (size_t)NORMFS_STORE_PACK_ZSTD_BLOCK)
		tail = ((size_t)NORMFS_STORE_PACK_ZSTD_BLOCK - n) / 2048;
	return n + n / 256 + tail;
}

/*@ assigns \nothing;
    behavior fits:
      assumes input_cap <= NORMFS_STORE_PACK_INPUT_MAX;
      ensures \result.status == NORMFS_STORE_PACK_OK;
      ensures \result.size == normfs_store_pack_slot_logic(input_cap);
    behavior too_large:
      assumes input_cap > NORMFS_STORE_PACK_INPUT_MAX;
      ensures \result.status == NORMFS_STORE_PACK_ERR_INPUT;
      ensures \result.size == 0;
    complete behaviors;
    disjoint behaviors;
*/
struct normfs_store_pack_size_result
normfs_store_pack_slot_size(size_t input_cap)
{
	struct normfs_store_pack_size_result r = { 0, NORMFS_STORE_PACK_ERR_INPUT };

	if (input_cap > NORMFS_STORE_PACK_INPUT_MAX)
		return r;
	r.size = input_cap + (size_t)NORMFS_STORE_PACK_OVERHEAD +
	    normfs_store_pack_compress_bound(input_cap);
	r.status = NORMFS_STORE_PACK_OK;
	return r;
}

/*@ assigns \nothing;
    behavior ok:
      assumes input_cap <= NORMFS_STORE_PACK_INPUT_MAX;
      assumes header_len <= NORMFS_STORE_HEADER_V1_MAX_SIZE;
      assumes slot_size >= normfs_store_pack_slot_logic(input_cap);
      ensures \result.status == NORMFS_STORE_PACK_OK;
      ensures normfs_store_pack_layout_ok(\result.layout, slot_size, input_cap,
                                          header_len, encrypted);
      ensures \forall integer len; 0 <= len <= input_cap ==>
                normfs_store_pack_bound_logic(len) <= \result.layout.data_cap;
    behavior bad_input:
      assumes input_cap > NORMFS_STORE_PACK_INPUT_MAX;
      ensures \result.status == NORMFS_STORE_PACK_ERR_INPUT;
    behavior bad_header:
      assumes input_cap <= NORMFS_STORE_PACK_INPUT_MAX;
      assumes header_len > NORMFS_STORE_HEADER_V1_MAX_SIZE;
      ensures \result.status == NORMFS_STORE_PACK_ERR_HEADER;
    behavior small_slot:
      assumes input_cap <= NORMFS_STORE_PACK_INPUT_MAX;
      assumes header_len <= NORMFS_STORE_HEADER_V1_MAX_SIZE;
      assumes slot_size < normfs_store_pack_slot_logic(input_cap);
      ensures \result.status == NORMFS_STORE_PACK_ERR_SLOT;
    complete behaviors;
    disjoint behaviors;
*/
struct normfs_store_pack_layout_result
normfs_store_pack_layout(size_t slot_size, size_t input_cap, size_t header_len,
    int encrypted)
{
	struct normfs_store_pack_layout_result r = {
		{ 0, 0, 0, 0, 0, 0 }, NORMFS_STORE_PACK_ERR_INPUT
	};
	size_t need;
	size_t nonce = encrypted ? (size_t)NORMFS_STORE_PACK_NONCE_SIZE : 0;
	size_t tag = encrypted ? (size_t)NORMFS_STORE_PACK_TAG_SIZE : 0;

	if (input_cap > NORMFS_STORE_PACK_INPUT_MAX)
		return r;
	r.status = NORMFS_STORE_PACK_ERR_HEADER;
	if (header_len > (size_t)NORMFS_STORE_HEADER_V1_MAX_SIZE)
		return r;
	need = input_cap + (size_t)NORMFS_STORE_PACK_OVERHEAD +
	    normfs_store_pack_compress_bound(input_cap);
	r.status = NORMFS_STORE_PACK_ERR_SLOT;
	if (slot_size < need)
		return r;

	/* Monotonicity of the bound, spelled out here rather than as a global
	 * lemma, because a -wp-fct list silently drops global lemma goals. */
	/*@ assert floor_step: \forall integer a, b; 0 <= a <= b ==>
	      a / 256 <= b / 256; */
	/*@ assert floor_gap: \forall integer a, b; 0 <= a <= b ==>
	      b / 2048 - a / 2048 <= b - a; */
	/*@ assert bound_monotonic: \forall integer len; 0 <= len <= input_cap ==>
	      normfs_store_pack_bound_logic(len) <=
	        normfs_store_pack_bound_logic(input_cap); */

	r.layout.file_at = input_cap;
	r.layout.header_at = input_cap + (size_t)NORMFS_STORE_PACK_AUTH_SIZE;
	r.layout.body_at = r.layout.header_at + header_len;
	r.layout.data_at = r.layout.body_at + nonce;
	r.layout.data_cap = slot_size - tag - r.layout.data_at;
	r.layout.encrypted = encrypted != 0;
	r.status = NORMFS_STORE_PACK_OK;
	return r;
}

/*@ requires \valid_read(layout);
    requires layout->file_at <= layout->body_at <= layout->data_at;
    requires layout->data_at - layout->body_at ==
               (layout->encrypted ? NORMFS_STORE_PACK_NONCE_SIZE : 0);
    requires layout->data_at + layout->data_cap +
               (layout->encrypted ? NORMFS_STORE_PACK_TAG_SIZE : 0) <= SIZE_MAX;
    assigns \nothing;
    behavior fits:
      assumes data_len <= layout->data_cap;
      ensures \result.status == NORMFS_STORE_PACK_OK;
      ensures \result.size == layout->data_at - layout->file_at + data_len +
                (layout->encrypted ? NORMFS_STORE_PACK_TAG_SIZE : 0);
    behavior overflow:
      assumes data_len > layout->data_cap;
      ensures \result.status == NORMFS_STORE_PACK_ERR_DATA;
      ensures \result.size == 0;
    complete behaviors;
    disjoint behaviors;
*/
struct normfs_store_pack_size_result
normfs_store_pack_file_len(const struct normfs_store_pack_layout *layout,
    size_t data_len)
{
	struct normfs_store_pack_size_result r = { 0, NORMFS_STORE_PACK_ERR_DATA };
	size_t tag = layout->encrypted ? (size_t)NORMFS_STORE_PACK_TAG_SIZE : 0;

	if (data_len > layout->data_cap)
		return r;
	r.size = layout->data_at - layout->file_at + data_len + tag;
	r.status = NORMFS_STORE_PACK_OK;
	return r;
}

/*
 * Fit: a slot sized by normfs_store_pack_slot_size for input_cap takes any
 * input of up to input_cap bytes whose data stays within zstd's bound, and
 * the store file built from it neither overlaps the input nor runs past the
 * slot. The proof is the body: WP discharges the asserts, so \result == 1
 * is a theorem about every such input, not a runtime check.
 */
/*@ requires input_cap <= NORMFS_STORE_PACK_INPUT_MAX;
    requires header_len <= NORMFS_STORE_HEADER_V1_MAX_SIZE;
    requires input_len <= input_cap;
    requires data_len <= normfs_store_pack_bound_logic(input_len);
    assigns \nothing;
    ensures \result == 1;
*/
int
normfs_store_pack_fits_holds(size_t input_cap, size_t header_len,
    int encrypted, size_t input_len, size_t data_len)
{
	struct normfs_store_pack_size_result slot;
	struct normfs_store_pack_layout_result l;
	struct normfs_store_pack_size_result file;

	slot = normfs_store_pack_slot_size(input_cap);
	/*@ assert slot.status == NORMFS_STORE_PACK_OK; */
	l = normfs_store_pack_layout(slot.size, input_cap, header_len, encrypted);
	/*@ assert l.status == NORMFS_STORE_PACK_OK; */
	file = normfs_store_pack_file_len(&l.layout, data_len);
	/*@ assert file.status == NORMFS_STORE_PACK_OK; */
	/*@ assert input_len <= l.layout.file_at; */
	/*@ assert l.layout.file_at + file.size <= slot.size; */
	return (input_len <= l.layout.file_at) &
	    (l.layout.file_at + file.size <= slot.size);
}
