#include "normfs/wal_page.h"

void
normfs_wal_page_init(struct normfs_wal_page *page, uint8_t *buf, size_t cap,
    uint64_t page_id, uint64_t first_entry_id)
{
	page->buf = buf;
	page->cap = cap;
	page->used_bytes = 0u;
	page->count = 0u;
	page->page_id = page_id;
	page->first_entry_id = first_entry_id;
	page->last_entry_id = first_entry_id;
	page->pin_count = 0u;
	page->published = 0;
}

void
normfs_wal_page_reset(struct normfs_wal_page *page, uint64_t page_id,
    uint64_t first_entry_id)
{
	page->used_bytes = 0u;
	page->count = 0u;
	page->page_id = page_id;
	page->first_entry_id = first_entry_id;
	page->last_entry_id = first_entry_id;
}

struct normfs_wal_page_append_result
normfs_wal_page_append(struct normfs_wal_page *page, const uint8_t *record,
    uint32_t record_size)
{
	struct normfs_wal_page_append_result r;
	size_t entry_size;
	size_t table_bytes;
	size_t free_gap;
	size_t off_slot;
	struct normfs_wal_entry_encode_result enc;

	entry_size = normfs_wal_entry_v1_size(record_size);
	/*@ assert entry_size == normfs_wal_entry_v1_size_logic(record_size); */
	/*@ assert entry_size >= NORMFS_WAL_ENTRY_V1_MIN_SIZE; */

	table_bytes = (size_t)page->count * 4u;
	/*@ assert table_bytes == 4 * (integer)page->count; */

	/* used_bytes + table_bytes <= cap by the layout invariant, so no wrap. */
	free_gap = page->cap - page->used_bytes - table_bytes;
	/*@ assert (integer)free_gap ==
	           (integer)page->cap - page->used_bytes - 4 * (integer)page->count; */

	if (entry_size + 4u > free_gap) {
		r.entry_offset = 0u;
		r.status = NORMFS_WAL_PAGE_ERR_NO_SPACE;
		return r;
	}

	/* The entry plus its 4 byte offset slot fit; write both. */
	off_slot = page->cap - table_bytes - 4u;
	/*@ assert off_slot == (integer)page->cap - 4 * ((integer)page->count + 1); */
	/*@ assert (integer)page->used_bytes + entry_size <= off_slot; */

	/* Every existing offset is below the (still unchanged) used_bytes. */
	/*@ assert \forall integer i; 0 <= i < page->count ==>
	             normfs_wal_page_offset_logic(page, i) < page->used_bytes; */

	enc = normfs_wal_entry_v1_encode(record, record_size,
	    page->buf + page->used_bytes, entry_size);
	/*@ assert enc.status == NORMFS_WAL_ENTRY_OK; */
	/*@ assert enc.written == entry_size; */

	normfs_uintn_le32_write(page->buf + off_slot, (uint32_t)page->used_bytes);
	/*@ assert normfs_wal_page_offset_logic(page, (integer)page->count) ==
	             page->used_bytes; */

	/* Both writes land in buf[used_bytes .. cap - 4*count); the old offset
	 * table buf[cap - 4*count .. cap) keeps every byte, so every earlier offset
	 * reads back unchanged and thus still below used_bytes. */
	/*@ assert \forall integer j;
	      (integer)page->cap - 4 * (integer)page->count <= j < (integer)page->cap ==>
	      page->buf[j] == \at(page->buf[j], Pre); */
	/*@ assert \forall integer i; 0 <= i < page->count ==>
	      normfs_wal_page_offset_logic(page, i) ==
	      \at(normfs_wal_page_offset_logic(page, i), Pre); */
	/*@ assert \forall integer i; 0 <= i < page->count ==>
	             normfs_wal_page_offset_logic(page, i) < page->used_bytes; */

	/* The offsets kept their order because they kept their bytes; the new one
	 * is at used_bytes, which the assert above puts strictly above all of them.
	 * Those two are the whole of offsets_wf's ordering half at the new count. */
	/*@ assert \forall integer i; 0 <= i < (integer)page->count - 1 ==>
	             normfs_wal_page_offset_logic(page, i) <
	               normfs_wal_page_offset_logic(page, i + 1); */

	r.entry_offset = page->used_bytes;
	page->used_bytes = page->used_bytes + enc.written;
	page->count = page->count + 1u;
	page->last_entry_id = page->first_entry_id + (uint64_t)page->count - 1u;
	r.status = NORMFS_WAL_PAGE_OK;

	return r;
}

uint32_t
normfs_wal_page_offset(struct normfs_wal_page *page, uint32_t index)
{
	/*@ assert 4 * ((integer)index + 1) <= page->cap; */
	/*@ assert 0 <= normfs_wal_page_offset_logic(page, index) < page->used_bytes; */
	return normfs_uintn_le32_read(page->buf + page->cap -
	    4u * ((size_t)index + 1u));
}

uint64_t
normfs_wal_page_entry_id(struct normfs_wal_page *page, uint32_t index)
{
	return page->first_entry_id + (uint64_t)index;
}

struct normfs_wal_page_find_result
normfs_wal_page_find(struct normfs_wal_page *page, uint64_t entry_id)
{
	struct normfs_wal_page_find_result r;

	if (page->count > 0u && entry_id >= page->first_entry_id &&
	    entry_id <= page->last_entry_id) {
		r.index = (uint32_t)(entry_id - page->first_entry_id);
		r.found = 1;
	} else {
		r.index = 0u;
		r.found = 0;
	}

	return r;
}

size_t
normfs_wal_page_cut(struct normfs_wal_page *page, uint64_t entry_id)
{
	uint32_t index;

	if (page->count == 0u || entry_id > page->last_entry_id)
		return page->used_bytes;

	if (entry_id <= page->first_entry_id)
		index = 0u;
	else
		index = (uint32_t)(entry_id - page->first_entry_id);
	/*@ assert index < page->count; */

	return normfs_wal_page_offset(page, index);
}

struct normfs_wal_page_find_result
normfs_wal_page_first_from(struct normfs_wal_page *page, size_t from)
{
	struct normfs_wal_page_find_result r;
	uint32_t i;

	r.index = 0u;
	r.found = 0;

	/*@ loop invariant 0 <= i <= page->count;
	    loop invariant \forall integer j; 0 <= j < i ==>
	      normfs_wal_page_offset_logic(page, j) < from;
	    loop assigns i;
	    loop variant page->count - i;
	*/
	for (i = 0u; i < page->count; i++) {
		if (normfs_wal_page_offset(page, i) >= from) {
			r.index = i;
			r.found = 1;
			return r;
		}
	}

	return r;
}

void
normfs_wal_page_pin(struct normfs_wal_page *page)
{
	page->pin_count = page->pin_count + 1u;
}

void
normfs_wal_page_unpin(struct normfs_wal_page *page)
{
	page->pin_count = page->pin_count - 1u;
}

int
normfs_wal_page_reusable(const struct normfs_wal_page *page,
    uint64_t min_essential_id)
{
	return page->pin_count == 0u &&
	    (page->count == 0u || page->last_entry_id < min_essential_id);
}
