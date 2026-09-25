#ifndef NORMFS_WAL_PAGE_H
#define NORMFS_WAL_PAGE_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/wal_entry.h"
#include "uintn/le.h"

/*
 * A WAL memory page holds V1 WAL entries in a Rust-owned byte buffer:
 *
 *   [entry bytes ... growing forward] .... [offset table growing backward]
 *
 * Each entry is a V1 WAL entry (see wal_entry.h). The offset table stores, for
 * entry i (0-based, in append order), the u32 LE byte offset at which the entry
 * begins; item i lives at buf + cap - 4 * (i + 1). The two regions grow toward
 * each other and the page is full when they would meet.
 *
 * C never allocates: buf and cap come from Rust. Entry ids are not stored; the
 * id of entry i is first_entry_id + i.
 */

#define NORMFS_WAL_PAGE_OFF_SIZE 4

enum normfs_wal_page_status {
	NORMFS_WAL_PAGE_OK = 0,
	NORMFS_WAL_PAGE_ERR_NO_SPACE = 1,
	NORMFS_WAL_PAGE_ERR_TOO_LARGE = 2
};

struct normfs_wal_page {
	uint8_t *buf;
	size_t cap;
	size_t used_bytes;
	uint32_t count;
	uint64_t page_id;
	uint64_t first_entry_id;
	uint64_t last_entry_id;
	uint32_t pin_count;
	int published;
};

struct normfs_wal_page_append_result {
	size_t entry_offset;
	int status;
};

struct normfs_wal_page_find_result {
	uint32_t index;
	int found;
};

/*@ axiomatic NormfsWalPage {
      // The stored start offset of entry i, read from the backward table.
      logic integer normfs_wal_page_offset_logic{L}(struct normfs_wal_page *p, integer i) =
        normfs_uintn_le32_logic(p->buf + p->cap - 4 * (i + 1));

      // Byte extent occupied by the offset table of a page with n entries.
      logic integer normfs_wal_page_table_bytes(integer n) = 4 * n;

      predicate normfs_wal_page_layout{L}(struct normfs_wal_page *p) =
        \valid(p) &&
        p->cap <= 0xFFFFFFFF &&
        p->used_bytes <= p->cap &&
        (integer)p->used_bytes + normfs_wal_page_table_bytes(p->count) <= p->cap &&
        \valid(p->buf + (0 .. p->cap - 1));

      // Every stored offset points inside the entries region: entry i begins
      // before the forward cursor used_bytes.
      //
      // And they increase with i, which is the ordering half: entry i begins
      // strictly before entry i + 1, so the bytes on a page are laid out in the
      // order the ids were given. Stated between neighbours rather than over
      // every pair -- a \forall over pairs is the shape the automatic provers
      // will not transport across a mutation, and append only ever adds at the
      // end, so neighbours are what it has to re-establish.
      //
      // This is what lets a file take a page whole. A flush writes
      // buf[from .. to) in one call and the reader derives each entry's id from
      // its position in the file, so bytes out of id order on a page would be
      // entries out of id order in the file -- which V1 does not report as a
      // missing record but as every later record answering to the wrong id.
      predicate normfs_wal_page_offsets_wf{L}(struct normfs_wal_page *p) =
        (\forall integer i; 0 <= i < p->count ==>
           0 <= normfs_wal_page_offset_logic(p, i) < p->used_bytes) &&
        (\forall integer i; 0 <= i < (integer)p->count - 1 ==>
           normfs_wal_page_offset_logic(p, i) <
             normfs_wal_page_offset_logic(p, i + 1));

      predicate normfs_wal_page_ids_wf{L}(struct normfs_wal_page *p) =
        (integer)p->first_entry_id + (integer)p->count <= 0xFFFFFFFFFFFFFFFF &&
        (p->count == 0 ==> p->used_bytes == 0) &&
        (p->count > 0 ==> p->last_entry_id == p->first_entry_id + (integer)p->count - 1);

      predicate normfs_wal_page_wf{L}(struct normfs_wal_page *p) =
        normfs_wal_page_layout(p) &&
        normfs_wal_page_offsets_wf(p) &&
        normfs_wal_page_ids_wf(p);

      // A page may be reset and reused once nothing pins it and all of its
      // entries are below the essential id.
      predicate normfs_wal_page_is_reusable{L}(struct normfs_wal_page *p,
                                               integer m) =
        p->pin_count == 0 &&
        (p->count == 0 || p->last_entry_id < m);
    }
*/

/*@ requires \valid(page);
    requires \valid(buf + (0 .. cap - 1));
    requires cap <= 0xFFFFFFFF;
    assigns page->buf, page->cap, page->used_bytes, page->count,
            page->page_id, page->first_entry_id, page->last_entry_id,
            page->pin_count, page->published;
    ensures normfs_wal_page_wf(page);
    ensures page->count == 0 && page->used_bytes == 0 && page->pin_count == 0;
    ensures page->buf == buf && page->cap == cap;
    ensures page->page_id == page_id && page->first_entry_id == first_entry_id;
*/
void normfs_wal_page_init(struct normfs_wal_page *page, uint8_t *buf, size_t cap,
    uint64_t page_id, uint64_t first_entry_id);

/*@ requires \valid(page);
    requires normfs_wal_page_layout(page);
    assigns page->used_bytes, page->count, page->page_id,
            page->first_entry_id, page->last_entry_id;
    ensures normfs_wal_page_wf(page);
    ensures page->count == 0 && page->used_bytes == 0;
    ensures page->page_id == page_id && page->first_entry_id == first_entry_id;
    ensures page->cap == \old(page->cap) && page->buf == \old(page->buf);
*/
void normfs_wal_page_reset(struct normfs_wal_page *page, uint64_t page_id,
    uint64_t first_entry_id);

/*@ requires normfs_wal_page_wf(page);
    requires (integer)page->first_entry_id + (integer)page->count < 0xFFFFFFFFFFFFFFFF;
    requires record_size == 0 || \valid_read(record + (0 .. record_size - 1));
    requires \separated(page, page->buf + (0 .. page->cap - 1));
    requires record_size == 0 ||
             \separated(page->buf + (0 .. page->cap - 1),
                        record + (0 .. record_size - 1));
    assigns page->buf[0 .. page->cap - 1], page->used_bytes, page->count,
            page->last_entry_id;
    ensures normfs_wal_page_wf(page);
    ensures \result.status == NORMFS_WAL_PAGE_OK ||
            \result.status == NORMFS_WAL_PAGE_ERR_NO_SPACE;
    // NO_SPACE exactly when the entry plus its offset slot would not fit.
    ensures \result.status == NORMFS_WAL_PAGE_ERR_NO_SPACE <==>
            \old((integer)page->used_bytes + 4 * (integer)page->count +
                 normfs_wal_entry_v1_size_logic(record_size) + 4) > page->cap;
    ensures \result.status == NORMFS_WAL_PAGE_OK ==>
            page->count == \old(page->count) + 1 &&
            \result.entry_offset == \old(page->used_bytes) &&
            page->used_bytes ==
              \old(page->used_bytes) + normfs_wal_entry_v1_size_logic(record_size) &&
            page->last_entry_id == page->first_entry_id + (integer)page->count - 1 &&
            // the new offset slot records where this entry begins
            normfs_wal_page_offset_logic(page, \old(page->count)) == \old(page->used_bytes) &&
            // The ordering theorem at its one point of use: an append lands
            // after everything already on the page, never between two entries.
            // Every byte a page holds is therefore in id order, and a flush can
            // hand a run of them to the file without looking inside.
            (\forall integer i; 0 <= i < \old(page->count) ==>
               normfs_wal_page_offset_logic(page, i) <
                 normfs_wal_page_offset_logic(page, \old(page->count)));
    ensures \result.status == NORMFS_WAL_PAGE_ERR_NO_SPACE ==>
            page->count == \old(page->count) &&
            page->used_bytes == \old(page->used_bytes);
*/
struct normfs_wal_page_append_result
normfs_wal_page_append(struct normfs_wal_page *page, const uint8_t *record,
    uint32_t record_size);

/*@ requires normfs_wal_page_wf(page);
    requires index < page->count;
    assigns \nothing;
    ensures 0 <= \result < page->used_bytes;
    ensures \result == normfs_wal_page_offset_logic(page, index);
*/
uint32_t normfs_wal_page_offset(struct normfs_wal_page *page, uint32_t index);

/*@ requires normfs_wal_page_wf(page);
    requires index < page->count;
    assigns \nothing;
    ensures \result == page->first_entry_id + (integer)index;
*/
uint64_t normfs_wal_page_entry_id(struct normfs_wal_page *page, uint32_t index);

/*@ requires normfs_wal_page_wf(page);
    assigns \nothing;
    ensures \result.found != 0 <==>
            (page->count > 0 && page->first_entry_id <= entry_id <= page->last_entry_id);
    ensures \result.found != 0 ==>
            \result.index < page->count &&
            page->first_entry_id + (integer)\result.index == entry_id;
*/
struct normfs_wal_page_find_result
normfs_wal_page_find(struct normfs_wal_page *page, uint64_t entry_id);

/*
 * Where a page splits into ids below entry_id and ids at or above it. A flush
 * bounded at some id takes buf[from .. cut(bound + 1)); the neighbour clause
 * is what proves the cut falls between two entries and never inside one.
 */
/*@ requires normfs_wal_page_wf(page);
    assigns \nothing;
    ensures \result <= page->used_bytes;
    ensures (page->count == 0 || entry_id > page->last_entry_id) ==>
            \result == page->used_bytes;
    ensures (page->count > 0 && entry_id <= page->first_entry_id) ==>
            \result == normfs_wal_page_offset_logic(page, 0);
    ensures (page->count > 0 &&
             page->first_entry_id < entry_id <= page->last_entry_id) ==>
            \result == normfs_wal_page_offset_logic(page,
                          entry_id - page->first_entry_id);
    ensures (page->count > 0 &&
             page->first_entry_id < entry_id <= page->last_entry_id) ==>
            normfs_wal_page_offset_logic(page, entry_id - page->first_entry_id - 1)
              < \result;
*/
size_t normfs_wal_page_cut(struct normfs_wal_page *page, uint64_t entry_id);

/*
 * The inverse: the first entry beginning at or after byte `from`, which names
 * the first id a writer resuming mid-page is about to take.
 */
/*@ requires normfs_wal_page_wf(page);
    assigns \nothing;
    ensures \result.found != 0 ==>
            \result.index < page->count &&
            normfs_wal_page_offset_logic(page, \result.index) >= from &&
            (\forall integer j; 0 <= j < \result.index ==>
               normfs_wal_page_offset_logic(page, j) < from);
    ensures \result.found == 0 ==>
            (\forall integer j; 0 <= j < page->count ==>
               normfs_wal_page_offset_logic(page, j) < from);
*/
struct normfs_wal_page_find_result
normfs_wal_page_first_from(struct normfs_wal_page *page, size_t from);

/*@ requires \valid(page);
    requires page->pin_count < 0xFFFFFFFF;
    assigns page->pin_count;
    ensures page->pin_count == \old(page->pin_count) + 1;
*/
void normfs_wal_page_pin(struct normfs_wal_page *page);

/*@ requires \valid(page);
    requires page->pin_count > 0;
    assigns page->pin_count;
    ensures page->pin_count == \old(page->pin_count) - 1;
*/
void normfs_wal_page_unpin(struct normfs_wal_page *page);

/*@ requires \valid_read(page);
    assigns \nothing;
    ensures \result != 0 <==> normfs_wal_page_is_reusable(page, min_essential_id);
*/
int normfs_wal_page_reusable(const struct normfs_wal_page *page,
    uint64_t min_essential_id);

#endif /* NORMFS_WAL_PAGE_H */
