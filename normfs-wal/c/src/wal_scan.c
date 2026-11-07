#include "normfs/wal_scan.h"

typedef char normfs_wal_scan_count_result_size_check[
    (sizeof(struct normfs_wal_scan_count_result) == 16) ? 1 : -1];

void
normfs_wal_scan_count_v1(const uint8_t *buf, size_t len, size_t start,
    uint64_t num_entries_before, struct normfs_wal_scan_count_result *res)
{
	struct normfs_wal_entry_iter_result e;
	size_t cursor = start;
	uint64_t count = 0u;

	/*@ loop invariant start <= cursor <= len;
	    loop invariant count * NORMFS_WAL_ENTRY_V1_MIN_SIZE <= cursor - start;
	    loop invariant count > 0 ==>
	                     num_entries_before + count - 1 <= 0xFFFFFFFFFFFFFFFF;
	    loop invariant normfs_wal_scan_run(buf + start, len - start, count,
	                     cursor - start);
	    loop assigns cursor, count, e;
	    loop variant len - cursor;
	*/
	while (cursor < len) {
		e = normfs_wal_entry_v1_iter_next(buf + cursor, len - cursor,
		    num_entries_before, count);
		if (e.status != NORMFS_WAL_ENTRY_OK)
			break;
		/*@ assert (buf + start) + (cursor - start) == buf + cursor; */
		/*@ assert normfs_wal_scan_run(buf + start, len - start, count + 1,
		      cursor - start + e.consumed); */
		cursor += e.consumed;
		count++;
	}
	res->end = cursor;
	res->count = count;
}
