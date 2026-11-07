#ifndef NORMFS_WAL_SCAN_H
#define NORMFS_WAL_SCAN_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/wal_entry.h"

/* buf[start .. end) is count whole V1 entries back to back, and at end there
 * is none, or one whose id would overflow: the prefix recovery keeps. */
struct normfs_wal_scan_count_result {
	size_t end;
	uint64_t count;
};

/*@ axiomatic NormfsWalScan {
      // The first end of the len bytes at p are n whole entries back to back.
      inductive normfs_wal_scan_run{L}(uint8_t *p, integer len, integer n,
          integer end) {
        case normfs_wal_scan_run_empty{L}:
          \forall uint8_t *p, integer len; normfs_wal_scan_run(p, len, 0, 0);
        case normfs_wal_scan_run_next{L}:
          \forall uint8_t *p, integer len, n, end;
            normfs_wal_scan_run(p, len, n, end) &&
            normfs_wal_entry_v1_whole(p + end, len - end) ==>
            normfs_wal_scan_run(p, len, n + 1, end +
              normfs_wal_entry_v1_body(p + end) + NORMFS_WAL_ENTRY_V1_CRC_SIZE);
      }
    }
*/

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires start <= len;
    requires \valid(res) && \separated(res, buf + (0 .. len - 1));
    assigns *res;
    ensures start <= res->end <= len;
    ensures res->count * NORMFS_WAL_ENTRY_V1_MIN_SIZE <= res->end - start;
    ensures res->count > 0 ==>
              num_entries_before + res->count - 1 <= 0xFFFFFFFFFFFFFFFF;
    ensures normfs_wal_scan_run(buf + start, len - start, res->count,
              res->end - start);
    ensures res->end < len ==>
              !normfs_wal_entry_v1_whole(buf + res->end, len - res->end) ||
              num_entries_before + res->count > 0xFFFFFFFFFFFFFFFF;
*/
void normfs_wal_scan_count_v1(const uint8_t *buf, size_t len, size_t start,
    uint64_t num_entries_before, struct normfs_wal_scan_count_result *res);

#endif /* NORMFS_WAL_SCAN_H */
