#ifndef NORMFS_WAL_ENTRY_H
#define NORMFS_WAL_ENTRY_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/crc32c.h"
#include "uintn/varint.h"

/*
 * V1 WAL entry:
 *
 *   [record_size varint32][record bytes][crc32c u32 LE]
 *
 * The CRC covers the record_size varint and the record bytes, so a corrupted
 * length is detected as well as corrupted data. Neither the entry id nor the
 * format version is stored: the id is derived from the file header's
 * num_entries_before plus the entry index, and the version comes from the
 * file header itself.
 */

#define NORMFS_WAL_ENTRY_V1_CRC_SIZE 4
#define NORMFS_WAL_ENTRY_V1_MIN_SIZE 5
#define NORMFS_WAL_ENTRY_V1_MAX_OVERHEAD 9

/* Codes 0 .. 4 take their values from enum normfs_uintn_varint_status so the
 * two cannot drift apart. */
enum normfs_wal_entry_status {
	NORMFS_WAL_ENTRY_OK = NORMFS_UINTN_VARINT_OK,
	NORMFS_WAL_ENTRY_ERR_TRUNCATED = NORMFS_UINTN_VARINT_ERR_TRUNCATED,
	NORMFS_WAL_ENTRY_ERR_OVERFLOW = NORMFS_UINTN_VARINT_ERR_OVERFLOW,
	NORMFS_WAL_ENTRY_ERR_NON_CANONICAL = NORMFS_UINTN_VARINT_ERR_NON_CANONICAL,
	NORMFS_WAL_ENTRY_ERR_NO_SPACE = NORMFS_UINTN_VARINT_ERR_NO_SPACE,
	NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH = 5,
	NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW = 6
};

struct normfs_wal_entry_encode_result {
	size_t written;
	int status;
};

struct normfs_wal_entry_decode_result {
	size_t record_offset;
	size_t record_size;
	size_t consumed;
	uint32_t crc;
	int status;
};

struct normfs_wal_entry_id_result {
	uint64_t entry_id;
	int status;
};

/*
 * One step of a forward iteration over a buffer of V1 entries. It decodes the
 * entry at the front of buf and, in the same call, derives that entry's id
 * from the file header's num_entries_before and the entry's 0-based index in
 * the file. The record fields mirror normfs_wal_entry_v1_decode (zero copy:
 * record_offset/record_size point into buf); consumed is how far to advance
 * the cursor before the next call. The 8-byte fields precede the 4-byte ones
 * so the struct has no internal padding.
 */
struct normfs_wal_entry_iter_result {
	size_t record_offset;
	size_t record_size;
	size_t consumed;
	uint64_t entry_id;
	uint32_t crc;
	int status;
};

/*@ axiomatic NormfsWalEntryV1 {
      logic integer normfs_wal_entry_v1_size_logic(integer record_size) =
        normfs_uintn_varint32_size_logic(record_size) + record_size +
        NORMFS_WAL_ENTRY_V1_CRC_SIZE;

      logic integer normfs_le32{L}(uint8_t *p) =
        p[0] + 256 * p[1] + 65536 * p[2] + 16777216 * p[3];

      // The record size varint and the record, before the CRC.
      logic integer normfs_wal_entry_v1_body{L}(uint8_t *p) =
        normfs_uintn_varint32_len(p) + normfs_uintn_varint32_value(p);

      // len bytes at p start with a whole entry: a canonical size, the
      // record and a CRC that matches them.
      predicate normfs_wal_entry_v1_whole{L}(uint8_t *p, integer len) =
        normfs_uintn_varint32_len(p) <= len &&
        normfs_uintn_varint32_canonical(p) &&
        normfs_wal_entry_v1_body(p) + NORMFS_WAL_ENTRY_V1_CRC_SIZE <= len &&
        normfs_le32(p + normfs_wal_entry_v1_body(p)) ==
          normfs_crc32c_logic(0, p, normfs_wal_entry_v1_body(p));
    }
*/

size_t normfs_wal_entry_v1_size(uint32_t record_size);

struct normfs_wal_entry_encode_result
normfs_wal_entry_v1_encode(const uint8_t *record, uint32_t record_size,
    uint8_t *out, size_t out_len);

/*
 * Decoding is zero copy: the record is reported as an offset and length
 * inside buf rather than being copied out.
 */
struct normfs_wal_entry_decode_result
normfs_wal_entry_v1_decode(const uint8_t *buf, size_t len);

struct normfs_wal_entry_id_result
normfs_wal_entry_v1_id(uint64_t num_entries_before, uint64_t index);

/*
 * Decode the entry at the front of buf and derive its id in one step. Rust
 * drives the loop: start with index 0, and after an NORMFS_WAL_ENTRY_OK
 * result advance the cursor by consumed and the index by one. Any non-OK
 * status stops the iteration (a corrupt or truncated entry means the ids of
 * every later entry are unknowable, since they are counted from the start of
 * the file).
 */
struct normfs_wal_entry_iter_result
normfs_wal_entry_v1_iter_next(const uint8_t *buf, size_t len,
    uint64_t num_entries_before, uint64_t index);

#endif /* NORMFS_WAL_ENTRY_H */
