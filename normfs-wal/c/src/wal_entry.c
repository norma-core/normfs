#include "normfs/wal_entry.h"

/*@ requires \valid_read(p + (0 .. 3));
    assigns \nothing;
    ensures \result == normfs_le32(p);
*/
static uint32_t
normfs_wal_entry_read_le32(const uint8_t *p)
{
	return (uint32_t)p[0] +
	    (uint32_t)p[1] * 256u +
	    (uint32_t)p[2] * 65536u +
	    (uint32_t)p[3] * 16777216u;
}

/*@ requires \valid(p + (0 .. 3));
    assigns p[0 .. 3];
    ensures normfs_le32(p) == value;
*/
static void
normfs_wal_entry_write_le32(uint8_t *p, uint32_t value)
{
	uint32_t b0 = value % 256u;
	uint32_t r1 = value / 256u;
	uint32_t b1 = r1 % 256u;
	uint32_t r2 = r1 / 256u;
	uint32_t b2 = r2 % 256u;
	uint32_t b3 = r2 / 256u;

	/*@ assert value == b0 + 256 * r1; */
	/*@ assert r1 == b1 + 256 * r2; */
	/*@ assert r2 == b2 + 256 * b3; */
	/*@ assert b3 < 256; */
	/*@ assert value == b0 + 256 * b1 + 65536 * b2 + 16777216 * b3; */

	p[0] = (uint8_t)b0;
	p[1] = (uint8_t)b1;
	p[2] = (uint8_t)b2;
	p[3] = (uint8_t)b3;
}

/*@ assigns \nothing;
    ensures \result == normfs_wal_entry_v1_size_logic(record_size);
    ensures NORMFS_WAL_ENTRY_V1_MIN_SIZE <= \result;
    ensures \result <= (size_t)record_size + NORMFS_WAL_ENTRY_V1_MAX_OVERHEAD;
*/
size_t
normfs_wal_entry_v1_size(uint32_t record_size)
{
	return normfs_uintn_varint32_size(record_size) + (size_t)record_size +
	    NORMFS_WAL_ENTRY_V1_CRC_SIZE;
}

/*@ requires record_size == 0 || \valid_read(record + (0 .. record_size - 1));
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    requires record_size == 0 || out_len == 0 ||
             \separated(out + (0 .. out_len - 1), record + (0 .. record_size - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_WAL_ENTRY_OK ||
            \result.status == NORMFS_WAL_ENTRY_ERR_NO_SPACE;
    ensures \result.status == NORMFS_WAL_ENTRY_OK <==>
              normfs_wal_entry_v1_size_logic(record_size) <= out_len;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.written == normfs_wal_entry_v1_size_logic(record_size) &&
              \result.written <= out_len &&
              NORMFS_WAL_ENTRY_V1_MIN_SIZE <= \result.written;
    // the length prefix is the canonical varint32 of record_size
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              normfs_uintn_varint32_value(out) == record_size &&
              normfs_uintn_varint32_len(out) ==
                normfs_uintn_varint32_size_logic(record_size);
    // the record bytes are copied verbatim, right after the length prefix
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \forall integer k; 0 <= k < record_size ==>
                out[normfs_uintn_varint32_size_logic(record_size) + k] ==
                  record[k];
    ensures \result.status != NORMFS_WAL_ENTRY_OK ==> \result.written == 0;
*/
struct normfs_wal_entry_encode_result
normfs_wal_entry_v1_encode(const uint8_t *record, uint32_t record_size,
    uint8_t *out, size_t out_len)
{
	struct normfs_wal_entry_encode_result r = {
	    0u,
	    NORMFS_WAL_ENTRY_ERR_NO_SPACE
	};
	struct normfs_uintn_varint_encode_result length;
	size_t total;
	size_t offset;
	size_t i;
	uint32_t crc;

	total = normfs_wal_entry_v1_size(record_size);
	if (out_len < total) return r;

	length = normfs_uintn_varint32_encode(record_size, out, out_len);
	offset = length.written;
	/*@ assert normfs_uintn_varint32_value(out) == record_size; */
	/*@ assert normfs_uintn_varint32_len(out) == offset; */
	/*@ assert offset == normfs_uintn_varint32_size_logic(record_size); */

	/*@ loop invariant 0 <= i <= record_size;
	    loop invariant \forall integer k; 0 <= k < i ==>
	                     out[offset + k] == record[k];
	    loop assigns i, out[offset .. offset + record_size - 1];
	    loop variant record_size - i;
	*/
	for (i = 0u; i < (size_t)record_size; i++) {
		out[offset + i] = record[i];
	}
	offset += (size_t)record_size;

	crc = normfs_crc32c(0u, out, offset);
	normfs_wal_entry_write_le32(out + offset, crc);
	/*@ assert normfs_le32(out + offset) == crc; */
	offset += NORMFS_WAL_ENTRY_V1_CRC_SIZE;

	r.written = offset;
	r.status = NORMFS_WAL_ENTRY_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ||
            \result.status == NORMFS_WAL_ENTRY_ERR_TRUNCATED ||
            \result.status == NORMFS_WAL_ENTRY_ERR_OVERFLOW ||
            \result.status == NORMFS_WAL_ENTRY_ERR_NON_CANONICAL ||
            \result.status == NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.record_offset ==
                normfs_uintn_varint32_size_logic(\result.record_size) &&
              1 <= \result.record_offset <= 5;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.consumed == \result.record_offset + \result.record_size +
                NORMFS_WAL_ENTRY_V1_CRC_SIZE &&
              \result.consumed <= len &&
              NORMFS_WAL_ENTRY_V1_MIN_SIZE <= \result.consumed;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.crc == normfs_crc32c_logic(0, buf,
                  \result.record_offset + \result.record_size) &&
              \result.crc == normfs_le32(buf + \result.consumed -
                  NORMFS_WAL_ENTRY_V1_CRC_SIZE);
    ensures \result.status == NORMFS_WAL_ENTRY_OK <==>
              normfs_wal_entry_v1_whole(buf, len);
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.consumed == normfs_wal_entry_v1_body(buf) +
                NORMFS_WAL_ENTRY_V1_CRC_SIZE;
    ensures \result.status != NORMFS_WAL_ENTRY_OK ==>
              \result.record_offset == 0 && \result.record_size == 0 &&
              \result.consumed == 0 && \result.crc == 0;
*/
struct normfs_wal_entry_decode_result
normfs_wal_entry_v1_decode(const uint8_t *buf, size_t len)
{
	struct normfs_wal_entry_decode_result r = {
	    0u,
	    0u,
	    0u,
	    0u,
	    NORMFS_WAL_ENTRY_ERR_TRUNCATED
	};
	struct normfs_uintn_varint32_decode_result length;
	size_t record_offset;
	size_t record_size;
	size_t consumed;
	uint32_t stored;
	uint32_t computed;

	length = normfs_uintn_varint32_decode(buf, len);
	if (length.status != NORMFS_UINTN_VARINT_OK) {
		r.status = length.status;
		return r;
	}
	record_offset = length.consumed;
	record_size = (size_t)length.value;

	if (len - record_offset < record_size) return r;
	if (len - record_offset - record_size < NORMFS_WAL_ENTRY_V1_CRC_SIZE)
		return r;

	consumed = record_offset + record_size + NORMFS_WAL_ENTRY_V1_CRC_SIZE;

	stored = normfs_wal_entry_read_le32(buf + record_offset + record_size);
	computed = normfs_crc32c(0u, buf, record_offset + record_size);
	if (stored != computed) {
		r.status = NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH;
		return r;
	}

	r.record_offset = record_offset;
	r.record_size = record_size;
	r.consumed = consumed;
	r.crc = computed;
	r.status = NORMFS_WAL_ENTRY_OK;
	return r;
}

/*@ assigns \nothing;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ||
            \result.status == NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW;
    ensures \result.status == NORMFS_WAL_ENTRY_OK <==>
              num_entries_before + index <= 0xFFFFFFFFFFFFFFFF;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.entry_id == num_entries_before + index;
    ensures \result.status != NORMFS_WAL_ENTRY_OK ==> \result.entry_id == 0;
*/
struct normfs_wal_entry_id_result
normfs_wal_entry_v1_id(uint64_t num_entries_before, uint64_t index)
{
	struct normfs_wal_entry_id_result r = {
	    0u,
	    NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW
	};

	if (index > 0xFFFFFFFFFFFFFFFFull - num_entries_before) return r;

	r.entry_id = num_entries_before + index;
	r.status = NORMFS_WAL_ENTRY_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ||
            \result.status == NORMFS_WAL_ENTRY_ERR_TRUNCATED ||
            \result.status == NORMFS_WAL_ENTRY_ERR_OVERFLOW ||
            \result.status == NORMFS_WAL_ENTRY_ERR_NON_CANONICAL ||
            \result.status == NORMFS_WAL_ENTRY_ERR_CRC_MISMATCH ||
            \result.status == NORMFS_WAL_ENTRY_ERR_ID_OVERFLOW;
    // framing: same guarantees the single-entry decode gives
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.record_offset ==
                normfs_uintn_varint32_size_logic(\result.record_size) &&
              1 <= \result.record_offset <= 5;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.consumed == \result.record_offset + \result.record_size +
                NORMFS_WAL_ENTRY_V1_CRC_SIZE &&
              \result.consumed <= len &&
              NORMFS_WAL_ENTRY_V1_MIN_SIZE <= \result.consumed;
    // the crc covers exactly [record_size varint][record bytes] and is stored
    // little-endian in the last NORMFS_WAL_ENTRY_V1_CRC_SIZE bytes
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.crc == normfs_crc32c_logic(0, buf,
                  \result.record_offset + \result.record_size) &&
              \result.crc == normfs_le32(buf + \result.consumed -
                  NORMFS_WAL_ENTRY_V1_CRC_SIZE);
    // the id is num_entries_before + index, and that sum did not wrap u64
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.entry_id == num_entries_before + index &&
              num_entries_before + index <= 0xFFFFFFFFFFFFFFFF;
    ensures \result.status == NORMFS_WAL_ENTRY_OK <==>
              normfs_wal_entry_v1_whole(buf, len) &&
              num_entries_before + index <= 0xFFFFFFFFFFFFFFFF;
    ensures \result.status == NORMFS_WAL_ENTRY_OK ==>
              \result.consumed == normfs_wal_entry_v1_body(buf) +
                NORMFS_WAL_ENTRY_V1_CRC_SIZE;
    ensures \result.status != NORMFS_WAL_ENTRY_OK ==>
              \result.record_offset == 0 && \result.record_size == 0 &&
              \result.consumed == 0 && \result.crc == 0 &&
              \result.entry_id == 0;
*/
struct normfs_wal_entry_iter_result
normfs_wal_entry_v1_iter_next(const uint8_t *buf, size_t len,
    uint64_t num_entries_before, uint64_t index)
{
	struct normfs_wal_entry_iter_result r = {
	    0u,
	    0u,
	    0u,
	    0u,
	    0u,
	    NORMFS_WAL_ENTRY_ERR_TRUNCATED
	};
	struct normfs_wal_entry_decode_result d;
	struct normfs_wal_entry_id_result id;

	d = normfs_wal_entry_v1_decode(buf, len);
	if (d.status != NORMFS_WAL_ENTRY_OK) {
		r.status = d.status;
		return r;
	}

	id = normfs_wal_entry_v1_id(num_entries_before, index);
	if (id.status != NORMFS_WAL_ENTRY_OK) {
		r.status = id.status;
		return r;
	}

	r.record_offset = d.record_offset;
	r.record_size = d.record_size;
	r.consumed = d.consumed;
	r.crc = d.crc;
	r.entry_id = id.entry_id;
	r.status = NORMFS_WAL_ENTRY_OK;
	return r;
}
