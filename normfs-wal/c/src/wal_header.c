#include "normfs/wal_header.h"

/*@ axiomatic NormfsWalHeaderV1Size {
      logic integer normfs_wal_header_v1_size_logic(integer data_size,
                                                    integer id_size,
                                                    integer entries) =
        NORMFS_WAL_HEADER_VERSION_SIZE +
        normfs_uintn_varint64_size_logic(data_size) +
        normfs_uintn_varint64_size_logic(id_size) +
        normfs_uintn_varint64_size_logic(entries);

      predicate normfs_wal_header_valid_field_size(integer size) =
        size == 1 || size == 2 || size == 4 || size == 8 || size == 16;
    }
*/

/*@ assigns \nothing;
    ensures \result != 0 <==> normfs_wal_header_valid_field_size(size);
*/
static int
normfs_wal_header_field_size_is_valid(uint64_t size)
{
	return size == 1ull || size == 2ull || size == 4ull ||
	    size == 8ull || size == 16ull;
}

/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == NORMFS_WAL_HEADER_OK ||
            \result == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE ||
            \result == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE;
    ensures \result == NORMFS_WAL_HEADER_OK <==>
              (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
               normfs_wal_header_valid_field_size(header->id_size_bytes));
    ensures \result == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE <==>
              !normfs_wal_header_valid_field_size(header->data_size_bytes);
    ensures \result == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE <==>
              (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
               !normfs_wal_header_valid_field_size(header->id_size_bytes));
*/
int
normfs_wal_header_v1_validate(const struct normfs_wal_header_v1 *header)
{
	if (!normfs_wal_header_field_size_is_valid(header->data_size_bytes))
		return NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE;
	if (!normfs_wal_header_field_size_is_valid(header->id_size_bytes))
		return NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE;
	return NORMFS_WAL_HEADER_OK;
}

/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == normfs_wal_header_v1_size_logic(header->data_size_bytes,
                                                       header->id_size_bytes,
                                                       header->num_entries_before);
    ensures NORMFS_WAL_HEADER_VERSION_SIZE + 3 <= \result <= 38;
    ensures (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
             normfs_wal_header_valid_field_size(header->id_size_bytes)) ==>
              NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result <=
                NORMFS_WAL_HEADER_V1_MAX_SIZE;
*/
size_t
normfs_wal_header_v1_size(const struct normfs_wal_header_v1 *header)
{
	return NORMFS_WAL_HEADER_VERSION_SIZE +
	    normfs_uintn_varint64_size(header->data_size_bytes) +
	    normfs_uintn_varint64_size(header->id_size_bytes) +
	    normfs_uintn_varint64_size(header->num_entries_before);
}

/*@ requires \valid_read(header);
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_WAL_HEADER_OK ||
            \result.status == NORMFS_WAL_HEADER_ERR_NO_SPACE ||
            \result.status == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE ||
            \result.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE;
    ensures \result.status != NORMFS_WAL_HEADER_OK ==> \result.written == 0;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              normfs_wal_header_valid_field_size(header->data_size_bytes) &&
              normfs_wal_header_valid_field_size(header->id_size_bytes);
    ensures (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
             normfs_wal_header_valid_field_size(header->id_size_bytes) &&
             normfs_wal_header_v1_size_logic(header->data_size_bytes,
                                             header->id_size_bytes,
                                             header->num_entries_before) <= out_len) ==>
              \result.status == NORMFS_WAL_HEADER_OK;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              \result.written ==
                normfs_wal_header_v1_size_logic(header->data_size_bytes,
                                                header->id_size_bytes,
                                                header->num_entries_before) &&
              NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result.written <=
                NORMFS_WAL_HEADER_V1_MAX_SIZE &&
              \result.written <= out_len;

    // version word: u64 little endian, always 1
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
              out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0;

    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              out[8] == header->data_size_bytes &&
              out[9] == header->id_size_bytes;

    // num_entries_before decodes back to itself: enough for the round trip proof
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              normfs_uintn_varint64_value(out + 10) == header->num_entries_before &&
              \result.written == 10 + normfs_uintn_varint64_len(out + 10);

    // num_entries_before, canonical varint, one clause per encoded width
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            header->num_entries_before < 0x80 ==>
              \result.written == 11 &&
              out[10] == header->num_entries_before;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x80 <= header->num_entries_before < 0x4000 ==>
              \result.written == 12 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == header->num_entries_before / 128;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x4000 <= header->num_entries_before < 0x200000 ==>
              \result.written == 13 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == header->num_entries_before / 0x4000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x200000 <= header->num_entries_before < 0x10000000 ==>
              \result.written == 14 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == header->num_entries_before / 0x200000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x10000000 <= header->num_entries_before < 0x800000000 ==>
              \result.written == 15 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == header->num_entries_before / 0x10000000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x800000000 <= header->num_entries_before < 0x40000000000 ==>
              \result.written == 16 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == header->num_entries_before / 0x800000000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x40000000000 <= header->num_entries_before < 0x2000000000000 ==>
              \result.written == 17 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == header->num_entries_before / 0x40000000000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x2000000000000 <= header->num_entries_before < 0x100000000000000 ==>
              \result.written == 18 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == header->num_entries_before / 0x2000000000000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x100000000000000 <= header->num_entries_before < 0x8000000000000000 ==>
              \result.written == 19 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == 128 + (header->num_entries_before / 0x2000000000000) % 128 &&
              out[18] == header->num_entries_before / 0x100000000000000;
    ensures \result.status == NORMFS_WAL_HEADER_OK &&
            0x8000000000000000 <= header->num_entries_before ==>
              \result.written == 20 &&
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 128) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == 128 + (header->num_entries_before / 0x2000000000000) % 128 &&
              out[18] == 128 + (header->num_entries_before / 0x100000000000000) % 128 &&
              out[19] == header->num_entries_before / 0x8000000000000000;
*/
struct normfs_wal_header_encode_result
normfs_wal_header_v1_encode(const struct normfs_wal_header_v1 *header,
    uint8_t *out, size_t out_len)
{
	struct normfs_wal_header_encode_result r = {
	    0u,
	    NORMFS_WAL_HEADER_ERR_NO_SPACE
	};
	struct normfs_uintn_varint_encode_result field;
	size_t offset;
	int valid;

	valid = normfs_wal_header_v1_validate(header);
	if (valid != NORMFS_WAL_HEADER_OK) {
		r.status = valid;
		return r;
	}

	if (out_len < NORMFS_WAL_HEADER_VERSION_SIZE) return r;
	normfs_uintn_le64_write(out, NORMFS_WAL_HEADER_V1);
	offset = NORMFS_WAL_HEADER_VERSION_SIZE;

	field = normfs_uintn_varint64_encode(header->data_size_bytes,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	/*@ assert field.written == 1; */
	offset += field.written;

	field = normfs_uintn_varint64_encode(header->id_size_bytes,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	/*@ assert field.written == 1; */
	offset += field.written;
	/*@ assert offset == 10; */

	field = normfs_uintn_varint64_encode(header->num_entries_before,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	r.written = offset;
	r.status = NORMFS_WAL_HEADER_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_WAL_HEADER_OK ||
            \result.status == NORMFS_WAL_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_WAL_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_WAL_HEADER_ERR_NON_CANONICAL ||
            \result.status == NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION ||
            \result.status == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE ||
            \result.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE;
    ensures \result.status != NORMFS_WAL_HEADER_OK ==> \result.consumed == 0;
    ensures len >= NORMFS_WAL_HEADER_VERSION_SIZE ==>
              \result.version == normfs_uintn_le64_logic(buf);
    ensures len < NORMFS_WAL_HEADER_VERSION_SIZE ==>
              \result.status == NORMFS_WAL_HEADER_ERR_TRUNCATED &&
              \result.version == 0;
    ensures \result.status == NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION <==>
              (len >= NORMFS_WAL_HEADER_VERSION_SIZE &&
               normfs_uintn_le64_logic(buf) != NORMFS_WAL_HEADER_V1);

    // completeness: a well formed V1 encoding always decodes
    ensures (normfs_uintn_le64_logic(buf) == NORMFS_WAL_HEADER_V1 &&
             normfs_wal_header_valid_field_size(buf[8]) &&
             normfs_wal_header_valid_field_size(buf[9]) &&
             normfs_uintn_varint64_canonical(buf + 10) &&
             10 + normfs_uintn_varint64_len(buf + 10) <= len) ==>
              \result.status == NORMFS_WAL_HEADER_OK;

    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              normfs_uintn_le64_logic(buf) == NORMFS_WAL_HEADER_V1 &&
              \result.header.data_size_bytes == buf[8] &&
              \result.header.id_size_bytes == buf[9] &&
              normfs_wal_header_valid_field_size(buf[8]) &&
              normfs_wal_header_valid_field_size(buf[9]) &&
              NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result.consumed <=
                NORMFS_WAL_HEADER_V1_MAX_SIZE &&
              \result.consumed <= len;

    // num_entries_before read back through the same logic the encoder pins
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              \result.header.num_entries_before ==
                normfs_uintn_varint64_value(buf + 10) &&
              \result.consumed == 10 + normfs_uintn_varint64_len(buf + 10);

    // num_entries_before, reconstructed byte by byte from offset 10
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 11 ==>
              buf[10] < 128 &&
              \result.header.num_entries_before == buf[10];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 12 ==>
              buf[10] >= 128 && buf[11] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * buf[11];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 13 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) + 16384 * buf[12];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 14 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * buf[13];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 15 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * buf[14];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 16 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] >= 128 && buf[15] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * (buf[14] - 128) + 34359738368 * buf[15];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 17 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] >= 128 && buf[15] >= 128 &&
              buf[16] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * (buf[14] - 128) + 34359738368 * (buf[15] - 128) +
                4398046511104 * buf[16];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 18 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] >= 128 && buf[15] >= 128 &&
              buf[16] >= 128 && buf[17] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * (buf[14] - 128) + 34359738368 * (buf[15] - 128) +
                4398046511104 * (buf[16] - 128) + 562949953421312 * buf[17];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 19 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] >= 128 && buf[15] >= 128 &&
              buf[16] >= 128 && buf[17] >= 128 && buf[18] < 128 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * (buf[14] - 128) + 34359738368 * (buf[15] - 128) +
                4398046511104 * (buf[16] - 128) +
                562949953421312 * (buf[17] - 128) +
                72057594037927936 * buf[18];
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.consumed == 20 ==>
              buf[10] >= 128 && buf[11] >= 128 && buf[12] >= 128 &&
              buf[13] >= 128 && buf[14] >= 128 && buf[15] >= 128 &&
              buf[16] >= 128 && buf[17] >= 128 && buf[18] >= 128 &&
              buf[19] < 2 &&
              \result.header.num_entries_before ==
                (buf[10] - 128) + 128 * (buf[11] - 128) +
                16384 * (buf[12] - 128) + 2097152 * (buf[13] - 128) +
                268435456 * (buf[14] - 128) + 34359738368 * (buf[15] - 128) +
                4398046511104 * (buf[16] - 128) +
                562949953421312 * (buf[17] - 128) +
                72057594037927936 * (buf[18] - 128) +
                9223372036854775808 * buf[19];

    ensures \result.status == NORMFS_WAL_HEADER_ERR_INVALID_DATA_SIZE ==>
              !normfs_wal_header_valid_field_size(\result.header.data_size_bytes);
    ensures \result.status == NORMFS_WAL_HEADER_ERR_INVALID_ID_SIZE ==>
              normfs_wal_header_valid_field_size(\result.header.data_size_bytes) &&
              !normfs_wal_header_valid_field_size(\result.header.id_size_bytes);
*/
struct normfs_wal_header_decode_result
normfs_wal_header_v1_decode(const uint8_t *buf, size_t len)
{
	struct normfs_wal_header_decode_result r = {
	    {0u, 0u, 0u},
	    0u,
	    0u,
	    NORMFS_WAL_HEADER_ERR_TRUNCATED
	};
	struct normfs_uintn_varint64_decode_result field;
	struct normfs_wal_header_v1 header = {0u, 0u, 0u};
	size_t offset;
	int valid;

	if (len < NORMFS_WAL_HEADER_VERSION_SIZE) return r;

	r.version = normfs_uintn_le64_read(buf);
	if (r.version != NORMFS_WAL_HEADER_V1) {
		r.status = NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION;
		return r;
	}
	offset = NORMFS_WAL_HEADER_VERSION_SIZE;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.data_size_bytes = field.value;
	offset += field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.id_size_bytes = field.value;
	offset += field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.num_entries_before = field.value;
	offset += field.consumed;

	valid = normfs_wal_header_v1_validate(&header);
	if (valid != NORMFS_WAL_HEADER_OK) {
		r.header = header;
		r.status = valid;
		return r;
	}

	r.header = header;
	r.consumed = offset;
	r.status = NORMFS_WAL_HEADER_OK;
	return r;
}

/*
 * Round trip: for any header with valid field sizes, decoding its encoding
 * recovers the header exactly and consumes exactly what was written. The proof
 * is the body: WP discharges the asserts, so \result == 1 is not a runtime
 * check but a theorem about every valid header.
 */
/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == 1 <==>
              (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
               normfs_wal_header_valid_field_size(header->id_size_bytes));
*/
int
normfs_wal_header_v1_roundtrip_holds(const struct normfs_wal_header_v1 *header)
{
	uint8_t buf[NORMFS_WAL_HEADER_V1_MAX_SIZE];
	struct normfs_wal_header_encode_result enc;
	struct normfs_wal_header_decode_result dec;

	enc = normfs_wal_header_v1_encode(header, buf, sizeof(buf));
	if (enc.status != NORMFS_WAL_HEADER_OK) return 0;

	/* The encoding is a valid V1 header, so the decoder must accept it. */
	/*@ assert normfs_uintn_le64_logic(&buf[0]) == NORMFS_WAL_HEADER_V1; */
	/*@ assert normfs_wal_header_valid_field_size(buf[8]); */
	/*@ assert normfs_wal_header_valid_field_size(buf[9]); */
	/*@ assert normfs_uintn_varint64_value(&buf[10]) ==
	             header->num_entries_before; */
	/*@ assert enc.written == 10 + normfs_uintn_varint64_len(&buf[10]); */

	dec = normfs_wal_header_v1_decode(buf, enc.written);

	/* Bitwise so every field is compared with no short-circuit branch. */
	int ok = (dec.status == NORMFS_WAL_HEADER_OK);
	ok &= (dec.consumed == enc.written);
	ok &= (dec.header.data_size_bytes == header->data_size_bytes);
	ok &= (dec.header.id_size_bytes == header->id_size_bytes);
	ok &= (dec.header.num_entries_before == header->num_entries_before);
	return ok;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_WAL_HEADER_OK ||
            \result.status == NORMFS_WAL_HEADER_ERR_TRUNCATED;
    ensures \result.status == NORMFS_WAL_HEADER_OK <==>
              len >= NORMFS_WAL_HEADER_VERSION_SIZE;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              \result.version == normfs_uintn_le64_logic(buf) &&
              \result.consumed == NORMFS_WAL_HEADER_VERSION_SIZE;
    ensures \result.status != NORMFS_WAL_HEADER_OK ==>
              \result.version == 0 && \result.consumed == 0;
*/
struct normfs_wal_header_version_result
normfs_wal_header_peek_version(const uint8_t *buf, size_t len)
{
	struct normfs_wal_header_version_result r = {
	    0u,
	    0u,
	    NORMFS_WAL_HEADER_ERR_TRUNCATED
	};

	if (len < NORMFS_WAL_HEADER_VERSION_SIZE) return r;

	r.version = normfs_uintn_le64_read(buf);
	r.consumed = NORMFS_WAL_HEADER_VERSION_SIZE;
	r.status = NORMFS_WAL_HEADER_OK;
	return r;
}
