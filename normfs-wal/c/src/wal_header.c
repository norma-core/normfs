#include "normfs/wal_header.h"

/*@ axiomatic NormfsWalHeaderV1Size {
      logic integer normfs_wal_header_v1_size_logic(integer data_size,
                                                    integer id_size,
                                                    integer entries) =
        1 +
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
    ensures NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result <= 31;
    ensures (normfs_wal_header_valid_field_size(header->data_size_bytes) &&
             normfs_wal_header_valid_field_size(header->id_size_bytes)) ==>
              \result <= NORMFS_WAL_HEADER_V1_MAX_SIZE;
*/
size_t
normfs_wal_header_v1_size(const struct normfs_wal_header_v1 *header)
{
	return normfs_uintn_varint64_size(NORMFS_WAL_HEADER_V1) +
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
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              \result.written ==
                normfs_wal_header_v1_size_logic(header->data_size_bytes,
                                                header->id_size_bytes,
                                                header->num_entries_before);
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result.written <=
                NORMFS_WAL_HEADER_V1_MAX_SIZE;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==> \result.written <= out_len;
    ensures \result.status != NORMFS_WAL_HEADER_OK ==> \result.written == 0;
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

	field = normfs_uintn_varint64_encode(NORMFS_WAL_HEADER_V1, out, out_len);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset = field.written;

	field = normfs_uintn_varint64_encode(header->data_size_bytes,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	field = normfs_uintn_varint64_encode(header->id_size_bytes,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

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
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              NORMFS_WAL_HEADER_V1_MIN_SIZE <= \result.consumed <=
                NORMFS_WAL_HEADER_V1_MAX_SIZE &&
              \result.consumed <= len;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              \result.consumed ==
                normfs_wal_header_v1_size_logic(\result.header.data_size_bytes,
                                                \result.header.id_size_bytes,
                                                \result.header.num_entries_before);
    ensures \result.status == NORMFS_WAL_HEADER_OK ==>
              normfs_wal_header_valid_field_size(\result.header.data_size_bytes) &&
              normfs_wal_header_valid_field_size(\result.header.id_size_bytes);
    ensures \result.status != NORMFS_WAL_HEADER_OK ==> \result.consumed == 0;
    ensures \result.status == NORMFS_WAL_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_WAL_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_WAL_HEADER_ERR_NON_CANONICAL ||
            \result.status == NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION ==>
              \result.header.data_size_bytes == 0 &&
              \result.header.id_size_bytes == 0 &&
              \result.header.num_entries_before == 0;
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
	    NORMFS_WAL_HEADER_ERR_TRUNCATED
	};
	struct normfs_uintn_varint64_decode_result field;
	struct normfs_wal_header_v1 header = {0u, 0u, 0u};
	size_t offset;
	int valid;

	field = normfs_uintn_varint64_decode(buf, len);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	if (field.value != NORMFS_WAL_HEADER_V1) {
		r.status = NORMFS_WAL_HEADER_ERR_UNSUPPORTED_VERSION;
		return r;
	}
	offset = field.consumed;

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

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_WAL_HEADER_OK ||
            \result.status == NORMFS_WAL_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_WAL_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_WAL_HEADER_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_WAL_HEADER_OK ==> \result.consumed <= len;
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.version == 0 ==>
              \result.consumed == 0;
    ensures \result.status == NORMFS_WAL_HEADER_OK && \result.version != 0 ==>
              1 <= \result.consumed <= 10;
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
	struct normfs_uintn_varint64_decode_result field;

	if (len < 1u) return r;

	if (buf[0] == 0u) {
		r.status = NORMFS_WAL_HEADER_OK;
		return r;
	}

	field = normfs_uintn_varint64_decode(buf, len);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}

	r.version = field.value;
	r.consumed = field.consumed;
	r.status = NORMFS_WAL_HEADER_OK;
	return r;
}
