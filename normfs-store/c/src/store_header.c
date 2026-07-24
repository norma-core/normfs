#include "normfs/store_header.h"

/*@ axiomatic NormfsStoreHeaderV1Size {
      logic integer normfs_store_header_v1_size_logic(integer compression,
                                                      integer encryption,
                                                      integer entries_before,
                                                      integer entries) =
        1 +
        normfs_uintn_varint64_size_logic(compression) +
        normfs_uintn_varint64_size_logic(encryption) +
        normfs_uintn_varint64_size_logic(entries_before) +
        normfs_uintn_varint64_size_logic(entries);
    }
*/

/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == normfs_store_header_v1_size_logic(header->compression,
                                                         header->encryption,
                                                         header->num_entries_before,
                                                         header->num_entries);
    ensures NORMFS_STORE_HEADER_V1_MIN_SIZE <= \result <=
              NORMFS_STORE_HEADER_V1_MAX_SIZE;
*/
size_t
normfs_store_header_v1_size(const struct normfs_store_header_v1 *header)
{
	return normfs_uintn_varint64_size(NORMFS_STORE_HEADER_V1) +
	    normfs_uintn_varint64_size(header->compression) +
	    normfs_uintn_varint64_size(header->encryption) +
	    normfs_uintn_varint64_size(header->num_entries_before) +
	    normfs_uintn_varint64_size(header->num_entries);
}

/*@ requires \valid_read(header);
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_NO_SPACE;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.written ==
                normfs_store_header_v1_size_logic(header->compression,
                                                  header->encryption,
                                                  header->num_entries_before,
                                                  header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              NORMFS_STORE_HEADER_V1_MIN_SIZE <= \result.written <=
                NORMFS_STORE_HEADER_V1_MAX_SIZE;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==> \result.written <= out_len;
    ensures \result.status != NORMFS_STORE_HEADER_OK ==> \result.written == 0;
*/
struct normfs_store_header_encode_result
normfs_store_header_v1_encode(const struct normfs_store_header_v1 *header,
    uint8_t *out, size_t out_len)
{
	struct normfs_store_header_encode_result r = {
	    0u,
	    NORMFS_STORE_HEADER_ERR_NO_SPACE
	};
	struct normfs_uintn_varint_encode_result field;
	size_t offset;

	field = normfs_uintn_varint64_encode(NORMFS_STORE_HEADER_V1, out, out_len);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset = field.written;

	field = normfs_uintn_varint64_encode(header->compression,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	field = normfs_uintn_varint64_encode(header->encryption,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	field = normfs_uintn_varint64_encode(header->num_entries_before,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	field = normfs_uintn_varint64_encode(header->num_entries,
	    out + offset, out_len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	offset += field.written;

	r.written = offset;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_STORE_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_STORE_HEADER_ERR_NON_CANONICAL ||
            \result.status == NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              NORMFS_STORE_HEADER_V1_MIN_SIZE <= \result.consumed <=
                NORMFS_STORE_HEADER_V1_MAX_SIZE &&
              \result.consumed <= len;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.consumed ==
                normfs_store_header_v1_size_logic(\result.header.compression,
                                                  \result.header.encryption,
                                                  \result.header.num_entries_before,
                                                  \result.header.num_entries);
    ensures \result.status != NORMFS_STORE_HEADER_OK ==>
              \result.consumed == 0 &&
              \result.header.compression == 0 &&
              \result.header.encryption == 0 &&
              \result.header.num_entries_before == 0 &&
              \result.header.num_entries == 0;
*/
struct normfs_store_header_decode_result
normfs_store_header_v1_decode(const uint8_t *buf, size_t len)
{
	struct normfs_store_header_decode_result r = {
	    {0u, 0u, 0u, 0u},
	    0u,
	    NORMFS_STORE_HEADER_ERR_TRUNCATED
	};
	struct normfs_uintn_varint64_decode_result field;
	struct normfs_store_header_v1 header = {0u, 0u, 0u, 0u};
	size_t offset;

	field = normfs_uintn_varint64_decode(buf, len);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	if (field.value != NORMFS_STORE_HEADER_V1) {
		r.status = NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION;
		return r;
	}
	offset = field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.compression = field.value;
	offset += field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.encryption = field.value;
	offset += field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.num_entries_before = field.value;
	offset += field.consumed;

	field = normfs_uintn_varint64_decode(buf + offset, len - offset);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}
	header.num_entries = field.value;
	offset += field.consumed;

	r.header = header;
	r.consumed = offset;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_STORE_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_STORE_HEADER_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==> \result.consumed <= len;
    ensures \result.status == NORMFS_STORE_HEADER_OK && \result.version == 0 ==>
              \result.consumed == 0;
    ensures \result.status == NORMFS_STORE_HEADER_OK && \result.version != 0 ==>
              1 <= \result.consumed <= 10;
    ensures \result.status != NORMFS_STORE_HEADER_OK ==>
              \result.version == 0 && \result.consumed == 0;
*/
struct normfs_store_header_version_result
normfs_store_header_peek_version(const uint8_t *buf, size_t len)
{
	struct normfs_store_header_version_result r = {
	    0u,
	    0u,
	    NORMFS_STORE_HEADER_ERR_TRUNCATED
	};
	struct normfs_uintn_varint64_decode_result field;

	if (len < 1u) return r;

	if (buf[0] == 0u) {
		r.status = NORMFS_STORE_HEADER_OK;
		return r;
	}

	field = normfs_uintn_varint64_decode(buf, len);
	if (field.status != NORMFS_UINTN_VARINT_OK) {
		r.status = field.status;
		return r;
	}

	r.version = field.value;
	r.consumed = field.consumed;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}
