#include "normfs/store_header.h"

/*@ axiomatic NormfsStoreHeaderV1Size {
      logic integer normfs_store_header_v1_size_logic(integer compression,
                                                      integer encryption,
                                                      integer entries_before,
                                                      integer entries) =
        NORMFS_STORE_HEADER_VERSION_SIZE +
        normfs_uintn_varint64_size_logic(compression) +
        normfs_uintn_varint64_size_logic(encryption) +
        normfs_uintn_varint64_size_logic(entries_before) +
        normfs_uintn_varint64_size_logic(entries);

      predicate normfs_store_header_valid_compression(integer c) =
        0 <= c <= NORMFS_STORE_COMPRESSION_MAX;

      predicate normfs_store_header_valid_encryption(integer e) =
        0 <= e <= NORMFS_STORE_ENCRYPTION_MAX;
    }
*/

/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == NORMFS_STORE_HEADER_OK ||
            \result == NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION ||
            \result == NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION;
    ensures \result == NORMFS_STORE_HEADER_OK <==>
              (normfs_store_header_valid_compression(header->compression) &&
               normfs_store_header_valid_encryption(header->encryption));
    ensures \result == NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION <==>
              !normfs_store_header_valid_compression(header->compression);
    ensures \result == NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION <==>
              (normfs_store_header_valid_compression(header->compression) &&
               !normfs_store_header_valid_encryption(header->encryption));
*/
int
normfs_store_header_v1_validate(const struct normfs_store_header_v1 *header)
{
	if (header->compression > (uint64_t)NORMFS_STORE_COMPRESSION_MAX)
		return NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION;
	if (header->encryption > (uint64_t)NORMFS_STORE_ENCRYPTION_MAX)
		return NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION;
	return NORMFS_STORE_HEADER_OK;
}

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
	return NORMFS_STORE_HEADER_VERSION_SIZE +
	    normfs_uintn_varint64_size(header->compression) +
	    normfs_uintn_varint64_size(header->encryption) +
	    normfs_uintn_varint64_size(header->num_entries_before) +
	    normfs_uintn_varint64_size(header->num_entries);
}

/*@ requires \valid_read(header);
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_NO_SPACE ||
            \result.status == NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION ||
            \result.status == NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION;
    ensures \result.status != NORMFS_STORE_HEADER_OK ==> \result.written == 0;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              normfs_store_header_valid_compression(header->compression) &&
              normfs_store_header_valid_encryption(header->encryption);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.written ==
                normfs_store_header_v1_size_logic(header->compression,
                                                  header->encryption,
                                                  header->num_entries_before,
                                                  header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              NORMFS_STORE_HEADER_V1_MIN_SIZE <= \result.written <=
                NORMFS_STORE_HEADER_V1_MAX_SIZE;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.written <= out_len;

    // version word: u64 little endian, always 1
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
              out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0;

    // Both type codes are validated below 128, so each is a single varint byte
    // spelling itself and everything after it sits at a constant offset. Stated
    // as plain bytes rather than quantified over widths, the way the WAL header
    // states its two size fields.
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              out[8] == header->compression &&
              out[9] == header->encryption;

    // num_entries_before, canonical varint, one clause per encoded width, and
    // num_entries read off the offset that width fixes. Enumerating the widths
    // is what the varint encoder's own contract does, and what lets the byte
    // level facts survive the write that follows them.
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            header->num_entries_before < 0x80 ==>
              out[10] == header->num_entries_before &&
              normfs_uintn_varint64_value(out + 11) == header->num_entries &&
              normfs_uintn_varint64_len(out + 11) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 11 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x80 <= header->num_entries_before < 0x4000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == header->num_entries_before / 0x80 &&
              normfs_uintn_varint64_value(out + 12) == header->num_entries &&
              normfs_uintn_varint64_len(out + 12) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 12 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x4000 <= header->num_entries_before < 0x200000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == header->num_entries_before / 0x4000 &&
              normfs_uintn_varint64_value(out + 13) == header->num_entries &&
              normfs_uintn_varint64_len(out + 13) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 13 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x200000 <= header->num_entries_before < 0x10000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == header->num_entries_before / 0x200000 &&
              normfs_uintn_varint64_value(out + 14) == header->num_entries &&
              normfs_uintn_varint64_len(out + 14) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 14 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x10000000 <= header->num_entries_before < 0x800000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == header->num_entries_before / 0x10000000 &&
              normfs_uintn_varint64_value(out + 15) == header->num_entries &&
              normfs_uintn_varint64_len(out + 15) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 15 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x800000000 <= header->num_entries_before < 0x40000000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == header->num_entries_before / 0x800000000 &&
              normfs_uintn_varint64_value(out + 16) == header->num_entries &&
              normfs_uintn_varint64_len(out + 16) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 16 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x40000000000 <= header->num_entries_before < 0x2000000000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == header->num_entries_before / 0x40000000000 &&
              normfs_uintn_varint64_value(out + 17) == header->num_entries &&
              normfs_uintn_varint64_len(out + 17) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 17 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x2000000000000 <= header->num_entries_before < 0x100000000000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == header->num_entries_before / 0x2000000000000 &&
              normfs_uintn_varint64_value(out + 18) == header->num_entries &&
              normfs_uintn_varint64_len(out + 18) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 18 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x100000000000000 <= header->num_entries_before < 0x8000000000000000 ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == 128 + (header->num_entries_before / 0x2000000000000) % 128 &&
              out[18] == header->num_entries_before / 0x100000000000000 &&
              normfs_uintn_varint64_value(out + 19) == header->num_entries &&
              normfs_uintn_varint64_len(out + 19) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 19 +
                normfs_uintn_varint64_size_logic(header->num_entries);
    ensures \result.status == NORMFS_STORE_HEADER_OK &&
            0x8000000000000000 <= header->num_entries_before ==>
              out[10] == 128 + header->num_entries_before % 128 &&
              out[11] == 128 + (header->num_entries_before / 0x80) % 128 &&
              out[12] == 128 + (header->num_entries_before / 0x4000) % 128 &&
              out[13] == 128 + (header->num_entries_before / 0x200000) % 128 &&
              out[14] == 128 + (header->num_entries_before / 0x10000000) % 128 &&
              out[15] == 128 + (header->num_entries_before / 0x800000000) % 128 &&
              out[16] == 128 + (header->num_entries_before / 0x40000000000) % 128 &&
              out[17] == 128 + (header->num_entries_before / 0x2000000000000) % 128 &&
              out[18] == 128 + (header->num_entries_before / 0x100000000000000) % 128 &&
              out[19] == header->num_entries_before / 0x8000000000000000 &&
              normfs_uintn_varint64_value(out + 20) == header->num_entries &&
              normfs_uintn_varint64_len(out + 20) ==
                normfs_uintn_varint64_size_logic(header->num_entries) &&
              \result.written == 20 +
                normfs_uintn_varint64_size_logic(header->num_entries);
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
	size_t off_compression = NORMFS_STORE_HEADER_VERSION_SIZE;
	size_t off_encryption;
	size_t off_entries_before;
	size_t off_entries;
	size_t end;
	int valid;

	valid = normfs_store_header_v1_validate(header);
	if (valid != NORMFS_STORE_HEADER_OK) {
		r.status = valid;
		return r;
	}

	if (out_len < NORMFS_STORE_HEADER_VERSION_SIZE) return r;
	normfs_uintn_le64_write(out, NORMFS_STORE_HEADER_V1);
	/*@ assert out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
	             out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0; */

	field = normfs_uintn_varint64_encode(header->compression,
	    out + off_compression, out_len - off_compression);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_encryption = off_compression + field.written;
	/* compression < 128, so one byte spelling itself, and off_encryption == 9. */
	/*@ assert normfs_uintn_varint64_size_logic(header->compression) == 1; */
	/*@ assert off_encryption == 9; */
	/*@ assert out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
	             out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0; */
	/*@ assert out[8] == header->compression; */

	field = normfs_uintn_varint64_encode(header->encryption,
	    out + off_encryption, out_len - off_encryption);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_entries_before = off_encryption + field.written;
	/*@ assert normfs_uintn_varint64_size_logic(header->encryption) == 1; */
	/*@ assert off_entries_before == 10; */
	/*@ assert out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
	             out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0; */
	/*@ assert out[8] == header->compression; */
	/*@ assert out[9] == header->encryption; */

	field = normfs_uintn_varint64_encode(header->num_entries_before,
	    out + off_entries_before, out_len - off_entries_before);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_entries = off_entries_before + field.written;
	/*@ assert normfs_uintn_varint64_value(out + 10) ==
	             header->num_entries_before; */
	/*@ assert normfs_uintn_varint64_len(out + 10) ==
	             normfs_uintn_varint64_size_logic(header->num_entries_before); */
	/*@ assert off_entries == 10 + normfs_uintn_varint64_len(out + 10); */
	/*@ assert out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
	             out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0; */
	/*@ assert out[8] == header->compression; */
	/*@ assert out[9] == header->encryption; */
	/*
	 * The last byte of a canonical varint is below 128. That is what makes
	 * normfs_uintn_varint64_len(out + 10) depend only on the bytes of this
	 * field, so the write that follows cannot change it.
	 */
	/*@ assert out[off_entries - 1] < 128; */
	/*@ assert normfs_uintn_varint64_canonical(out + 10); */

	field = normfs_uintn_varint64_encode(header->num_entries,
	    out + off_entries, out_len - off_entries);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	end = off_entries + field.written;
	/*@ assert end ==
	             normfs_store_header_v1_size_logic(header->compression,
	                                               header->encryption,
	                                               header->num_entries_before,
	                                               header->num_entries); */
	/*@ assert NORMFS_STORE_HEADER_V1_MIN_SIZE <= end <=
	             NORMFS_STORE_HEADER_V1_MAX_SIZE; */
	/*@ assert normfs_uintn_varint64_value(out + off_entries) ==
	             header->num_entries; */
	/*@ assert normfs_uintn_varint64_len(out + off_entries) ==
	             normfs_uintn_varint64_size_logic(header->num_entries); */

	/*@ assert out[0] == 1 && out[1] == 0 && out[2] == 0 && out[3] == 0 &&
	             out[4] == 0 && out[5] == 0 && out[6] == 0 && out[7] == 0; */
	/*@ assert out[8] == header->compression; */
	/*@ assert out[9] == header->encryption; */
	r.written = end;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_TRUNCATED ||
            \result.status == NORMFS_STORE_HEADER_ERR_OVERFLOW ||
            \result.status == NORMFS_STORE_HEADER_ERR_NON_CANONICAL ||
            \result.status == NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION ||
            \result.status == NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION ||
            \result.status == NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION;
    ensures \result.status != NORMFS_STORE_HEADER_OK ==> \result.consumed == 0;
    ensures (\result.status != NORMFS_STORE_HEADER_OK &&
             \result.status != NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION &&
             \result.status != NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION) ==>
              \result.header.compression == 0 &&
              \result.header.encryption == 0 &&
              \result.header.num_entries_before == 0 &&
              \result.header.num_entries == 0;
    ensures len >= NORMFS_STORE_HEADER_VERSION_SIZE ==>
              \result.version == normfs_uintn_le64_logic(buf);
    ensures len < NORMFS_STORE_HEADER_VERSION_SIZE ==>
              \result.status == NORMFS_STORE_HEADER_ERR_TRUNCATED &&
              \result.version == 0;
    ensures \result.status == NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION <==>
              (len >= NORMFS_STORE_HEADER_VERSION_SIZE &&
               normfs_uintn_le64_logic(buf) != NORMFS_STORE_HEADER_V1);

    // completeness: a well formed V1 encoding always decodes. Without this the
    // round trip cannot conclude that decode accepted what encode produced.
    ensures (normfs_uintn_le64_logic(buf) == NORMFS_STORE_HEADER_V1 &&
             normfs_store_header_valid_compression(buf[8]) &&
             normfs_store_header_valid_encryption(buf[9]) &&
             normfs_uintn_varint64_canonical(buf + 10) &&
             normfs_uintn_varint64_canonical(
               buf + 10 + normfs_uintn_varint64_len(buf + 10)) &&
             10 + normfs_uintn_varint64_len(buf + 10) +
               normfs_uintn_varint64_len(
                 buf + 10 + normfs_uintn_varint64_len(buf + 10)) <= len) ==>
              \result.status == NORMFS_STORE_HEADER_OK;

    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              normfs_store_header_valid_compression(\result.header.compression) &&
              normfs_store_header_valid_encryption(\result.header.encryption);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              normfs_uintn_le64_logic(buf) == NORMFS_STORE_HEADER_V1 &&
              NORMFS_STORE_HEADER_V1_MIN_SIZE <= \result.consumed <=
                NORMFS_STORE_HEADER_V1_MAX_SIZE &&
              \result.consumed <= len;

    // each field read back from the bytes it was decoded from
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.header.compression ==
                normfs_uintn_varint64_value(buf + 8) &&
              \result.header.encryption ==
                normfs_uintn_varint64_value(buf +
                  normfs_store_header_off_encryption(&buf[0])) &&
              \result.header.num_entries_before ==
                normfs_uintn_varint64_value(buf +
                  normfs_store_header_off_entries_before(&buf[0])) &&
              \result.header.num_entries ==
                normfs_uintn_varint64_value(buf +
                  normfs_store_header_off_entries(&buf[0]));
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.consumed == normfs_store_header_end(&buf[0]);
*/
struct normfs_store_header_decode_result
normfs_store_header_v1_decode(const uint8_t *buf, size_t len)
{
	struct normfs_store_header_decode_result r = {
	    {0u, 0u, 0u, 0u},
	    0u,
	    0u,
	    NORMFS_STORE_HEADER_ERR_TRUNCATED
	};
	struct normfs_uintn_varint64_decode_result field;
	struct normfs_store_header_v1 header = {0u, 0u, 0u, 0u};
	size_t offset;
	int valid;

	if (len < NORMFS_STORE_HEADER_VERSION_SIZE) return r;

	r.version = normfs_uintn_le64_read(buf);
	if (r.version != NORMFS_STORE_HEADER_V1) {
		r.status = NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION;
		return r;
	}
	offset = NORMFS_STORE_HEADER_VERSION_SIZE;

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

	valid = normfs_store_header_v1_validate(&header);
	if (valid != NORMFS_STORE_HEADER_OK) {
		r.header = header;
		r.status = valid;
		return r;
	}

	r.header = header;
	r.consumed = offset;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}

/*
 * Round trip: for any header carrying supported compression and encryption
 * codes, decoding its encoding recovers the header exactly and consumes
 * exactly what was written. The proof is the body: WP discharges the asserts,
 * so \result == 1 is not a runtime check but a theorem about every valid
 * header. Same shape as normfs_wal_header_v1_roundtrip_holds.
 */
/*@ requires \valid_read(header);
    assigns \nothing;
    ensures \result == 1 <==>
              (normfs_store_header_valid_compression(header->compression) &&
               normfs_store_header_valid_encryption(header->encryption));
*/
int
normfs_store_header_v1_roundtrip_holds(
    const struct normfs_store_header_v1 *header)
{
	uint8_t buf[NORMFS_STORE_HEADER_V1_MAX_SIZE];
	struct normfs_store_header_encode_result enc;
	struct normfs_store_header_decode_result dec;
	int ok;

	enc = normfs_store_header_v1_encode(header, buf, sizeof(buf));
	if (enc.status != NORMFS_STORE_HEADER_OK) return 0;

	/*
	 * The encoding is a valid V1 header, so the decoder must accept it.
	 * Both type codes are below 128, so each occupies exactly one byte and
	 * the two varints that follow start at fixed offsets 10 and beyond.
	 */
	/*@ assert normfs_uintn_le64_logic(&buf[0]) == NORMFS_STORE_HEADER_V1; */

	/* Both codes are below 128, so each is a one byte varint spelling itself. */
	/*@ assert normfs_uintn_varint64_size_logic(header->compression) == 1; */
	/*@ assert normfs_uintn_varint64_size_logic(header->encryption) == 1; */
	/*@ assert normfs_uintn_varint64_byte(header->compression, 0) ==
	             header->compression; */
	/*@ assert normfs_uintn_varint64_byte(header->encryption, 0) ==
	             header->encryption; */
	/*@ assert buf[8] == header->compression; */
	/*@ assert normfs_store_header_voff_encryption(header->compression) == 9; */
	/*@ assert buf[9] == header->encryption; */
	/*@ assert normfs_store_header_voff_entries_before(header->compression,
	                                                   header->encryption) == 10; */
	/*@ assert normfs_uintn_varint64_value(&buf[10]) ==
	             header->num_entries_before; */
	/*@ assert normfs_uintn_varint64_len(&buf[10]) ==
	             normfs_uintn_varint64_size_logic(header->num_entries_before); */
	/*@ assert normfs_uintn_varint64_value(
	               &buf[10 + normfs_uintn_varint64_len(&buf[10])]) ==
	             header->num_entries; */
	/*@ assert enc.written == 10 + normfs_uintn_varint64_len(&buf[10]) +
	             normfs_uintn_varint64_len(
	               &buf[10 + normfs_uintn_varint64_len(&buf[10])]); */
	/*@ assert normfs_uintn_varint64_canonical(&buf[10]); */
	/*@ assert normfs_uintn_varint64_canonical(
	             &buf[10 + normfs_uintn_varint64_len(&buf[10])]); */

	/*
	 * The decoder states what it consumed in terms of the layout it reads
	 * back, so tie that to what the encoder wrote: both type codes are one
	 * byte, which fixes the read-side offsets at 9 and 10.
	 */
	/*@ assert normfs_uintn_varint64_len(&buf[8]) == 1; */
	/*@ assert normfs_uintn_varint64_len(&buf[9]) == 1; */
	/*@ assert normfs_store_header_off_encryption(&buf[0]) == 9; */
	/*@ assert normfs_store_header_off_entries_before(&buf[0]) == 10; */
	/*@ assert normfs_store_header_off_entries(&buf[0]) ==
	             10 + normfs_uintn_varint64_len(&buf[10]); */
	/*@ assert normfs_store_header_end(&buf[0]) == enc.written; */
	/*@ assert normfs_uintn_varint64_value(&buf[8]) == header->compression; */
	/*@ assert normfs_uintn_varint64_value(&buf[9]) == header->encryption; */

	dec = normfs_store_header_v1_decode(buf, enc.written);

	/* Bitwise so every field is compared with no short-circuit branch. */
	ok = (dec.status == NORMFS_STORE_HEADER_OK);
	ok &= (dec.consumed == enc.written);
	ok &= (dec.header.compression == header->compression);
	ok &= (dec.header.encryption == header->encryption);
	ok &= (dec.header.num_entries_before == header->num_entries_before);
	ok &= (dec.header.num_entries == header->num_entries);
	return ok;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_STORE_HEADER_OK ||
            \result.status == NORMFS_STORE_HEADER_ERR_TRUNCATED;
    ensures \result.status == NORMFS_STORE_HEADER_OK <==>
              len >= NORMFS_STORE_HEADER_VERSION_SIZE;
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.version == normfs_uintn_le64_logic(buf) &&
              \result.consumed == NORMFS_STORE_HEADER_VERSION_SIZE;
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

	if (len < NORMFS_STORE_HEADER_VERSION_SIZE) return r;

	r.version = normfs_uintn_le64_read(buf);
	r.consumed = NORMFS_STORE_HEADER_VERSION_SIZE;
	r.status = NORMFS_STORE_HEADER_OK;
	return r;
}
