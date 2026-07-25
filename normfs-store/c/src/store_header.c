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

    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \forall integer k;
                0 <= k < normfs_uintn_varint64_size_logic(header->compression) ==>
                  out[8 + k] ==
                    normfs_uintn_varint64_byte(header->compression, k);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \forall integer k;
                0 <= k < normfs_uintn_varint64_size_logic(header->encryption) ==>
                  out[normfs_store_header_voff_encryption(header->compression) + k] ==
                    normfs_uintn_varint64_byte(header->encryption, k);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \forall integer k;
                0 <= k <
                  normfs_uintn_varint64_size_logic(header->num_entries_before) ==>
                  out[normfs_store_header_voff_entries_before(header->compression,
                        header->encryption) + k] ==
                    normfs_uintn_varint64_byte(header->num_entries_before, k);
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \forall integer k;
                0 <= k < normfs_uintn_varint64_size_logic(header->num_entries) ==>
                  out[normfs_store_header_voff_entries(header->compression,
                        header->encryption, header->num_entries_before) + k] ==
                    normfs_uintn_varint64_byte(header->num_entries, k);
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

	field = normfs_uintn_varint64_encode(header->compression,
	    out + off_compression, out_len - off_compression);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_encryption = off_compression + field.written;
	/*@ assert off_encryption ==
	             normfs_store_header_voff_encryption(header->compression); */
	/*@ assert \forall integer k;
	             0 <= k < normfs_uintn_varint64_size_logic(header->compression) ==>
	               out[off_compression + k] ==
	                 normfs_uintn_varint64_byte(header->compression, k); */

	field = normfs_uintn_varint64_encode(header->encryption,
	    out + off_encryption, out_len - off_encryption);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_entries_before = off_encryption + field.written;
	/*@ assert off_entries_before ==
	             normfs_store_header_voff_entries_before(header->compression,
	                                                     header->encryption); */
	/*@ assert \forall integer k;
	             0 <= k < normfs_uintn_varint64_size_logic(header->encryption) ==>
	               out[off_encryption + k] ==
	                 normfs_uintn_varint64_byte(header->encryption, k); */

	field = normfs_uintn_varint64_encode(header->num_entries_before,
	    out + off_entries_before, out_len - off_entries_before);
	if (field.status != NORMFS_UINTN_VARINT_OK) return r;
	off_entries = off_entries_before + field.written;
	/*@ assert off_entries ==
	             normfs_store_header_voff_entries(header->compression,
	                                              header->encryption,
	                                              header->num_entries_before); */
	/*@ assert \forall integer k;
	             0 <= k <
	               normfs_uintn_varint64_size_logic(header->num_entries_before) ==>
	               out[off_entries_before + k] ==
	                 normfs_uintn_varint64_byte(header->num_entries_before, k); */

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
	/*@ assert \forall integer k;
	             0 <= k < normfs_uintn_varint64_size_logic(header->num_entries) ==>
	               out[off_entries + k] ==
	                 normfs_uintn_varint64_byte(header->num_entries, k); */

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
                  normfs_store_header_off_encryption(buf)) &&
              \result.header.num_entries_before ==
                normfs_uintn_varint64_value(buf +
                  normfs_store_header_off_entries_before(buf)) &&
              \result.header.num_entries ==
                normfs_uintn_varint64_value(buf +
                  normfs_store_header_off_entries(buf));
    ensures \result.status == NORMFS_STORE_HEADER_OK ==>
              \result.consumed == normfs_store_header_end(buf);
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
