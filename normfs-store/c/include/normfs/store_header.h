#ifndef NORMFS_STORE_HEADER_H
#define NORMFS_STORE_HEADER_H

#include <stddef.h>
#include <stdint.h>

#include "uintn/le.h"
#include "uintn/varint.h"

#define NORMFS_STORE_HEADER_V0 0
#define NORMFS_STORE_HEADER_V1 1

/*
 * The version word keeps the V0 encoding: a fixed 8 byte little-endian u64 at
 * offset 0, so existing readers still report an unsupported version rather
 * than misparsing. Only the fields after it become varints.
 */
#define NORMFS_STORE_HEADER_VERSION_SIZE 8

/*
 * version(8) + compression + encryption + num_entries_before + num_entries.
 * The compression and encryption values are range checked in C, so each is a
 * single varint byte in every accepted header; the bound still allows a full
 * width varint for each so that the buffer size is independent of the check.
 */
#define NORMFS_STORE_HEADER_V1_MAX_SIZE 48
#define NORMFS_STORE_HEADER_V1_MIN_SIZE 12

/*
 * Supported type codes. These are the on-disk encoding of the Rust
 * CompressionType and EncryptionType enums; the mapping is checked here so
 * that Rust does not repeat the decision.
 */
#define NORMFS_STORE_COMPRESSION_MAX 3 /* None, Gzip, Xz, Zstd */
#define NORMFS_STORE_ENCRYPTION_MAX 2  /* None, Aes, AesV2 */

/* Codes 0 .. 4 take their values from enum normfs_uintn_varint_status so the
 * two cannot drift apart. */
enum normfs_store_header_status {
	NORMFS_STORE_HEADER_OK = NORMFS_UINTN_VARINT_OK,
	NORMFS_STORE_HEADER_ERR_TRUNCATED = NORMFS_UINTN_VARINT_ERR_TRUNCATED,
	NORMFS_STORE_HEADER_ERR_OVERFLOW = NORMFS_UINTN_VARINT_ERR_OVERFLOW,
	NORMFS_STORE_HEADER_ERR_NON_CANONICAL = NORMFS_UINTN_VARINT_ERR_NON_CANONICAL,
	NORMFS_STORE_HEADER_ERR_NO_SPACE = NORMFS_UINTN_VARINT_ERR_NO_SPACE,
	NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION = 5,
	NORMFS_STORE_HEADER_ERR_INVALID_COMPRESSION = 6,
	NORMFS_STORE_HEADER_ERR_INVALID_ENCRYPTION = 7
};

struct normfs_store_header_v1 {
	uint64_t compression;
	uint64_t encryption;
	uint64_t num_entries_before;
	uint64_t num_entries;
};

struct normfs_store_header_encode_result {
	size_t written;
	int status;
};

struct normfs_store_header_decode_result {
	struct normfs_store_header_v1 header;
	uint64_t version;
	size_t consumed;
	int status;
};

struct normfs_store_header_version_result {
	uint64_t version;
	size_t consumed;
	int status;
};

/*
 * Offsets of the varint fields. Each one starts where the previous ended, so
 * they are expressed by composing the encoded varint lengths rather than by
 * enumerating every combination of widths.
 */
/*
 * Encoding side offsets, computed from the field values. They read no memory,
 * so a clause about the bytes of one field stays valid once the following
 * fields have been written.
 */
/*@ axiomatic NormfsStoreHeaderEncodeLayout {
      logic integer normfs_store_header_voff_encryption(integer compression) =
        8 + normfs_uintn_varint64_size_logic(compression);

      logic integer normfs_store_header_voff_entries_before(integer compression,
                                                            integer encryption) =
        normfs_store_header_voff_encryption(compression) +
        normfs_uintn_varint64_size_logic(encryption);

      logic integer normfs_store_header_voff_entries(integer compression,
                                                     integer encryption,
                                                     integer entries_before) =
        normfs_store_header_voff_entries_before(compression, encryption) +
        normfs_uintn_varint64_size_logic(entries_before);
    }
*/

/*@ axiomatic NormfsStoreHeaderLayout {
      logic integer normfs_store_header_off_compression = 8;

      logic integer normfs_store_header_off_encryption{L}(uint8_t *b) =
        normfs_store_header_off_compression +
        normfs_uintn_varint64_len(b + normfs_store_header_off_compression);

      logic integer normfs_store_header_off_entries_before{L}(uint8_t *b) =
        normfs_store_header_off_encryption(b) +
        normfs_uintn_varint64_len(b + normfs_store_header_off_encryption(b));

      logic integer normfs_store_header_off_entries{L}(uint8_t *b) =
        normfs_store_header_off_entries_before(b) +
        normfs_uintn_varint64_len(b + normfs_store_header_off_entries_before(b));

      logic integer normfs_store_header_end{L}(uint8_t *b) =
        normfs_store_header_off_entries(b) +
        normfs_uintn_varint64_len(b + normfs_store_header_off_entries(b));
    }
*/

int normfs_store_header_v1_validate(const struct normfs_store_header_v1 *header);

size_t normfs_store_header_v1_size(const struct normfs_store_header_v1 *header);

struct normfs_store_header_encode_result
normfs_store_header_v1_encode(const struct normfs_store_header_v1 *header,
    uint8_t *out, size_t out_len);

struct normfs_store_header_decode_result
normfs_store_header_v1_decode(const uint8_t *buf, size_t len);

/* Reads the version word without consuming the rest of the header. */
struct normfs_store_header_version_result
normfs_store_header_peek_version(const uint8_t *buf, size_t len);

/*
 * Returns 1 exactly when the header carries supported compression and
 * encryption codes. The point is the proof, not the runtime value: Frama-C
 * establishes that for every such header decoding its encoding recovers it
 * byte for byte, i.e. decode(encode(h)) == h. This mirrors
 * normfs_wal_header_v1_roundtrip_holds.
 */
int normfs_store_header_v1_roundtrip_holds(
    const struct normfs_store_header_v1 *header);

#endif /* NORMFS_STORE_HEADER_H */
