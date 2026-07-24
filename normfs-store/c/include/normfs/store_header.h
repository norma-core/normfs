#ifndef NORMFS_STORE_HEADER_H
#define NORMFS_STORE_HEADER_H

#include <stddef.h>
#include <stdint.h>

#include "uintn/varint.h"

#define NORMFS_STORE_HEADER_V0 0
#define NORMFS_STORE_HEADER_V1 1

/*
 * version(1) + compression(10) + encryption(10) + num_entries_before(10) +
 * num_entries(10). Compression and encryption are decoded as raw u64 here;
 * mapping them onto the supported type sets stays on the Rust side.
 */
#define NORMFS_STORE_HEADER_V1_MAX_SIZE 41
#define NORMFS_STORE_HEADER_V1_MIN_SIZE 5

/* Codes 0 .. 4 mirror enum normfs_uintn_varint_status. */
enum normfs_store_header_status {
	NORMFS_STORE_HEADER_OK = 0,
	NORMFS_STORE_HEADER_ERR_TRUNCATED = 1,
	NORMFS_STORE_HEADER_ERR_OVERFLOW = 2,
	NORMFS_STORE_HEADER_ERR_NON_CANONICAL = 3,
	NORMFS_STORE_HEADER_ERR_NO_SPACE = 4,
	NORMFS_STORE_HEADER_ERR_UNSUPPORTED_VERSION = 5
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
	size_t consumed;
	int status;
};

struct normfs_store_header_version_result {
	uint64_t version;
	size_t consumed;
	int status;
};

size_t normfs_store_header_v1_size(const struct normfs_store_header_v1 *header);

struct normfs_store_header_encode_result
normfs_store_header_v1_encode(const struct normfs_store_header_v1 *header,
    uint8_t *out, size_t out_len);

struct normfs_store_header_decode_result
normfs_store_header_v1_decode(const uint8_t *buf, size_t len);

/*
 * A V0 header opens with a u64 little-endian version of zero, so its first
 * byte is 0x00. A V1 header opens with a canonical varint version. Byte zero
 * therefore separates the two encodings. V0 reports consumed == 0 because its
 * version field is not a varint and has to be re-read by the V0 decoder.
 */
struct normfs_store_header_version_result
normfs_store_header_peek_version(const uint8_t *buf, size_t len);

#endif /* NORMFS_STORE_HEADER_H */
