#ifndef NORMFS_FRAME_H
#define NORMFS_FRAME_H

#include <stddef.h>
#include <stdint.h>

#include "uintn/le.h"

/* Every TCP message is an 8 byte little-endian payload length, then the payload. */
#define NORMFS_FRAME_HEADER_SIZE 8
/* 5 GiB, the limit the Go client applies to what it reads as well. */
#define NORMFS_FRAME_MAX_PAYLOAD 5368709120ull

enum normfs_frame_status {
	NORMFS_FRAME_OK = 0,
	NORMFS_FRAME_ERR_BAD_LENGTH = 1,
	NORMFS_FRAME_ERR_TOO_LARGE = 2
};

struct normfs_frame_encode_result {
	size_t written;
	int status;
};

struct normfs_frame_decode_result {
	uint64_t payload_len;
	size_t consumed;
	int status;
};

/* Both take a buffer of exactly NORMFS_FRAME_HEADER_SIZE bytes. */
struct normfs_frame_encode_result
normfs_frame_header_encode(uint64_t payload_len, uint8_t *out, size_t out_len);

struct normfs_frame_decode_result
normfs_frame_header_decode(const uint8_t *buf, size_t len);

/* Returns 1 exactly when payload_len fits in a frame; WP proves decode(encode(n)) == n. */
int normfs_frame_header_roundtrip_holds(uint64_t payload_len);

#endif /* NORMFS_FRAME_H */
