#ifndef NORMFS_STORE_PACK_H
#define NORMFS_STORE_PACK_H

#include <stddef.h>

#include "normfs/store_header.h"

/*
 * Layout of a pack slot. The front input_cap bytes hold the WAL bytes of one
 * file; the store file is built after them as
 *
 *     auth ++ header ++ [nonce] ++ data ++ [tag]
 *
 * contiguous, so it is written or sent without another copy. Offsets are
 * from the start of the slot.
 */
#define NORMFS_STORE_PACK_AUTH_SIZE 152
#define NORMFS_STORE_PACK_NONCE_SIZE 12
#define NORMFS_STORE_PACK_TAG_SIZE 16
#define NORMFS_STORE_PACK_ZSTD_BLOCK (128 * 1024)

/* Inputs above this are refused, so no sum below can wrap a 64-bit size_t. */
#define NORMFS_STORE_PACK_INPUT_MAX ((size_t)1 << 40)

#define NORMFS_STORE_PACK_OVERHEAD                                    \
	(NORMFS_STORE_PACK_AUTH_SIZE + NORMFS_STORE_HEADER_V1_MAX_SIZE + \
	 NORMFS_STORE_PACK_NONCE_SIZE + NORMFS_STORE_PACK_TAG_SIZE)

enum normfs_store_pack_status {
	NORMFS_STORE_PACK_OK = 0,
	NORMFS_STORE_PACK_ERR_INPUT = 1,
	NORMFS_STORE_PACK_ERR_SLOT = 2,
	NORMFS_STORE_PACK_ERR_HEADER = 3,
	NORMFS_STORE_PACK_ERR_DATA = 4
};

struct normfs_store_pack_layout {
	size_t file_at;   /* auth; also input_cap */
	size_t header_at;
	size_t body_at;   /* nonce when encrypted, else data */
	size_t data_at;
	size_t data_cap;  /* room for compressed or copied data */
	int encrypted;
};

struct normfs_store_pack_size_result {
	size_t size;
	int status;
};

struct normfs_store_pack_layout_result {
	struct normfs_store_pack_layout layout;
	int status;
};

size_t normfs_store_pack_compress_bound(size_t n);
struct normfs_store_pack_size_result normfs_store_pack_slot_size(size_t input_cap);
struct normfs_store_pack_layout_result normfs_store_pack_layout(size_t slot_size,
    size_t input_cap, size_t header_len, int encrypted);
struct normfs_store_pack_size_result normfs_store_pack_file_len(
    const struct normfs_store_pack_layout *layout, size_t data_len);
int normfs_store_pack_fits_holds(size_t input_cap, size_t header_len,
    int encrypted, size_t input_len, size_t data_len);

#endif
