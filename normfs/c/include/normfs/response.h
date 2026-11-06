#ifndef NORMFS_RESPONSE_H
#define NORMFS_RESPONSE_H

#include <stddef.h>
#include <stdint.h>

#include "normfs.pb.h"

enum normfs_response_status {
	NORMFS_RESPONSE_OK = 0,
	NORMFS_RESPONSE_ERR_BAD_LENGTH = 1
};

/* Encoded size of m; fills the _size caches of m and its children. */
size_t normfs_response_size(const normfs_ServerResponse *m);

/* Encodes m, sized by normfs_response_size and unchanged since, into buf when len
 * is its size, else BAD_LENGTH. */
int normfs_response_encode(const normfs_ServerResponse *m, uint8_t *buf, size_t len);

#endif /* NORMFS_RESPONSE_H */
