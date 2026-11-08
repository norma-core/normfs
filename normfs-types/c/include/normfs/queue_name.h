#ifndef NORMFS_QUEUE_NAME_H
#define NORMFS_QUEUE_NAME_H

#include <stddef.h>
#include <stdint.h>

/* Copies name to out without each '/' that another '/' or the end follows,
 * so a//b and a/b/ give a/b; returns the length written. out holds len. */
size_t normfs_queue_name_canonical(const uint8_t *name, size_t len, uint8_t *out);

/* Returns 1 when the name is not empty, no segment is . or .. or in any
 * case wal, store or closed, the first is not tmp and does not start with a
 * dot, the last is not three lowercase hex digits, and there is no NUL; with
 * windows set, also no backslash, no colon and no segment ending in a dot or
 * space. */
int normfs_queue_name_ok(const uint8_t *name, size_t len, int windows);

#endif /* NORMFS_QUEUE_NAME_H */
