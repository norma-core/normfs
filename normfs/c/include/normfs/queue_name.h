#ifndef NORMFS_QUEUE_NAME_H
#define NORMFS_QUEUE_NAME_H

#include <stddef.h>
#include <stdint.h>

/* Returns 1 when no segment is . or .. and there is no NUL; with windows set,
 * also no backslash, no colon and no segment ending in a dot or space. */
int normfs_queue_name_ok(const uint8_t *name, size_t len, int windows);

#endif /* NORMFS_QUEUE_NAME_H */
