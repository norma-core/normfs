#ifndef NORMFS_H
#define NORMFS_H

#include <stddef.h>
#include <stdint.h>

/*
 * NormFS as a library: link libnormfs_capi.a (normfs_capi.lib on MSVC) and
 * call these from C or C++. Every call blocks until done. A handle may be used
 * from several threads at once; one data_dir takes one open handle at a time.
 * Queues only grow: a record is appended and read back by its id, and old
 * records go only by the disk limit.
 *
 * Calls return NORMFS_OK or an error code; normfs_last_error() has the text
 * for the last failed call on the calling thread.
 */

#ifdef __cplusplus
extern "C" {
#endif

enum normfs_status {
	NORMFS_OK = 0,
	NORMFS_ERR_INVALID = 1,    /* NULL argument, bad UTF-8 or queue name */
	NORMFS_ERR_NOT_FOUND = 2,  /* record does not exist */
	NORMFS_ERR_EMPTY = 3,      /* queue has no record yet */
	NORMFS_ERR_TOO_LARGE = 4,  /* record and its framing exceed a 256 KiB page */
	NORMFS_ERR_CLOSED = 5,     /* the instance is closing */
	NORMFS_ERR_ID_RANGE = 6,   /* id does not fit 64 bits */
	NORMFS_ERR_IO = 7,         /* disk, cloud or any other failure */
	NORMFS_ERR_PANIC = 8       /* a bug in NormFS; the handle may be unusable */
};

typedef struct normfs normfs_t;

/* Called once per record, in id order, on the calling thread; `data` is valid
 * only during the call. A nonzero return stops the read or follow. It must not
 * throw: the exception would unwind through Rust. */
typedef int (*normfs_record_cb)(void *ctx, uint64_t id, const uint8_t *data,
    size_t len);

/* Opens (or creates) the instance in `data_dir`. */
int normfs_open(const char *data_dir, normfs_t **out);

/* Flushes every queue to disk and frees the handle, even on error. No other
 * call may be running on the handle. */
int normfs_close(normfs_t *fs);

/* Appends one record, creating the queue on first use; `id` gets its id. */
int normfs_append(normfs_t *fs, const char *queue, const uint8_t *data,
    size_t len, uint64_t *id);

/* Returns once every record appended to `queue` is written and synced, or
 * NORMFS_ERR_IO if that takes more than 10 s. */
int normfs_flush(normfs_t *fs, const char *queue);

/* The id of the newest record in `queue`. */
int normfs_last_id(normfs_t *fs, const char *queue, uint64_t *id);

/* Calls `cb` for up to `limit` records from id `from` on, then returns; a
 * `limit` of 0 reads nothing. A queue never written reads as empty. */
int normfs_read(normfs_t *fs, const char *queue, uint64_t from,
    uint64_t limit, normfs_record_cb cb, void *ctx);

/* Like normfs_read without a limit: waits for new records until `cb` returns
 * nonzero or normfs_stop_follows is called. */
int normfs_follow(normfs_t *fs, const char *queue, uint64_t from,
    normfs_record_cb cb, void *ctx);

/* Makes every normfs_follow running on `fs` return NORMFS_OK, so they can be
 * joined before normfs_close. */
int normfs_stop_follows(normfs_t *fs);

/* Text of the last error on this thread; valid until the next call on it. */
const char *normfs_last_error(void);

#ifdef __cplusplus
}
#endif

#endif /* NORMFS_H */
