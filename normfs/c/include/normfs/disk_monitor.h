#ifndef NORMFS_DISK_MONITOR_H
#define NORMFS_DISK_MONITOR_H

#include <stddef.h>
#include <stdint.h>

/*
 * Per-queue disk budget. Proved by WP: the id -> path layout (the sharding
 * of UintN::to_file_path, byte for byte), the id arithmetic, and that the
 * walk and the eviction stay in their buffers and terminate. Syscalls are
 * assumed shims in normfs/disk_monitor_sys.h, discharged by the tests. Rust
 * keeps the timer, the offloader and the byte count.
 *
 * An id is its hex digits, lowercase, no leading zeros: what to_file_path
 * renders and from_hex_digits parses. Numeric order is then length first,
 * then bytewise, and precision is capped by the layout alone.
 */

#define NORMFS_DISK_PATH_MAX 4096
#define NORMFS_DISK_MAX_DEPTH 16
#define NORMFS_DISK_ID_MAX (3 * NORMFS_DISK_MAX_DEPTH)
#define NORMFS_DISK_CHUNKS 4096

/* Refused, not truncated, past these; they exist so every loop has a
 * variant. A layout directory holds at most 4096 + 4096 entries. */
#define NORMFS_DISK_DIR_ENTRIES_MAX (1u << 20)
#define NORMFS_DISK_WALK_STEPS_MAX (1u << 24)

enum normfs_disk_kind {
	NORMFS_DISK_STORE = 0,
	NORMFS_DISK_WAL = 1
};

enum normfs_disk_status {
	NORMFS_DISK_OK = 0,
	NORMFS_DISK_ERR_INVALID_ARG = 1,
	NORMFS_DISK_ERR_PATH_TOO_LONG = 2,
	NORMFS_DISK_ERR_ID_OVERFLOW = 3,
	NORMFS_DISK_ERR_TOO_DEEP = 4,
	NORMFS_DISK_ERR_TOO_MANY = 5,
	NORMFS_DISK_ERR_NOT_FOUND = 6,
	NORMFS_DISK_ERR_IO = 7
};

/* MORE: the event buffer filled; call again, the request carries the
 * position. */
enum normfs_disk_stop {
	NORMFS_DISK_STOP_MORE = 0,
	NORMFS_DISK_STOP_FREED = 1,
	NORMFS_DISK_STOP_GAP = 2,
	NORMFS_DISK_STOP_BOUND = 3
};

/* os_error is the errno of the failing syscall, 0 when none ran. Two 4 byte
 * members, so the Rust mirror cannot disagree about the layout. */
struct normfs_disk_result {
	int os_error;
	int status;
};

/* 48 bytes then a size_t: no padding on LP64. */
struct normfs_disk_id {
	char hex[NORMFS_DISK_ID_MAX];
	size_t len;
};

struct normfs_disk_scan {
	uint64_t total;
	struct normfs_disk_id min;
	int has_min;
};

/*
 * Walks ids upward from `next`, deleting the store file at each id, or the
 * WAL file when there is none, until `to_free` bytes are gone, an id has
 * neither file, or the id passes `bound`. `next` and `to_free` are updated in
 * place. has_bound == 0 means every id may go: the no-offloader case.
 */
struct normfs_disk_evict_req {
	const char *store_dir;
	size_t store_dir_len;
	const char *wal_dir;
	size_t wal_dir_len;
	struct normfs_disk_id next;
	struct normfs_disk_id bound;
	int has_bound;
	uint64_t to_free;
};

/* deleted == 0 carries the errno of the failed stat or unlink; the walk
 * moves on and Rust logs it. */
struct normfs_disk_event {
	struct normfs_disk_id id;
	uint64_t size;
	int kind;
	int deleted;
	int os_error;
};

struct normfs_disk_result
normfs_disk_path(const char *dir, size_t dir_len,
    const struct normfs_disk_id *id, int kind,
    char *out, size_t out_len, size_t *used);

struct normfs_disk_result
normfs_disk_file_size(const char *dir, size_t dir_len,
    const struct normfs_disk_id *id, int kind, uint64_t *size);

/* A missing directory scans as empty. */
struct normfs_disk_result
normfs_disk_scan(const char *dir, size_t dir_len, int kind,
    struct normfs_disk_scan *out);

struct normfs_disk_result
normfs_disk_evict(struct normfs_disk_evict_req *req,
    struct normfs_disk_event *events, size_t cap,
    size_t *count, int *stop);

#endif /* NORMFS_DISK_MONITOR_H */
