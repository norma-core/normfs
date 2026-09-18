#ifndef NORMFS_DISK_MONITOR_SYS_H
#define NORMFS_DISK_MONITOR_SYS_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/disk_monitor.h"

/*
 * The syscall boundary, implemented in src/disk_monitor_sys.c, which Frama-C
 * never sees; src/disk_monitor.c is proved against these contracts and must
 * never include a system header (normfs/seed_sys.h has the reason). DIR and
 * struct stat stay out of the proved unit: a listing is an opaque handle, an
 * entry is a name, a kind and a size.
 */

#define NORMFS_DISK_SYS_NAME_MAX 256

enum normfs_disk_sys_kind {
	NORMFS_DISK_SYS_OTHER = 0,
	NORMFS_DISK_SYS_FILE = 1,
	NORMFS_DISK_SYS_DIR = 2
};

/* opendir(3). NULL with os_error 0 is a path that is not a directory
 * (ENOENT, ENOTDIR); NULL with os_error > 0 is a failure. */
/*@ requires path_len < NORMFS_DISK_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == \null ==> *os_error >= 0;
    ensures \result != \null ==> *os_error == 0;
*/
void *normfs_disk_sys_dir_open(const char *path, size_t path_len,
    int *os_error);

/*
 * 1 with an entry, 0 at the end, -1 on failure. "." and ".." are skipped.
 * kind and size come from lstat, so a symlink is OTHER. An entry whose lstat
 * fails with ENOENT went between readdir and stat, which the store worker
 * and the WAL cleanup cause routinely; it is skipped. A name of name_cap or
 * more bytes is truncated and reported OTHER.
 */
/*@ requires name_cap >= 1;
    requires \valid(name + (0 .. name_cap - 1));
    requires \valid(name_len);
    requires \valid(kind);
    requires \valid(size);
    requires \valid(os_error);
    requires \separated(name + (0 .. name_cap - 1), name_len, kind, size,
                        os_error);
    assigns name[0 .. name_cap - 1], *name_len, *kind, *size, *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result == 1 ==> *name_len < name_cap;
    ensures \result == 1 ==> name[*name_len] == 0;
    ensures \result == 1 ==> (*kind == NORMFS_DISK_SYS_OTHER ||
                              *kind == NORMFS_DISK_SYS_FILE ||
                              *kind == NORMFS_DISK_SYS_DIR);
    ensures \result == -1 ==> *os_error > 0;
    ensures \result >= 0 ==> *os_error == 0;
*/
int normfs_disk_sys_dir_next(void *handle, char *name, size_t name_cap,
    size_t *name_len, int *kind, uint64_t *size, int *os_error);

/*@ assigns \nothing; */
void normfs_disk_sys_dir_close(void *handle);

/* lstat(2): 1 for a regular file with its size, 0 for anything absent or
 * not a regular file, -1 on any other failure. */
/*@ requires path_len < NORMFS_DISK_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(size);
    requires \valid(os_error);
    requires \separated(size, os_error, path + (0 .. path_len));
    assigns *size, *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result >= 0 ==> *os_error == 0;
*/
int normfs_disk_sys_file_size(const char *path, size_t path_len,
    uint64_t *size, int *os_error);

/*@ requires path_len < NORMFS_DISK_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result == 0 ==> *os_error == 0;
*/
int normfs_disk_sys_unlink(const char *path, size_t path_len, int *os_error);

#endif /* NORMFS_DISK_MONITOR_SYS_H */
