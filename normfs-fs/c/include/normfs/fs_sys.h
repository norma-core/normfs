#ifndef NORMFS_FS_SYS_H
#define NORMFS_FS_SYS_H

#include <stddef.h>
#include <stdint.h>

/*
 * The kernel, as the planner in normfs/fs_plan.h sees it.
 *
 * Two kinds of function live here and neither has a body Frama-C reads.
 *
 * The completion functions, normfs_fs_world_*, are the model: each states
 * what the kernel promises once an operation has completed, on four ghost
 * worlds. Their bodies in src/fs_sys.c are empty. The planner calls one from
 * each apply step, so the executor's obligation is a single sentence -- an
 * apply is reported only after the operation it names has really completed,
 * with the result it names -- and everything the planner proves rests on it.
 *
 * The shims, normfs_fs_sys_*, are the syscalls the thread-pool executor uses.
 * They carry errno contracts only, EINTR is retried inside them except for
 * close, and tests/test_fs.c checks them against a real filesystem. The
 * executor may also implement these operations without the shims.
 *
 * Four worlds rather than one so that frames do the work: a rename touches
 * names and nothing durable; a file fsync touches durable data and no name.
 * With one world every step would need a clause for every fact it leaves
 * alone, and the disk monitor's proof already hit that wall.
 *
 * The model excludes changes to a path by anything but this plan between two
 * of its steps. Durable content is tracked as a synced prefix: the bytes in
 * [0, fs_dur_synced(ino)) are exactly those written, and nothing is claimed
 * about bytes past it until the next fsync.
 */

/*@ ghost extern int normfs_fs_vol_names; */
/*@ ghost extern int normfs_fs_vol_data; */
/*@ ghost extern int normfs_fs_dur_names; */
/*@ ghost extern int normfs_fs_dur_data; */
/*@ axiomatic NormfsFsKernel {
      // The inode a name resolves to in the page cache, 0 when absent.
      logic integer fs_vol_ino{L}(char *path, integer len)
        reads normfs_fs_vol_names, path[0 .. len];
      logic integer fs_vol_len{L}(integer ino) reads normfs_fs_vol_data;
      // The inode a name resolves to after a power cut, 0 when absent.
      logic integer fs_dur_ino{L}(char *path, integer len)
        reads normfs_fs_dur_names, path[0 .. len];
      logic integer fs_dur_synced{L}(integer ino) reads normfs_fs_dur_data;
      logic integer fs_dur_len{L}(integer ino) reads normfs_fs_dur_data;
      logic integer fs_cached_byte{L}(integer ino, integer off) reads normfs_fs_vol_data;
      logic integer fs_certified_byte{L}(integer ino, integer off) reads normfs_fs_dur_data;
      predicate fs_path_equal{L}(char *a, integer an, char *b, integer bn) =
        an == bn && (\forall integer i; 0 <= i <= an ==> a[i] == b[i]);
      axiom fs_vol_path_value{L}: \forall char *a, *b, integer an, bn;
        fs_path_equal(a, an, b, bn) ==> fs_vol_ino(a, an) == fs_vol_ino(b, bn);
      axiom fs_dur_path_value{L}: \forall char *a, *b, integer an, bn;
        fs_path_equal(a, an, b, bn) ==> fs_dur_ino(a, an) == fs_dur_ino(b, bn);
    }
*/

/*@ axiomatic NormfsFsDirectories {
      predicate fs_parent_durable(char *path, integer len);
    }
*/

#define NORMFS_FS_PATH_MAX 4096
#define NORMFS_FS_SYS_IO_MAX 0x7FFFFFFF

/* ------------------------------------------------------------------------
 * Completions: what a finished operation lets the planner conclude. */

/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires ino > 0;
    assigns normfs_fs_vol_names, normfs_fs_vol_data, normfs_fs_dur_data;
    ensures fs_dur_synced(ino) == 0;
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures fs_vol_ino(path, path_len) == ino;
    ensures fs_vol_len(ino) == 0;
    ensures \forall integer j; j != ino ==> fs_vol_len(j) == \old(fs_vol_len(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; j != ino ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_open_ok(const char *path, size_t path_len, uint64_t ino);

/* A write of n bytes at the end of the file: the page cache grew, the synced
 * prefix did not move, and the durable length is now anything at all. */
/*@ requires n > 0;
    assigns normfs_fs_vol_data, normfs_fs_dur_data;
    ensures fs_vol_len(ino) == \old(fs_vol_len(ino)) + n;
    ensures \forall integer j; j != ino ==> fs_vol_len(j) == \old(fs_vol_len(j));
    ensures fs_dur_synced(ino) == \old(fs_dur_synced(ino));
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; (j != ino || i < \old(fs_dur_synced(ino))) ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_write_ok(uint64_t ino, uint64_t n);

/* A failed write may have written some of its bytes; only the synced prefix
 * is known afterwards. */
/*@ assigns normfs_fs_vol_data, normfs_fs_dur_data;
    ensures fs_dur_synced(ino) == \old(fs_dur_synced(ino));
    ensures \forall integer j; j != ino ==> fs_vol_len(j) == \old(fs_vol_len(j));
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; (j != ino || i < \old(fs_dur_synced(ino))) ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_write_err(uint64_t ino);

/*@ assigns normfs_fs_dur_data;
    ensures fs_dur_synced(ino) == fs_vol_len(ino);
    ensures fs_dur_len(ino) == fs_vol_len(ino);
    ensures \forall integer i; 0 <= i < fs_vol_len(ino) ==>
        fs_certified_byte(ino, i) == fs_cached_byte(ino, i);
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; j != ino ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_fsync_ok(uint64_t ino);

/* A failed fsync: Linux may drop the dirty pages, so the page cache length is
 * unknown too. What was synced before stays synced. */
/*@ assigns normfs_fs_vol_data, normfs_fs_dur_data;
    ensures fs_dur_synced(ino) == \old(fs_dur_synced(ino));
    ensures \forall integer j; j != ino ==> fs_vol_len(j) == \old(fs_vol_len(j));
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; (j != ino || i < \old(fs_dur_synced(ino))) ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_fsync_err(uint64_t ino);

/*@ requires src_len < NORMFS_FS_PATH_MAX && dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(src + (0 .. src_len));
    requires \valid_read(dst + (0 .. dst_len));
    assigns normfs_fs_vol_names;
    ensures fs_vol_ino(dst, dst_len) == \old(fs_vol_ino(src, src_len));
    ensures \old(fs_vol_ino(src, src_len)) != \old(fs_vol_ino(dst, dst_len)) ==>
        fs_vol_ino(src, src_len) == 0;
*/
void normfs_fs_world_rename_ok(const char *src, size_t src_len,
    const char *dst, size_t dst_len);

/* fsync(2) on the parent directory: the entry for `name` is on the medium.
 * One name, not the directory: a claim about every entry is a quantifier
 * over paths, which the provers do not carry across a mutation. */
/*@ requires name_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(name + (0 .. name_len));
    requires fs_parent_durable(name, name_len);
    assigns normfs_fs_dur_names;
    ensures fs_dur_ino(name, name_len) == fs_vol_ino(name, name_len);
*/
void normfs_fs_world_fsync_dir_ok(const char *name, size_t name_len);

/* A failed directory fsync: the entry either reached the medium or did not;
 * there is no third inode it could point to. */
/*@ requires name_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(name + (0 .. name_len));
    assigns normfs_fs_dur_names;
    ensures fs_dur_ino(name, name_len) == \old(fs_dur_ino(name, name_len)) ||
            fs_dur_ino(name, name_len) == fs_vol_ino(name, name_len);
*/
void normfs_fs_world_fsync_dir_err(const char *name, size_t name_len);

/* ftruncate(2): the page cache is cut to len; the synced prefix is cut to len
 * where it was longer and untouched where it was not. */
/*@ assigns normfs_fs_vol_data, normfs_fs_dur_data;
    ensures fs_vol_len(ino) == len;
    ensures fs_dur_synced(ino) ==
              (\old(fs_dur_synced(ino)) <= len ? \old(fs_dur_synced(ino)) : len);
    ensures \forall integer j; j != ino ==> fs_vol_len(j) == \old(fs_vol_len(j));
    ensures \forall integer j; j != ino ==> fs_dur_synced(j) == \old(fs_dur_synced(j));
    ensures \forall integer j; j != ino ==> fs_dur_len(j) == \old(fs_dur_len(j));
    ensures \forall integer j, i; (j != ino || i < len) ==>
        fs_certified_byte(j, i) == \old(fs_certified_byte(j, i));
*/
void normfs_fs_world_truncate_ok(uint64_t ino, uint64_t len);

/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    assigns normfs_fs_vol_names;
    ensures fs_vol_ino(path, path_len) == 0;
*/
void normfs_fs_world_unlink_ok(const char *path, size_t path_len);

/* ------------------------------------------------------------------------
 * Shims for the thread-pool executor. Paths are (pointer, length) with an
 * explicit NUL; results are long so this header needs no <sys/types.h>. Every
 * failure leaves *os_error > 0, EIO where errno said nothing. */

#define NORMFS_FS_TMP_EXCL 0
#define NORMFS_FS_TMP_TRUNC 1

/* open(O_WRONLY|O_CREAT|O_CLOEXEC, 0644) with O_EXCL or O_TRUNC, then fstat
 * for the inode number. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires mode == NORMFS_FS_TMP_EXCL || mode == NORMFS_FS_TMP_TRUNC;
    requires \valid(ino);
    requires \valid(os_error);
    requires \separated(ino, os_error, path + (0 .. path_len));
    assigns *ino, *os_error;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0 && *ino > 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_open_create(const char *path, size_t path_len, int mode,
    uint64_t *ino, int *os_error);

struct normfs_fs_iov {
	const uint8_t *base;
	size_t len;
};

/* pwritev(2) at off, looped until every byte of every run is written. The
 * runs are not modified; a write that reports no progress is a failure. */
/*@ requires 0 < cnt <= 1024;
    requires \valid_read(iov + (0 .. cnt - 1));
    requires \valid(os_error);
    requires \separated(os_error, iov + (0 .. cnt - 1));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_pwritev_all(int fd, const struct normfs_fs_iov *iov,
    size_t cnt, uint64_t off, int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_fsync(int fd, int *os_error);

/* Not retried on EINTR: Linux releases the descriptor regardless, so a
 * retry could close one already handed to another thread. */
/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_close(int fd, int *os_error);

/* lstat(2): 1 with the length of a regular file, 0 when the name is absent,
 * -1 on any other failure. A name that is present but not a regular file is
 * a failure, because renaming over it would not replace a file. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(len);
    requires \valid(os_error);
    requires \separated(len, os_error, path + (0 .. path_len));
    assigns *len, *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_file_len(const char *path, size_t path_len, uint64_t *len,
    int *os_error);

/*@ requires src_len < NORMFS_FS_PATH_MAX && dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(src + (0 .. src_len));
    requires \valid_read(dst + (0 .. dst_len));
    requires src[src_len] == 0 && dst[dst_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, src + (0 .. src_len), dst + (0 .. dst_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_rename(const char *src, size_t src_len, const char *dst,
    size_t dst_len, int *os_error);

/* open(O_RDONLY|O_DIRECTORY) + fsync + close on the directory holding `path`:
 * everything up to the last '/' of path, or "." when there is none. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_fsync_parent(const char *path, size_t path_len,
    int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_ftruncate(int fd, uint64_t len, int *os_error);

/* unlink(2): 0 removed, 1 already absent, -1 on any other failure. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_unlink(const char *path, size_t path_len, int *os_error);

/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_sync_dir(const char *path, size_t path_len, int *os_error);

#endif /* NORMFS_FS_SYS_H */
