#ifndef NORMFS_FS_SYS_H
#define NORMFS_FS_SYS_H

#include <stddef.h>
#include <stdint.h>

/* A descriptor on Unix and a HANDLE on Windows, so a Rust File passes as is. */
#if defined(_WIN32)
typedef intptr_t normfs_fs_fd;
#else
typedef int normfs_fs_fd;
#endif

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
 * failure leaves *os_error > 0, EIO where errno said nothing.
 *
 * With root > 0 a path is the data directory path[0 .. root), which may be a
 * link, then segments opened with normfs_fs_open_parent: no link below the
 * data directory is followed.
 * With root 0 the path is resolved as given. */

#define NORMFS_FS_TMP_EXCL 0
#define NORMFS_FS_TMP_TRUNC 1

/* open(O_WRONLY|O_CREAT|O_CLOEXEC, 0644) with O_EXCL or O_TRUNC, then fstat
 * for the inode number. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires mode == NORMFS_FS_TMP_EXCL || mode == NORMFS_FS_TMP_TRUNC;
    requires \valid(ino);
    requires \valid(os_error);
    requires \separated(ino, os_error, path + (0 .. path_len));
    assigns *ino, *os_error;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0 && *ino > 0;
    ensures \result == -1 ==> *os_error > 0;
*/
normfs_fs_fd normfs_fs_sys_open_create(const char *path, size_t path_len,
    size_t root, int mode, uint64_t *ino, int *os_error);

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
int normfs_fs_sys_pwritev_all(normfs_fs_fd fd, const struct normfs_fs_iov *iov,
    size_t cnt, uint64_t off, int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_fsync(normfs_fs_fd fd, int *os_error);

/* Not retried on EINTR: Linux releases the descriptor regardless, so a
 * retry could close one already handed to another thread. */
/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_close(normfs_fs_fd fd, int *os_error);

/* lstat(2): 1 with the length of a regular file, 0 when the name is absent,
 * -1 on any other failure. A name that is present but not a regular file is
 * a failure, because renaming over it would not replace a file. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(len);
    requires \valid(os_error);
    requires \separated(len, os_error, path + (0 .. path_len));
    assigns *len, *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_file_len(const char *path, size_t path_len, size_t root,
    uint64_t *len, int *os_error);

/*@ requires src_len < NORMFS_FS_PATH_MAX && dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(src + (0 .. src_len));
    requires \valid_read(dst + (0 .. dst_len));
    requires src[src_len] == 0 && dst[dst_len] == 0;
    requires root == 0 || (root < src_len && root < dst_len);
    requires \valid(os_error);
    requires \separated(os_error, src + (0 .. src_len), dst + (0 .. dst_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_rename(const char *src, size_t src_len, const char *dst,
    size_t dst_len, size_t root, int *os_error);

/* open(O_RDONLY|O_DIRECTORY) + fsync + close on the directory holding `path`:
 * everything up to the last '/' of path, or "." when there is none. With
 * root > 0 that is the directory normfs_fs_open_parent opens. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_fsync_parent(const char *path, size_t path_len, size_t root,
    int *os_error);

/*@ requires \valid(os_error);
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_ftruncate(normfs_fs_fd fd, uint64_t len, int *os_error);

/* unlink(2): 0 removed, 1 already absent, -1 on any other failure. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_unlink(const char *path, size_t path_len, size_t root,
    int *os_error);

/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_sync_dir(const char *path, size_t path_len, size_t root,
    int *os_error);

/* fstatat(2): 0 absent, 1 a regular file of *len bytes, 2 a directory, 3
 * anything else. A link at the name is followed only with follow set and root
 * 0, where the path is resolved as given. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(len);
    requires \valid(os_error);
    requires \separated(len, os_error, path + (0 .. path_len));
    assigns *len, *os_error;
    ensures -1 <= \result <= 3;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_stat(const char *path, size_t path_len, size_t root,
    int follow, uint64_t *len, int *os_error);

/* open(O_RDONLY or O_WRONLY, O_CLOEXEC) of an existing file, with O_NOFOLLOW
 * when root > 0. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
normfs_fs_fd normfs_fs_sys_open_file(const char *path, size_t path_len,
    size_t root, int write, int *os_error);

/* mkdir(2) with mode 0777 or rmdir(2). */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result == 0 || \result == -1;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_mkdir(const char *path, size_t path_len, size_t root,
    int remove, int *os_error);

/* A listing of the directory at path, or NULL. */
/*@ requires path_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires root == 0 || root < path_len;
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. path_len));
    assigns *os_error;
    ensures \result != \null ==> *os_error == 0;
    ensures \result == \null ==> *os_error > 0;
*/
void *normfs_fs_sys_list_open(const char *path, size_t path_len, size_t root,
    int *os_error);

/* The next entry other than . and .., with its kind as normfs_fs_sys_stat
 * gives it and the name in name[0 .. *name_len): 1 an entry, 0 the end. A
 * name gone before it was looked at is skipped. */
/*@ requires cap > 0 && \valid(name + (0 .. cap - 1));
    requires \valid(name_len) && \valid(kind) && \valid(len) && \valid(os_error);
    requires \separated(name + (0 .. cap - 1), name_len, kind, len, os_error);
    assigns name[0 .. cap - 1], *name_len, *kind, *len, *os_error;
    ensures -1 <= \result <= 1;
    ensures \result == 1 ==> *name_len <= cap && 1 <= *kind <= 3 && *os_error == 0;
    ensures \result == 0 ==> *os_error == 0;
    ensures \result == -1 ==> *os_error > 0;
*/
int normfs_fs_sys_list_next(void *list, char *name, size_t cap,
    size_t *name_len, int *kind, uint64_t *len, int *os_error);

/*@ assigns \nothing; */
void normfs_fs_sys_list_close(void *list);

/* ------------------------------------------------------------------------
 * The walk below the data directory, for normfs_fs_open_parent. Each open
 * adds one to normfs_fs_held and each release takes one: the count of opens
 * less releases, not which descriptor was released. */

/*@ ghost extern int normfs_fs_held; */

/*@ predicate fs_segment(char *s, integer n) =
      n > 0 && (\forall integer k; 0 <= k < n ==> s[k] != '/' && s[k] != 0) &&
      !(n == 1 && s[0] == '.') && !(n == 2 && s[0] == '.' && s[1] == '.');
*/

/* open(O_RDONLY|O_DIRECTORY|O_CLOEXEC) of path[0 .. len), following links. */
/*@ requires 0 < len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. len - 1));
    requires \valid(os_error);
    requires \separated(os_error, path + (0 .. len - 1));
    assigns *os_error, normfs_fs_held;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0 && normfs_fs_held == \old(normfs_fs_held) + 1;
    ensures \result == -1 ==> *os_error > 0 && normfs_fs_held == \old(normfs_fs_held);
*/
normfs_fs_fd normfs_fs_sys_open_root(const char *path, size_t len, int *os_error);

/* openat(dir, name, O_RDONLY|O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC). */
/*@ requires 0 < len < NORMFS_FS_PATH_MAX;
    requires \valid_read(name + (0 .. len - 1));
    requires fs_segment(name, len);
    requires \valid(os_error);
    requires \separated(os_error, name + (0 .. len - 1));
    assigns *os_error, normfs_fs_held;
    ensures \result >= 0 || \result == -1;
    ensures \result >= 0 ==> *os_error == 0 && normfs_fs_held == \old(normfs_fs_held) + 1;
    ensures \result == -1 ==> *os_error > 0 && normfs_fs_held == \old(normfs_fs_held);
*/
normfs_fs_fd normfs_fs_sys_open_below(normfs_fs_fd dir, const char *name,
    size_t len, int *os_error);

/* close(2); a walk has no use for its error. */
/*@ requires fd >= 0;
    assigns normfs_fs_held;
    ensures normfs_fs_held == \old(normfs_fs_held) - 1;
*/
void normfs_fs_sys_release(normfs_fs_fd fd);

#endif /* NORMFS_FS_SYS_H */
