#ifndef NORMFS_FS_DIR_H
#define NORMFS_FS_DIR_H

#include "normfs/fs_sys.h"

struct normfs_fs_status {
	int code;
	int os_error;
};

/* The caller retains the stage on failure and serializes creation through
 * completion. Stage 1 follows mkdir; stage 2 follows the new directory's sync. */
/*@ requires len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. len));
    requires path[len] == 0;
    requires root == 0 || root < len;
    requires \valid(stage);
    requires \separated(stage, path + (0 .. len));
    requires 1 <= *stage <= 3;
    assigns *stage;
    ensures \result.code == 0 || \result.code == 1;
    ensures \result.code == 0 ==> \result.os_error == 0 && *stage == 3;
    ensures \result.code == 1 ==> \result.os_error > 0 && *stage < 3;
    ensures \old(*stage) <= *stage <= 3;
*/
struct normfs_fs_status normfs_fs_sync_created_dir(const char *path, size_t len,
    size_t root, int *stage);

/*@ predicate fs_piece(char *p, integer root, integer len, integer a, integer b) =
      root < a <= b <= len && p[a - 1] == '/' && (b == len || p[b] == '/') &&
      \forall integer k; a <= k < b ==> p[k] != '/';

    // Every piece between slashes is empty or a segment, the last one not empty.
    predicate fs_walkable(char *p, integer root, integer len) =
      p[root] == '/' && p[len - 1] != '/' &&
      \forall integer a, b; fs_piece(p, root, len, a, b) ==>
        a == b || fs_segment(p + a, b - a);
*/

/* Opens the directory holding the last segment of path[0 .. len), from the
 * data directory path[0 .. root) down, one segment at a time; *name is where
 * the last segment starts. Empty segments are skipped. Code 1 is an OS error,
 * 2 a path that is not root, '/', then segments. */
/*@ requires 0 < root < len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. len - 1));
    requires \valid(dir) && \valid(name);
    requires \separated(dir, name, path + (0 .. len - 1));
    assigns *dir, *name, normfs_fs_held;
    ensures 0 <= \result.code <= 2;
    ensures \result.code == 2 <==> !fs_walkable(path, root, len);
    ensures \result.code == 0 ==> \result.os_error == 0 && *dir >= 0 &&
              root < *name < len && path[*name - 1] == '/' &&
              fs_segment(path + *name, len - *name) &&
              normfs_fs_held == \old(normfs_fs_held) + 1;
    ensures \result.code == 1 ==> \result.os_error > 0;
    ensures \result.code == 2 ==> \result.os_error == 0;
    ensures \result.code != 0 ==> normfs_fs_held == \old(normfs_fs_held);
*/
struct normfs_fs_status normfs_fs_open_parent(const char *path, size_t len,
    size_t root, normfs_fs_fd *dir, size_t *name);

#endif
