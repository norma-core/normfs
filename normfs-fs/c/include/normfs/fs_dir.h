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
    int *stage);

/*@ predicate fs_segment(char *p, integer from, integer len, integer s, integer e) =
      from < s < e <= len && p[s - 1] == '/' && (e == len || p[e] == '/') &&
      \forall integer k; s <= k < e ==> p[k] != '/';
*/

/* path is a root of from bytes, then '/'-separated names. Code 2 when a name
 * that exists is spelled otherwise in its directory, as a/B is for a/b on a
 * filesystem that folds case; 0 when each is listed as given, up to the first
 * that is absent; 1 with os_error on a failed read. */
/*@ requires 0 < from <= len < NORMFS_FS_PATH_MAX;
    requires \valid_read(path + (0 .. len - 1));
    requires from == len || path[from] == '/';
    assigns \nothing;
    ensures \result.code == 0 || \result.code == 1 || \result.code == 2;
    ensures \result.code == 1 <==> \result.os_error > 0;
    ensures \result.code != 1 ==> \result.os_error == 0;
    ensures \result.code == 2 ==>
              \exists integer s, e; fs_segment(path, from, len, s, e) &&
                !fs_lists(path, s - 1, path + s, e - s) && fs_exists(path, e);
    ensures \result.code == 0 ==>
              \forall integer s, e; fs_segment(path, from, len, s, e) ==>
                fs_lists(path, s - 1, path + s, e - s) ||
                \exists integer f; from < f <= e && (f == len || path[f] == '/') &&
                  !fs_exists(path, f);
*/
struct normfs_fs_status normfs_fs_exact_names(const char *path, size_t len, size_t from);

#endif
