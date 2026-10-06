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

#endif
