#include "normfs/fs_dir.h"

struct normfs_fs_status
normfs_fs_sync_created_dir(const char *path, size_t len, int *stage)
{
	int e = 0;
	struct normfs_fs_status result = {0, 0};

	if (*stage == 1) {
		if (normfs_fs_sys_sync_dir(path, len, &e) != 0) {
			result.code = 1;
			result.os_error = e;
			return result;
		}
		*stage = 2;
	}
	if (*stage == 2) {
		if (normfs_fs_sys_fsync_parent(path, len, &e) != 0) {
			result.code = 1;
			result.os_error = e;
			return result;
		}
		*stage = 3;
	}
	return result;
}
