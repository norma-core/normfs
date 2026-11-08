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

struct normfs_fs_status
normfs_fs_exact_names(const char *path, size_t len, size_t from)
{
	struct normfs_fs_status result = {0, 0};
	size_t i = from;
	int e = 0;

	/*@ loop invariant from <= i <= len;
	    loop invariant i == len || path[i] == '/';
	    loop invariant \forall integer s, f; fs_segment(path, from, len, s, f) && f <= i ==>
	                     fs_lists(path, s - 1, path + s, f - s);
	    loop assigns i, e;
	    loop variant len - i;
	*/
	while (i < len) {
		size_t s = i + 1u;
		size_t end = s;
		int r;

		/*@ loop invariant s <= end <= len;
		    loop invariant \forall integer k; s <= k < end ==> path[k] != '/';
		    loop assigns end;
		    loop variant len - end;
		*/
		while (end < len && path[end] != '/')
			end++;
		if (end > s) {
			r = normfs_fs_sys_dir_lists(path, s - 1u, path + s, end - s, &e);
			if (r < 0) {
				result.code = 1;
				result.os_error = e;
				return result;
			}
			if (r == 0) {
				r = normfs_fs_sys_exists(path, end, &e);
				if (r < 0) {
					result.code = 1;
					result.os_error = e;
				} else if (r == 1) {
					result.code = 2;
				}
				return result;
			}
		}
		i = end;
	}
	return result;
}
