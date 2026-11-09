#include "normfs/fs_dir.h"

struct normfs_fs_status
normfs_fs_sync_created_dir(const char *path, size_t len, size_t root, int *stage)
{
	int e = 0;
	struct normfs_fs_status result = {0, 0};

	if (*stage == 1) {
		if (normfs_fs_sys_sync_dir(path, len, root, &e) != 0) {
			result.code = 1;
			result.os_error = e;
			return result;
		}
		*stage = 2;
	}
	if (*stage == 2) {
		if (normfs_fs_sys_fsync_parent(path, len, root, &e) != 0) {
			result.code = 1;
			result.os_error = e;
			return result;
		}
		*stage = 3;
	}
	return result;
}

/*@ requires 0 < n < NORMFS_FS_PATH_MAX;
    requires \valid_read(seg + (0 .. n - 1));
    assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==> fs_segment(seg, n);
*/
static int
normfs_fs_is_segment(const char *seg, size_t n)
{
	size_t k;

	/*@ loop invariant 0 <= k <= n;
	    loop invariant \forall integer j; 0 <= j < k ==> seg[j] != '/' && seg[j] != 0;
	    loop assigns k;
	    loop variant n - k;
	*/
	for (k = 0u; k < n; k++) {
		if (seg[k] == '/' || seg[k] == 0)
			return 0;
	}
	if (seg[0] == '.' && (n == 1u || (n == 2u && seg[1] == '.')))
		return 0;
	return 1;
}

struct normfs_fs_status
normfs_fs_open_parent(const char *path, size_t len, size_t root,
    normfs_fs_fd *dir, size_t *name)
{
	struct normfs_fs_status result = {0, 0};
	normfs_fs_fd d;
	size_t last = len;
	size_t i;
	int e = 0;

	if (path[root] != '/') {
		result.code = 2;
		return result;
	}
	/*@ loop invariant root + 1 <= last <= len;
	    loop invariant \forall integer j; last <= j < len ==> path[j] != '/';
	    loop assigns last;
	    loop variant last;
	*/
	while (last > root + 1u && path[last - 1u] != '/')
		last--;
	if (last == len) {
		/*@ assert path[len - 1] == '/'; */
		result.code = 2;
		return result;
	}
	/*@ assert path[last - 1] == '/'; */
	if (!normfs_fs_is_segment(path + last, len - last)) {
		/*@ assert fs_piece(path, root, len, last, len); */
		result.code = 2;
		return result;
	}
	i = root + 1u;
	/*@ loop invariant root < i <= last;
	    loop invariant path[i - 1] == '/' || path[i] == '/';
	    loop invariant \forall integer a, b; fs_piece(path, root, len, a, b) && b <= i &&
	                     b < len ==> a == b || fs_segment(path + a, b - a);
	    loop assigns i;
	    loop variant last - i;
	*/
	while (i < last) {
		size_t s = i;

		/*@ loop invariant s <= i <= last;
		    loop invariant \forall integer k; s <= k < i ==> path[k] != '/';
		    loop assigns i;
		    loop variant last - i;
		*/
		while (i < last && path[i] != '/')
			i++;
		if (i == s) {
			i++;
			continue;
		}
		/*@ assert path[i] == '/' && path[s - 1] == '/'; */
		if (!normfs_fs_is_segment(path + s, i - s)) {
			/*@ assert fs_piece(path, root, len, s, i); */
			result.code = 2;
			return result;
		}
	}
	/*@ assert fs_walkable(path, root, len); */
	d = normfs_fs_sys_open_root(path, root, &e);
	if (d == -1) {
		result.code = 1;
		result.os_error = e;
		return result;
	}
	i = root + 1u;
	/*@ loop invariant root < i <= last;
	    loop invariant path[i - 1] == '/' || path[i] == '/';
	    loop invariant d >= 0;
	    loop invariant normfs_fs_held == \at(normfs_fs_held, Pre) + 1;
	    loop assigns i, d, e, normfs_fs_held;
	    loop variant last - i;
	*/
	while (i < last) {
		size_t s = i;
		normfs_fs_fd next;

		/*@ loop invariant s <= i <= last;
		    loop invariant \forall integer k; s <= k < i ==> path[k] != '/';
		    loop assigns i;
		    loop variant last - i;
		*/
		while (i < last && path[i] != '/')
			i++;
		if (i == s) {
			i++;
			continue;
		}
		/*@ assert fs_piece(path, root, len, s, i); */
		next = normfs_fs_sys_open_below(d, path + s, i - s, &e);
		normfs_fs_sys_release(d);
		if (next == -1) {
			result.code = 1;
			result.os_error = e;
			return result;
		}
		d = next;
	}
	*dir = d;
	*name = last;
	return result;
}
