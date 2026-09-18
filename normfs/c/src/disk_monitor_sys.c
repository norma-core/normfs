/*
 * Bodies behind normfs/disk_monitor_sys.h; never given to Frama-C. The
 * macros must precede every #include: -std=c99 hides the POSIX declarations.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "normfs/disk_monitor_sys.h"

/* A failing syscall that left errno at 0 would break the Rust side's
 * io::Error::from_raw_os_error, so normalise to EIO. */
static int
normfs_disk_sys_fail(int *os_error)
{
	int e = errno;

	*os_error = (e > 0) ? e : EIO;
	return -1;
}

void *
normfs_disk_sys_dir_open(const char *path, size_t path_len, int *os_error)
{
	DIR *d;

	(void)path_len;
	*os_error = 0;

	errno = 0;
	d = opendir(path);
	if (d == NULL) {
		if (errno != ENOENT && errno != ENOTDIR)
			(void)normfs_disk_sys_fail(os_error);
		return NULL;
	}

	return d;
}

int
normfs_disk_sys_dir_next(void *handle, char *name, size_t name_cap,
    size_t *name_len, int *kind, uint64_t *size, int *os_error)
{
	DIR *d = handle;
	struct dirent *ent;
	struct stat st;
	size_t len;

	*os_error = 0;
	*kind = NORMFS_DISK_SYS_OTHER;
	*size = 0u;
	*name_len = 0u;
	name[0] = '\0';

	for (;;) {
		/* readdir reports the end and a failure the same way; errno
		 * tells them apart, so it is cleared first. */
		errno = 0;
		ent = readdir(d);
		if (ent == NULL) {
			if (errno != 0)
				return normfs_disk_sys_fail(os_error);
			return 0;
		}

		if (ent->d_name[0] == '.' &&
		    (ent->d_name[1] == '\0' ||
		     (ent->d_name[1] == '.' && ent->d_name[2] == '\0')))
			continue;

		len = strlen(ent->d_name);
		if (len >= name_cap) {
			memcpy(name, ent->d_name, name_cap - 1u);
			name[name_cap - 1u] = '\0';
			*name_len = name_cap - 1u;
			return 1;
		}

		errno = 0;
		if (fstatat(dirfd(d), ent->d_name, &st,
		    AT_SYMLINK_NOFOLLOW) != 0) {
			if (errno == ENOENT)
				continue;
			return normfs_disk_sys_fail(os_error);
		}

		memcpy(name, ent->d_name, len + 1u);
		*name_len = len;
		if (S_ISREG(st.st_mode)) {
			*kind = NORMFS_DISK_SYS_FILE;
			*size = (st.st_size > 0) ? (uint64_t)st.st_size : 0u;
		} else if (S_ISDIR(st.st_mode)) {
			*kind = NORMFS_DISK_SYS_DIR;
		}
		return 1;
	}
}

void
normfs_disk_sys_dir_close(void *handle)
{
	(void)closedir((DIR *)handle);
}

int
normfs_disk_sys_file_size(const char *path, size_t path_len, uint64_t *size,
    int *os_error)
{
	struct stat st;

	(void)path_len;
	*os_error = 0;
	*size = 0u;

	errno = 0;
	if (lstat(path, &st) != 0) {
		if (errno == ENOENT || errno == ENOTDIR)
			return 0;
		return normfs_disk_sys_fail(os_error);
	}

	if (!S_ISREG(st.st_mode))
		return 0;

	*size = (st.st_size > 0) ? (uint64_t)st.st_size : 0u;
	return 1;
}

int
normfs_disk_sys_unlink(const char *path, size_t path_len, int *os_error)
{
	(void)path_len;
	*os_error = 0;

	errno = 0;
	if (unlink(path) != 0)
		return normfs_disk_sys_fail(os_error);

	return 0;
}
