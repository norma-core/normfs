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
#include "normfs/fs_dir.h"

/* A failing syscall that left errno at 0 would break the Rust side's
 * io::Error::from_raw_os_error, so normalise to EIO. */
static int
normfs_disk_sys_fail(int *os_error)
{
	int e = errno;

	*os_error = (e > 0) ? e : EIO;
	return -1;
}

/* The directory holding the last name of path and that name, walked from the
 * data directory as normfs/fs_sys.h does; AT_FDCWD and the whole path for root
 * 0. errno is set on failure. */
static int
normfs_disk_sys_at(const char *path, size_t path_len, size_t root, int *dir,
    const char **name)
{
	struct normfs_fs_status st;
	size_t at = 0u;

	if (root == 0u) {
		*dir = AT_FDCWD;
		*name = path;
		return 0;
	}
	if (root >= path_len || path_len >= (size_t)NORMFS_FS_PATH_MAX) {
		errno = EINVAL;
		return -1;
	}
	st = normfs_fs_open_parent(path, path_len, root, dir, &at);
	if (st.code != 0) {
		errno = (st.code == 1) ? st.os_error : EINVAL;
		return -1;
	}
	*name = path + at;
	return 0;
}

static void
normfs_disk_sys_done(int dir)
{
	int saved = errno;

	if (dir != AT_FDCWD)
		(void)close(dir);
	errno = saved;
}

void *
normfs_disk_sys_dir_open(const char *path, size_t path_len, size_t root,
    int *os_error)
{
	const char *name;
	DIR *d = NULL;
	int dir;
	int fd;

	*os_error = 0;

	errno = 0;
	if (normfs_disk_sys_at(path, path_len, root, &dir, &name) == 0) {
		fd = openat(dir, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC |
		    (root != 0u ? O_NOFOLLOW : 0));
		normfs_disk_sys_done(dir);
		if (fd >= 0) {
			d = fdopendir(fd);
			if (d == NULL) {
				int saved = errno;

				(void)close(fd);
				errno = saved;
			}
		}
	}
	if (d == NULL) {
		if (errno != ENOENT && errno != ENOTDIR && errno != ELOOP)
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
normfs_disk_sys_file_size(const char *path, size_t path_len, size_t root,
    uint64_t *size, int *os_error)
{
	struct stat st;
	const char *name;
	int dir;
	int rc;

	*os_error = 0;
	*size = 0u;

	errno = 0;
	rc = normfs_disk_sys_at(path, path_len, root, &dir, &name);
	if (rc == 0) {
		rc = fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW);
		normfs_disk_sys_done(dir);
	}
	if (rc != 0) {
		if (errno == ENOENT || errno == ENOTDIR || errno == ELOOP)
			return 0;
		return normfs_disk_sys_fail(os_error);
	}

	if (!S_ISREG(st.st_mode))
		return 0;

	*size = (st.st_size > 0) ? (uint64_t)st.st_size : 0u;
	return 1;
}

int
normfs_disk_sys_unlink(const char *path, size_t path_len, size_t root,
    int *os_error)
{
	const char *name;
	int dir;
	int rc;

	*os_error = 0;

	errno = 0;
	rc = normfs_disk_sys_at(path, path_len, root, &dir, &name);
	if (rc == 0) {
		rc = unlinkat(dir, name, 0);
		normfs_disk_sys_done(dir);
	}
	if (rc != 0)
		return normfs_disk_sys_fail(os_error);

	return 0;
}
