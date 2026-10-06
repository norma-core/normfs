/*
 * The bodies behind normfs/fs_sys.h. Never given to Frama-C, and the only
 * file in the module that includes a system header.
 *
 * The completion functions are empty: they exist so the planner's calls to
 * them are the points where the kernel model advances, and WP reads their
 * contracts, not this file.
 *
 * -std=c99 sets __STRICT_ANSI__, which hides the POSIX declarations used
 * here; the macros below must precede every #include.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "normfs/fs_sys.h"
#include "normfs/fs_dir.h"

#if !defined(O_CLOEXEC)
#define O_CLOEXEC 0
#endif

void
normfs_fs_world_open_ok(const char *path, size_t path_len, uint64_t ino)
{
	(void)path;
	(void)path_len;
	(void)ino;
}

void
normfs_fs_world_write_ok(uint64_t ino, uint64_t n)
{
	(void)ino;
	(void)n;
}

void
normfs_fs_world_write_err(uint64_t ino)
{
	(void)ino;
}

void
normfs_fs_world_fsync_ok(uint64_t ino)
{
	(void)ino;
}

void
normfs_fs_world_fsync_err(uint64_t ino)
{
	(void)ino;
}

void
normfs_fs_world_rename_ok(const char *src, size_t src_len, const char *dst,
	size_t dst_len)
{
	(void)src;
	(void)src_len;
	(void)dst;
	(void)dst_len;
}

void
normfs_fs_world_fsync_dir_ok(const char *name, size_t name_len)
{
	(void)name;
	(void)name_len;
}

void
normfs_fs_world_fsync_dir_err(const char *name, size_t name_len)
{
	(void)name;
	(void)name_len;
}

void
normfs_fs_world_truncate_ok(uint64_t ino, uint64_t len)
{
	(void)ino;
	(void)len;
}

void
normfs_fs_world_unlink_ok(const char *path, size_t path_len)
{
	(void)path;
	(void)path_len;
}

/* A failing syscall that left errno at 0 would break the Rust side's
 * io::Error::from_raw_os_error, so normalise to EIO. */
static int
normfs_fs_sys_fail(int *os_error)
{
	int e = errno;

	*os_error = (e > 0) ? e : EIO;
	return -1;
}

int
normfs_fs_sys_open_create(const char *path, size_t path_len, int mode,
	uint64_t *ino, int *os_error)
{
	struct stat st;
	int flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_NOFOLLOW;
	int fd;

	(void)path_len;
	flags |= (mode == NORMFS_FS_TMP_TRUNC) ? O_TRUNC : O_EXCL;
	*os_error = 0;
	*ino = 0u;
	do {
		errno = 0;
		fd = open(path, flags, 0644);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0)
		return normfs_fs_sys_fail(os_error);
	if (fstat(fd, &st) != 0 || st.st_ino == 0) {
		int saved = errno;

		(void)close(fd);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	*ino = (uint64_t)st.st_ino;
	return fd;
}

int
normfs_fs_sys_pwritev_all(int fd, const struct normfs_fs_iov *iov,
	size_t cnt, uint64_t off, int *os_error)
{
	struct iovec vec[1024];
	size_t first = 0u;
	size_t i;

	*os_error = 0;
	for (i = 0u; i < cnt; i++) {
		vec[i].iov_base = (void *)iov[i].base;
		vec[i].iov_len = iov[i].len;
	}
	while (first < cnt) {
		ssize_t n;
		size_t left;

		errno = 0;
		n = pwritev(fd, &vec[first], (int)(cnt - first), (off_t)off);
		if (n < 0) {
			if (errno == EINTR)
				continue;
			return normfs_fs_sys_fail(os_error);
		}
		/* No progress would loop forever. POSIX permits it only in
		 * corners that do not apply to a regular file. */
		if (n == 0) {
			size_t total = 0u;

			for (i = first; i < cnt; i++)
				total += vec[i].iov_len;
			if (total == 0u)
				break;
			*os_error = EIO;
			return -1;
		}
		off += (uint64_t)n;
		left = (size_t)n;
		while (first < cnt && left >= vec[first].iov_len) {
			left -= vec[first].iov_len;
			first++;
		}
		if (first < cnt && left > 0u) {
			vec[first].iov_base = (char *)vec[first].iov_base + left;
			vec[first].iov_len -= left;
		}
	}
	return 0;
}

int
normfs_fs_sys_fsync(int fd, int *os_error)
{
	int rc;

	*os_error = 0;
#if defined(__APPLE__)
	/* fsync(2) here stops at the drive's cache; F_FULLFSYNC is what
	 * reaches the medium, and what Rust's sync_all does on this host. It
	 * is refused on some descriptors, and fsync is the fallback then. */
	do {
		errno = 0;
		rc = fcntl(fd, F_FULLFSYNC);
	} while (rc != 0 && errno == EINTR);
	if (rc == 0)
		return 0;
#endif
	do {
		errno = 0;
		rc = fsync(fd);
	} while (rc != 0 && errno == EINTR);
	if (rc != 0)
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_close(int fd, int *os_error)
{
	*os_error = 0;
	errno = 0;
	if (close(fd) != 0)
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_file_len(const char *path, size_t path_len, uint64_t *len,
	int *os_error)
{
	struct stat st;

	(void)path_len;
	*os_error = 0;
	*len = 0u;
	errno = 0;
	if (lstat(path, &st) != 0) {
		if (errno == ENOENT)
			return 0;
		return normfs_fs_sys_fail(os_error);
	}
	if (!S_ISREG(st.st_mode)) {
		*os_error = EEXIST;
		return -1;
	}
	*len = (uint64_t)st.st_size;
	return 1;
}

int
normfs_fs_sys_rename(const char *src, size_t src_len, const char *dst,
	size_t dst_len, int *os_error)
{
	(void)src_len;
	(void)dst_len;
	*os_error = 0;
	errno = 0;
	if (rename(src, dst) != 0)
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_fsync_parent(const char *path, size_t path_len, int *os_error)
{
	char dir[NORMFS_FS_PATH_MAX];
	size_t cut = path_len;
	int fd;
	int rc;

	*os_error = 0;
	while (cut > 0u && path[cut - 1u] != '/')
		cut--;
	if (cut == 0u) {
		dir[0] = '.';
		dir[1] = '\0';
	} else {
		/* Keep the slash so "/x" syncs "/", not "". */
		memcpy(dir, path, cut);
		dir[cut] = '\0';
	}
	do {
		errno = 0;
		fd = open(dir, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0)
		return normfs_fs_sys_fail(os_error);
	rc = normfs_fs_sys_fsync(fd, os_error);
	(void)close(fd);
	return rc;
}

int
normfs_fs_sys_ftruncate(int fd, uint64_t len, int *os_error)
{
	int rc;

	*os_error = 0;
	do {
		errno = 0;
		rc = ftruncate(fd, (off_t)len);
	} while (rc != 0 && errno == EINTR);
	if (rc != 0)
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_unlink(const char *path, size_t path_len, int *os_error)
{
	(void)path_len;
	*os_error = 0;
	errno = 0;
	if (unlink(path) != 0) {
		if (errno == ENOENT)
			return 1;
		return normfs_fs_sys_fail(os_error);
	}
	return 0;
}

int
normfs_fs_sys_sync_dir(const char *path, size_t path_len, int *os_error)
{
	int fd;
	int rc;
	(void)path_len;
    *os_error = 0;
	do {
		fd = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0)
		return normfs_fs_sys_fail(os_error);
	rc = normfs_fs_sys_fsync(fd, os_error);
	(void)close(fd);
	return rc;
}
