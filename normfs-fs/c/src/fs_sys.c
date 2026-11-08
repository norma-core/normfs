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

#include <dirent.h>
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

/* The directory to resolve the last segment from, and that segment. With
 * root 0 that is the working directory and the whole path. */
static int
normfs_fs_sys_at(const char *path, size_t path_len, size_t root, int *dir,
	const char **name, int *os_error)
{
	struct normfs_fs_status st;
	size_t at = 0u;

	if (root == 0u) {
		*dir = AT_FDCWD;
		*name = path;
		return 0;
	}
	st = normfs_fs_open_parent(path, path_len, root, dir, &at);
	if (st.code != 0) {
		*os_error = (st.code == 1) ? st.os_error : EINVAL;
		return -1;
	}
	*name = path + at;
	return 0;
}

static void
normfs_fs_sys_done(int dir)
{
	if (dir != AT_FDCWD)
		(void)close(dir);
}

normfs_fs_fd
normfs_fs_sys_open_create(const char *path, size_t path_len, size_t root,
	int mode, uint64_t *ino, int *os_error)
{
	struct stat st;
	int flags = O_WRONLY | O_CREAT | O_CLOEXEC | O_NOFOLLOW;
	const char *name;
	int dir;
	int fd;

	flags |= (mode == NORMFS_FS_TMP_TRUNC) ? O_TRUNC : O_EXCL;
	*os_error = 0;
	*ino = 0u;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0)
		return -1;
	do {
		errno = 0;
		fd = openat(dir, name, flags, 0644);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
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
normfs_fs_sys_pwritev_all(normfs_fs_fd fd, const struct normfs_fs_iov *iov,
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
normfs_fs_sys_fsync(normfs_fs_fd fd, int *os_error)
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
normfs_fs_sys_close(normfs_fs_fd fd, int *os_error)
{
	*os_error = 0;
	errno = 0;
	if (close(fd) != 0)
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_file_len(const char *path, size_t path_len, size_t root,
	uint64_t *len, int *os_error)
{
	struct stat st;
	const char *name;
	int dir;
	int rc;

	*os_error = 0;
	*len = 0u;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0) {
		if (*os_error != ENOENT)
			return -1;
		*os_error = 0;
		return 0;
	}
	errno = 0;
	rc = fstatat(dir, name, &st, AT_SYMLINK_NOFOLLOW);
	if (rc != 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		if (saved == ENOENT)
			return 0;
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	if (!S_ISREG(st.st_mode)) {
		*os_error = EEXIST;
		return -1;
	}
	*len = (uint64_t)st.st_size;
	return 1;
}

int
normfs_fs_sys_rename(const char *src, size_t src_len, const char *dst,
	size_t dst_len, size_t root, int *os_error)
{
	const char *from;
	const char *to;
	int src_dir;
	int dst_dir;
	int rc;

	*os_error = 0;
	if (normfs_fs_sys_at(src, src_len, root, &src_dir, &from, os_error) != 0)
		return -1;
	if (normfs_fs_sys_at(dst, dst_len, root, &dst_dir, &to, os_error) != 0) {
		normfs_fs_sys_done(src_dir);
		return -1;
	}
	errno = 0;
	rc = renameat(src_dir, from, dst_dir, to);
	if (rc != 0) {
		int saved = errno;

		normfs_fs_sys_done(src_dir);
		normfs_fs_sys_done(dst_dir);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(src_dir);
	normfs_fs_sys_done(dst_dir);
	return 0;
}

int
normfs_fs_sys_fsync_parent(const char *path, size_t path_len, size_t root,
	int *os_error)
{
	char dir[NORMFS_FS_PATH_MAX];
	const char *name;
	size_t cut = path_len;
	int fd;
	int rc;

	*os_error = 0;
	if (root != 0u) {
		if (normfs_fs_sys_at(path, path_len, root, &fd, &name, os_error) != 0)
			return -1;
		rc = normfs_fs_sys_fsync(fd, os_error);
		(void)close(fd);
		return rc;
	}
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
normfs_fs_sys_ftruncate(normfs_fs_fd fd, uint64_t len, int *os_error)
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
normfs_fs_sys_unlink(const char *path, size_t path_len, size_t root,
	int *os_error)
{
	const char *name;
	int dir;
	int rc;

	*os_error = 0;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0) {
		if (*os_error != ENOENT)
			return -1;
		*os_error = 0;
		return 1;
	}
	errno = 0;
	rc = unlinkat(dir, name, 0);
	if (rc != 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		if (saved == ENOENT)
			return 1;
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	return 0;
}

/* openat of a directory, refusing a link at the name when walked from root. */
static int
normfs_fs_sys_open_dir(const char *path, size_t path_len, size_t root,
	int *os_error)
{
	int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC;
	const char *name;
	int dir;
	int fd;

	if (root != 0u)
		flags |= O_NOFOLLOW;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0)
		return -1;
	do {
		errno = 0;
		fd = openat(dir, name, flags);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	return fd;
}

int
normfs_fs_sys_sync_dir(const char *path, size_t path_len, size_t root,
	int *os_error)
{
	int fd;
	int rc;

	*os_error = 0;
	fd = normfs_fs_sys_open_dir(path, path_len, root, os_error);
	if (fd < 0)
		return -1;
	rc = normfs_fs_sys_fsync(fd, os_error);
	(void)close(fd);
	return rc;
}

static int
normfs_fs_sys_kind(const struct stat *st, uint64_t *len)
{
	if (S_ISREG(st->st_mode)) {
		*len = (uint64_t)st->st_size;
		return 1;
	}
	if (S_ISDIR(st->st_mode))
		return 2;
	return 3;
}

int
normfs_fs_sys_stat(const char *path, size_t path_len, size_t root,
	int follow, uint64_t *len, int *os_error)
{
	struct stat st;
	const char *name;
	int dir;
	int rc;

	*os_error = 0;
	*len = 0u;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0) {
		if (*os_error != ENOENT)
			return -1;
		*os_error = 0;
		return 0;
	}
	errno = 0;
	rc = fstatat(dir, name, &st,
	    (follow && root == 0u) ? 0 : AT_SYMLINK_NOFOLLOW);
	if (rc != 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		if (saved == ENOENT)
			return 0;
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	return normfs_fs_sys_kind(&st, len);
}

normfs_fs_fd
normfs_fs_sys_open_file(const char *path, size_t path_len, size_t root,
	int write, int *os_error)
{
	int flags = (write ? O_WRONLY : O_RDONLY) | O_CLOEXEC;
	const char *name;
	int dir;
	int fd;

	*os_error = 0;
	if (root != 0u)
		flags |= O_NOFOLLOW;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0)
		return -1;
	do {
		errno = 0;
		fd = openat(dir, name, flags);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	return fd;
}

int
normfs_fs_sys_mkdir(const char *path, size_t path_len, size_t root,
	int remove, int *os_error)
{
	const char *name;
	int dir;
	int rc;

	*os_error = 0;
	if (normfs_fs_sys_at(path, path_len, root, &dir, &name, os_error) != 0)
		return -1;
	errno = 0;
	rc = remove ? unlinkat(dir, name, AT_REMOVEDIR) : mkdirat(dir, name, 0777);
	if (rc != 0) {
		int saved = errno;

		normfs_fs_sys_done(dir);
		errno = saved;
		return normfs_fs_sys_fail(os_error);
	}
	normfs_fs_sys_done(dir);
	return 0;
}

void *
normfs_fs_sys_list_open(const char *path, size_t path_len, size_t root,
	int *os_error)
{
	DIR *d;
	int fd;

	*os_error = 0;
	fd = normfs_fs_sys_open_dir(path, path_len, root, os_error);
	if (fd < 0)
		return NULL;
	errno = 0;
	d = fdopendir(fd);
	if (d == NULL) {
		int saved = errno;

		(void)close(fd);
		errno = saved;
		(void)normfs_fs_sys_fail(os_error);
		return NULL;
	}
	return d;
}

int
normfs_fs_sys_list_next(void *list, char *name, size_t cap, size_t *name_len,
	int *kind, uint64_t *len, int *os_error)
{
	DIR *d = list;
	struct dirent *ent;
	struct stat st;
	size_t n;

	*os_error = 0;
	for (;;) {
		errno = 0;
		ent = readdir(d);
		if (ent == NULL) {
			if (errno != 0)
				return normfs_fs_sys_fail(os_error);
			return 0;
		}
		if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
			continue;
		errno = 0;
		if (fstatat(dirfd(d), ent->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
			if (errno == ENOENT)
				continue;
			return normfs_fs_sys_fail(os_error);
		}
		n = strlen(ent->d_name);
		if (n > cap) {
			*os_error = ENAMETOOLONG;
			return -1;
		}
		memcpy(name, ent->d_name, n);
		*name_len = n;
		*len = 0u;
		*kind = normfs_fs_sys_kind(&st, len);
		return 1;
	}
}

void
normfs_fs_sys_list_close(void *list)
{
	(void)closedir(list);
}

/* Copies bytes the caller holds without a NUL. */
static int
normfs_fs_sys_terminated(char *buf, const char *path, size_t len, int *os_error)
{
	if (len >= NORMFS_FS_PATH_MAX) {
		*os_error = ENAMETOOLONG;
		return -1;
	}
	memcpy(buf, path, len);
	buf[len] = '\0';
	return 0;
}

normfs_fs_fd
normfs_fs_sys_open_root(const char *path, size_t len, int *os_error)
{
	char buf[NORMFS_FS_PATH_MAX];
	int fd;

	*os_error = 0;
	if (normfs_fs_sys_terminated(buf, path, len, os_error) != 0)
		return -1;
	do {
		errno = 0;
		fd = open(buf, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0)
		return normfs_fs_sys_fail(os_error);
	return fd;
}

normfs_fs_fd
normfs_fs_sys_open_below(normfs_fs_fd dir, const char *name, size_t len,
	int *os_error)
{
	char buf[NORMFS_FS_PATH_MAX];
	int fd;

	*os_error = 0;
	if (normfs_fs_sys_terminated(buf, name, len, os_error) != 0)
		return -1;
	do {
		errno = 0;
		fd = openat(dir, buf, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	} while (fd < 0 && errno == EINTR);
	if (fd < 0)
		return normfs_fs_sys_fail(os_error);
	return fd;
}

void
normfs_fs_sys_release(normfs_fs_fd fd)
{
	(void)close(fd);
}
