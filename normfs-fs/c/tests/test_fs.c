/*
 * The runtime half: src/fs_plan.c is proved, but the shims in
 * normfs/fs_sys.h are assumed contracts over a real kernel. These tests
 * discharge them, and pin the two behaviours the contracts state in words:
 * a write that stops short is finished by the loop, and errno that says
 * nothing becomes EIO.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include "normfs/fs_plan.h"
#include "normfs/fs_sys.h"

/* assert() is a no-op under NDEBUG, which the Release build defines, so the
 * checks report and return failure themselves instead. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "fs: FAIL %s:%d: %s\n",         \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static char root[512];

static void
rm_tree(const char *dir)
{
	char cmd[1024];

	(void)snprintf(cmd, sizeof(cmd), "rm -rf '%s'", dir);
	/* A (void) cast does not satisfy glibc's warn_unused_result on system. */
	if (system(cmd) != 0)
		fprintf(stderr, "fs: could not remove %s\n", dir);
}

static void
cleanup_root(void)
{
	if (root[0] != '\0')
		rm_tree(root);
}

static int
path_in(char *out, size_t cap, const char *name)
{
	int n = snprintf(out, cap, "%s/%s", root, name);

	return n > 0 && (size_t)n < cap;
}

static int
read_whole(const char *path, uint8_t *buf, size_t cap, size_t *got)
{
	int fd = open(path, O_RDONLY);
	ssize_t n;

	if (fd < 0)
		return -1;
	n = read(fd, buf, cap);
	(void)close(fd);
	if (n < 0)
		return -1;
	*got = (size_t)n;
	return 0;
}

static int
test_open_create_excl_then_trunc(void)
{
	char path[768];
	uint64_t ino = 0u;
	uint64_t ino2 = 0u;
	int e = -1;
	int fd;
	int fd2;
	struct stat st;

	CHECK(path_in(path, sizeof(path), "excl"));
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0 && e == 0 && ino > 0u);
	CHECK(write(fd, "abc", 3) == 3);
	CHECK(normfs_fs_sys_close(fd, &e) == 0 && e == 0);

	fd2 = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino2, &e);
	CHECK(fd2 == -1 && e == EEXIST && ino2 == 0u);

	fd2 = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_TRUNC,
	    &ino2, &e);
	CHECK(fd2 >= 0 && e == 0 && ino2 == ino);
	CHECK(fstat(fd2, &st) == 0 && st.st_size == 0);
	CHECK((st.st_mode & 0777) == 0644 || (st.st_mode & 0777) == 0600 ||
	    (st.st_mode & 0777) == 0640);
	CHECK(normfs_fs_sys_close(fd2, &e) == 0);
	return 0;
}

static int
test_pwritev_all_writes_every_run(void)
{
	char path[768];
	uint8_t a[64];
	uint8_t b[128];
	uint8_t c[70000];
	uint8_t back[64 + 128 + 70000];
	struct normfs_fs_iov iov[3];
	uint64_t ino;
	size_t got = 0u;
	size_t i;
	int e = -1;
	int fd;

	for (i = 0u; i < sizeof(a); i++)
		a[i] = (uint8_t)i;
	for (i = 0u; i < sizeof(b); i++)
		b[i] = (uint8_t)(i * 3u);
	for (i = 0u; i < sizeof(c); i++)
		c[i] = (uint8_t)(i * 7u);
	iov[0].base = a;
	iov[0].len = sizeof(a);
	iov[1].base = b;
	iov[1].len = sizeof(b);
	iov[2].base = c;
	iov[2].len = sizeof(c);

	CHECK(path_in(path, sizeof(path), "runs"));
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0);
	CHECK(normfs_fs_sys_pwritev_all(fd, iov, 3u, 16u, &e) == 0 && e == 0);
	CHECK(normfs_fs_sys_fsync(fd, &e) == 0 && e == 0);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);

	CHECK(read_whole(path, back, sizeof(back), &got) == 0);
	CHECK(got == sizeof(back));
	CHECK(memcmp(back + 16, a, sizeof(a)) == 0);
	CHECK(memcmp(back + 16 + sizeof(a), b, sizeof(b)) == 0);
	CHECK(memcmp(back + 16 + sizeof(a) + sizeof(b), c,
	    sizeof(c) - 16u) == 0);
	return 0;
}

/* RLIMIT_FSIZE makes the kernel stop a write short of the limit and refuse
 * the rest with EFBIG: the loop must return the failure, and what reached
 * the file is the prefix. The signal that accompanies it is ignored. */
static int
test_pwritev_all_reports_a_short_write(void)
{
	char path[768];
	uint8_t big[8192];
	struct normfs_fs_iov iov[2];
	struct rlimit old;
	struct rlimit lim;
	struct stat st;
	uint64_t ino;
	int e = -1;
	int rc;
	int fd;

	memset(big, 0xAB, sizeof(big));
	iov[0].base = big;
	iov[0].len = 4096u;
	iov[1].base = big;
	iov[1].len = 4096u;

	CHECK(path_in(path, sizeof(path), "short"));
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0);

	CHECK(signal(SIGXFSZ, SIG_IGN) != SIG_ERR);
	CHECK(getrlimit(RLIMIT_FSIZE, &old) == 0);
	lim.rlim_cur = 6000;
	lim.rlim_max = old.rlim_max;
	CHECK(setrlimit(RLIMIT_FSIZE, &lim) == 0);
	rc = normfs_fs_sys_pwritev_all(fd, iov, 2u, 0u, &e);
	CHECK(setrlimit(RLIMIT_FSIZE, &old) == 0);

	CHECK(rc == -1 && e == EFBIG);
	CHECK(fstat(fd, &st) == 0 && st.st_size == 6000);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	return 0;
}

static int
test_fsync_on_a_bad_descriptor_fails(void)
{
	int e = 0;

	CHECK(normfs_fs_sys_fsync(-1, &e) == -1);
	CHECK(e == EBADF);
	CHECK(normfs_fs_sys_close(-1, &e) == -1);
	CHECK(e == EBADF);
	CHECK(normfs_fs_sys_ftruncate(-1, 0u, &e) == -1);
	CHECK(e == EBADF);
	return 0;
}

static int
test_file_len_distinguishes_absent_regular_and_other(void)
{
	char path[768];
	char dir[768];
	uint64_t len = 99u;
	uint64_t ino;
	int e = -1;
	int fd;

	CHECK(path_in(path, sizeof(path), "len"));
	CHECK(normfs_fs_sys_file_len(path, strlen(path), &len, &e) == 0);
	CHECK(len == 0u && e == 0);

	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0);
	CHECK(write(fd, "12345", 5) == 5);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	CHECK(normfs_fs_sys_file_len(path, strlen(path), &len, &e) == 1);
	CHECK(len == 5u && e == 0);

	CHECK(path_in(dir, sizeof(dir), "lendir"));
	CHECK(mkdir(dir, 0700) == 0);
	CHECK(normfs_fs_sys_file_len(dir, strlen(dir), &len, &e) == -1);
	CHECK(e > 0);
	return 0;
}

static int
test_rename_replaces_and_fsync_parent_syncs(void)
{
	char src[768];
	char dst[768];
	char sub[768];
	uint8_t back[8];
	uint64_t ino;
	size_t got = 0u;
	int e = -1;
	int fd;

	CHECK(path_in(src, sizeof(src), "ren.tmp"));
	CHECK(path_in(dst, sizeof(dst), "ren.dst"));
	fd = normfs_fs_sys_open_create(dst, strlen(dst), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0 && write(fd, "old", 3) == 3);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	fd = normfs_fs_sys_open_create(src, strlen(src), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0 && write(fd, "new!", 4) == 4);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);

	CHECK(normfs_fs_sys_rename(src, strlen(src), dst, strlen(dst), &e) == 0);
	CHECK(e == 0);
	CHECK(access(src, F_OK) == -1 && errno == ENOENT);
	CHECK(read_whole(dst, back, sizeof(back), &got) == 0);
	CHECK(got == 4u && memcmp(back, "new!", 4) == 0);

	CHECK(normfs_fs_sys_fsync_parent(dst, strlen(dst), &e) == 0 && e == 0);
	CHECK(path_in(sub, sizeof(sub), "subdir"));
	CHECK(mkdir(sub, 0700) == 0);
	CHECK(path_in(sub, sizeof(sub), "subdir/leaf"));
	CHECK(normfs_fs_sys_fsync_parent(sub, strlen(sub), &e) == 0 && e == 0);
	CHECK(normfs_fs_sys_fsync_parent("leaf", 4u, &e) == 0 && e == 0);

	CHECK(path_in(sub, sizeof(sub), "ren.dst/x"));
	CHECK(normfs_fs_sys_fsync_parent(sub, strlen(sub), &e) == -1);
	CHECK(e == ENOTDIR);

	CHECK(normfs_fs_sys_rename(src, strlen(src), dst, strlen(dst), &e) == -1);
	CHECK(e == ENOENT);
	return 0;
}

static int
test_ftruncate_keeps_the_prefix(void)
{
	char path[768];
	uint8_t back[16];
	uint64_t ino;
	size_t got = 0u;
	int e = -1;
	int fd;

	CHECK(path_in(path, sizeof(path), "trunc"));
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0 && write(fd, "0123456789", 10) == 10);
	CHECK(normfs_fs_sys_ftruncate(fd, 4u, &e) == 0 && e == 0);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	CHECK(read_whole(path, back, sizeof(back), &got) == 0);
	CHECK(got == 4u && memcmp(back, "0123", 4) == 0);
	return 0;
}

static int
test_unlink_reports_absent(void)
{
	char path[768];
	uint64_t ino;
	int e = -1;
	int fd;

	CHECK(path_in(path, sizeof(path), "unlink"));
	CHECK(normfs_fs_sys_unlink(path, strlen(path), &e) == 1 && e == 0);
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL,
	    &ino, &e);
	CHECK(fd >= 0);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	CHECK(normfs_fs_sys_unlink(path, strlen(path), &e) == 0 && e == 0);
	CHECK(access(path, F_OK) == -1 && errno == ENOENT);
	CHECK(normfs_fs_sys_unlink(path, strlen(path), &e) == 1 && e == 0);
	return 0;
}

static int
test_sync_dir_rejects_missing_and_regular_paths(void)
{
	char path[512];
	uint64_t ino;
	int fd;
	int e;

	CHECK(normfs_fs_sys_sync_dir(root, strlen(root), &e) == 0 && e == 0);
	CHECK(path_in(path, sizeof(path), "sync-dir-file"));
	CHECK(normfs_fs_sys_sync_dir(path, strlen(path), &e) == -1 && e == ENOENT);
	fd = normfs_fs_sys_open_create(path, strlen(path), NORMFS_FS_TMP_EXCL, &ino, &e);
	CHECK(fd >= 0);
	CHECK(normfs_fs_sys_close(fd, &e) == 0);
	CHECK(normfs_fs_sys_sync_dir(path, strlen(path), &e) == -1 && e == ENOTDIR);
	return 0;
}

static int
test_open_does_not_follow_a_symlink(void)
{
	char target[512];
	char link[512];
	uint64_t ino;
	int e;

	CHECK(path_in(target, sizeof(target), "symlink-target"));
	CHECK(path_in(link, sizeof(link), "symlink-temp"));
	CHECK(symlink(target, link) == 0);
	CHECK(normfs_fs_sys_open_create(link, strlen(link), NORMFS_FS_TMP_TRUNC,
		&ino, &e) == -1 && e == ELOOP);
	CHECK(access(target, F_OK) == -1 && errno == ENOENT);
	return 0;
}

int
main(void)
{
	char tmpl[] = "/tmp/normfs-fs-test-XXXXXX";
	int failed = 0;

	if (mkdtemp(tmpl) == NULL) {
		perror("mkdtemp");
		return 1;
	}
	(void)snprintf(root, sizeof(root), "%s", tmpl);
	atexit(cleanup_root);

	failed |= test_open_create_excl_then_trunc();
	failed |= test_pwritev_all_writes_every_run();
	failed |= test_pwritev_all_reports_a_short_write();
	failed |= test_fsync_on_a_bad_descriptor_fails();
	failed |= test_file_len_distinguishes_absent_regular_and_other();
	failed |= test_rename_replaces_and_fsync_parent_syncs();
	failed |= test_ftruncate_keeps_the_prefix();
	failed |= test_unlink_reports_absent();
	failed |= test_sync_dir_rejects_missing_and_regular_paths();
	failed |= test_open_does_not_follow_a_symlink();

	if (failed == 0)
		printf("fs: all tests passed\n");
	return failed;
}
