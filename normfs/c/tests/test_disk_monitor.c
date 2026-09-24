/*
 * The runtime half: src/disk_monitor.c is proved, but every syscall it
 * reaches is an assumed contract in normfs/disk_monitor_sys.h. These tests
 * discharge them against a real tree, and pin the layout bytes the ACSL
 * sizes but does not spell.
 */
#define _POSIX_C_SOURCE 200809L
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "normfs/disk_monitor.h"
#include "normfs/disk_monitor_sys.h"

/* assert() is a no-op under NDEBUG, which the Release build defines. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "disk_monitor: FAIL %s:%d: %s\n", \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static char root[512];

static void
rm_tree(const char *path)
{
	DIR *d = opendir(path);
	struct dirent *ent;
	struct stat st;
	char child[1024];

	if (d == NULL) {
		(void)unlink(path);
		return;
	}
	while ((ent = readdir(d)) != NULL) {
		if (strcmp(ent->d_name, ".") == 0 ||
		    strcmp(ent->d_name, "..") == 0)
			continue;
		(void)snprintf(child, sizeof(child), "%s/%s", path,
		    ent->d_name);
		(void)chmod(child, 0700);
		if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode))
			rm_tree(child);
		else
			(void)unlink(child);
	}
	(void)closedir(d);
	(void)rmdir(path);
}

static void
cleanup_root(void)
{
	if (root[0] != '\0')
		rm_tree(root);
}

static struct normfs_disk_id
id(const char *hex)
{
	struct normfs_disk_id out;

	memset(&out, 0, sizeof(out));
	out.len = strlen(hex);
	memcpy(out.hex, hex, out.len);
	return out;
}

static int
id_is(const struct normfs_disk_id *a, const char *hex)
{
	return a->len == strlen(hex) && memcmp(a->hex, hex, a->len) == 0;
}

static int
mkdir_p(const char *path)
{
	char buf[1024];
	size_t i;

	(void)snprintf(buf, sizeof(buf), "%s", path);
	for (i = 1u; buf[i] != '\0'; i++) {
		if (buf[i] == '/') {
			buf[i] = '\0';
			if (mkdir(buf, 0700) != 0 && errno != EEXIST)
				return -1;
			buf[i] = '/';
		}
	}
	if (mkdir(buf, 0700) != 0 && errno != EEXIST)
		return -1;
	return 0;
}

static int
put_file(const char *dir, const char *hex, int kind, size_t len)
{
	struct normfs_disk_id i = id(hex);
	struct normfs_disk_result r;
	char path[1024];
	char parent[1024];
	size_t used = 0u;
	char *slash;
	int fd;

	r = normfs_disk_path(dir, strlen(dir), &i, kind, path, sizeof(path),
	    &used);
	if (r.status != NORMFS_DISK_OK)
		return -1;

	(void)snprintf(parent, sizeof(parent), "%s", path);
	slash = strrchr(parent, '/');
	if (slash != NULL) {
		*slash = '\0';
		if (mkdir_p(parent) != 0)
			return -1;
	}

	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0)
		return -1;
	while (len > 0u) {
		ssize_t n = write(fd, "x", 1);

		if (n != 1) {
			(void)close(fd);
			return -1;
		}
		len--;
	}
	return close(fd);
}

static int
file_exists(const char *dir, const char *hex, int kind)
{
	struct normfs_disk_id i = id(hex);
	char path[1024];
	size_t used = 0u;
	struct stat st;

	if (normfs_disk_path(dir, strlen(dir), &i, kind, path, sizeof(path),
	    &used).status != NORMFS_DISK_OK)
		return -1;
	return lstat(path, &st) == 0;
}

static int
path_is(const char *dir, const char *hex, int kind, const char *want)
{
	struct normfs_disk_id i = id(hex);
	char out[256];
	size_t used = 1u;
	struct normfs_disk_result r;

	r = normfs_disk_path(dir, strlen(dir), &i, kind, out, sizeof(out),
	    &used);
	return r.status == NORMFS_DISK_OK && r.os_error == 0 &&
	    used == strlen(want) && out[used] == '\0' &&
	    strcmp(out, want) == 0;
}

static int
test_path_pins_layout(void)
{
	CHECK(path_is("/q", "0", NORMFS_DISK_STORE, "/q/000.store"));
	CHECK(path_is("/q", "1", NORMFS_DISK_STORE, "/q/001.store"));
	CHECK(path_is("/q", "fff", NORMFS_DISK_STORE, "/q/fff.store"));
	CHECK(path_is("/q", "1000", NORMFS_DISK_STORE, "/q/001/000.store"));
	CHECK(path_is("/q", "abcdef", NORMFS_DISK_WAL, "/q/abc/def.wal"));
	CHECK(path_is("/q", "12345678", NORMFS_DISK_STORE,
	    "/q/012/345/678.store"));
	CHECK(path_is("/q/", "1", NORMFS_DISK_STORE, "/q/001.store"));
	/* Matches Rust's Path::new("").join(): relative, not rooted. */
	CHECK(path_is("", "1", NORMFS_DISK_WAL, "001.wal"));
	CHECK(path_is("/", "1", NORMFS_DISK_WAL, "/001.wal"));
	return 0;
}

static int
test_path_too_long(void)
{
	struct normfs_disk_id i = id("1000");
	char out[64];
	size_t used = 1u;
	size_t need = strlen("/q/001/000.store") + 1u;
	size_t k;

	memset(out, 0x5A, sizeof(out));
	CHECK(normfs_disk_path("/q", 2u, &i, NORMFS_DISK_STORE, out,
	    need - 1u, &used).status == NORMFS_DISK_ERR_PATH_TOO_LONG);
	CHECK(used == 0u);
	for (k = 0u; k < sizeof(out); k++)
		CHECK((unsigned char)out[k] == 0x5Au);

	CHECK(normfs_disk_path("/q", 2u, &i, NORMFS_DISK_STORE, out, need,
	    &used).status == NORMFS_DISK_OK);
	CHECK(used == need - 1u);
	return 0;
}

static int
test_path_rejects_malformed_ids(void)
{
	struct normfs_disk_id i;
	char out[64];
	size_t used = 1u;

	i = id("01");
	CHECK(normfs_disk_path("/q", 2u, &i, NORMFS_DISK_STORE, out,
	    sizeof(out), &used).status == NORMFS_DISK_ERR_INVALID_ARG);
	CHECK(used == 0u);
	i = id("A");
	CHECK(normfs_disk_path("/q", 2u, &i, NORMFS_DISK_STORE, out,
	    sizeof(out), &used).status == NORMFS_DISK_ERR_INVALID_ARG);
	i = id("");
	CHECK(normfs_disk_path("/q", 2u, &i, NORMFS_DISK_STORE, out,
	    sizeof(out), &used).status == NORMFS_DISK_ERR_INVALID_ARG);
	i = id("1");
	CHECK(normfs_disk_path("/q", 2u, &i, 7, out, sizeof(out),
	    &used).status == NORMFS_DISK_ERR_INVALID_ARG);
	return 0;
}

static int
test_scan_missing_dir_is_empty(void)
{
	char dir[640];
	struct normfs_disk_scan s;
	struct normfs_disk_result r;

	(void)snprintf(dir, sizeof(dir), "%s/nowhere", root);
	memset(&s, 0xFF, sizeof(s));
	r = normfs_disk_scan(dir, strlen(dir), NORMFS_DISK_STORE, &s);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(r.os_error == 0);
	CHECK(s.total == 0u);
	CHECK(s.has_min == 0);
	return 0;
}

static int
test_scan_sums_and_finds_the_minimum(void)
{
	char dir[640];
	char junk[768];
	struct normfs_disk_scan s;
	struct normfs_disk_result r;
	int fd;

	(void)snprintf(dir, sizeof(dir), "%s/scan/store", root);
	CHECK(mkdir_p(dir) == 0);
	CHECK(put_file(dir, "100", NORMFS_DISK_STORE, 20u) == 0);
	CHECK(put_file(dir, "1000", NORMFS_DISK_STORE, 30u) == 0);
	CHECK(put_file(dir, "abcdef012", NORMFS_DISK_STORE, 40u) == 0);
	CHECK(put_file(dir, "5", NORMFS_DISK_STORE, 10u) == 0);

	/* Not layout names: neither counted nor descended. */
	(void)snprintf(junk, sizeof(junk), "%s/readme.txt", dir);
	fd = open(junk, O_WRONLY | O_CREAT, 0600);
	CHECK(fd >= 0 && write(fd, "junkjunk", 8) == 8 && close(fd) == 0);
	(void)snprintf(junk, sizeof(junk), "%s/zzz.store", dir);
	fd = open(junk, O_WRONLY | O_CREAT, 0600);
	CHECK(fd >= 0 && write(fd, "junkjunk", 8) == 8 && close(fd) == 0);
	(void)snprintf(junk, sizeof(junk), "%s/tmp", dir);
	CHECK(mkdir(junk, 0700) == 0);
	(void)snprintf(junk, sizeof(junk), "%s/tmp/001.store", dir);
	fd = open(junk, O_WRONLY | O_CREAT, 0600);
	CHECK(fd >= 0 && write(fd, "junkjunk", 8) == 8 && close(fd) == 0);
	/* A symlink is OTHER: not counted, not followed. */
	(void)snprintf(junk, sizeof(junk), "%s/002.store", dir);
	CHECK(symlink("readme.txt", junk) == 0);
	/* Wrong extension for the kind. */
	CHECK(put_file(dir, "3", NORMFS_DISK_WAL, 70u) == 0);

	memset(&s, 0, sizeof(s));
	r = normfs_disk_scan(dir, strlen(dir), NORMFS_DISK_STORE, &s);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(s.total == 100u);
	CHECK(s.has_min == 1);
	CHECK(id_is(&s.min, "5"));

	memset(&s, 0, sizeof(s));
	r = normfs_disk_scan(dir, strlen(dir), NORMFS_DISK_WAL, &s);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(s.total == 70u);
	CHECK(id_is(&s.min, "3"));
	return 0;
}

/* "001/000.store" sorts before "005.store" bytewise; numerically 0x1000 is
 * larger. Only the parsed id decides. */
static int
test_scan_minimum_is_numeric(void)
{
	char dir[640];
	struct normfs_disk_scan s;

	(void)snprintf(dir, sizeof(dir), "%s/numeric", root);
	CHECK(mkdir_p(dir) == 0);
	CHECK(put_file(dir, "1000", NORMFS_DISK_STORE, 1u) == 0);
	CHECK(put_file(dir, "5", NORMFS_DISK_STORE, 1u) == 0);
	CHECK(put_file(dir, "0", NORMFS_DISK_STORE, 1u) == 0);

	memset(&s, 0, sizeof(s));
	CHECK(normfs_disk_scan(dir, strlen(dir), NORMFS_DISK_STORE,
	    &s).status == NORMFS_DISK_OK);
	CHECK(s.total == 3u);
	CHECK(id_is(&s.min, "0"));
	return 0;
}

static int
test_scan_refuses_a_tree_too_deep(void)
{
	char dir[640];
	char deep[1024];
	struct normfs_disk_scan s;
	int k;

	(void)snprintf(dir, sizeof(dir), "%s/deep", root);
	(void)snprintf(deep, sizeof(deep), "%s", dir);
	for (k = 0; k < NORMFS_DISK_MAX_DEPTH; k++)
		(void)strcat(deep, "/000");
	CHECK(mkdir_p(deep) == 0);

	CHECK(normfs_disk_scan(dir, strlen(dir), NORMFS_DISK_STORE,
	    &s).status == NORMFS_DISK_ERR_TOO_DEEP);
	return 0;
}

static int
test_file_size(void)
{
	char dir[640];
	struct normfs_disk_id i;
	struct normfs_disk_result r;
	uint64_t size = 99u;

	(void)snprintf(dir, sizeof(dir), "%s/size", root);
	CHECK(mkdir_p(dir) == 0);
	CHECK(put_file(dir, "7", NORMFS_DISK_STORE, 123u) == 0);

	i = id("7");
	r = normfs_disk_file_size(dir, strlen(dir), &i, NORMFS_DISK_STORE,
	    &size);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(size == 123u);

	i = id("8");
	r = normfs_disk_file_size(dir, strlen(dir), &i, NORMFS_DISK_STORE,
	    &size);
	CHECK(r.status == NORMFS_DISK_ERR_NOT_FOUND);
	CHECK(r.os_error == 0);
	CHECK(size == 0u);
	return 0;
}

static int
test_unlink_removes_name_but_preserves_other_references(void)
{
	char path[640];
	char alias[640];
	struct stat st;
	uint64_t size = 0u;
	int error = -1;
	int fd;
	char byte = 0;

	(void)snprintf(path, sizeof(path), "%s/unlink-file", root);
	(void)snprintf(alias, sizeof(alias), "%s/unlink-alias", root);
	fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0600);
	CHECK(fd >= 0);
	CHECK(write(fd, "x", 1) == 1);
	CHECK(link(path, alias) == 0);
	CHECK(normfs_disk_sys_file_size(path, strlen(path), &size, &error) == 1);
	CHECK(size == 1u && error == 0);
	CHECK(normfs_disk_sys_unlink(path, strlen(path), &error) == 0);
	CHECK(error == 0);
	CHECK(lstat(path, &st) == -1 && errno == ENOENT);
	CHECK(lstat(alias, &st) == 0 && st.st_size == 1);
	CHECK(fstat(fd, &st) == 0 && st.st_size == 1);
	CHECK(normfs_disk_sys_unlink(alias, strlen(alias), &error) == 0);
	CHECK(pread(fd, &byte, 1, 0) == 1 && byte == 'x');
	CHECK(normfs_disk_sys_unlink(path, strlen(path), &error) == -1);
	CHECK(error == ENOENT);
	CHECK(normfs_disk_sys_file_size(path, strlen(path), &size, &error) == 0);
	CHECK(size == 0u && error == 0);
	CHECK(normfs_disk_sys_file_size(root, strlen(root), &size, &error) == 0);
	CHECK(size == 0u && error == 0);
	CHECK(close(fd) == 0);
	return 0;
}

static int
setup_queue(const char *name, char *store, size_t store_len, char *wal,
    size_t wal_len)
{
	(void)snprintf(store, store_len, "%s/%s/store", root, name);
	(void)snprintf(wal, wal_len, "%s/%s/wal", root, name);
	if (mkdir_p(store) != 0 || mkdir_p(wal) != 0)
		return -1;
	return 0;
}

static struct normfs_disk_evict_req
req(const char *store, const char *wal, const char *next, uint64_t to_free)
{
	struct normfs_disk_evict_req r;

	memset(&r, 0, sizeof(r));
	r.store_dir = store;
	r.store_dir_len = strlen(store);
	r.wal_dir = wal;
	r.wal_dir_len = strlen(wal);
	r.next = id(next);
	r.to_free = to_free;
	return r;
}

static int
test_evict_frees_from_the_bottom(void)
{
	char store[640];
	char wal[640];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;
	size_t k;

	CHECK(setup_queue("bottom", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	CHECK(put_file(store, "1", NORMFS_DISK_STORE, 100u) == 0);
	CHECK(put_file(store, "2", NORMFS_DISK_STORE, 100u) == 0);
	CHECK(put_file(store, "3", NORMFS_DISK_STORE, 100u) == 0);
	CHECK(put_file(store, "4", NORMFS_DISK_STORE, 100u) == 0);
	CHECK(put_file(wal, "5", NORMFS_DISK_WAL, 50u) == 0);

	rq = req(store, wal, "1", 250u);
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(r.os_error == 0);
	CHECK(stop == NORMFS_DISK_STOP_FREED);
	CHECK(count == 3u);
	CHECK(rq.to_free == 0u);
	CHECK(id_is(&rq.next, "4"));
	for (k = 0u; k < 3u; k++) {
		CHECK(ev[k].deleted == 1);
		CHECK(ev[k].os_error == 0);
		CHECK(ev[k].kind == NORMFS_DISK_STORE);
		CHECK(ev[k].size == 100u);
	}
	CHECK(id_is(&ev[0].id, "1"));
	CHECK(id_is(&ev[2].id, "3"));
	CHECK(ev[0].freed == 100u && ev[2].freed == 300u);
	CHECK(file_exists(store, "3", NORMFS_DISK_STORE) == 0);
	CHECK(file_exists(store, "4", NORMFS_DISK_STORE) == 1);
	CHECK(file_exists(wal, "5", NORMFS_DISK_WAL) == 1);
	return 0;
}

static int
test_evict_takes_wal_only_without_store_and_stops_at_the_gap(void)
{
	char store[640];
	char wal[640];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;

	CHECK(setup_queue("pair", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	CHECK(put_file(store, "1", NORMFS_DISK_STORE, 10u) == 0);
	CHECK(put_file(wal, "1", NORMFS_DISK_WAL, 20u) == 0);
	CHECK(put_file(wal, "2", NORMFS_DISK_WAL, 30u) == 0);
	CHECK(put_file(store, "4", NORMFS_DISK_STORE, 40u) == 0);

	rq = req(store, wal, "1", 1000u);
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_GAP);
	CHECK(count == 2u);
	CHECK(id_is(&rq.next, "3"));
	CHECK(rq.to_free == 1000u - 10u - 30u);
	CHECK(ev[0].kind == NORMFS_DISK_STORE && ev[0].size == 10u);
	CHECK(ev[1].kind == NORMFS_DISK_WAL && ev[1].size == 30u);
	CHECK(file_exists(store, "1", NORMFS_DISK_STORE) == 0);
	CHECK(file_exists(wal, "1", NORMFS_DISK_WAL) == 1);
	CHECK(file_exists(wal, "2", NORMFS_DISK_WAL) == 0);
	CHECK(file_exists(store, "4", NORMFS_DISK_STORE) == 1);
	return 0;
}

static int
test_evict_stops_at_the_bound(void)
{
	char store[640];
	char wal[640];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;

	CHECK(setup_queue("bound", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	CHECK(put_file(store, "1", NORMFS_DISK_STORE, 10u) == 0);
	CHECK(put_file(store, "2", NORMFS_DISK_STORE, 10u) == 0);
	CHECK(put_file(store, "3", NORMFS_DISK_STORE, 10u) == 0);

	rq = req(store, wal, "1", 1000u);
	rq.bound = id("2");
	rq.has_bound = 1;
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_BOUND);
	CHECK(count == 2u);
	CHECK(id_is(&rq.next, "3"));
	CHECK(file_exists(store, "3", NORMFS_DISK_STORE) == 1);

	/* The gap is reported before the bound, as the Rust loop did. */
	rq = req(store, wal, "9", 1000u);
	rq.bound = id("2");
	rq.has_bound = 1;
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_GAP);
	CHECK(count == 0u);
	return 0;
}

static int
test_evict_resumes_after_a_full_buffer(void)
{
	char store[640];
	char wal[640];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[2];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;
	int k;

	CHECK(setup_queue("resume", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	for (k = 1; k <= 5; k++) {
		char hex[4];

		(void)snprintf(hex, sizeof(hex), "%x", k);
		CHECK(put_file(store, hex, NORMFS_DISK_STORE, 1u) == 0);
	}

	rq = req(store, wal, "1", 1000u);
	r = normfs_disk_evict(&rq, ev, 2u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_MORE);
	CHECK(count == 2u);
	CHECK(id_is(&rq.next, "3"));

	r = normfs_disk_evict(&rq, ev, 2u, &count, &stop);
	CHECK(stop == NORMFS_DISK_STOP_MORE);
	CHECK(count == 2u);
	CHECK(id_is(&ev[1].id, "4"));

	r = normfs_disk_evict(&rq, ev, 2u, &count, &stop);
	CHECK(stop == NORMFS_DISK_STOP_GAP);
	CHECK(count == 1u);
	CHECK(id_is(&rq.next, "6"));
	CHECK(rq.to_free == 995u);

	r = normfs_disk_evict(&rq, ev, 0u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_MORE);
	CHECK(count == 0u);
	return 0;
}

static int
test_evict_stops_at_a_failed_unlink(void)
{
	char store[640];
	char wal[640];
	char chunk[768];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;

	/* root unlinks regardless of the directory mode. */
	if (geteuid() == 0)
		return 0;

	CHECK(setup_queue("locked", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	CHECK(put_file(store, "1000", NORMFS_DISK_STORE, 10u) == 0);
	CHECK(put_file(store, "1001", NORMFS_DISK_STORE, 10u) == 0);
	(void)snprintf(chunk, sizeof(chunk), "%s/001", store);
	CHECK(chmod(chunk, 0500) == 0);

	rq = req(store, wal, "1000", 1000u);
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(chmod(chunk, 0700) == 0);
	CHECK(r.status == NORMFS_DISK_OK);
	CHECK(stop == NORMFS_DISK_STOP_ERROR);
	CHECK(count == 1u);
	CHECK(ev[0].deleted == 0);
	CHECK(ev[0].os_error == EACCES || ev[0].os_error == EPERM);
	CHECK(ev[0].freed == 0u);
	CHECK(id_is(&rq.next, "1000"));
	CHECK(rq.to_free == 1000u);
	CHECK(file_exists(store, "1000", NORMFS_DISK_STORE) == 1);
	CHECK(file_exists(store, "1001", NORMFS_DISK_STORE) == 1);
	return 0;
}

static int
test_evict_overflows_at_the_layout_cap(void)
{
	char store[640];
	char wal[640];
	char hex[NORMFS_DISK_ID_MAX + 1];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	struct normfs_disk_result r;
	size_t count = 99u;
	int stop = -1;

	CHECK(setup_queue("cap", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	memset(hex, 'f', NORMFS_DISK_ID_MAX);
	hex[NORMFS_DISK_ID_MAX] = '\0';
	CHECK(put_file(store, hex, NORMFS_DISK_STORE, 1u) == 0);

	rq = req(store, wal, hex, 1000u);
	r = normfs_disk_evict(&rq, ev, 8u, &count, &stop);
	CHECK(r.status == NORMFS_DISK_ERR_ID_OVERFLOW);
	CHECK(count == 1u);
	CHECK(ev[0].deleted == 1);
	CHECK(id_is(&rq.next, hex));
	return 0;
}

static int
test_evict_rejects_malformed_ids(void)
{
	char store[640];
	char wal[640];
	struct normfs_disk_evict_req rq;
	struct normfs_disk_event ev[8];
	size_t count = 99u;
	int stop = -1;

	CHECK(setup_queue("bad", store, sizeof(store), wal,
	    sizeof(wal)) == 0);
	rq = req(store, wal, "007", 1u);
	CHECK(normfs_disk_evict(&rq, ev, 8u, &count, &stop).status ==
	    NORMFS_DISK_ERR_INVALID_ARG);
	CHECK(count == 0u);

	rq = req(store, wal, "7", 1u);
	rq.bound = id("");
	rq.has_bound = 1;
	CHECK(normfs_disk_evict(&rq, ev, 8u, &count, &stop).status ==
	    NORMFS_DISK_ERR_INVALID_ARG);
	return 0;
}

int
main(void)
{
	const char *tmp = getenv("TMPDIR");

	(void)snprintf(root, sizeof(root), "%s/normfs_disk_XXXXXX",
	    (tmp != NULL && tmp[0] != '\0') ? tmp : "/tmp");
	if (mkdtemp(root) == NULL) {
		perror("disk_monitor: mkdtemp");
		return 1;
	}
	if (atexit(cleanup_root) != 0) {
		fprintf(stderr, "disk_monitor: atexit failed\n");
		cleanup_root();
		return 1;
	}

	if (test_path_pins_layout() != 0)
		return 1;
	if (test_path_too_long() != 0)
		return 1;
	if (test_path_rejects_malformed_ids() != 0)
		return 1;
	if (test_scan_missing_dir_is_empty() != 0)
		return 1;
	if (test_scan_sums_and_finds_the_minimum() != 0)
		return 1;
	if (test_scan_minimum_is_numeric() != 0)
		return 1;
	if (test_scan_refuses_a_tree_too_deep() != 0)
		return 1;
	if (test_file_size() != 0)
		return 1;
	if (test_unlink_removes_name_but_preserves_other_references() != 0)
		return 1;
	if (test_evict_frees_from_the_bottom() != 0)
		return 1;
	if (test_evict_takes_wal_only_without_store_and_stops_at_the_gap() != 0)
		return 1;
	if (test_evict_stops_at_the_bound() != 0)
		return 1;
	if (test_evict_resumes_after_a_full_buffer() != 0)
		return 1;
	if (test_evict_stops_at_a_failed_unlink() != 0)
		return 1;
	if (test_evict_overflows_at_the_layout_cap() != 0)
		return 1;
	if (test_evict_rejects_malformed_ids() != 0)
		return 1;

	printf("disk_monitor: all tests passed\n");
	return 0;
}
