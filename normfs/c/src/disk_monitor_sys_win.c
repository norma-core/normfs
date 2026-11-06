/*
 * The Windows bodies behind normfs/disk_monitor_sys.h; never given to Frama-C.
 * os_error is a Win32 code, which is what io::Error::from_raw_os_error reads
 * on Windows.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include "normfs/disk_monitor.h"
#include "normfs/disk_monitor_sys.h"

/* FindFirstFileW hands back the first entry with the handle; dir is kept to
 * look up each file's current size. */
struct normfs_disk_sys_dir {
	HANDLE find;
	WIN32_FIND_DATAW entry;
	int have;
	wchar_t *dir;
	size_t dir_len;
};

static int
normfs_disk_sys_fail(int *os_error)
{
	DWORD e = GetLastError();

	*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
	return -1;
}

static int
normfs_disk_sys_absent(DWORD e)
{
	return e == ERROR_FILE_NOT_FOUND || e == ERROR_PATH_NOT_FOUND ||
	    e == ERROR_DIRECTORY;
}

/*
 * UTF-8 in, a full \\?\ path out with room for extra more characters: the
 * ANSI calls would read the bytes in the local code page, and without the
 * prefix a path past 260 characters fails as if absent. The caller frees it.
 */
static wchar_t *
normfs_disk_sys_wpath(const char *path, size_t path_len, size_t extra,
    int *os_error)
{
	wchar_t *rel;
	wchar_t *full;
	wchar_t *p;
	DWORD need;
	DWORD got;
	int n = 0;

	rel = malloc((path_len + 1u) * sizeof(wchar_t));
	if (rel == NULL) {
		*os_error = ERROR_NOT_ENOUGH_MEMORY;
		return NULL;
	}
	if (path_len > 0u)
		n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, (int)path_len,
		    rel, (int)path_len);
	if (n <= 0) {
		free(rel);
		*os_error = ERROR_INVALID_NAME;
		return NULL;
	}
	rel[n] = L'\0';

	need = GetFullPathNameW(rel, 0u, NULL, NULL);
	if (need == 0u) {
		(void)normfs_disk_sys_fail(os_error);
		free(rel);
		return NULL;
	}
	full = malloc((need + 8u + extra) * sizeof(wchar_t));
	if (full == NULL) {
		free(rel);
		*os_error = ERROR_NOT_ENOUGH_MEMORY;
		return NULL;
	}
	/* Seven characters of room in front for "\\?\UNC". */
	p = full + 7;
	got = GetFullPathNameW(rel, need, p, NULL);
	if (got == 0u || got >= need) {
		(void)normfs_disk_sys_fail(os_error);
		free(rel);
		free(full);
		return NULL;
	}
	free(rel);

	if (p[0] == L'\\' && p[1] == L'\\' && (p[2] == L'?' || p[2] == L'.')) {
		memmove(full, p, (wcslen(p) + 1u) * sizeof(wchar_t));
	} else if (p[0] == L'\\' && p[1] == L'\\') {
		memmove(full + 7, p + 1, wcslen(p + 1) * sizeof(wchar_t) + sizeof(wchar_t));
		memcpy(full, L"\\\\?\\UNC", 7u * sizeof(wchar_t));
	} else {
		memmove(full + 4, p, (wcslen(p) + 1u) * sizeof(wchar_t));
		memcpy(full, L"\\\\?\\", 4u * sizeof(wchar_t));
	}
	return full;
}

/* From ntdll: the NTSTATUS behind the last error, which tells a file pending
 * deletion apart from one access is denied to. */
__declspec(dllimport) LONG __stdcall RtlGetLastNtStatus(void);
#define NORMFS_DISK_STATUS_DELETE_PENDING ((LONG)0xC0000056L)

/* Gone, or going: a file deleted while a reader still holds it stays listed
 * where deletes are not POSIX ones, and answers ERROR_ACCESS_DENIED. Read
 * right after the failing call. */
static int
normfs_disk_sys_gone(DWORD e)
{
	return normfs_disk_sys_absent(e) ||
	    (e == ERROR_ACCESS_DENIED &&
	     RtlGetLastNtStatus() == NORMFS_DISK_STATUS_DELETE_PENDING);
}

/* Symlinks and junctions, as lstat would see them; other reparse points,
 * such as OneDrive and deduplicated files, are files. */
static int
normfs_disk_sys_link_tag(DWORD attrs, DWORD tag)
{
	return (attrs & FILE_ATTRIBUTE_REPARSE_POINT) &&
	    (tag == IO_REPARSE_TAG_SYMLINK || tag == IO_REPARSE_TAG_MOUNT_POINT);
}

static int
normfs_disk_sys_dots(const wchar_t *name)
{
	return name[0] == L'.' &&
	    (name[1] == L'\0' || (name[1] == L'.' && name[2] == L'\0'));
}

void *
normfs_disk_sys_dir_open(const char *path, size_t path_len, int *os_error)
{
	struct normfs_disk_sys_dir *d;
	wchar_t *wide;
	size_t n;

	*os_error = 0;
	wide = normfs_disk_sys_wpath(path, path_len, 2u, os_error);
	if (wide == NULL)
		return NULL;
	n = wcslen(wide);
	if (wide[n - 1u] != L'\\')
		wide[n++] = L'\\';
	wide[n] = L'*';
	wide[n + 1u] = L'\0';

	d = malloc(sizeof(*d));
	if (d == NULL) {
		free(wide);
		*os_error = ERROR_NOT_ENOUGH_MEMORY;
		return NULL;
	}
	d->find = FindFirstFileW(wide, &d->entry);
	if (d->find == INVALID_HANDLE_VALUE) {
		DWORD e = GetLastError();

		free(wide);
		free(d);
		if (!normfs_disk_sys_absent(e))
			*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
		return NULL;
	}
	wide[n] = L'\0';
	d->dir = wide;
	d->dir_len = n;
	d->have = 1;
	return d;
}

/* The listing's size lags a file still being written, so a file's size is
 * read from the file itself; 0 means it went since the listing. */
static int
normfs_disk_sys_live_size(struct normfs_disk_sys_dir *d, const wchar_t *name,
    uint64_t *size, int *os_error)
{
	WIN32_FILE_ATTRIBUTE_DATA data;
	size_t len = wcslen(name);
	wchar_t *path;
	BOOL ok;

	path = malloc((d->dir_len + len + 1u) * sizeof(wchar_t));
	if (path == NULL) {
		*os_error = ERROR_NOT_ENOUGH_MEMORY;
		return -1;
	}
	memcpy(path, d->dir, d->dir_len * sizeof(wchar_t));
	memcpy(path + d->dir_len, name, (len + 1u) * sizeof(wchar_t));
	ok = GetFileAttributesExW(path, GetFileExInfoStandard, &data);
	if (!ok) {
		DWORD e = GetLastError();
		int gone = normfs_disk_sys_gone(e);

		free(path);
		if (gone)
			return 0;
		*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
		return -1;
	}
	free(path);
	*size = ((uint64_t)data.nFileSizeHigh << 32) | data.nFileSizeLow;
	return 1;
}

int
normfs_disk_sys_dir_next(void *handle, char *name, size_t name_cap,
    size_t *name_len, int *kind, uint64_t *size, int *os_error)
{
	struct normfs_disk_sys_dir *d = handle;
	const WIN32_FIND_DATAW *e = &d->entry;
	int n;

	*os_error = 0;
	*kind = NORMFS_DISK_SYS_OTHER;
	*size = 0u;
	*name_len = 0u;
	name[0] = '\0';

	for (;;) {
		if (!d->have) {
			if (!FindNextFileW(d->find, &d->entry)) {
				if (GetLastError() == ERROR_NO_MORE_FILES)
					return 0;
				return normfs_disk_sys_fail(os_error);
			}
		}
		d->have = 0;
		if (normfs_disk_sys_dots(e->cFileName))
			continue;
		if (normfs_disk_sys_link_tag(e->dwFileAttributes, e->dwReserved0) ||
		    (e->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
			break;
		n = normfs_disk_sys_live_size(d, e->cFileName, size, os_error);
		if (n < 0)
			return -1;
		if (n == 1)
			break;
	}

	n = WideCharToMultiByte(CP_UTF8, 0, e->cFileName, -1, name,
	    name_cap > 0x7FFFFFFFu ? 0x7FFFFFFF : (int)name_cap, NULL, NULL);
	if (n <= 0) {
		/* Too long for name: reported truncated and OTHER, as on Unix. */
		name[name_cap - 1u] = '\0';
		*name_len = strlen(name);
		*size = 0u;
		return 1;
	}
	*name_len = (size_t)n - 1u;
	if (normfs_disk_sys_link_tag(e->dwFileAttributes, e->dwReserved0)) {
		*size = 0u;
		return 1;
	}
	if (e->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
		*kind = NORMFS_DISK_SYS_DIR;
	else
		*kind = NORMFS_DISK_SYS_FILE;
	return 1;
}

void
normfs_disk_sys_dir_close(void *handle)
{
	struct normfs_disk_sys_dir *d = handle;

	(void)FindClose(d->find);
	free(d->dir);
	free(d);
}

int
normfs_disk_sys_file_size(const char *path, size_t path_len, uint64_t *size,
    int *os_error)
{
	WIN32_FILE_ATTRIBUTE_DATA data;
	WIN32_FIND_DATAW found;
	wchar_t *wide;
	HANDLE find;
	DWORD tag = 0u;

	*os_error = 0;
	*size = 0u;
	wide = normfs_disk_sys_wpath(path, path_len, 0u, os_error);
	if (wide == NULL)
		return -1;
	if (!GetFileAttributesExW(wide, GetFileExInfoStandard, &data)) {
		DWORD e = GetLastError();
		int gone = normfs_disk_sys_gone(e);

		free(wide);
		if (gone)
			return 0;
		*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
		return -1;
	}
	if (data.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
		/* Unreadable tag: taken as a link, which is never counted. */
		tag = IO_REPARSE_TAG_SYMLINK;
		find = FindFirstFileW(wide, &found);
		if (find != INVALID_HANDLE_VALUE) {
			tag = found.dwReserved0;
			(void)FindClose(find);
		}
	}
	free(wide);
	if ((data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) ||
	    normfs_disk_sys_link_tag(data.dwFileAttributes, tag))
		return 0;
	*size = ((uint64_t)data.nFileSizeHigh << 32) | data.nFileSizeLow;
	return 1;
}

int
normfs_disk_sys_unlink(const char *path, size_t path_len, int *os_error)
{
	wchar_t *wide;

	*os_error = 0;
	wide = normfs_disk_sys_wpath(path, path_len, 0u, os_error);
	if (wide == NULL)
		return -1;
	if (!DeleteFileW(wide)) {
		(void)normfs_disk_sys_fail(os_error);
		free(wide);
		return -1;
	}
	free(wide);
	return 0;
}
