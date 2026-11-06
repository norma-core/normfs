/*
 * The Windows bodies behind normfs/fs_sys.h. Never given to Frama-C. A
 * descriptor is a HANDLE and os_error a Win32 code, which is what
 * io::Error::from_raw_os_error reads on Windows.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>

#include "normfs/fs_sys.h"
#include "normfs/fs_dir.h"

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

static int
normfs_fs_sys_fail(int *os_error)
{
	DWORD e = GetLastError();

	*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
	return -1;
}

/*
 * UTF-8 in, a full \\?\ path out: the ANSI calls would read the bytes in the
 * local code page, and without the prefix a path past 260 characters fails
 * as if absent. The caller frees it.
 */
static wchar_t *
normfs_fs_sys_wpath(const char *path, size_t path_len, int *os_error)
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
	full = need > 0u ? malloc((need + 8u) * sizeof(wchar_t)) : NULL;
	if (full == NULL) {
		if (need == 0u)
			(void)normfs_fs_sys_fail(os_error);
		else
			*os_error = ERROR_NOT_ENOUGH_MEMORY;
		free(rel);
		return NULL;
	}
	/* Seven characters of room in front for "\\?\UNC". */
	p = full + 7;
	got = GetFullPathNameW(rel, need, p, NULL);
	if (got == 0u || got >= need) {
		(void)normfs_fs_sys_fail(os_error);
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

static int
normfs_fs_sys_absent(DWORD e)
{
	return e == ERROR_FILE_NOT_FOUND || e == ERROR_PATH_NOT_FOUND;
}

/* A symlink or a junction, as lstat would see it; other reparse points, such
 * as OneDrive and deduplicated files, are files. */
static int
normfs_fs_sys_is_link(const wchar_t *path, DWORD attrs)
{
	WIN32_FIND_DATAW data;
	HANDLE find;

	if (!(attrs & FILE_ATTRIBUTE_REPARSE_POINT))
		return 0;
	find = FindFirstFileW(path, &data);
	if (find == INVALID_HANDLE_VALUE)
		return 1;
	(void)FindClose(find);
	return data.dwReserved0 == IO_REPARSE_TAG_SYMLINK ||
	    data.dwReserved0 == IO_REPARSE_TAG_MOUNT_POINT;
}

/* FILE_SHARE_DELETE lets a later publish replace, or a cleanup remove, a
 * file a reader still has open, as on Unix. A link already at the path is
 * refused, checked just before the open rather than by it as O_NOFOLLOW is. */
normfs_fs_fd
normfs_fs_sys_open_create(const char *path, size_t path_len, int mode,
	uint64_t *ino, int *os_error)
{
	BY_HANDLE_FILE_INFORMATION info;
	wchar_t *wide;
	DWORD attrs;
	HANDLE h;

	*os_error = 0;
	*ino = 0u;
	wide = normfs_fs_sys_wpath(path, path_len, os_error);
	if (wide == NULL)
		return -1;

	attrs = GetFileAttributesW(wide);
	if (attrs != INVALID_FILE_ATTRIBUTES && normfs_fs_sys_is_link(wide, attrs)) {
		free(wide);
		*os_error = ERROR_CANT_RESOLVE_FILENAME;
		return -1;
	}
	h = CreateFileW(wide, GENERIC_WRITE,
	    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
	    mode == NORMFS_FS_TMP_TRUNC ? CREATE_ALWAYS : CREATE_NEW,
	    FILE_ATTRIBUTE_NORMAL, NULL);
	if (h == INVALID_HANDLE_VALUE) {
		(void)normfs_fs_sys_fail(os_error);
		free(wide);
		return -1;
	}
	free(wide);
	if (!GetFileInformationByHandle(h, &info)) {
		(void)normfs_fs_sys_fail(os_error);
		(void)CloseHandle(h);
		return -1;
	}
	*ino = ((uint64_t)info.nFileIndexHigh << 32) | info.nFileIndexLow;
	if (*ino == 0u) {
		*os_error = ERROR_INVALID_DATA;
		(void)CloseHandle(h);
		return -1;
	}
	return (normfs_fs_fd)h;
}

int
normfs_fs_sys_pwritev_all(normfs_fs_fd fd, const struct normfs_fs_iov *iov,
	size_t cnt, uint64_t off, int *os_error)
{
	HANDLE h = (HANDLE)fd;
	size_t i;

	*os_error = 0;
	for (i = 0u; i < cnt; i++) {
		const uint8_t *p = iov[i].base;
		size_t left = iov[i].len;

		while (left > 0u) {
			OVERLAPPED ov;
			DWORD want = left > 0x40000000u ? 0x40000000u : (DWORD)left;
			DWORD n = 0u;

			memset(&ov, 0, sizeof(ov));
			ov.Offset = (DWORD)off;
			ov.OffsetHigh = (DWORD)(off >> 32);
			if (!WriteFile(h, p, want, &n, &ov))
				return normfs_fs_sys_fail(os_error);
			/* No progress would loop forever. */
			if (n == 0u) {
				*os_error = ERROR_WRITE_FAULT;
				return -1;
			}
			p += n;
			left -= n;
			off += n;
		}
	}
	return 0;
}

int
normfs_fs_sys_fsync(normfs_fs_fd fd, int *os_error)
{
	*os_error = 0;
	if (!FlushFileBuffers((HANDLE)fd))
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_close(normfs_fs_fd fd, int *os_error)
{
	*os_error = 0;
	if (!CloseHandle((HANDLE)fd))
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_file_len(const char *path, size_t path_len, uint64_t *len,
	int *os_error)
{
	WIN32_FILE_ATTRIBUTE_DATA data;
	wchar_t *wide;
	int link;

	*os_error = 0;
	*len = 0u;
	wide = normfs_fs_sys_wpath(path, path_len, os_error);
	if (wide == NULL)
		return -1;

	if (!GetFileAttributesExW(wide, GetFileExInfoStandard, &data)) {
		DWORD e = GetLastError();

		free(wide);
		if (normfs_fs_sys_absent(e))
			return 0;
		*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
		return -1;
	}
	link = normfs_fs_sys_is_link(wide, data.dwFileAttributes);
	free(wide);
	if (link || (data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)) {
		*os_error = ERROR_ALREADY_EXISTS;
		return -1;
	}
	*len = ((uint64_t)data.nFileSizeHigh << 32) | data.nFileSizeLow;
	return 1;
}

#if !defined(FILE_RENAME_FLAG_REPLACE_IF_EXISTS)
#define FILE_RENAME_FLAG_REPLACE_IF_EXISTS 0x00000001
#endif
#if !defined(FILE_RENAME_FLAG_POSIX_SEMANTICS)
#define FILE_RENAME_FLAG_POSIX_SEMANTICS 0x00000002
#endif
/* FileRenameInfoEx, missing from older headers. */
#define NORMFS_FS_RENAME_INFO_EX ((FILE_INFO_BY_HANDLE_CLASS)22)

/*
 * POSIX rename: the target is replaced even while a reader holds it open,
 * which MoveFileEx refuses. FAT and exFAT lack the POSIX flag; there the
 * plain replacing rename is the fallback.
 */
int
normfs_fs_sys_rename(const char *src, size_t src_len, const char *dst,
	size_t dst_len, int *os_error)
{
	FILE_RENAME_INFO *info;
	wchar_t *wsrc;
	wchar_t *wdst;
	size_t bytes;
	DWORD size;
	DWORD flags;
	HANDLE h;
	int ok;

	*os_error = 0;
	wsrc = normfs_fs_sys_wpath(src, src_len, os_error);
	if (wsrc == NULL)
		return -1;
	wdst = normfs_fs_sys_wpath(dst, dst_len, os_error);
	if (wdst == NULL) {
		free(wsrc);
		return -1;
	}
	h = CreateFileW(wsrc, DELETE | SYNCHRONIZE,
	    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
	    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, NULL);
	if (h == INVALID_HANDLE_VALUE)
		(void)normfs_fs_sys_fail(os_error);
	free(wsrc);
	if (h == INVALID_HANDLE_VALUE) {
		free(wdst);
		return -1;
	}

	bytes = wcslen(wdst) * sizeof(wchar_t);
	size = (DWORD)(sizeof(FILE_RENAME_INFO) + bytes);
	info = calloc(1u, size);
	if (info == NULL) {
		*os_error = ERROR_NOT_ENOUGH_MEMORY;
		(void)CloseHandle(h);
		free(wdst);
		return -1;
	}
	/* The first field is Flags for the Ex class and ReplaceIfExists for the
	 * plain one; older headers only know the latter. */
	flags = FILE_RENAME_FLAG_REPLACE_IF_EXISTS | FILE_RENAME_FLAG_POSIX_SEMANTICS;
	memcpy(info, &flags, sizeof(flags));
	info->RootDirectory = NULL;
	info->FileNameLength = (DWORD)bytes;
	memcpy(info->FileName, wdst, bytes);
	free(wdst);

	ok = SetFileInformationByHandle(h, NORMFS_FS_RENAME_INFO_EX, info, size);
	if (!ok) {
		DWORD e = GetLastError();

		if (e == ERROR_INVALID_PARAMETER || e == ERROR_NOT_SUPPORTED ||
		    e == ERROR_INVALID_FUNCTION) {
			flags = 1u;
			memcpy(info, &flags, sizeof(flags));
			ok = SetFileInformationByHandle(h, FileRenameInfo, info, size);
		} else {
			SetLastError(e);
		}
	}
	if (!ok)
		(void)normfs_fs_sys_fail(os_error);
	free(info);
	(void)CloseHandle(h);
	return ok ? 0 : -1;
}

/* A directory handle opened for writing can be flushed; that is what makes a
 * name change durable, as fsync on the directory does on Unix. */
static int
normfs_fs_sys_flush_dir(const wchar_t *dir, int *os_error)
{
	HANDLE h;

	h = CreateFileW(dir, GENERIC_READ | GENERIC_WRITE,
	    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
	    FILE_FLAG_BACKUP_SEMANTICS, NULL);
	if (h == INVALID_HANDLE_VALUE)
		return normfs_fs_sys_fail(os_error);
	if (!FlushFileBuffers(h)) {
		(void)normfs_fs_sys_fail(os_error);
		(void)CloseHandle(h);
		return -1;
	}
	(void)CloseHandle(h);
	return 0;
}

int
normfs_fs_sys_fsync_parent(const char *path, size_t path_len, int *os_error)
{
	wchar_t *wide;
	wchar_t *cut;
	int rc;

	*os_error = 0;
	wide = normfs_fs_sys_wpath(path, path_len, os_error);
	if (wide == NULL)
		return -1;
	/* The full path has at least the root after its prefix, so a separator
	 * past the prefix always exists; keep it for a root like "C:\". */
	cut = wcsrchr(wide, L'\\');
	if (cut != NULL && cut > wide + 6)
		cut[0] = L'\0';
	else if (cut != NULL)
		cut[1] = L'\0';
	rc = normfs_fs_sys_flush_dir(wide, os_error);
	free(wide);
	return rc;
}

int
normfs_fs_sys_ftruncate(normfs_fs_fd fd, uint64_t len, int *os_error)
{
	FILE_END_OF_FILE_INFO info;

	*os_error = 0;
	info.EndOfFile.QuadPart = (LONGLONG)len;
	if (!SetFileInformationByHandle((HANDLE)fd, FileEndOfFileInfo, &info, sizeof(info)))
		return normfs_fs_sys_fail(os_error);
	return 0;
}

int
normfs_fs_sys_unlink(const char *path, size_t path_len, int *os_error)
{
	wchar_t *wide;

	*os_error = 0;
	wide = normfs_fs_sys_wpath(path, path_len, os_error);
	if (wide == NULL)
		return -1;
	if (!DeleteFileW(wide)) {
		DWORD e = GetLastError();

		free(wide);
		if (normfs_fs_sys_absent(e))
			return 1;
		*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
		return -1;
	}
	free(wide);
	return 0;
}

int
normfs_fs_sys_sync_dir(const char *path, size_t path_len, int *os_error)
{
	wchar_t *wide;
	int rc;

	*os_error = 0;
	wide = normfs_fs_sys_wpath(path, path_len, os_error);
	if (wide == NULL)
		return -1;
	rc = normfs_fs_sys_flush_dir(wide, os_error);
	free(wide);
	return rc;
}
