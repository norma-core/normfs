/*
 * The Windows bodies behind the assumed contracts in normfs/seed_sys.h. The CRT
 * keeps descriptors ints, as on Unix; os_error carries the Win32 code from
 * _doserrno, which is what io::Error::from_raw_os_error reads on Windows.
 */
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <bcrypt.h>
#include <sddl.h>
#include <errno.h>
#include <fcntl.h>
#include <io.h>
#include <share.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <string.h>
#include <wchar.h>

#include "normfs/seed_sys.h"

static int
normfs_seed_sys_fail(int *os_error)
{
	unsigned long e = 0;

	(void)_get_doserrno(&e);
	*os_error = (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
	return -1;
}

/*
 * UTF-8 in, a full \\?\ path out: the ANSI calls would read the bytes in the
 * local code page, and without the prefix a path past 260 characters fails
 * as if absent. The caller frees it.
 */
static wchar_t *
normfs_seed_sys_wpath(const char *path, size_t path_len)
{
	wchar_t *rel;
	wchar_t *full;
	wchar_t *p;
	DWORD need;
	DWORD got;
	int n = 0;

	rel = malloc((path_len + 1u) * sizeof(wchar_t));
	if (rel == NULL)
		return NULL;
	if (path_len > 0u)
		n = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, (int)path_len,
		    rel, (int)path_len);
	if (n <= 0) {
		free(rel);
		return NULL;
	}
	rel[n] = L'\0';

	need = GetFullPathNameW(rel, 0u, NULL, NULL);
	full = need > 0u ? malloc((need + 8u) * sizeof(wchar_t)) : NULL;
	if (full == NULL) {
		free(rel);
		return NULL;
	}
	/* Seven characters of room in front for "\\?\UNC". */
	p = full + 7;
	got = GetFullPathNameW(rel, need, p, NULL);
	free(rel);
	if (got == 0u || got >= need) {
		free(full);
		return NULL;
	}

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

int
normfs_seed_sys_entropy(uint8_t *buf, size_t len, int *os_error)
{
	*os_error = 0;
	if (len == 0u)
		return 0;

	if (!BCRYPT_SUCCESS(BCryptGenRandom(NULL, buf, (ULONG)len,
	    BCRYPT_USE_SYSTEM_PREFERRED_RNG))) {
		*os_error = ERROR_GEN_FAILURE;
		return -1;
	}
	return 0;
}

static int
normfs_seed_sys_open(const char *path, size_t path_len, int flags, int *os_error)
{
	wchar_t *wide;
	int fd;

	*os_error = 0;
	wide = normfs_seed_sys_wpath(path, path_len);
	if (wide == NULL) {
		*os_error = ERROR_INVALID_NAME;
		return -1;
	}

	_set_doserrno(0);
	if (_wsopen_s(&fd, wide, flags, _SH_DENYNO, _S_IREAD | _S_IWRITE) != 0 || fd < 0) {
		(void)normfs_seed_sys_fail(os_error);
		free(wide);
		return -1;
	}
	free(wide);
	return fd;
}

static int
normfs_seed_sys_last_error(void)
{
	DWORD e = GetLastError();

	return (e > 0u && e <= 0x7FFFFFFFu) ? (int)e : ERROR_GEN_FAILURE;
}

/*
 * The seed gets a protected DACL granting only its owner, the 0600 of the
 * Unix side; the CRT open would leave it whatever the directory hands down.
 * A link at the path is not followed, as O_EXCL does not on Unix.
 */
int
normfs_seed_sys_create_excl(const char *path, size_t path_len, int *os_error)
{
	SECURITY_ATTRIBUTES sa;
	PSECURITY_DESCRIPTOR sd = NULL;
	wchar_t *wide;
	HANDLE h;
	int fd;

	*os_error = 0;
	wide = normfs_seed_sys_wpath(path, path_len);
	if (wide == NULL) {
		*os_error = ERROR_INVALID_NAME;
		return -1;
	}
	if (!ConvertStringSecurityDescriptorToSecurityDescriptorW(L"D:P(A;;FA;;;OW)",
	    SDDL_REVISION_1, &sd, NULL)) {
		*os_error = normfs_seed_sys_last_error();
		free(wide);
		return -1;
	}

	sa.nLength = sizeof(sa);
	sa.lpSecurityDescriptor = sd;
	sa.bInheritHandle = FALSE;
	h = CreateFileW(wide, GENERIC_WRITE | DELETE,
	    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, &sa, CREATE_NEW,
	    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
	if (h == INVALID_HANDLE_VALUE)
		*os_error = normfs_seed_sys_last_error();
	LocalFree(sd);
	free(wide);
	if (h == INVALID_HANDLE_VALUE)
		return -1;

	fd = _open_osfhandle((intptr_t)h, _O_WRONLY | _O_BINARY);
	if (fd < 0) {
		FILE_DISPOSITION_INFO gone;

		/* An empty seed left behind would refuse every later start. */
		gone.DeleteFile = TRUE;
		(void)SetFileInformationByHandle(h, FileDispositionInfo, &gone, sizeof(gone));
		*os_error = ERROR_TOO_MANY_OPEN_FILES;
		CloseHandle(h);
		return -1;
	}
	return fd;
}

int
normfs_seed_sys_open_read(const char *path, size_t path_len, int *os_error)
{
	return normfs_seed_sys_open(path, path_len, _O_RDONLY | _O_BINARY | _O_NOINHERIT,
	    os_error);
}

long
normfs_seed_sys_read(int fd, uint8_t *buf, size_t len, int *os_error)
{
	int n;

	*os_error = 0;
	if (len == 0u)
		return 0L;

	_set_doserrno(0);
	n = _read(fd, buf, (unsigned int)len);
	if (n < 0)
		return (long)normfs_seed_sys_fail(os_error);
	return (long)n;
}

long
normfs_seed_sys_write(int fd, const uint8_t *buf, size_t len, int *os_error)
{
	int n;

	*os_error = 0;
	if (len == 0u)
		return 0L;

	_set_doserrno(0);
	n = _write(fd, buf, (unsigned int)len);
	if (n < 0)
		return (long)normfs_seed_sys_fail(os_error);
	if (n == 0) {
		*os_error = ERROR_WRITE_FAULT;
		return -1L;
	}
	return (long)n;
}

int
normfs_seed_sys_fsync(int fd, int *os_error)
{
	*os_error = 0;

	_set_doserrno(0);
	if (_commit(fd) != 0)
		return normfs_seed_sys_fail(os_error);
	return 0;
}

int
normfs_seed_sys_close(int fd, int *os_error)
{
	*os_error = 0;

	_set_doserrno(0);
	if (_close(fd) != 0)
		return normfs_seed_sys_fail(os_error);
	return 0;
}

int
normfs_seed_sys_exists(const char *path, size_t path_len)
{
	struct _stat64 st;
	wchar_t *wide;
	int found;

	wide = normfs_seed_sys_wpath(path, path_len);
	if (wide == NULL)
		return 0;
	found = _wstat64(wide, &st) == 0;
	free(wide);
	return found;
}

void
normfs_seed_sys_zero(uint8_t *buf, size_t len)
{
	if (len == 0u)
		return;
	SecureZeroMemory(buf, len);
}
