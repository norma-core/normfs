/*
 * The Windows bodies behind normfs/net_sys.h; never given to Frama-C. tokio
 * owns the sockets there, so only reads, writes and shutdown have a body.
 * os_error is the WSA code, which io::Error::from_raw_os_error reads.
 */
#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>

#include "normfs/net_sys.h"

/* recv and send take an int length. */
#define NORMFS_NET_SYS_IO_MAX 0x7ffff000u

static int
normfs_net_sys_wsa_error(void)
{
	int e = WSAGetLastError();

	return e > 0 ? e : WSAEINVAL;
}

long
normfs_net_sys_read(normfs_net_fd fd, uint8_t *buf, size_t off, size_t len, int *os_error)
{
	int n;

	*os_error = 0;
	if (len > NORMFS_NET_SYS_IO_MAX)
		len = NORMFS_NET_SYS_IO_MAX;
	n = recv((SOCKET)fd, (char *)(buf + off), (int)len, 0);
	if (n >= 0)
		return (long)n;
	if (WSAGetLastError() == WSAEWOULDBLOCK)
		return NORMFS_NET_SYS_WOULD_BLOCK;
	*os_error = normfs_net_sys_wsa_error();
	return -1;
}

long
normfs_net_sys_write(normfs_net_fd fd, const uint8_t *buf, size_t off, size_t len,
    int *os_error)
{
	int n;

	*os_error = 0;
	if (len > NORMFS_NET_SYS_IO_MAX)
		len = NORMFS_NET_SYS_IO_MAX;
	n = send((SOCKET)fd, (const char *)(buf + off), (int)len, 0);
	if (n > 0)
		return (long)n;
	if (n < 0 && WSAGetLastError() == WSAEWOULDBLOCK)
		return NORMFS_NET_SYS_WOULD_BLOCK;
	*os_error = n == 0 ? WSAECONNABORTED : normfs_net_sys_wsa_error();
	return -1;
}

void
normfs_net_sys_shutdown_write(normfs_net_fd fd)
{
	(void)shutdown((SOCKET)fd, SD_SEND);
}
