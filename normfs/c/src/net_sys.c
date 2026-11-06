/*
 * Bodies behind normfs/net_sys.h; never given to Frama-C. The macros must
 * precede every #include: -std=c99 hides the POSIX declarations.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__linux__)
#define _GNU_SOURCE 1
#endif
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "normfs/net_sys.h"

/* The kernel would otherwise queue megabytes per socket on a slow link, and
 * a pong or write ack would wait behind all of it. */
#define NORMFS_NET_SYS_UNSENT_LIMIT (128 * 1024)

/* macOS fails a read or write of more than INT_MAX bytes with EINVAL; Linux
 * caps one at this much itself. */
#define NORMFS_NET_SYS_IO_MAX 0x7ffff000u

static int
normfs_net_sys_errno(void)
{
	return errno > 0 ? errno : EIO;
}

static int
normfs_net_sys_would_block(int e)
{
	return e == EAGAIN || e == EWOULDBLOCK;
}

/* Failed connections one accept call takes off the queue before Rust gets a turn. */
#define NORMFS_NET_SYS_ACCEPT_TRIES 64

long
normfs_net_sys_read(int fd, uint8_t *buf, size_t off, size_t len, int *os_error)
{
	ssize_t n;

	*os_error = 0;
	if (len > NORMFS_NET_SYS_IO_MAX)
		len = NORMFS_NET_SYS_IO_MAX;
	do {
		n = read(fd, buf + off, len);
	} while (n < 0 && errno == EINTR);
	if (n >= 0)
		return (long)n;
	if (normfs_net_sys_would_block(errno))
		return NORMFS_NET_SYS_WOULD_BLOCK;
	*os_error = normfs_net_sys_errno();
	return -1;
}

long
normfs_net_sys_write(int fd, const uint8_t *buf, size_t off, size_t len,
    int *os_error)
{
	ssize_t n;

	*os_error = 0;
	if (len > NORMFS_NET_SYS_IO_MAX)
		len = NORMFS_NET_SYS_IO_MAX;
	do {
#if defined(MSG_NOSIGNAL)
		n = send(fd, buf + off, len, MSG_NOSIGNAL);
#else
		n = write(fd, buf + off, len);
#endif
	} while (n < 0 && errno == EINTR);
	if (n > 0)
		return (long)n;
	if (n < 0 && normfs_net_sys_would_block(errno))
		return NORMFS_NET_SYS_WOULD_BLOCK;
	*os_error = n == 0 ? EIO : normfs_net_sys_errno();
	return -1;
}

#if !(defined(SOCK_CLOEXEC) && defined(SOCK_NONBLOCK))
static int
normfs_net_sys_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL);

	if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
		return -1;
	if (fcntl(fd, F_SETFD, FD_CLOEXEC) < 0)
		return -1;
	return 0;
}
#endif

static socklen_t
normfs_net_sys_to_sockaddr(const struct normfs_net_sys_addr *addr,
    struct sockaddr_storage *ss)
{
	memset(ss, 0, sizeof(*ss));
	if (addr->family == 4) {
		struct sockaddr_in *in = (struct sockaddr_in *)ss;
		in->sin_family = AF_INET;
		in->sin_port = htons(addr->port);
		memcpy(&in->sin_addr, addr->ip, 4);
		return sizeof(*in);
	}
	{
		struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)ss;
		in6->sin6_family = AF_INET6;
		in6->sin6_port = htons(addr->port);
		in6->sin6_scope_id = addr->scope_id;
		memcpy(&in6->sin6_addr, addr->ip, 16);
		return sizeof(*in6);
	}
}

static void
normfs_net_sys_from_sockaddr(const struct sockaddr_storage *ss,
    struct normfs_net_sys_addr *addr)
{
	memset(addr, 0, sizeof(*addr));
	if (ss->ss_family == AF_INET) {
		const struct sockaddr_in *in = (const struct sockaddr_in *)ss;
		addr->family = 4;
		addr->port = ntohs(in->sin_port);
		memcpy(addr->ip, &in->sin_addr, 4);
	} else if (ss->ss_family == AF_INET6) {
		const struct sockaddr_in6 *in6 = (const struct sockaddr_in6 *)ss;
		addr->family = 6;
		addr->port = ntohs(in6->sin6_port);
		addr->scope_id = in6->sin6_scope_id;
		memcpy(addr->ip, &in6->sin6_addr, 16);
	}
}

int
normfs_net_sys_listen(const struct normfs_net_sys_addr *addr, int *os_error)
{
	struct sockaddr_storage ss;
	socklen_t ss_len = normfs_net_sys_to_sockaddr(addr, &ss);
	int one = 1;
	int fd;

	*os_error = 0;
#if defined(SOCK_CLOEXEC) && defined(SOCK_NONBLOCK)
	fd = socket(ss.ss_family, SOCK_STREAM | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
#else
	fd = socket(ss.ss_family, SOCK_STREAM, 0);
	if (fd >= 0 && normfs_net_sys_nonblocking(fd) < 0) {
		*os_error = normfs_net_sys_errno();
		close(fd);
		return -1;
	}
#endif
	if (fd < 0) {
		*os_error = normfs_net_sys_errno();
		return -1;
	}
	/* What tokio's TcpListener::bind sets as well; accepted sockets inherit
	 * SO_NOSIGPIPE. */
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
#ifdef SO_NOSIGPIPE
	    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0 ||
#endif
	    bind(fd, (struct sockaddr *)&ss, ss_len) < 0 ||
	    listen(fd, 1024) < 0) {
		*os_error = normfs_net_sys_errno();
		close(fd);
		return -1;
	}
	return fd;
}

int
normfs_net_sys_local_addr(int fd, struct normfs_net_sys_addr *addr, int *os_error)
{
	struct sockaddr_storage ss;
	socklen_t len = sizeof(ss);

	*os_error = 0;
	if (getsockname(fd, (struct sockaddr *)&ss, &len) < 0) {
		*os_error = normfs_net_sys_errno();
		return -1;
	}
	normfs_net_sys_from_sockaddr(&ss, addr);
	return 0;
}

int
normfs_net_sys_accept(int fd, struct normfs_net_sys_accepted *out, int *os_error)
{
	struct sockaddr_storage ss;
	socklen_t len;
	int one = 1;
	int tries = 0;
	int c;

	*os_error = 0;
	memset(out, 0, sizeof(*out));
	out->fd = -1;
	for (;;) {
		len = sizeof(ss);
#if defined(SOCK_CLOEXEC) && defined(SOCK_NONBLOCK)
		c = accept4(fd, (struct sockaddr *)&ss, &len, SOCK_CLOEXEC | SOCK_NONBLOCK);
#else
		c = accept(fd, (struct sockaddr *)&ss, &len);
#endif
		if (c >= 0)
			break;
		if (errno == EINTR)
			continue;
		if (errno == ECONNABORTED) {
			if (++tries < NORMFS_NET_SYS_ACCEPT_TRIES)
				continue;
			return 2;
		}
		if (normfs_net_sys_would_block(errno))
			return 0;
		*os_error = normfs_net_sys_errno();
		return -1;
	}
#if !(defined(SOCK_CLOEXEC) && defined(SOCK_NONBLOCK))
	if (normfs_net_sys_nonblocking(c) < 0) {
		*os_error = normfs_net_sys_errno();
		close(c);
		return -1;
	}
#endif

#if defined(SO_NOSIGPIPE)
	(void)setsockopt(c, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

	out->fd = c;
	normfs_net_sys_from_sockaddr(&ss, &out->peer);
	/* A response is one write, but Nagle would still hold one that follows
	 * an unacknowledged response until the client's delayed ACK. */
	if (setsockopt(c, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
		out->nodelay_error = normfs_net_sys_errno();
#if defined(__linux__)
	{
		int limit = NORMFS_NET_SYS_UNSENT_LIMIT;
		if (setsockopt(c, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &limit,
		    sizeof(limit)) < 0)
			out->unsent_error = normfs_net_sys_errno();
	}
#endif
	return 1;
}

void
normfs_net_sys_shutdown_write(int fd)
{
	(void)shutdown(fd, SHUT_WR);
}

void
normfs_net_sys_close(int fd)
{
	/* Not retried on EINTR: the descriptor is released either way. */
	(void)close(fd);
}
