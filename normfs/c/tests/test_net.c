/*
 * The socket syscalls behind normfs/net_sys.h, on loopback.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "normfs/net_sys.h"

/* assert() is a no-op under NDEBUG, which the Release build defines. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "net: FAIL %s:%d: %s\n",        \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static void
wait_fd(int fd, short events)
{
	struct pollfd p;

	p.fd = fd;
	p.events = events;
	p.revents = 0;
	(void)poll(&p, 1, 2000);
}

static int
connect_to(const struct normfs_net_sys_addr *addr)
{
	struct sockaddr_storage ss;
	socklen_t len;
	int fd;

	memset(&ss, 0, sizeof(ss));
	if (addr->family == 4) {
		struct sockaddr_in *in = (struct sockaddr_in *)&ss;
		in->sin_family = AF_INET;
		in->sin_port = htons(addr->port);
		memcpy(&in->sin_addr, addr->ip, 4);
		len = sizeof(*in);
	} else {
		struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&ss;
		in6->sin6_family = AF_INET6;
		in6->sin6_port = htons(addr->port);
		memcpy(&in6->sin6_addr, addr->ip, 16);
		len = sizeof(*in6);
	}
	fd = socket(ss.ss_family, SOCK_STREAM, 0);
	if (fd < 0 || connect(fd, (struct sockaddr *)&ss, len) < 0)
		return -1;
	return fd;
}

int
main(void)
{
	struct normfs_net_sys_addr v4 = {{127, 0, 0, 1}, 0, 0, 4};
	struct normfs_net_sys_addr v6 = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 0, 0, 6};
	struct normfs_net_sys_addr bound;
	struct normfs_net_sys_accepted acc;
	int os_error, lfd, client, value;
	socklen_t len;

	/* The Rust mirrors in net.rs assert the same sizes. */
	CHECK(sizeof(struct normfs_net_sys_addr) == 24 &&
	    sizeof(struct normfs_net_sys_accepted) == 36);

	lfd = normfs_net_sys_listen(&v4, &os_error);
	CHECK(lfd >= 0 && os_error == 0);
	CHECK((fcntl(lfd, F_GETFL) & O_NONBLOCK) != 0);
	CHECK((fcntl(lfd, F_GETFD) & FD_CLOEXEC) != 0);
	CHECK(normfs_net_sys_local_addr(lfd, &bound, &os_error) == 0);
	CHECK(bound.family == 4 && bound.port != 0 && bound.ip[0] == 127);

	CHECK(normfs_net_sys_accept(lfd, &acc, &os_error) == 0 && os_error == 0);
	CHECK(acc.fd == -1);

	client = connect_to(&bound);
	CHECK(client >= 0);
	wait_fd(lfd, POLLIN);
	CHECK(normfs_net_sys_accept(lfd, &acc, &os_error) == 1 && os_error == 0);
	CHECK(acc.fd >= 0 && acc.peer.family == 4 && acc.peer.port != 0 &&
	    acc.peer.ip[0] == 127);
	CHECK((fcntl(acc.fd, F_GETFL) & O_NONBLOCK) != 0);
	CHECK((fcntl(acc.fd, F_GETFD) & FD_CLOEXEC) != 0);
	CHECK(acc.nodelay_error == 0 && acc.unsent_error == 0);
	value = 0;
	len = sizeof(value);
	CHECK(getsockopt(acc.fd, IPPROTO_TCP, TCP_NODELAY, &value, &len) == 0 && value != 0);
#if defined(SO_NOSIGPIPE)
	CHECK(getsockopt(acc.fd, SOL_SOCKET, SO_NOSIGPIPE, &value, &len) == 0 && value != 0);
#endif
#if defined(__linux__)
	CHECK(getsockopt(acc.fd, IPPROTO_TCP, TCP_NOTSENT_LOWAT, &value, &len) == 0 &&
	    value == 128 * 1024);
#endif
	close(client);
	normfs_net_sys_close(acc.fd);

	/* A port already taken. */
	CHECK(normfs_net_sys_listen(&bound, &os_error) == -1 && os_error > 0);
	normfs_net_sys_close(lfd);

	/* ::1, where the host has IPv6 at all. */
	lfd = normfs_net_sys_listen(&v6, &os_error);
	if (lfd >= 0) {
		CHECK(normfs_net_sys_local_addr(lfd, &bound, &os_error) == 0);
		CHECK(bound.family == 6 && bound.port != 0 && bound.ip[15] == 1);
		client = connect_to(&bound);
		CHECK(client >= 0);
		wait_fd(lfd, POLLIN);
		CHECK(normfs_net_sys_accept(lfd, &acc, &os_error) == 1);
		CHECK(acc.peer.family == 6 && acc.peer.ip[15] == 1);
		close(client);
		normfs_net_sys_close(acc.fd);
		normfs_net_sys_close(lfd);
	}
	return 0;
}
