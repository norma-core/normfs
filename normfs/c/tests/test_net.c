/*
 * src/net.c is proved, but the socket syscalls it reaches are assumed
 * contracts in normfs/net_sys.h. These tests discharge them on loopback.
 */
#define _POSIX_C_SOURCE 200809L
#define _DEFAULT_SOURCE 1
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#endif

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "normfs/net.h"
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

/* Reads what fd has, checking it continues want; -1 on a byte out of place. */
static int
drain(int fd, const uint8_t *want, size_t want_len, size_t *got)
{
	uint8_t sink[65536];
	ssize_t n;

	while ((n = read(fd, sink, sizeof(sink))) > 0) {
		if ((size_t)n > want_len - *got || memcmp(sink, want + *got, (size_t)n) != 0)
			return -1;
		*got += (size_t)n;
	}
	return 0;
}

int
main(void)
{
	struct normfs_net_sys_addr v4 = {{127, 0, 0, 1}, 0, 0, 4};
	struct normfs_net_sys_addr v6 = {{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, 0, 0, 6};
	struct normfs_net_sys_addr bound;
	struct normfs_net_sys_accepted acc;
	struct normfs_net_reader r;
	struct normfs_net_result res;
	uint8_t frame[8 + 5] = {5, 0, 0, 0, 0, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'};
	uint8_t payload[5];
	uint8_t big[1 << 20];
	size_t off, got, i;
	int rounds;
	int os_error, lfd, client, value;
	socklen_t len;

	/* The Rust mirrors in net.rs assert the same sizes. */
	CHECK(sizeof(struct normfs_net_sys_addr) == 24 &&
	    sizeof(struct normfs_net_sys_accepted) == 36);
	CHECK(sizeof(struct normfs_net_result) == 16 && sizeof(struct normfs_net_reader) == 40);

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
	CHECK(fcntl(client, F_SETFL, fcntl(client, F_GETFL) | O_NONBLOCK) == 0);
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

	normfs_net_reader_init(&r);
	res = normfs_net_read_header(&r, acc.fd);
	CHECK(res.status == NORMFS_NET_WOULD_BLOCK && r.header_have == 0);

	/* A header split over two writes, then the payload. */
	CHECK(write(client, frame, 3) == 3);
	wait_fd(acc.fd, POLLIN);
	res = normfs_net_read_header(&r, acc.fd);
	CHECK(res.status == NORMFS_NET_WOULD_BLOCK && r.header_have == 3);
	/* A write between the two halves takes nothing from the input. */
	off = 0u;
	res = normfs_net_write(acc.fd, frame, sizeof(frame), &off);
	CHECK(res.status == NORMFS_NET_OK && off == sizeof(frame));
	wait_fd(client, POLLIN);
	got = 0u;
	CHECK(drain(client, frame, sizeof(frame), &got) == 0 && got == sizeof(frame));
	CHECK(write(client, frame + 3, sizeof(frame) - 3) == (ssize_t)(sizeof(frame) - 3));
	wait_fd(acc.fd, POLLIN);
	res = normfs_net_read_header(&r, acc.fd);
	CHECK(res.status == NORMFS_NET_OK && res.value == 5);
	res = normfs_net_read_payload(&r, acc.fd, payload, sizeof(payload));
	CHECK(res.status == NORMFS_NET_OK && memcmp(payload, "hello", 5) == 0);

	/* Writes until the socket buffer is full, draining it in between; the
	 * peer gets every byte once and in order. */
	for (i = 0; i < sizeof(big); i++)
		big[i] = (uint8_t)(i * 31 + i / 251);
	off = 0;
	got = 0;
	for (rounds = 0; rounds < 10000; rounds++) {
		res = normfs_net_write(acc.fd, big, sizeof(big), &off);
		if (res.status != NORMFS_NET_WOULD_BLOCK && res.status != NORMFS_NET_YIELD)
			break;
		CHECK(drain(client, big, sizeof(big), &got) == 0);
		if (res.status == NORMFS_NET_WOULD_BLOCK)
			wait_fd(acc.fd, POLLOUT);
	}
	CHECK(res.status == NORMFS_NET_OK && off == sizeof(big));
	for (rounds = 0; rounds < 100 && got < sizeof(big); rounds++) {
		wait_fd(client, POLLIN);
		CHECK(drain(client, big, sizeof(big), &got) == 0);
	}
	CHECK(got == sizeof(big));

	/* A clean close with nothing pending is the end of the stream. */
	CHECK(shutdown(client, SHUT_WR) == 0);
	wait_fd(acc.fd, POLLIN);
	res = normfs_net_read_header(&r, acc.fd);
	CHECK(res.status == NORMFS_NET_EOF && res.os_error == 0);

	/* Unread data on close makes a reset: writes fail, and the one after the
	 * reset with EPIPE, without raising SIGPIPE. */
	CHECK(write(acc.fd, "x", 1) == 1);
	wait_fd(client, POLLIN);
	close(client);
	off = 0;
	for (rounds = 0; rounds < 10000; rounds++) {
		res = normfs_net_write(acc.fd, big, sizeof(big), &off);
		if (res.status == NORMFS_NET_ERR_IO && res.os_error == EPIPE)
			break;
		CHECK(res.status != NORMFS_NET_ERR_IO || res.os_error == ECONNRESET);
		if (res.status == NORMFS_NET_OK || res.status == NORMFS_NET_ERR_IO)
			off = 0;
		if (res.status == NORMFS_NET_WOULD_BLOCK)
			wait_fd(acc.fd, POLLOUT);
	}
	CHECK(res.status == NORMFS_NET_ERR_IO && res.os_error == EPIPE);

	normfs_net_sys_shutdown_write(acc.fd);
	normfs_net_sys_close(acc.fd);

	/* SO_LINGER 0 closes with a reset, which the next read reports. */
	client = connect_to(&bound);
	CHECK(client >= 0);
	wait_fd(lfd, POLLIN);
	CHECK(normfs_net_sys_accept(lfd, &acc, &os_error) == 1);
	{
		struct linger lg = {1, 0};
		CHECK(setsockopt(client, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg)) == 0);
	}
	close(client);
	normfs_net_reader_init(&r);
	for (rounds = 0; rounds < 100; rounds++) {
		wait_fd(acc.fd, POLLIN);
		res = normfs_net_read_header(&r, acc.fd);
		if (res.status != NORMFS_NET_WOULD_BLOCK)
			break;
	}
	CHECK(res.status == NORMFS_NET_ERR_IO && res.os_error > 0);
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
