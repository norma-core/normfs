#ifndef NORMFS_NET_SYS_H
#define NORMFS_NET_SYS_H

#include <stddef.h>
#include <stdint.h>

/*
 * The socket syscalls, implemented in src/net_sys.c. Every descriptor is
 * non-blocking and close-on-exec; accept retries EINTR.
 */

/* Family 4 or 6, the 4 or 16 address bytes in network order, the IPv6 scope
 * and the port. */
struct normfs_net_sys_addr {
	uint8_t ip[16];
	uint32_t scope_id;
	uint16_t port;
	uint8_t family;
};

/* nodelay_error and unsent_error are the errno of an option that could not be
 * set on the accepted socket, which does not fail the accept. */
struct normfs_net_sys_accepted {
	struct normfs_net_sys_addr peer;
	int fd;
	int nodelay_error;
	int unsent_error;
};

/* A listening, non-blocking socket bound to addr. fd, or -1 with os_error. */
int normfs_net_sys_listen(const struct normfs_net_sys_addr *addr, int *os_error);

/* The address a listening socket is bound to, port 0 resolved. 0 or -1. */
int normfs_net_sys_local_addr(int fd, struct normfs_net_sys_addr *addr, int *os_error);

/* 1 with a connection, 0 when none is pending, 2 after a run of connections
 * that failed while more may wait, -1 with os_error. */
int normfs_net_sys_accept(int fd, struct normfs_net_sys_accepted *out, int *os_error);

void normfs_net_sys_close(int fd);

#endif /* NORMFS_NET_SYS_H */
