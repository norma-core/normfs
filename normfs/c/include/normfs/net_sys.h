#ifndef NORMFS_NET_SYS_H
#define NORMFS_NET_SYS_H

#include <stddef.h>
#include <stdint.h>

/*
 * The socket syscalls, implemented in src/net_sys.c, which Frama-C never
 * sees; src/net.c is proved against the read and write contracts below.
 * Every descriptor is non-blocking and close-on-exec; read, write and accept
 * retry EINTR.
 */

#define NORMFS_NET_SYS_WOULD_BLOCK (-2)

/*
 * A connection as two byte streams. net_in is what the peer sends, fixed in
 * advance; net_pos counts how much of it has been read. net_out holds what has
 * been written and net_opos how much. net_rlast and net_wlast are the last
 * read and write results, net_rcalls and net_wcalls count the calls. A read
 * changes only its fd's read side and a write only its write side, which is
 * what lets one task read while another writes.
 */
/*@ ghost extern int normfs_net_sys_world; */
/*@ axiomatic NormfsNetKernel {
      logic integer net_in(integer fd, integer i);
      logic integer net_pos{L}(integer fd) reads normfs_net_sys_world;
      logic integer net_out{L}(integer fd, integer i) reads normfs_net_sys_world;
      logic integer net_opos{L}(integer fd) reads normfs_net_sys_world;
      logic integer net_rlast{L}(integer fd) reads normfs_net_sys_world;
      logic integer net_wlast{L}(integer fd) reads normfs_net_sys_world;
      logic integer net_rcalls{L}(integer fd) reads normfs_net_sys_world;
      logic integer net_wcalls{L}(integer fd) reads normfs_net_sys_world;
    }
*/

/* read(2) into buf[off .. off + len): bytes read, 0 at end of stream,
 * WOULD_BLOCK, or -1. */
/*@ requires len >= 1;
    requires \valid(buf + (off .. off + len - 1));
    requires \valid(os_error);
    requires \separated(os_error, buf + (off .. off + len - 1));
    assigns buf[off .. off + len - 1], *os_error, normfs_net_sys_world;
    ensures NORMFS_NET_SYS_WOULD_BLOCK <= \result <= len;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result != -1 ==> *os_error == 0;
    ensures net_rlast(fd) == \result;
    ensures \result > 0 ==> net_pos(fd) == \old(net_pos(fd)) + \result;
    ensures \result <= 0 ==> net_pos(fd) == \old(net_pos(fd));
    ensures \forall integer j; off <= j < off + \result ==>
              buf[j] == net_in(fd, \old(net_pos(fd)) + (j - off));
    ensures net_rcalls(fd) == \old(net_rcalls(fd)) + 1;
    ensures net_opos(fd) == \old(net_opos(fd)) && net_wlast(fd) == \old(net_wlast(fd)) &&
            net_wcalls(fd) == \old(net_wcalls(fd));
    ensures \forall integer g, i; net_out(g, i) == \old(net_out(g, i));
    ensures \forall integer g; g != fd ==>
              net_pos(g) == \old(net_pos(g)) && net_rlast(g) == \old(net_rlast(g)) &&
              net_rcalls(g) == \old(net_rcalls(g)) && net_opos(g) == \old(net_opos(g)) &&
              net_wlast(g) == \old(net_wlast(g)) && net_wcalls(g) == \old(net_wcalls(g));
*/
long normfs_net_sys_read(int fd, uint8_t *buf, size_t off, size_t len,
    int *os_error);

/* write(2) of buf[off .. off + len): bytes written, at least one,
 * WOULD_BLOCK, or -1. */
/*@ requires len >= 1;
    requires \valid_read(buf + (off .. off + len - 1));
    requires \valid(os_error);
    requires \separated(os_error, buf + (off .. off + len - 1));
    assigns *os_error, normfs_net_sys_world;
    ensures \result == NORMFS_NET_SYS_WOULD_BLOCK || \result == -1 ||
            1 <= \result <= len;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result != -1 ==> *os_error == 0;
    ensures net_wlast(fd) == \result;
    ensures \result > 0 ==> net_opos(fd) == \old(net_opos(fd)) + \result;
    ensures \result <= 0 ==> net_opos(fd) == \old(net_opos(fd));
    ensures \forall integer j; off <= j < off + \result ==>
              net_out(fd, \old(net_opos(fd)) + (j - off)) == buf[j];
    ensures \forall integer i; i < \old(net_opos(fd)) ==>
              net_out(fd, i) == \old(net_out(fd, i));
    ensures net_wcalls(fd) == \old(net_wcalls(fd)) + 1;
    ensures net_pos(fd) == \old(net_pos(fd)) && net_rlast(fd) == \old(net_rlast(fd)) &&
            net_rcalls(fd) == \old(net_rcalls(fd));
    ensures \forall integer g, i; g != fd ==> net_out(g, i) == \old(net_out(g, i));
    ensures \forall integer g; g != fd ==>
              net_pos(g) == \old(net_pos(g)) && net_rlast(g) == \old(net_rlast(g)) &&
              net_rcalls(g) == \old(net_rcalls(g)) && net_opos(g) == \old(net_opos(g)) &&
              net_wlast(g) == \old(net_wlast(g)) && net_wcalls(g) == \old(net_wcalls(g));
*/
long normfs_net_sys_write(int fd, const uint8_t *buf, size_t off, size_t len,
    int *os_error);

/* Family 4 or 6, the 4 or 16 address bytes in network order, the IPv6 scope
 * and the port. */
struct normfs_net_sys_addr {
	uint8_t ip[16];
	uint32_t scope_id;
	uint16_t port;
	uint8_t family;
};

/* A connection and its peer. nodelay_error and unsent_error are the errno of an
 * option that could not be set, which does not fail the accept. */
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

/* One accept, EINTR retried: 1 with a connection, 0 when none is pending, 2
 * when the pending one failed and left the queue, -1 with os_error. */
/*@ requires \valid(out) && \valid(os_error) && \separated(out, os_error);
    assigns *out, *os_error, normfs_net_sys_world;
    ensures -1 <= \result <= 2;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result != -1 ==> *os_error == 0;
    ensures \result == 1 ==> out->fd >= 0;
    ensures \result != 1 ==> out->fd == -1;
*/
int normfs_net_sys_accept(int fd, struct normfs_net_sys_accepted *out, int *os_error);

/* shutdown(SHUT_WR); errors are ignored, the peer may already be gone. */
void normfs_net_sys_shutdown_write(int fd);

void normfs_net_sys_close(int fd);

#endif /* NORMFS_NET_SYS_H */
