#include "normfs/net.h"
#include "normfs/net_sys.h"

/*@ requires \valid(r);
    assigns *r;
    ensures normfs_net_reader_ok(r);
    ensures r->in_payload == 0 && r->header_have == 0 &&
            r->payload_len == 0 && r->payload_have == 0;
    ensures \forall integer i; 0 <= i < NORMFS_FRAME_HEADER_SIZE ==> r->header[i] == 0;
*/
void
normfs_net_reader_init(struct normfs_net_reader *r)
{
	size_t i;

	/*@ loop invariant 0 <= i <= NORMFS_FRAME_HEADER_SIZE;
	    loop invariant \forall integer k; 0 <= k < i ==> r->header[k] == 0;
	    loop assigns i, r->header[0 .. NORMFS_FRAME_HEADER_SIZE - 1];
	    loop variant NORMFS_FRAME_HEADER_SIZE - i;
	*/
	for (i = 0u; i < NORMFS_FRAME_HEADER_SIZE; i++)
		r->header[i] = 0u;
	r->header_have = 0u;
	r->payload_len = 0u;
	r->payload_have = 0u;
	r->in_payload = 0;
}

/*@ assigns \nothing;
    ensures \result.status == status && \result.os_error == os_error &&
            \result.value == value;
*/
static struct normfs_net_result
normfs_net_result(int status, int os_error, uint64_t value)
{
	struct normfs_net_result r;

	r.value = value;
	r.os_error = os_error;
	r.status = status;
	return r;
}

/*@ requires \valid(r);
    requires normfs_net_reader_ok(r);
    requires normfs_net_header_read(r, fd);
    assigns *r, normfs_net_sys_world;
    ensures normfs_net_reader_ok(r);
    ensures normfs_net_header_read(r, fd);
    ensures normfs_net_reported(\result, net_rlast(fd));
    ensures \result.status == NORMFS_NET_OK || \result.status == NORMFS_NET_WOULD_BLOCK ||
            \result.status == NORMFS_NET_EOF || \result.status == NORMFS_NET_ERR_IO ||
            \result.status == NORMFS_NET_ERR_TOO_LARGE ||
            \result.status == NORMFS_NET_ERR_BAD_LENGTH;
    ensures \result.status == NORMFS_NET_OK || \result.status == NORMFS_NET_ERR_TOO_LARGE ==>
              \result.value == normfs_uintn_le64_logic(&r->header[0]) &&
              \forall integer i; 0 <= i < NORMFS_FRAME_HEADER_SIZE ==>
                r->header[i] == net_in(fd, net_pos(fd) - NORMFS_FRAME_HEADER_SIZE + i);
    ensures \result.status == NORMFS_NET_ERR_TOO_LARGE ==>
              \result.value > NORMFS_FRAME_MAX_PAYLOAD && r->header_have == 0 &&
              r->in_payload == 0;
    ensures \result.status == NORMFS_NET_OK ==>
              \result.value <= NORMFS_FRAME_MAX_PAYLOAD &&
              r->header_have == 0 && r->payload_have == 0 &&
              r->payload_len == \result.value &&
              (\result.value == 0 ==> r->in_payload == 0) &&
              (\result.value > 0 ==> r->in_payload == 1);
    ensures \result.status == NORMFS_NET_OK || \result.status == NORMFS_NET_ERR_TOO_LARGE ==>
              net_pos(fd) == \old(net_pos(fd)) + (NORMFS_FRAME_HEADER_SIZE - \old(r->header_have));
    ensures \result.status != NORMFS_NET_OK && \result.status != NORMFS_NET_ERR_TOO_LARGE ==>
              net_pos(fd) - \old(net_pos(fd)) == r->header_have - \old(r->header_have);
    ensures net_rcalls(fd) - \old(net_rcalls(fd)) <= NORMFS_FRAME_HEADER_SIZE;
    ensures \forall integer g; g != fd ==> net_pos(g) == \old(net_pos(g));
    // A read leaves every output stream as it was, so the write side's facts hold across it.
    ensures \forall integer g; net_opos(g) == \old(net_opos(g));
    ensures \forall integer g, i; net_out(g, i) == \old(net_out(g, i));
    ensures \old(r->in_payload) == 1 <==> \result.status == NORMFS_NET_ERR_BAD_LENGTH;
    ensures \result.status != NORMFS_NET_OK && \result.status != NORMFS_NET_ERR_BAD_LENGTH ==>
              r->in_payload == 0;
    ensures \result.status == NORMFS_NET_ERR_BAD_LENGTH ==>
              r->in_payload == 1 && r->payload_len == \old(r->payload_len) &&
              r->payload_have == \old(r->payload_have);
*/
struct normfs_net_result
normfs_net_read_header(struct normfs_net_reader *r, int fd)
{
	struct normfs_frame_decode_result h;
	int os_error = 0;
	long n;

	if (r->in_payload)
		return normfs_net_result(NORMFS_NET_ERR_BAD_LENGTH, 0, 0u);

	/*@ loop invariant normfs_net_reader_ok(r);
	    loop invariant normfs_net_header_read(r, fd);
	    loop invariant r->in_payload == 0;
	    loop invariant net_pos(fd) - \at(net_pos(fd), Pre) ==
	                   r->header_have - \at(r->header_have, Pre);
	    loop invariant net_rcalls(fd) - \at(net_rcalls(fd), Pre) <=
	                   r->header_have - \at(r->header_have, Pre);
	    loop invariant \forall integer g; g != fd ==> net_pos(g) == \at(net_pos(g), Pre);
	    loop invariant \forall integer g; net_opos(g) == \at(net_opos(g), Pre);
	    loop invariant \forall integer g, i; net_out(g, i) == \at(net_out(g, i), Pre);
	    loop invariant os_error == 0;
	    loop assigns n, os_error, normfs_net_sys_world,
	                 r->header[0 .. NORMFS_FRAME_HEADER_SIZE - 1], r->header_have;
	    loop variant NORMFS_FRAME_HEADER_SIZE - r->header_have;
	*/
	while (r->header_have < NORMFS_FRAME_HEADER_SIZE) {
		n = normfs_net_sys_read(fd, r->header, r->header_have,
		    NORMFS_FRAME_HEADER_SIZE - r->header_have, &os_error);
		if (n == NORMFS_NET_SYS_WOULD_BLOCK)
			return normfs_net_result(NORMFS_NET_WOULD_BLOCK, 0, 0u);
		if (n == -1)
			return normfs_net_result(NORMFS_NET_ERR_IO, os_error, 0u);
		if (n == 0)
			return normfs_net_result(NORMFS_NET_EOF, 0, 0u);
		r->header_have += (size_t)n;
	}

	h = normfs_frame_header_decode(r->header, NORMFS_FRAME_HEADER_SIZE);
	r->header_have = 0u;
	if (h.status != NORMFS_FRAME_OK)
		return normfs_net_result(NORMFS_NET_ERR_TOO_LARGE, 0, h.payload_len);

	r->payload_len = h.payload_len;
	r->in_payload = h.payload_len > 0u ? 1 : 0;
	return normfs_net_result(NORMFS_NET_OK, 0, h.payload_len);
}

/*@ requires \valid(r);
    requires normfs_net_reader_ok(r);
    requires len == 0 || \valid(buf + (0 .. len - 1));
    requires \separated(r, buf + (0 .. len - 1));
    requires r->in_payload == 1 && r->payload_have <= len <= r->payload_len ==>
               normfs_net_payload_read(buf, r->payload_have, fd);
    assigns *r, buf[0 .. len - 1], normfs_net_sys_world;
    ensures normfs_net_reader_ok(r);
    ensures normfs_net_reported(\result, net_rlast(fd));
    ensures \result.status == NORMFS_NET_OK || \result.status == NORMFS_NET_WOULD_BLOCK ||
            \result.status == NORMFS_NET_EOF || \result.status == NORMFS_NET_ERR_IO ||
            \result.status == NORMFS_NET_ERR_BAD_LENGTH || \result.status == NORMFS_NET_YIELD ||
            \result.status == NORMFS_NET_FULL;
    ensures (\old(r->in_payload) == 0 || len < \old(r->payload_have) ||
             len > \old(r->payload_len)) <==>
              \result.status == NORMFS_NET_ERR_BAD_LENGTH;
    ensures \result.status == NORMFS_NET_ERR_BAD_LENGTH ==>
              r->in_payload == \old(r->in_payload) &&
              r->payload_len == \old(r->payload_len) &&
              r->payload_have == \old(r->payload_have);
    ensures \result.status != NORMFS_NET_ERR_BAD_LENGTH && \result.status != NORMFS_NET_OK ==>
              r->in_payload == 1 && r->payload_len == \old(r->payload_len) &&
              \old(r->payload_have) <= r->payload_have <= len &&
              normfs_net_payload_read(buf, r->payload_have, fd);
    ensures \result.status == NORMFS_NET_FULL <==>
              \result.status != NORMFS_NET_ERR_BAD_LENGTH && r->in_payload == 1 &&
              r->payload_have == len && len < r->payload_len;
    ensures \result.status == NORMFS_NET_YIELD ==>
              r->payload_have >= \old(r->payload_have) + NORMFS_NET_BATCH;
    ensures net_rcalls(fd) - \old(net_rcalls(fd)) <= NORMFS_NET_BATCH;
    ensures \forall integer g; g != fd ==> net_pos(g) == \old(net_pos(g));
    ensures \forall integer g; net_opos(g) == \old(net_opos(g));
    ensures \forall integer g, i; net_out(g, i) == \old(net_out(g, i));
    ensures \result.status == NORMFS_NET_OK ==>
              net_pos(fd) == \old(net_pos(fd)) + (len - \old(r->payload_have));
    ensures \result.status != NORMFS_NET_OK ==>
              net_pos(fd) - \old(net_pos(fd)) == r->payload_have - \old(r->payload_have);
    ensures \result.status == NORMFS_NET_OK ==>
              len == \old(r->payload_len) && \result.value == len && r->in_payload == 0 &&
              r->payload_have == 0 && r->payload_len == 0 && r->header_have == 0 &&
              normfs_net_payload_read(buf, len, fd);
*/
struct normfs_net_result
normfs_net_read_payload(struct normfs_net_reader *r, int fd, uint8_t *buf,
    size_t len)
{
	int os_error = 0;
	int calls = 0;
	long n;

	if (!r->in_payload || len < r->payload_have || len > r->payload_len)
		return normfs_net_result(NORMFS_NET_ERR_BAD_LENGTH, 0, 0u);

	/*@ loop invariant normfs_net_reader_ok(r);
	    loop invariant r->in_payload == 1 && r->payload_len == \at(r->payload_len, Pre);
	    loop invariant \at(r->payload_have, Pre) <= r->payload_have <= len;
	    loop invariant normfs_net_payload_read(buf, r->payload_have, fd);
	    loop invariant net_pos(fd) - \at(net_pos(fd), Pre) ==
	                   r->payload_have - \at(r->payload_have, Pre);
	    loop invariant 0 <= calls <= NORMFS_NET_BATCH;
	    loop invariant r->payload_have - \at(r->payload_have, Pre) >= calls;
	    loop invariant net_rcalls(fd) - \at(net_rcalls(fd), Pre) == calls;
	    loop invariant \forall integer g; g != fd ==> net_pos(g) == \at(net_pos(g), Pre);
	    loop invariant \forall integer g; net_opos(g) == \at(net_opos(g), Pre);
	    loop invariant \forall integer g, i; net_out(g, i) == \at(net_out(g, i), Pre);
	    loop invariant os_error == 0;
	    loop assigns n, os_error, calls, r->payload_have, buf[0 .. len - 1],
	                 normfs_net_sys_world;
	    loop variant len - r->payload_have;
	*/
	while (r->payload_have < len) {
		if (calls == NORMFS_NET_BATCH)
			return normfs_net_result(NORMFS_NET_YIELD, 0, 0u);
		n = normfs_net_sys_read(fd, buf, (size_t)r->payload_have,
		    len - (size_t)r->payload_have, &os_error);
		if (n == NORMFS_NET_SYS_WOULD_BLOCK)
			return normfs_net_result(NORMFS_NET_WOULD_BLOCK, 0, 0u);
		if (n == -1)
			return normfs_net_result(NORMFS_NET_ERR_IO, os_error, 0u);
		if (n == 0)
			return normfs_net_result(NORMFS_NET_EOF, 0, 0u);
		r->payload_have += (uint64_t)n;
		calls++;
	}
	if (len < r->payload_len)
		return normfs_net_result(NORMFS_NET_FULL, 0, 0u);

	r->in_payload = 0;
	r->payload_have = 0u;
	r->payload_len = 0u;
	return normfs_net_result(NORMFS_NET_OK, 0, len);
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires \valid(off);
    requires \separated(off, buf + (0 .. len - 1));
    requires *off <= len;
    assigns *off, normfs_net_sys_world;
    ensures \old(*off) <= *off <= len;
    ensures normfs_net_reported(\result, net_wlast(fd));
    ensures \result.status == NORMFS_NET_OK || \result.status == NORMFS_NET_WOULD_BLOCK ||
            \result.status == NORMFS_NET_ERR_IO || \result.status == NORMFS_NET_YIELD;
    ensures \result.status == NORMFS_NET_OK <==> *off == len;
    ensures \result.status == NORMFS_NET_YIELD ==> *off >= \old(*off) + NORMFS_NET_BATCH;
    ensures net_wcalls(fd) - \old(net_wcalls(fd)) <= NORMFS_NET_BATCH;
    // Every read side stays where it was, so a reader's predicates hold across a write.
    ensures \forall integer g; net_pos(g) == \old(net_pos(g));
    ensures net_opos(fd) == \old(net_opos(fd)) + (*off - \old(*off));
    ensures \forall integer k; \old(*off) <= k < *off ==>
              net_out(fd, \old(net_opos(fd)) + (k - \old(*off))) == buf[k];
    ensures \forall integer i; i < \old(net_opos(fd)) ==>
              net_out(fd, i) == \old(net_out(fd, i));
    ensures \forall integer g; g != fd ==> net_opos(g) == \old(net_opos(g));
    ensures \forall integer g, i; g != fd ==> net_out(g, i) == \old(net_out(g, i));
*/
struct normfs_net_result
normfs_net_write(int fd, const uint8_t *buf, size_t len, size_t *off)
{
	int os_error = 0;
	int calls = 0;
	long n;

	/*@ loop invariant \at(*off, Pre) <= *off <= len;
	    loop invariant 0 <= calls <= NORMFS_NET_BATCH;
	    loop invariant *off - \at(*off, Pre) >= calls;
	    loop invariant net_wcalls(fd) - \at(net_wcalls(fd), Pre) == calls;
	    loop invariant \forall integer g; net_pos(g) == \at(net_pos(g), Pre);
	    loop invariant os_error == 0;
	    loop invariant net_opos(fd) == \at(net_opos(fd), Pre) + (*off - \at(*off, Pre));
	    loop invariant \forall integer k; \at(*off, Pre) <= k < *off ==>
	      net_out(fd, \at(net_opos(fd), Pre) + (k - \at(*off, Pre))) == buf[k];
	    loop invariant \forall integer i; i < \at(net_opos(fd), Pre) ==>
	      net_out(fd, i) == \at(net_out(fd, i), Pre);
	    loop invariant \forall integer g; g != fd ==> net_opos(g) == \at(net_opos(g), Pre);
	    loop invariant \forall integer g, i; g != fd ==>
	      net_out(g, i) == \at(net_out(g, i), Pre);
	    loop assigns n, os_error, calls, *off, normfs_net_sys_world;
	    loop variant len - *off;
	*/
	while (*off < len) {
		if (calls == NORMFS_NET_BATCH)
			return normfs_net_result(NORMFS_NET_YIELD, 0, 0u);
		n = normfs_net_sys_write(fd, buf, *off, len - *off, &os_error);
		if (n == NORMFS_NET_SYS_WOULD_BLOCK)
			return normfs_net_result(NORMFS_NET_WOULD_BLOCK, 0, 0u);
		if (n == -1)
			return normfs_net_result(NORMFS_NET_ERR_IO, os_error, 0u);
		*off += (size_t)n;
		calls++;
	}
	return normfs_net_result(NORMFS_NET_OK, 0, 0u);
}

/*@ requires \valid(out) && \valid(os_error) && \separated(out, os_error);
    assigns *out, *os_error, normfs_net_sys_world;
    ensures -1 <= \result <= 2;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result != -1 ==> *os_error == 0;
    ensures \result == 1 ==> out->fd >= 0;
    ensures \result != 1 ==> out->fd == -1;
*/
int
normfs_net_accept(int fd, struct normfs_net_sys_accepted *out, int *os_error)
{
	int tries;
	int rc;

	/*@ loop invariant 0 <= tries <= NORMFS_NET_ACCEPT_TRIES;
	    loop invariant tries > 0 ==> *os_error == 0 && out->fd == -1;
	    loop assigns tries, rc, *out, *os_error, normfs_net_sys_world;
	    loop variant NORMFS_NET_ACCEPT_TRIES - tries;
	*/
	for (tries = 0; tries < NORMFS_NET_ACCEPT_TRIES; tries++) {
		rc = normfs_net_sys_accept(fd, out, os_error);
		if (rc != 2)
			return rc;
	}
	return 2;
}
