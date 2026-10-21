#include "normfs/conn.h"
#include "normfs/net.h"
#include "normfs/net_sys.h"

/* client_handler.rs mirrors the event structs and asserts the same sizes. */
typedef char normfs_conn_event_size_ok[
    sizeof(struct normfs_conn_event) == 40 && sizeof(struct normfs_conn_events) == 24 ? 1 : -1];

/* Spelt out so WP splits them: an opaque predicate makes every field write
 * re-prove the whole invariant at once. */
#define NORMFS_CONN_RX_OK(r) \
	normfs_conn_rx_reader_ok(r) && normfs_conn_rx_flags_ok(r) && normfs_conn_rx_payload_ok(r)
#define NORMFS_CONN_TX_OK(t) \
	normfs_conn_tx_flags_ok(t) && normfs_conn_queue_ok(&(t)->q[0], t) && \
	normfs_conn_queue_ok(&(t)->q[1], t)
#define NORMFS_CONN_NOW_OK(now) 0 <= now <= 0x7fffffffffffffff - NORMFS_CONN_IDLE_MS

/*@ predicate normfs_conn_room(struct normfs_conn_events *out, integer k) =
      out->cap - out->n >= k;
*/

/*@ requires \valid(out) && out->n < out->cap && \valid(out->ev + out->n);
    requires \separated(out, out->ev + out->n);
    assigns out->ev[out->n], out->n;
    ensures out->n == \old(out->n) + 1;
    ensures out->ev[\old(out->n)].conn == conn && out->ev[\old(out->n)].kind == kind &&
            out->ev[\old(out->n)].value == value && out->ev[\old(out->n)].buf == buf &&
            out->ev[\old(out->n)].status == status &&
            out->ev[\old(out->n)].os_error == os_error &&
            out->ev[\old(out->n)].in_payload == in_payload;
*/
static void
normfs_conn_emit(struct normfs_conn_events *out, uint64_t conn, int kind, uint64_t value,
    uint8_t *buf, int status, int os_error, int in_payload)
{
	struct normfs_conn_event *e = &out->ev[out->n];

	e->conn = conn;
	e->kind = kind;
	e->value = value;
	e->buf = buf;
	e->status = status;
	e->os_error = os_error;
	e->in_payload = in_payload;
	out->n++;
}

/*@ requires \valid(r);
    requires NORMFS_CONN_NOW_OK(now);
    assigns *r;
    ensures NORMFS_CONN_RX_OK(r);
    ensures r->fd == fd && r->token == token;
    ensures r->reading == 1 && r->want_buffer == 0 && r->pending == 0 &&
            r->payload == \null && r->rd.in_payload == 0;
    ensures r->deadline_ms == now + NORMFS_CONN_IDLE_MS;
*/
void
normfs_conn_rx_init(struct normfs_conn_rx *r, int fd, uint64_t token, int64_t now)
{
	normfs_net_reader_init(&r->rd);
	r->payload = (uint8_t *)0;
	r->token = token;
	r->deadline_ms = now + NORMFS_CONN_IDLE_MS;
	r->fd = fd;
	r->pending = 0u;
	r->reading = 1;
	r->want_buffer = 0;
}

/* Ends the read side; a payload buffer goes back with the event. */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r) && r->reading == 1;
    requires \valid(out) && normfs_conn_rx_events_ok(out, r) && normfs_conn_room(out, 1);
    assigns r->reading, r->want_buffer, r->payload, out->n, out->ev[out->n];
    ensures NORMFS_CONN_RX_OK(r);
    ensures r->reading == 0;
    ensures out->n == \old(out->n) + 1;
    ensures out->ev[\old(out->n)].kind == NORMFS_CONN_EV_READ_CLOSED &&
            out->ev[\old(out->n)].conn == r->token &&
            out->ev[\old(out->n)].buf == \old(r->payload) &&
            out->ev[\old(out->n)].status == status;
*/
static void
normfs_conn_close_read(struct normfs_conn_rx *r, struct normfs_conn_events *out, int status,
    int os_error, uint64_t value)
{
	normfs_conn_emit(out, r->token, NORMFS_CONN_EV_READ_CLOSED, value, r->payload, status,
	    os_error, r->rd.in_payload);
	/*@ assert NORMFS_CONN_RX_OK(r); */
	r->payload = (uint8_t *)0;
	r->want_buffer = 0;
	r->reading = 0;
}

/*
 * Reads frames until the socket would block, a payload needs a buffer,
 * NORMFS_CONN_PENDING requests wait on Rust, the read side closes, or
 * NORMFS_CONN_FRAMES frames are through. 1 when the socket would block,
 * so the caller waits for readiness, 0 otherwise.
 */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r);
    requires \valid(out) && normfs_conn_rx_events_ok(out, r);
    requires normfs_conn_room(out, NORMFS_CONN_FRAMES);
    requires NORMFS_CONN_NOW_OK(now);
    assigns r->rd, r->payload, r->deadline_ms, r->pending, r->reading, r->want_buffer,
            r->payload[0 .. r->rd.payload_len - 1],
            out->n, out->ev[0 .. out->cap - 1], normfs_net_sys_world;
    ensures NORMFS_CONN_RX_OK(r);
    ensures \old(out->n) <= out->n <= \old(out->n) + NORMFS_CONN_FRAMES;
    ensures r->pending >= \old(r->pending);
    ensures \old(r->reading) == 0 ==> out->n == \old(out->n) && r->reading == 0;
    ensures \forall integer g; g != r->fd ==> net_pos(g) == \old(net_pos(g));
    ensures \forall integer k; \old(out->n) <= k < out->n ==>
              out->ev[k].conn == r->token &&
              (out->ev[k].kind == NORMFS_CONN_EV_NEED_BUFFER ||
               out->ev[k].kind == NORMFS_CONN_EV_REQUEST ||
               out->ev[k].kind == NORMFS_CONN_EV_READ_CLOSED);
    ensures \forall integer k; \old(out->n) < k < out->n ==>
              out->ev[k].kind != NORMFS_CONN_EV_REQUEST;
    ensures \forall integer k; \old(out->n) <= k < out->n ==>
              out->ev[k].kind != NORMFS_CONN_EV_REQUEST ==> k == out->n - 1;
    ensures out->n > \old(out->n) && out->ev[\old(out->n)].kind == NORMFS_CONN_EV_REQUEST ==>
              r->pending == \old(r->pending) + 1 && r->payload == \null &&
              \old(r->payload) != \null && out->ev[\old(out->n)].buf == \old(r->payload) &&
              out->ev[\old(out->n)].value == \old(r->rd.payload_len) &&
              normfs_net_payload_read(\old(r->payload), \old(r->rd.payload_len), r->fd);
    ensures !(out->n > \old(out->n) && out->ev[\old(out->n)].kind == NORMFS_CONN_EV_REQUEST) ==>
              r->pending == \old(r->pending);
    ensures \forall integer k; \old(out->n) <= k < out->n ==>
              out->ev[k].kind == NORMFS_CONN_EV_NEED_BUFFER ==>
                r->want_buffer == 1 && out->ev[k].value == r->rd.payload_len;
    ensures \forall integer k; \old(out->n) <= k < out->n ==>
              out->ev[k].kind == NORMFS_CONN_EV_READ_CLOSED ==> r->reading == 0;
    ensures \result == 0 || \result == 1;
*/
int
normfs_conn_read(struct normfs_conn_rx *r, struct normfs_conn_events *out, int64_t now)
{
	struct normfs_net_result res;
	int i;

	/*@ loop invariant 0 <= i <= NORMFS_CONN_FRAMES;
	    loop invariant NORMFS_CONN_RX_OK(r);
	    loop invariant normfs_conn_rx_events_ok(out, r);
	    loop invariant out->ev == \at(out->ev, Pre) && out->cap == \at(out->cap, Pre);
	    loop invariant \at(out->n, Pre) <= out->n <= \at(out->n, Pre) + i;
	    loop invariant r->pending >= \at(r->pending, Pre);
	    loop invariant \at(r->reading, Pre) == 0 ==>
	                     out->n == \at(out->n, Pre) && r->reading == 0;
	    loop invariant r->payload != \null ==>
	                     r->payload == \at(r->payload, Pre) &&
	                     r->rd.payload_len == \at(r->rd.payload_len, Pre);
	    loop invariant \forall integer g; g != r->fd ==> net_pos(g) == \at(net_pos(g), Pre);
	    loop invariant out->n <= \at(out->n, Pre) + 1;
	    loop invariant \forall integer k; \at(out->n, Pre) <= k < out->n ==>
	                     out->ev[k].kind == NORMFS_CONN_EV_REQUEST && out->ev[k].conn == r->token;
	    loop invariant i == 0 ==> out->n == \at(out->n, Pre) && r->payload == \at(r->payload, Pre) &&
	                     r->rd.in_payload == \at(r->rd.in_payload, Pre) &&
	                     r->rd.payload_len == \at(r->rd.payload_len, Pre) &&
	                     r->pending == \at(r->pending, Pre);
	    loop invariant i > 0 ==> r->payload == \null && r->rd.in_payload == 0;
	    loop invariant out->n == \at(out->n, Pre) ==> r->pending == \at(r->pending, Pre);
	    loop invariant out->n > \at(out->n, Pre) ==>
	                     r->pending == \at(r->pending, Pre) + 1 &&
	                     \at(r->payload, Pre) != \null &&
	                     out->ev[\at(out->n, Pre)].buf == \at(r->payload, Pre) &&
	                     out->ev[\at(out->n, Pre)].value == \at(r->rd.payload_len, Pre) &&
	                     normfs_net_payload_read(\at(r->payload, Pre), \at(r->rd.payload_len, Pre),
	                       r->fd);
	    loop assigns i, res, r->rd, r->payload, r->deadline_ms, r->pending, r->reading,
	                 r->want_buffer,
	                 \at(r->payload, Pre)[0 .. \at(r->rd.payload_len, Pre) - 1],
	                 out->n, \at(out->ev, Pre)[0 .. \at(out->cap, Pre) - 1], normfs_net_sys_world;
	    loop variant NORMFS_CONN_FRAMES - i;
	*/
	for (i = 0; i < NORMFS_CONN_FRAMES; i++) {
		if (!r->reading || r->want_buffer || r->pending >= NORMFS_CONN_PENDING)
			return 0;
		if (!r->rd.in_payload) {
			/*@ assert r->payload == \null; */
			res = normfs_net_read_header(&r->rd, r->fd);
			if (res.status == NORMFS_NET_WOULD_BLOCK)
				return 1;
			if (res.status != NORMFS_NET_OK) {
				normfs_conn_close_read(r, out, res.status, res.os_error, res.value);
				return 0;
			}
			r->deadline_ms = now + NORMFS_CONN_IDLE_MS;
			if (res.value == 0u)
				continue;
			r->want_buffer = 1;
			normfs_conn_emit(out, r->token, NORMFS_CONN_EV_NEED_BUFFER, res.value,
			    (uint8_t *)0, 0, 0, 1);
			return 0;
		}
		res = normfs_net_read_payload(&r->rd, r->fd, r->payload, r->rd.payload_len);
		if (res.status == NORMFS_NET_WOULD_BLOCK)
			return 1;
		if (res.status == NORMFS_NET_YIELD)
			return 0;
		if (res.status != NORMFS_NET_OK) {
			normfs_conn_close_read(r, out, res.status, res.os_error, 0u);
			return 0;
		}
		normfs_conn_emit(out, r->token, NORMFS_CONN_EV_REQUEST, res.value, r->payload, 0, 0,
		    0);
		r->payload = (uint8_t *)0;
		r->pending++;
		r->deadline_ms = now + NORMFS_CONN_IDLE_MS;
	}
	return 0;
}

/* Hands over the buffer a NEED_BUFFER event asked for, rd.payload_len bytes. */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r) && r->want_buffer == 1;
    requires \valid(buf + (0 .. r->rd.payload_len - 1));
    requires \separated(r, buf + (0 .. r->rd.payload_len - 1));
    requires NORMFS_CONN_NOW_OK(now);
    assigns r->payload, r->want_buffer, r->deadline_ms;
    ensures NORMFS_CONN_RX_OK(r);
    ensures r->payload == buf && r->want_buffer == 0;
*/
void
normfs_conn_give_buffer(struct normfs_conn_rx *r, uint8_t *buf, int64_t now)
{
	r->payload = buf;
	r->want_buffer = 0;
	r->deadline_ms = now + NORMFS_CONN_IDLE_MS;
}

/* Rust took a request; reading resumes if it had paused for it. */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r) && r->pending > 0;
    requires NORMFS_CONN_NOW_OK(now);
    assigns r->pending, r->deadline_ms;
    ensures NORMFS_CONN_RX_OK(r);
    ensures r->pending == \old(r->pending) - 1;
*/
void
normfs_conn_ack(struct normfs_conn_rx *r, int64_t now)
{
	if (r->pending == NORMFS_CONN_PENDING)
		r->deadline_ms = now + NORMFS_CONN_IDLE_MS;
	r->pending--;
}

/* Closes the read side once a wait for a header or payload outlives its deadline. */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r);
    requires \valid(out) && normfs_conn_rx_events_ok(out, r) && normfs_conn_room(out, 1);
    assigns r->reading, r->want_buffer, r->payload, out->n, out->ev[out->n];
    ensures NORMFS_CONN_RX_OK(r);
    ensures \old(out->n) <= out->n <= \old(out->n) + 1;
    ensures out->n == \old(out->n) + 1 <==>
              normfs_conn_rx_active{Pre}(r) && now >= \old(r->deadline_ms);
    ensures out->n == \old(out->n) + 1 ==>
              r->reading == 0 && out->ev[\old(out->n)].kind == NORMFS_CONN_EV_READ_CLOSED &&
              out->ev[\old(out->n)].buf == \old(r->payload);
*/
void
normfs_conn_tick(struct normfs_conn_rx *r, struct normfs_conn_events *out, int64_t now)
{
	if (r->reading && !r->want_buffer && r->pending < NORMFS_CONN_PENDING &&
	    now >= r->deadline_ms)
		normfs_conn_close_read(r, out, NORMFS_CONN_TIMEOUT, 0, 0u);
}

/* Rust is done with the connection: the read side stops. */
/*@ requires \valid(r) && NORMFS_CONN_RX_OK(r);
    requires \valid(out) && normfs_conn_rx_events_ok(out, r) && normfs_conn_room(out, 1);
    assigns r->reading, r->want_buffer, r->payload, out->n, out->ev[out->n];
    ensures NORMFS_CONN_RX_OK(r);
    ensures r->reading == 0;
    ensures \old(out->n) <= out->n <= \old(out->n) + 1;
    ensures out->n == \old(out->n) + 1 ==>
              out->ev[\old(out->n)].kind == NORMFS_CONN_EV_READ_CLOSED &&
              out->ev[\old(out->n)].buf == \old(r->payload);
*/
void
normfs_conn_stop(struct normfs_conn_rx *r, struct normfs_conn_events *out)
{
	if (r->reading)
		normfs_conn_close_read(r, out, NORMFS_NET_OK, 0, 0u);
}

/*@ requires \valid_read(r) && NORMFS_CONN_RX_OK(r);
    assigns \nothing;
    ensures \result == 0 || \result == NORMFS_CONN_WANT_READ;
    ensures \result != 0 <==> normfs_conn_rx_active(r);
*/
int
normfs_conn_rx_interest(const struct normfs_conn_rx *r)
{
	if (r->reading && !r->want_buffer && r->pending < NORMFS_CONN_PENDING)
		return NORMFS_CONN_WANT_READ;
	return 0;
}

/* When the read side times out, or -1 while it is not waiting on the peer. */
/*@ requires \valid_read(r) && NORMFS_CONN_RX_OK(r);
    assigns \nothing;
    ensures normfs_conn_rx_active(r) ==> \result == r->deadline_ms;
    ensures !normfs_conn_rx_active(r) ==> \result == -1;
*/
int64_t
normfs_conn_deadline(const struct normfs_conn_rx *r)
{
	if (r->reading && !r->want_buffer && r->pending < NORMFS_CONN_PENDING)
		return r->deadline_ms;
	return -1;
}

/*@ requires \valid(t);
    assigns *t;
    ensures NORMFS_CONN_TX_OK(t);
    ensures t->fd == fd && t->token == token && t->write_open == 1;
    ensures t->q[0].count == 0 && t->q[1].count == 0 && t->wclass == -1;
*/
void
normfs_conn_tx_init(struct normfs_conn_tx *t, int fd, uint64_t token)
{
	t->q[0].count = 0u;
	t->q[1].count = 0u;
	t->token = token;
	t->woff = 0u;
	t->fd = fd;
	t->wclass = -1;
	t->write_open = 1;
}

/*
 * Queues a frame of class cls. With the write side closed the frame comes
 * straight back as SENT with status 0. -1 when the class's queue is full.
 */
/*@ requires \valid(t) && NORMFS_CONN_TX_OK(t);
    requires \valid(out) && normfs_conn_tx_events_ok(out, t) && normfs_conn_room(out, 1);
    requires cls == NORMFS_CONN_REPLY || cls == NORMFS_CONN_READ;
    requires len >= 1 && \valid_read(buf + (0 .. len - 1));
    requires \separated(t, buf + (0 .. len - 1));
    assigns t->q[cls], out->n, out->ev[out->n];
    ensures NORMFS_CONN_TX_OK(t);
    ensures \result == 0 || \result == -1;
    ensures \result == -1 <==> t->write_open == 1 &&
                               \old(t->q[cls].count) == NORMFS_CONN_QUEUE;
    ensures \result == -1 ==> out->n == \old(out->n) &&
                               t->q[cls].count == \old(t->q[cls].count);
    ensures \result == 0 && t->write_open == 1 ==>
              out->n == \old(out->n) &&
              t->q[cls].count == \old(t->q[cls].count) + 1 &&
              t->q[cls].f[\old(t->q[cls].count)].buf == buf &&
              t->q[cls].f[\old(t->q[cls].count)].len == len &&
              t->q[cls].f[\old(t->q[cls].count)].cookie == cookie &&
              \forall integer j; 0 <= j < \old(t->q[cls].count) ==>
                t->q[cls].f[j] == \old(t->q[cls].f[j]);
    ensures t->write_open == 0 ==>
              out->n == \old(out->n) + 1 &&
              out->ev[\old(out->n)].kind == NORMFS_CONN_EV_SENT &&
              out->ev[\old(out->n)].value == cookie && out->ev[\old(out->n)].status == 0;
*/
int
normfs_conn_submit(struct normfs_conn_tx *t, struct normfs_conn_events *out, int cls,
    const uint8_t *buf, size_t len, uint64_t cookie)
{
	struct normfs_conn_queue *q = &t->q[cls];

	if (!t->write_open) {
		normfs_conn_emit(out, t->token, NORMFS_CONN_EV_SENT, cookie, (uint8_t *)0, 0, 0, 0);
		return 0;
	}
	if (q->count == NORMFS_CONN_QUEUE)
		return -1;
	q->f[q->count].buf = buf;
	q->f[q->count].len = len;
	q->f[q->count].cookie = cookie;
	q->count++;
	return 0;
}

/* Gives back every queued frame as dropped, oldest first, and closes the write side. */
/*@ requires \valid(t) && NORMFS_CONN_TX_OK(t) && t->write_open == 1;
    requires \valid(out) && normfs_conn_tx_events_ok(out, t);
    requires normfs_conn_room(out, t->q[0].count + t->q[1].count + 1);
    assigns t->q[0].count, t->q[1].count, t->wclass, t->woff, t->write_open,
            out->n, out->ev[0 .. out->cap - 1];
    ensures NORMFS_CONN_TX_OK(t);
    ensures t->write_open == 0;
    ensures out->n == \old(out->n) + \old(t->q[0].count) + \old(t->q[1].count) + 1;
    ensures \forall integer j; \old(out->n) <= j < out->n - 1 ==>
              out->ev[j].kind == NORMFS_CONN_EV_SENT;
    ensures \forall integer j; 0 <= j < \old(t->q[0].count) ==>
              out->ev[\old(out->n) + j].value == \old(t->q[0].f[j].cookie);
    ensures \forall integer j; 0 <= j < \old(t->q[1].count) ==>
              out->ev[\old(out->n) + \old(t->q[0].count) + j].value ==
                \old(t->q[1].f[j].cookie);
    ensures out->ev[out->n - 1].kind == NORMFS_CONN_EV_WRITE_CLOSED;
    ensures \forall integer k; \old(out->n) <= k < out->n ==> out->ev[k].conn == t->token;
    ensures \forall integer k; 0 <= k < \old(out->n) ==>
              out->ev[k].kind == \old(out->ev[k].kind) &&
              out->ev[k].value == \old(out->ev[k].value) &&
              out->ev[k].conn == \old(out->ev[k].conn);
*/
static void
normfs_conn_close_write(struct normfs_conn_tx *t, struct normfs_conn_events *out, int os_error)
{
	uint32_t i;
	int k;

	t->wclass = -1;
	t->woff = 0u;
	/*@ loop invariant 0 <= k <= 2;
	    loop invariant normfs_conn_tx_events_ok(out, t);
	    loop invariant out->ev == \at(out->ev, Pre) && out->cap == \at(out->cap, Pre);
	    loop invariant t->q[0].count == (k > 0 ? 0 : \at(t->q[0].count, Pre));
	    loop invariant t->q[1].count == (k > 1 ? 0 : \at(t->q[1].count, Pre));
	    loop invariant out->n == \at(out->n, Pre) +
	                     (k > 0 ? \at(t->q[0].count, Pre) : 0) +
	                     (k > 1 ? \at(t->q[1].count, Pre) : 0);
	    loop invariant \forall integer c, j; 0 <= c < 2 && 0 <= j < NORMFS_CONN_QUEUE ==>
	                     t->q[c].f[j] == \at(t->q[c].f[j], Pre);
	    loop invariant \forall integer m; 0 <= m < \at(out->n, Pre) ==>
	                     out->ev[m].kind == \at(out->ev[m].kind, Pre) &&
	                     out->ev[m].value == \at(out->ev[m].value, Pre) &&
	                     out->ev[m].conn == \at(out->ev[m].conn, Pre);
	    loop invariant \forall integer m; \at(out->n, Pre) <= m < out->n ==>
	                     out->ev[m].conn == t->token;
	    loop invariant k > 0 ==> \forall integer j; 0 <= j < \at(t->q[0].count, Pre) ==>
	                     out->ev[\at(out->n, Pre) + j].kind == NORMFS_CONN_EV_SENT;
	    loop invariant k > 0 ==> \forall integer j; 0 <= j < \at(t->q[0].count, Pre) ==>
	                     out->ev[\at(out->n, Pre) + j].value == \at(t->q[0].f[j].cookie, Pre);
	    loop invariant k > 1 ==> \forall integer j; 0 <= j < \at(t->q[1].count, Pre) ==>
	                     out->ev[\at(out->n, Pre) + \at(t->q[0].count, Pre) + j].kind ==
	                       NORMFS_CONN_EV_SENT;
	    loop invariant k > 1 ==> \forall integer j; 0 <= j < \at(t->q[1].count, Pre) ==>
	                     out->ev[\at(out->n, Pre) + \at(t->q[0].count, Pre) + j].value ==
	                       \at(t->q[1].f[j].cookie, Pre);
	    loop assigns k, i, t->q[0].count, t->q[1].count, out->n,
	                 \at(out->ev, Pre)[0 .. \at(out->cap, Pre) - 1];
	    loop variant 2 - k;
	*/
	for (k = 0; k < 2; k++) {
		/*@ loop invariant 0 <= i <= t->q[k].count;
		    loop invariant normfs_conn_tx_events_ok(out, t);
		    loop invariant out->ev == \at(out->ev, Pre) && out->cap == \at(out->cap, Pre);
		    loop invariant out->n == \at(out->n, LoopEntry) + i;
		    loop invariant \forall integer m; 0 <= m < \at(out->n, LoopEntry) ==>
		                     out->ev[m].kind == \at(out->ev[m].kind, LoopEntry) &&
		                     out->ev[m].value == \at(out->ev[m].value, LoopEntry) &&
		                     out->ev[m].conn == \at(out->ev[m].conn, LoopEntry);
		    loop invariant \forall integer m; \at(out->n, Pre) <= m < out->n ==>
		                     out->ev[m].conn == t->token;
		    loop invariant \forall integer m; 0 <= m < i ==>
		                     out->ev[\at(out->n, LoopEntry) + m].kind == NORMFS_CONN_EV_SENT;
		    loop invariant \forall integer m; 0 <= m < i ==>
		                     out->ev[\at(out->n, LoopEntry) + m].value == t->q[k].f[m].cookie;
		    loop assigns i, out->n, \at(out->ev, Pre)[0 .. \at(out->cap, Pre) - 1];
		    loop variant t->q[k].count - i;
		*/
		for (i = 0u; i < t->q[k].count; i++)
			normfs_conn_emit(out, t->token, NORMFS_CONN_EV_SENT, t->q[k].f[i].cookie,
			    (uint8_t *)0, 0, 0, 0);
		t->q[k].count = 0u;
	}
	t->write_open = 0;
	normfs_conn_emit(out, t->token, NORMFS_CONN_EV_WRITE_CLOSED, 0u, (uint8_t *)0, 0,
	    os_error, 0);
}

/* Drops the oldest frame of class cls, which has been written. */
/*@ requires \valid(t) && NORMFS_CONN_TX_OK(t);
    requires cls == 0 || cls == 1;
    requires t->q[cls].count > 0;
    assigns t->q[cls].f[0 .. NORMFS_CONN_QUEUE - 2], t->q[cls].count;
    ensures normfs_conn_queue_ok(&t->q[cls], t);
    ensures t->q[cls].count == \old(t->q[cls].count) - 1;
    ensures \forall integer j; 0 <= j < t->q[cls].count ==>
              t->q[cls].f[j] == \old(t->q[cls].f[j + 1]);
*/
static void
normfs_conn_pop(struct normfs_conn_tx *t, int cls)
{
	struct normfs_conn_queue *q = &t->q[cls];
	uint32_t j;

	/*@ loop invariant 0 <= j <= q->count - 1;
	    loop invariant \forall integer m; 0 <= m < j ==> q->f[m] == \at(t->q[cls].f[m + 1], Pre);
	    loop invariant \forall integer m; j <= m < NORMFS_CONN_QUEUE ==>
	                     q->f[m] == \at(t->q[cls].f[m], Pre);
	    loop assigns j, q->f[0 .. NORMFS_CONN_QUEUE - 2];
	    loop variant q->count - 1 - j;
	*/
	for (j = 0u; j + 1u < q->count; j++)
		q->f[j] = q->f[j + 1u];
	q->count--;
}

/* The class the next frame comes from: a reply whenever one waits. */
/*@ requires \valid_read(t);
    assigns \nothing;
    ensures t->q[NORMFS_CONN_REPLY].count > 0 ==> \result == NORMFS_CONN_REPLY;
    ensures t->q[NORMFS_CONN_REPLY].count == 0 && t->q[NORMFS_CONN_READ].count > 0 ==>
              \result == NORMFS_CONN_READ;
    ensures t->q[NORMFS_CONN_REPLY].count == 0 && t->q[NORMFS_CONN_READ].count == 0 ==>
              \result == -1;
*/
static int
normfs_conn_pick(const struct normfs_conn_tx *t)
{
	if (t->q[NORMFS_CONN_REPLY].count > 0u)
		return NORMFS_CONN_REPLY;
	if (t->q[NORMFS_CONN_READ].count > 0u)
		return NORMFS_CONN_READ;
	return -1;
}

/*
 * Writes queued frames whole and in order, a reply ahead of read data
 * whenever a new frame starts, until the socket would block or a batch is
 * spent. A failed write drops the rest and closes the write side. 1 when the
 * socket would block, 0 otherwise.
 */
/*@ requires \valid(t) && NORMFS_CONN_TX_OK(t);
    requires \valid(out) && normfs_conn_tx_events_ok(out, t);
    requires normfs_conn_room(out, 2 * NORMFS_CONN_QUEUE + 1);
    assigns t->q[0 .. 1], t->wclass, t->woff, t->write_open,
            out->n, out->ev[0 .. out->cap - 1], normfs_net_sys_world;
    ensures NORMFS_CONN_TX_OK(t);
    ensures \old(out->n) <= out->n <= \old(out->n) + 2 * NORMFS_CONN_QUEUE + 1;
    ensures t->write_open <= \old(t->write_open);
    ensures \forall integer g; net_pos(g) == \old(net_pos(g));
    ensures t->q[0].count <= \old(t->q[0].count) && t->q[1].count <= \old(t->q[1].count);
    ensures \forall integer k; \old(out->n) <= k < out->n ==> out->ev[k].conn == t->token;
    ensures \forall integer k; \old(out->n) <= k < out->n ==>
              out->ev[k].kind == NORMFS_CONN_EV_SENT ||
              out->ev[k].kind == NORMFS_CONN_EV_WRITE_CLOSED;
    ensures \result == 0 || \result == 1;
*/
int
normfs_conn_write(struct normfs_conn_tx *t, struct normfs_conn_events *out)
{
	struct normfs_net_result res;
	struct normfs_conn_queue *q;
	/*@ ghost int sent = 0; */

	/*@ loop invariant NORMFS_CONN_TX_OK(t);
	    loop invariant normfs_conn_tx_events_ok(out, t);
	    loop invariant out->ev == \at(out->ev, Pre) && out->cap == \at(out->cap, Pre);
	    loop invariant 0 <= sent;
	    loop invariant t->q[0].count + t->q[1].count + sent ==
	                     \at(t->q[0].count, Pre) + \at(t->q[1].count, Pre);
	    loop invariant out->n == \at(out->n, Pre) + sent;
	    loop invariant t->write_open == \at(t->write_open, Pre);
	    loop invariant \forall integer g; net_pos(g) == \at(net_pos(g), Pre);
	    loop invariant t->token == \at(t->token, Pre);
	    loop invariant t->q[0].count <= \at(t->q[0].count, Pre);
	    loop invariant t->q[1].count <= \at(t->q[1].count, Pre);
	    loop invariant \forall integer k; \at(out->n, Pre) <= k < out->n ==>
	                     out->ev[k].conn == t->token;
	    loop invariant \forall integer k; \at(out->n, Pre) <= k < out->n ==>
	                     out->ev[k].kind == NORMFS_CONN_EV_SENT;
	    loop assigns sent, res, q, t->q[0 .. 1], t->wclass, t->woff,
	                 normfs_net_sys_world, out->n, \at(out->ev, Pre)[0 .. \at(out->cap, Pre) - 1];
	    loop variant t->q[0].count + t->q[1].count;
	*/
	while (t->write_open) {
		if (t->wclass < 0) {
			t->wclass = (int8_t)normfs_conn_pick(t);
			if (t->wclass < 0)
				return 0;
		}
		q = &t->q[t->wclass];
		res = normfs_net_write(t->fd, q->f[0].buf, q->f[0].len, &t->woff);
		if (res.status == NORMFS_NET_WOULD_BLOCK)
			return 1;
		if (res.status == NORMFS_NET_YIELD)
			return 0;
		if (res.status != NORMFS_NET_OK) {
			normfs_conn_close_write(t, out, res.os_error);
			return 0;
		}
		normfs_conn_emit(out, t->token, NORMFS_CONN_EV_SENT, q->f[0].cookie,
		    (uint8_t *)0, 1, 0, 0);
		normfs_conn_pop(t, t->wclass);
		t->wclass = -1;
		t->woff = 0u;
		/*@ ghost sent++; */
	}
	return 0;
}

/*@ requires \valid_read(t) && NORMFS_CONN_TX_OK(t);
    assigns \nothing;
    ensures \result == 0 || \result == NORMFS_CONN_WANT_WRITE;
    ensures \result != 0 <==> t->write_open == 1 && t->q[0].count + t->q[1].count > 0;
*/
int
normfs_conn_tx_interest(const struct normfs_conn_tx *t)
{
	if (t->write_open && t->q[0].count + t->q[1].count > 0u)
		return NORMFS_CONN_WANT_WRITE;
	return 0;
}

/* Nothing is left to write, or nothing can be. */
/*@ requires \valid_read(t) && NORMFS_CONN_TX_OK(t);
    assigns \nothing;
    ensures \result != 0 <==> t->write_open == 0 || t->q[0].count + t->q[1].count == 0;
*/
int
normfs_conn_tx_drained(const struct normfs_conn_tx *t)
{
	return !t->write_open || t->q[0].count + t->q[1].count == 0u;
}

/* Rust allocates the two sides, so it asks for their sizes. */
/*@ assigns \nothing;
    ensures \result == sizeof(struct normfs_conn_rx);
*/
size_t
normfs_conn_rx_size(void)
{
	return sizeof(struct normfs_conn_rx);
}

/*@ assigns \nothing;
    ensures \result == sizeof(struct normfs_conn_tx);
*/
size_t
normfs_conn_tx_size(void)
{
	return sizeof(struct normfs_conn_tx);
}

/* Room for the events of any one call: a write that fails with both queues full. */
/*@ assigns \nothing;
    ensures \result == 2 * NORMFS_CONN_QUEUE + 1;
    ensures \result >= NORMFS_CONN_FRAMES;
*/
size_t
normfs_conn_events_room(void)
{
	return 2u * NORMFS_CONN_QUEUE + 1u;
}

/* The requests Rust may hold before reading pauses, for its acknowledgements. */
/*@ assigns \nothing;
    ensures \result == NORMFS_CONN_PENDING;
*/
size_t
normfs_conn_pending_max(void)
{
	return NORMFS_CONN_PENDING;
}
