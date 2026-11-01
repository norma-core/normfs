#ifndef NORMFS_CONN_H
#define NORMFS_CONN_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/net.h"

/* Frames of one class a connection holds before Rust must wait. */
#define NORMFS_CONN_QUEUE 16

/* Requests handed to Rust and not yet taken before reading pauses. */
#define NORMFS_CONN_PENDING 10

/* How long the read side waits for a header, or for the rest of a payload. */
#define NORMFS_CONN_IDLE_MS 10000

/* A payload buffer starts this big and doubles as bytes arrive, so a header
 * alone cannot claim the memory it announces. */
#define NORMFS_CONN_FIRST_BUFFER 65536

/* Frames a call reads before it hands control back, keepalives included.
 * One, so tokio's task budget is spent per frame, as it was per read before. */
#define NORMFS_CONN_FRAMES 1

/* The readiness a side waits for. */
#define NORMFS_CONN_WANT_READ 1
#define NORMFS_CONN_WANT_WRITE 2

/* Pongs and write acks go ahead of read data, at frame boundaries. */
enum normfs_conn_class {
	NORMFS_CONN_REPLY = 0,
	NORMFS_CONN_READ = 1
};

enum normfs_conn_event_kind {
	NORMFS_CONN_EV_NEED_BUFFER = 1,
	NORMFS_CONN_EV_REQUEST = 2,
	NORMFS_CONN_EV_READ_CLOSED = 3,
	NORMFS_CONN_EV_WRITE_CLOSED = 4,
	NORMFS_CONN_EV_SENT = 5
};

/* Why the read side closed, beside the net statuses EOF, ERR_IO and
 * ERR_TOO_LARGE; NORMFS_NET_OK when Rust stopped it. */
#define NORMFS_CONN_TIMEOUT 100

/*
 * NEED_BUFFER: value is the size the payload buffer must have, with the bytes
 * read so far at its start; reading, and its deadline, wait for
 * normfs_conn_give_buffer, which Rust calls before anything else.
 * REQUEST: buf holds value bytes, Rust's now.
 * READ_CLOSED, WRITE_CLOSED: status and os_error say why, value carries the
 * size of a frame over the limit, buf a payload buffer to free or NULL.
 * SENT: value is the frame's cookie, status 1 when written whole, 0 when
 * dropped because the write side closed.
 */
struct normfs_conn_event {
	uint64_t conn;
	uint64_t value;
	uint8_t *buf;
	int kind;
	int status;
	int os_error;
	int in_payload;
};

struct normfs_conn_events {
	struct normfs_conn_event *ev;
	size_t n;
	size_t cap;
};

/*
 * The field types are chosen so that WP's typed memory model keeps the
 * fields a call changes apart from the ones the invariants quantify over.
 */

struct normfs_conn_rx {
	struct normfs_net_reader rd;
	uint8_t *payload;
	uint64_t cap;
	uint64_t token;
	int64_t deadline_ms;
	int fd;
	uint16_t pending;
	int8_t reading;
	int8_t want_buffer;
};

/* buf is owned by Rust and stays valid until its SENT event. */
struct normfs_conn_frame {
	const uint8_t *buf;
	size_t len;
	uint64_t cookie;
};

/* Oldest frame first; a queue this short is shifted rather than wrapped. */
struct normfs_conn_queue {
	struct normfs_conn_frame f[NORMFS_CONN_QUEUE];
	uint32_t count;
};

struct normfs_conn_tx {
	struct normfs_conn_queue q[2];
	uint64_t token;
	size_t woff;
	int fd;
	int8_t wclass;
	int8_t write_open;
};

/*@ predicate normfs_conn_queue_ok{L}(struct normfs_conn_queue *q, struct normfs_conn_tx *t) =
      q->count <= NORMFS_CONN_QUEUE &&
      \forall integer j; 0 <= j < q->count ==>
        q->f[j].len >= 1 && \valid_read(q->f[j].buf + (0 .. q->f[j].len - 1)) &&
        \separated(t, q->f[j].buf + (0 .. q->f[j].len - 1));

    predicate normfs_conn_tx_flags_ok{L}(struct normfs_conn_tx *t) =
      (t->write_open == 0 || t->write_open == 1) &&
      (t->wclass == -1 || t->wclass == 0 || t->wclass == 1) &&
      (t->wclass == -1 ==> t->woff == 0) &&
      (t->wclass != -1 ==> t->write_open == 1 && t->q[t->wclass].count > 0 &&
         t->woff <= t->q[t->wclass].f[0].len) &&
      (t->write_open == 0 ==> t->q[0].count == 0 && t->q[1].count == 0);

    predicate normfs_conn_rx_flags_ok{L}(struct normfs_conn_rx *r) =
      (r->reading == 0 || r->reading == 1) &&
      (r->want_buffer == 0 || r->want_buffer == 1) &&
      r->pending <= NORMFS_CONN_PENDING &&
      (r->reading == 0 ==> r->payload == \null && r->want_buffer == 0) &&
      (r->want_buffer == 1 ==>
         r->payload == \null && r->rd.in_payload == 1 && r->rd.payload_have < r->cap) &&
      (r->rd.in_payload == 1 ==>
         1 <= r->cap && r->rd.payload_have <= r->cap <= r->rd.payload_len &&
         (r->cap <= NORMFS_CONN_FIRST_BUFFER || r->cap <= 2 * r->rd.payload_have)) &&
      (r->reading == 1 && r->rd.in_payload == 1 ==>
         r->want_buffer == 1 || r->payload != \null);

    predicate normfs_conn_rx_reader_ok{L}(struct normfs_conn_rx *r) =
      normfs_net_reader_ok(&r->rd) && normfs_net_header_read(&r->rd, r->fd);

    predicate normfs_conn_rx_payload_ok{L}(struct normfs_conn_rx *r) =
      r->payload != \null ==>
        r->rd.in_payload == 1 && r->want_buffer == 0 &&
        \valid(r->payload + (0 .. r->cap - 1)) &&
        \separated(r, r->payload + (0 .. r->cap - 1)) &&
        normfs_net_payload_read(r->payload, r->rd.payload_have, r->fd);

    predicate normfs_conn_rx_active(struct normfs_conn_rx *r) =
      r->reading == 1 && r->want_buffer == 0 && r->pending < NORMFS_CONN_PENDING;

    predicate normfs_conn_rx_events_ok{L}(struct normfs_conn_events *out,
                                          struct normfs_conn_rx *r) =
      out->n <= out->cap && \valid(out->ev + (0 .. out->cap - 1)) &&
      \separated(out, r) && \separated(out->ev + (0 .. out->cap - 1), r, out) &&
      (r->payload != \null ==>
         \separated(out->ev + (0 .. out->cap - 1), r->payload + (0 .. r->cap - 1)));

    predicate normfs_conn_tx_events_ok{L}(struct normfs_conn_events *out,
                                          struct normfs_conn_tx *t) =
      out->n <= out->cap && \valid(out->ev + (0 .. out->cap - 1)) &&
      \separated(out, t) && \separated(out->ev + (0 .. out->cap - 1), t, out);
*/

void normfs_conn_rx_init(struct normfs_conn_rx *r, int fd, uint64_t token, int64_t now);
int normfs_conn_read(struct normfs_conn_rx *r, struct normfs_conn_events *out, int64_t now);
void normfs_conn_give_buffer(struct normfs_conn_rx *r, uint8_t *buf, int64_t now);
void normfs_conn_ack(struct normfs_conn_rx *r, int64_t now);
void normfs_conn_tick(struct normfs_conn_rx *r, struct normfs_conn_events *out, int64_t now);
void normfs_conn_stop(struct normfs_conn_rx *r, struct normfs_conn_events *out);
int normfs_conn_rx_interest(const struct normfs_conn_rx *r);
int64_t normfs_conn_deadline(const struct normfs_conn_rx *r);

void normfs_conn_tx_init(struct normfs_conn_tx *t, int fd, uint64_t token);
int normfs_conn_submit(struct normfs_conn_tx *t, struct normfs_conn_events *out, int cls,
    const uint8_t *buf, size_t len, uint64_t cookie);
int normfs_conn_write(struct normfs_conn_tx *t, struct normfs_conn_events *out);
int normfs_conn_tx_interest(const struct normfs_conn_tx *t);
int normfs_conn_tx_drained(const struct normfs_conn_tx *t);
size_t normfs_conn_rx_size(void);
size_t normfs_conn_tx_size(void);
size_t normfs_conn_events_room(void);
size_t normfs_conn_pending_max(void);

#endif /* NORMFS_CONN_H */
