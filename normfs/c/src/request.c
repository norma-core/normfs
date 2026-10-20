#include "normfs/request.h"
#include "normfs.pb.h"

/* The statuses pass gremlin's codes through; c99 has no _Static_assert. */
typedef char normfs_request_truncated_ok[
    (int)GREMLIN_ERROR_TRUNCATED == NORMFS_REQUEST_ERR_TRUNCATED ? 1 : -1];
typedef char normfs_request_overflow_ok[
    (int)GREMLIN_ERROR_OVERFLOW == NORMFS_REQUEST_ERR_OVERFLOW ? 1 : -1];
typedef char normfs_request_wire_type_ok[
    (int)GREMLIN_ERROR_INVALID_WIRE_TYPE == NORMFS_REQUEST_ERR_BAD_WIRE_TYPE ? 1 : -1];
typedef char normfs_request_field_num_ok[
    (int)GREMLIN_ERROR_INVALID_FIELD_NUM == NORMFS_REQUEST_ERR_BAD_FIELD_NUM ? 1 : -1];
typedef char normfs_request_ok_ok[(int)GREMLIN_OK == NORMFS_REQUEST_OK ? 1 : -1];

/* request.rs mirrors the structs and asserts the same sizes. */
typedef char normfs_request_size_ok[
    sizeof(struct normfs_request) == 152 && sizeof(struct normfs_request_span) == 16 ? 1 : -1];

/* Each part is decoded into its own local and req is filled once at the end,
 * so no fact about req has to be carried across a call or a branch. */
struct normfs_request_write {
	uint64_t id;
	struct normfs_request_span queue_id;
	size_t packets;
	int present;
};

struct normfs_request_read {
	uint64_t id;
	struct normfs_request_span queue_id;
	uint64_t step;
	size_t encodings;
	struct normfs_request_span offset_id_raw;
	int64_t offset_type;
	uint64_t limit;
	int has_offset;
	int has_offset_id;
	int present;
};

/*@ predicate normfs_request_span_in(struct normfs_request_span s, integer n) =
      s.off <= n && s.len <= n - s.off;

    predicate normfs_request_spans_in{L}(struct normfs_request *req, integer n) =
      normfs_request_span_in(req->write_queue_id, n) &&
      normfs_request_span_in(req->read_queue_id, n) &&
      normfs_request_span_in(req->read_offset_id_raw, n);

    predicate normfs_request_packets_in{L}(struct normfs_request_span *p,
                                          integer count, integer cap, integer n) =
      \forall integer i; 0 <= i < count && i < cap ==> normfs_request_span_in(p[i], n);

    predicate normfs_request_status(integer s) =
      s == NORMFS_REQUEST_OK ||
      s == NORMFS_REQUEST_ERR_TRUNCATED ||
      s == NORMFS_REQUEST_ERR_OVERFLOW ||
      s == NORMFS_REQUEST_ERR_BAD_WIRE_TYPE ||
      s == NORMFS_REQUEST_ERR_BAD_FIELD_NUM;
*/

/*@ requires b.len == 0 || gremlin_span_in(b, (uint8_t *)buf, len);
    assigns \nothing;
    ensures normfs_request_span_in(\result, len);
*/
static struct normfs_request_span
normfs_request_span(const uint8_t *buf, size_t len, struct gremlin_bytes b)
{
	struct normfs_request_span s = {0u, 0u};

	(void)len;
	if (b.len == 0u) return s;
	s.off = (size_t)(b.data - buf);
	s.len = b.len;
	return s;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires gremlin_span_in(m, (uint8_t *)buf, len);
    requires \valid(version);
    assigns *version;
    ensures normfs_request_status(\result);
*/
static int
normfs_request_decode_setup(const uint8_t *buf, size_t len, struct gremlin_bytes m,
    uint64_t *version)
{
	normfs_SetupRequest_reader r;
	enum gremlin_error err;

	(void)buf;
	(void)len;
	err = normfs_SetupRequest_reader_init(&r, m.data, m.len);
	if (err != GREMLIN_OK) return (int)err;
	*version = normfs_SetupRequest_reader_get_version(&r);
	return NORMFS_REQUEST_OK;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires gremlin_span_in(m, (uint8_t *)buf, len);
    requires \valid(sequence) && \valid(timestamp) && \separated(sequence, timestamp);
    assigns *sequence, *timestamp;
    ensures normfs_request_status(\result);
*/
static int
normfs_request_decode_ping(const uint8_t *buf, size_t len, struct gremlin_bytes m,
    uint64_t *sequence, uint64_t *timestamp)
{
	normfs_PingRequest_reader r;
	enum gremlin_error err;

	(void)buf;
	(void)len;
	err = normfs_PingRequest_reader_init(&r, m.data, m.len);
	if (err != GREMLIN_OK) return (int)err;
	*sequence = normfs_PingRequest_reader_get_sequence(&r);
	*timestamp = normfs_PingRequest_reader_get_client_timestamp_ns(&r);
	return NORMFS_REQUEST_OK;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires gremlin_span_in(m, (uint8_t *)buf, len);
    requires \valid(w);
    requires packets_cap == 0 || \valid(packets + (0 .. packets_cap - 1));
    requires \separated(w, packets + (0 .. packets_cap - 1));
    assigns *w, packets[0 .. packets_cap - 1];
    ensures normfs_request_status(\result);
    ensures normfs_request_span_in(w->queue_id, len);
    ensures normfs_request_packets_in(packets, w->packets, packets_cap, len);
*/
static int
normfs_request_decode_write(const uint8_t *buf, size_t len, struct gremlin_bytes m,
    struct normfs_request_write *w, struct normfs_request_span *packets, size_t packets_cap)
{
	normfs_WriteRequest_reader r;
	normfs_WriteRequest_reader_packets_iter it;
	struct gremlin_bytes p;
	enum gremlin_error err;
	size_t i;

	w->id = 0u;
	w->queue_id.off = 0u;
	w->queue_id.len = 0u;
	w->packets = 0u;
	err = normfs_WriteRequest_reader_init(&r, m.data, m.len);
	if (err != GREMLIN_OK) return (int)err;
	w->id = normfs_WriteRequest_reader_get_write_id(&r);
	if (r._has.queue_id)
		w->queue_id = normfs_request_span(buf, len, r.queue_id);
	it = normfs_WriteRequest_reader_packets_begin(&r);
	/*@ loop invariant 0 <= i <= r.packets_count;
	    loop invariant normfs_WriteRequest_reader_packets_iter_ok(it);
	    loop invariant r.packets_count > 0 ==> it.src == m.data && it.src_len == m.len;
	    loop invariant normfs_request_packets_in(packets, i, packets_cap, len);
	    loop invariant normfs_request_span_in(w->queue_id, len);
	    loop assigns i, it, p, err, packets[0 .. packets_cap - 1];
	    loop variant r.packets_count - i;
	*/
	for (i = 0u; i < r.packets_count; i++) {
		err = normfs_WriteRequest_reader_packets_next(&it, &p);
		if (err != GREMLIN_OK) return (int)err;
		if (i < packets_cap)
			packets[i] = normfs_request_span(buf, len, p);
	}
	w->packets = r.packets_count;
	return NORMFS_REQUEST_OK;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires gremlin_span_in(m, (uint8_t *)buf, len);
    requires \valid(rd);
    assigns rd->offset_type, rd->offset_id_raw, rd->has_offset_id;
    ensures normfs_request_status(\result);
    ensures rd->has_offset_id != 0 ==> \result == NORMFS_REQUEST_OK;
    ensures normfs_request_span_in(rd->offset_id_raw, len);
*/
static int
normfs_request_decode_offset(const uint8_t *buf, size_t len, struct gremlin_bytes m,
    struct normfs_request_read *rd)
{
	normfs_Offset_reader o;
	normfs_Id_reader id;
	enum gremlin_error err;

	rd->offset_type = 0;
	rd->offset_id_raw.off = 0u;
	rd->offset_id_raw.len = 0u;
	rd->has_offset_id = 0;
	err = normfs_Offset_reader_init(&o, m.data, m.len);
	if (err != GREMLIN_OK) return (int)err;
	rd->offset_type = (int64_t)normfs_Offset_reader_get_type(&o);
	if (!o._has.id) return NORMFS_REQUEST_OK;
	err = normfs_Id_reader_init(&id, o.id.data, o.id.len);
	if (err != GREMLIN_OK) return (int)err;
	if (id._has.raw)
		rd->offset_id_raw = normfs_request_span(buf, len, id.raw);
	rd->has_offset_id = 1;
	return NORMFS_REQUEST_OK;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires gremlin_span_in(m, (uint8_t *)buf, len);
    requires \valid(rd);
    requires encodings_cap == 0 || \valid(encodings + (0 .. encodings_cap - 1));
    requires \separated(rd, encodings + (0 .. encodings_cap - 1));
    assigns *rd, encodings[0 .. encodings_cap - 1];
    ensures normfs_request_status(\result);
    ensures normfs_request_span_in(rd->queue_id, len);
    ensures normfs_request_span_in(rd->offset_id_raw, len);
    ensures rd->has_offset_id != 0 ==> rd->has_offset != 0;
*/
static int
normfs_request_decode_read(const uint8_t *buf, size_t len, struct gremlin_bytes m,
    struct normfs_request_read *rd, int32_t *encodings, size_t encodings_cap)
{
	normfs_ReadRequest_reader r;
	normfs_ReadRequest_reader_accept_encodings_iter it;
	normfs_Encoding e;
	enum gremlin_error err;
	size_t i;
	int status;

	rd->id = 0u;
	rd->step = 0u;
	rd->limit = 0u;
	rd->offset_type = 0;
	rd->queue_id.off = 0u;
	rd->queue_id.len = 0u;
	rd->offset_id_raw.off = 0u;
	rd->offset_id_raw.len = 0u;
	rd->encodings = 0u;
	rd->has_offset = 0;
	rd->has_offset_id = 0;
	err = normfs_ReadRequest_reader_init(&r, m.data, m.len);
	if (err != GREMLIN_OK) return (int)err;
	rd->id = normfs_ReadRequest_reader_get_read_id(&r);
	rd->step = normfs_ReadRequest_reader_get_step(&r);
	rd->limit = normfs_ReadRequest_reader_get_limit(&r);
	if (r._has.queue_id)
		rd->queue_id = normfs_request_span(buf, len, r.queue_id);
	it = normfs_ReadRequest_reader_accept_encodings_begin(&r);
	/*@ loop invariant 0 <= i <= r.accept_encodings_count;
	    loop invariant normfs_ReadRequest_reader_accept_encodings_iter_ok(it);
	    loop invariant normfs_request_span_in(rd->queue_id, len);
	    loop invariant normfs_request_span_in(rd->offset_id_raw, len);
	    loop assigns i, it, e, err, encodings[0 .. encodings_cap - 1];
	    loop variant r.accept_encodings_count - i;
	*/
	for (i = 0u; i < r.accept_encodings_count; i++) {
		err = normfs_ReadRequest_reader_accept_encodings_next(&it, &e);
		if (err != GREMLIN_OK) return (int)err;
		if (i < encodings_cap)
			encodings[i] = (int32_t)e;
	}
	rd->encodings = r.accept_encodings_count;
	if (r._has.offset) {
		status = normfs_request_decode_offset(buf, len, r.offset, rd);
		if (status != NORMFS_REQUEST_OK) return status;
		rd->has_offset = 1;
	}
	return NORMFS_REQUEST_OK;
}

/*@ requires \valid(req) && \valid_read(rd) && \valid_read(w);
    requires \separated(req, rd, w);
    requires normfs_request_span_in(w->queue_id, len);
    requires normfs_request_span_in(rd->queue_id, len);
    requires normfs_request_span_in(rd->offset_id_raw, len);
    requires rd->has_offset_id != 0 ==> rd->has_offset != 0;
    requires rd->has_offset != 0 ==> rd->present != 0;
    assigns *req;
    ensures normfs_request_spans_in(req, len);
    ensures req->write_packets == w->packets && req->read_encodings == rd->encodings;
    ensures req->has_read_offset_id != 0 ==> req->has_read_offset != 0;
    ensures req->has_read_offset != 0 ==> req->has_read != 0;
*/
static void
normfs_request_fill(struct normfs_request *req, size_t len,
    const struct normfs_request_read *rd, const struct normfs_request_write *w,
    uint64_t version, uint64_t sequence, uint64_t timestamp,
    unsigned has_setup, unsigned has_ping)
{
	(void)len;
	req->setup_version = version;
	req->ping_sequence = sequence;
	req->ping_client_timestamp_ns = timestamp;
	req->write_id = w->id;
	req->write_queue_id = w->queue_id;
	req->write_packets = w->packets;
	req->read_id = rd->id;
	req->read_queue_id = rd->queue_id;
	req->read_step = rd->step;
	req->read_encodings = rd->encodings;
	req->read_offset_id_raw = rd->offset_id_raw;
	req->read_offset_type = rd->offset_type;
	req->read_limit = rd->limit;
	req->has_setup = has_setup != 0u;
	req->has_ping = has_ping != 0u;
	req->has_write = w->present;
	req->has_read = rd->present;
	req->has_read_offset = rd->has_offset;
	req->has_read_offset_id = rd->has_offset_id;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    requires \valid(req);
    requires packets_cap == 0 || \valid(packets + (0 .. packets_cap - 1));
    requires encodings_cap == 0 || \valid(encodings + (0 .. encodings_cap - 1));
    requires \separated(req, packets + (0 .. packets_cap - 1),
                        encodings + (0 .. encodings_cap - 1));
    assigns *req, packets[0 .. packets_cap - 1], encodings[0 .. encodings_cap - 1];
    ensures normfs_request_status(\result) || \result == NORMFS_REQUEST_ERR_NO_SPACE;
    ensures \result == NORMFS_REQUEST_OK || \result == NORMFS_REQUEST_ERR_NO_SPACE ==>
              (req->has_read_offset_id != 0 ==> req->has_read_offset != 0) &&
              (req->has_read_offset != 0 ==> req->has_read != 0);
    ensures \result == NORMFS_REQUEST_OK || \result == NORMFS_REQUEST_ERR_NO_SPACE ==>
              normfs_request_spans_in(req, len) &&
              normfs_request_packets_in(packets, req->write_packets, packets_cap, len);
    ensures \result == NORMFS_REQUEST_OK ==>
              req->write_packets <= packets_cap &&
              req->read_encodings <= encodings_cap;
    ensures \result == NORMFS_REQUEST_ERR_NO_SPACE ==>
              req->write_packets > packets_cap ||
              req->read_encodings > encodings_cap;
*/
int
normfs_request_decode(const uint8_t *buf, size_t len,
    struct normfs_request *req,
    struct normfs_request_span *packets, size_t packets_cap,
    int32_t *encodings, size_t encodings_cap)
{
	normfs_ClientRequest_reader r;
	struct normfs_request_read rd = {0};
	struct normfs_request_write w = {0};
	uint64_t version = 0u, sequence = 0u, timestamp = 0u;
	struct gremlin_bytes setup_m, ping_m, write_m, read_m;
	unsigned has_setup, has_ping, has_write, has_read;
	enum gremlin_error err;
	int status;

	err = normfs_ClientRequest_reader_init(&r, buf, len);
	if (err != GREMLIN_OK) return (int)err;
	has_setup = r._has.setup;
	has_ping = r._has.ping;
	has_write = r._has.write;
	has_read = r._has.read;
	setup_m = r.setup;
	ping_m = r.ping;
	write_m = r.write;
	read_m = r.read;
	if (has_setup) {
		status = normfs_request_decode_setup(buf, len, setup_m, &version);
		if (status != NORMFS_REQUEST_OK) return status;
	}
	if (has_ping) {
		status = normfs_request_decode_ping(buf, len, ping_m, &sequence, &timestamp);
		if (status != NORMFS_REQUEST_OK) return status;
	}
	if (has_read) {
		status = normfs_request_decode_read(buf, len, read_m, &rd, encodings, encodings_cap);
		if (status != NORMFS_REQUEST_OK) return status;
		rd.present = 1;
	}
	if (has_write) {
		status = normfs_request_decode_write(buf, len, write_m, &w, packets, packets_cap);
		if (status != NORMFS_REQUEST_OK) return status;
		w.present = 1;
	}

	normfs_request_fill(req, len, &rd, &w, version, sequence, timestamp,
	    has_setup, has_ping);
	if (w.packets > packets_cap || rd.encodings > encodings_cap)
		return NORMFS_REQUEST_ERR_NO_SPACE;
	return NORMFS_REQUEST_OK;
}
