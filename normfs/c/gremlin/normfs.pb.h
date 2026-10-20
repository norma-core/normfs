#ifndef _NORMFS_PROTO_PB_H_
#define _NORMFS_PROTO_PB_H_

#include <stdbool.h>
#include "gremlin.h"

typedef enum normfs_OffsetType {
	normfs_OffsetType_OT_ABSOLUTE = 0,
	normfs_OffsetType_OT_SHIFT_FROM_TAIL = 1,
} normfs_OffsetType;

typedef enum normfs_WriteResponse_Result {
	normfs_WriteResponse_Result_WR_DONE = 0,
	normfs_WriteResponse_Result_WR_SERVER_ERROR = 1,
} normfs_WriteResponse_Result;

typedef enum normfs_Encoding {
	normfs_Encoding_ENC_NONE = 0,
	normfs_Encoding_ENC_ZSTD = 1,
} normfs_Encoding;

typedef enum normfs_ReadResponse_Result {
	normfs_ReadResponse_Result_RR_START = 0,
	normfs_ReadResponse_Result_RR_ENTRY = 1,
	normfs_ReadResponse_Result_RR_END = 2,
	normfs_ReadResponse_Result_RR_QUEUE_NOT_FOUND = 3,
	normfs_ReadResponse_Result_RR_NOT_FOUND = 4,
	normfs_ReadResponse_Result_RR_SERVER_ERROR = 5,
} normfs_ReadResponse_Result;

typedef enum normfs_ReadResponse_DataSource {
	normfs_ReadResponse_DataSource_DS_NONE = 0,
	normfs_ReadResponse_DataSource_DS_CLOUD = 1,
	normfs_ReadResponse_DataSource_DS_DISK_STORE = 2,
	normfs_ReadResponse_DataSource_DS_DISK_WAL = 3,
	normfs_ReadResponse_DataSource_DS_MEMORY = 4,
} normfs_ReadResponse_DataSource;

typedef struct normfs_SetupRequest {
	uint64_t	version;
	size_t	_size;
} normfs_SetupRequest;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_SetupRequest_cached_size(const normfs_SetupRequest *m)
{
	return m->_size;
}

/*@
    logic integer normfs_SetupRequest_version_size{L}(normfs_SetupRequest *m) =
      m->version != 0 ? 1 + varint_size(m->version) : 0;

    logic integer normfs_SetupRequest_tsize{L}(normfs_SetupRequest *m) =
      normfs_SetupRequest_version_size(m);

    predicate normfs_SetupRequest_own{L1,L2}(normfs_SetupRequest *m) =
      \at(m->version, L1) == \at(m->version, L2);

    predicate normfs_SetupRequest_nown{L1,L2}(normfs_SetupRequest *m) =
      \at(m->version, L1) == \at(m->version, L2);

    predicate normfs_SetupRequest_dsame{L1,L2}(normfs_SetupRequest *m) =
      normfs_SetupRequest_own{L1,L2}(m);

    predicate normfs_SetupRequest_csame{L1,L2}(normfs_SetupRequest *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_SetupRequest_sized{L}(normfs_SetupRequest *m) =
      m->_size == normfs_SetupRequest_tsize(m);

    predicate normfs_SetupRequest_valid{L}(normfs_SetupRequest *m) =
      \valid(m);

    predicate normfs_SetupRequest_bufsep{L}(normfs_SetupRequest *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m));

    lemma normfs_SetupRequest_version_size_frame{L1,L2}: \forall normfs_SetupRequest *m;
      normfs_SetupRequest_dsame{L1,L2}(m) ==> \at(normfs_SetupRequest_version_size(m), L1) == \at(normfs_SetupRequest_version_size(m), L2);

    lemma normfs_SetupRequest_tsize_frame{L1,L2}: \forall normfs_SetupRequest *m;
      normfs_SetupRequest_dsame{L1,L2}(m) ==> \at(normfs_SetupRequest_tsize(m), L1) == \at(normfs_SetupRequest_tsize(m), L2);

    lemma normfs_SetupRequest_sized_frame{L1,L2}: \forall normfs_SetupRequest *m;
      normfs_SetupRequest_dsame{L1,L2}(m) ==> normfs_SetupRequest_csame{L1,L2}(m) ==> normfs_SetupRequest_sized{L1}(m) ==> normfs_SetupRequest_sized{L2}(m);

    lemma normfs_SetupRequest_dsame_trans{L1,L2,L3}: \forall normfs_SetupRequest *m;
      normfs_SetupRequest_dsame{L1,L2}(m) ==> normfs_SetupRequest_dsame{L2,L3}(m) ==> normfs_SetupRequest_dsame{L1,L3}(m);

    lemma normfs_SetupRequest_bufsep_sub{L}: \forall normfs_SetupRequest *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_SetupRequest_bufsep(m, b, lo, hi) ==> normfs_SetupRequest_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_SetupRequest_valid(m);
     assigns \nothing;
     ensures normfs_SetupRequest_tsize(m) >= 0;
  @/
  void normfs_SetupRequest_tsize_nonneg(normfs_SetupRequest *m)
  {
  }
*/

/*@ requires normfs_SetupRequest_valid(m);
    requires normfs_SetupRequest_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_SetupRequest_tsize(m));
    ensures normfs_SetupRequest_sized(m);
    ensures normfs_SetupRequest_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_SetupRequest_size(const normfs_SetupRequest *m)
{
	size_t s = 0;
	/*@ ghost normfs_SetupRequest_tsize_nonneg((normfs_SetupRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->version != 0) {
		s += 1
		   + gremlin_varint_size(m->version);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_SetupRequest_version_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert s == \at(normfs_SetupRequest_tsize(m), Pre); */
	/*@ assert normfs_SetupRequest_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_SetupRequest *)m)->_size = s;
	/*@ assert normfs_SetupRequest_own{_Aw,Here}(m); */
	/*@ assert normfs_SetupRequest_dsame{_Aw,Here}(m); */
	/*@ assert normfs_SetupRequest_dsame{Pre,Here}(m); */
	/*@ assert normfs_SetupRequest_tsize(m) == \at(normfs_SetupRequest_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_SetupRequest_valid(m) && normfs_SetupRequest_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_SetupRequest_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_SetupRequest_encode_at(const normfs_SetupRequest *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_SetupRequest_tsize_nonneg((normfs_SetupRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->version != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->version);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_SetupRequest_version_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_SetupRequest_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_SetupRequest_valid(m) && normfs_SetupRequest_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_SetupRequest_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_SetupRequest_encode(const normfs_SetupRequest *m, struct gremlin_writer *w)
{
	w->offset = normfs_SetupRequest_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_SetupRequest_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	version : 1;
	} _has;
	uint64_t	version;
} normfs_SetupRequest_reader;

/*@ predicate normfs_SetupRequest_reader_ok{L}(normfs_SetupRequest_reader *r, integer n) =
      r->src_len >= n;
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_SetupRequest_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_SetupRequest_reader_init(normfs_SetupRequest_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_SetupRequest_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_SetupRequest_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->version = d.value;
			r->_has.version = true;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.version ==> \result == r->version;
*/
static inline uint64_t
normfs_SetupRequest_reader_get_version(const normfs_SetupRequest_reader *r)
{
	if (r == NULL || !r->_has.version) return 0;
	return r->version;
}

typedef struct normfs_PingRequest {
	uint64_t	sequence;
	uint64_t	client_timestamp_ns;
	size_t	_size;
} normfs_PingRequest;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_PingRequest_cached_size(const normfs_PingRequest *m)
{
	return m->_size;
}

/*@ predicate normfs_PingRequest_avoid_normfs_SetupRequest{L}(normfs_PingRequest *m, normfs_SetupRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_SetupRequest_navoid_normfs_PingRequest{L}(normfs_SetupRequest *m, normfs_PingRequest *t) =
      normfs_PingRequest_avoid_normfs_SetupRequest(t, m);
*/

/*@
    logic integer normfs_PingRequest_sequence_size{L}(normfs_PingRequest *m) =
      m->sequence != 0 ? 1 + varint_size(m->sequence) : 0;

    logic integer normfs_PingRequest_client_timestamp_ns_size{L}(normfs_PingRequest *m) =
      m->client_timestamp_ns != 0 ? 1 + varint_size(m->client_timestamp_ns) : 0;

    logic integer normfs_PingRequest_tsize{L}(normfs_PingRequest *m) =
      normfs_PingRequest_sequence_size(m) +
      normfs_PingRequest_client_timestamp_ns_size(m);

    predicate normfs_PingRequest_own{L1,L2}(normfs_PingRequest *m) =
      \at(m->sequence, L1) == \at(m->sequence, L2) &&
      \at(m->client_timestamp_ns, L1) == \at(m->client_timestamp_ns, L2);

    predicate normfs_PingRequest_nown{L1,L2}(normfs_PingRequest *m) =
      \at(m->sequence, L1) == \at(m->sequence, L2) &&
      \at(m->client_timestamp_ns, L1) == \at(m->client_timestamp_ns, L2);

    predicate normfs_PingRequest_dsame{L1,L2}(normfs_PingRequest *m) =
      normfs_PingRequest_own{L1,L2}(m);

    predicate normfs_PingRequest_csame{L1,L2}(normfs_PingRequest *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_PingRequest_sized{L}(normfs_PingRequest *m) =
      m->_size == normfs_PingRequest_tsize(m);

    predicate normfs_PingRequest_valid{L}(normfs_PingRequest *m) =
      \valid(m);

    predicate normfs_PingRequest_bufsep{L}(normfs_PingRequest *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m));

    lemma normfs_PingRequest_sequence_size_frame{L1,L2}: \forall normfs_PingRequest *m;
      normfs_PingRequest_dsame{L1,L2}(m) ==> \at(normfs_PingRequest_sequence_size(m), L1) == \at(normfs_PingRequest_sequence_size(m), L2);

    lemma normfs_PingRequest_client_timestamp_ns_size_frame{L1,L2}: \forall normfs_PingRequest *m;
      normfs_PingRequest_dsame{L1,L2}(m) ==> \at(normfs_PingRequest_client_timestamp_ns_size(m), L1) == \at(normfs_PingRequest_client_timestamp_ns_size(m), L2);

    lemma normfs_PingRequest_tsize_frame{L1,L2}: \forall normfs_PingRequest *m;
      normfs_PingRequest_dsame{L1,L2}(m) ==> \at(normfs_PingRequest_tsize(m), L1) == \at(normfs_PingRequest_tsize(m), L2);

    lemma normfs_PingRequest_sized_frame{L1,L2}: \forall normfs_PingRequest *m;
      normfs_PingRequest_dsame{L1,L2}(m) ==> normfs_PingRequest_csame{L1,L2}(m) ==> normfs_PingRequest_sized{L1}(m) ==> normfs_PingRequest_sized{L2}(m);

    lemma normfs_PingRequest_dsame_trans{L1,L2,L3}: \forall normfs_PingRequest *m;
      normfs_PingRequest_dsame{L1,L2}(m) ==> normfs_PingRequest_dsame{L2,L3}(m) ==> normfs_PingRequest_dsame{L1,L3}(m);

    lemma normfs_PingRequest_bufsep_sub{L}: \forall normfs_PingRequest *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_PingRequest_bufsep(m, b, lo, hi) ==> normfs_PingRequest_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_PingRequest_valid(m);
     assigns \nothing;
     ensures normfs_PingRequest_tsize(m) >= 0;
  @/
  void normfs_PingRequest_tsize_nonneg(normfs_PingRequest *m)
  {
  }
*/

/*@ requires normfs_PingRequest_valid(m);
    requires normfs_PingRequest_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_PingRequest_tsize(m));
    ensures normfs_PingRequest_sized(m);
    ensures normfs_PingRequest_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_PingRequest_size(const normfs_PingRequest *m)
{
	size_t s = 0;
	/*@ ghost normfs_PingRequest_tsize_nonneg((normfs_PingRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->sequence != 0) {
		s += 1
		   + gremlin_varint_size(m->sequence);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_PingRequest_sequence_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->client_timestamp_ns != 0) {
		s += 1
		   + gremlin_varint_size(m->client_timestamp_ns);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_PingRequest_client_timestamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert s == \at(normfs_PingRequest_tsize(m), Pre); */
	/*@ assert normfs_PingRequest_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_PingRequest *)m)->_size = s;
	/*@ assert normfs_PingRequest_own{_Aw,Here}(m); */
	/*@ assert normfs_PingRequest_dsame{_Aw,Here}(m); */
	/*@ assert normfs_PingRequest_dsame{Pre,Here}(m); */
	/*@ assert normfs_PingRequest_tsize(m) == \at(normfs_PingRequest_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_PingRequest_valid(m) && normfs_PingRequest_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_PingRequest_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_PingRequest_encode_at(const normfs_PingRequest *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_PingRequest_tsize_nonneg((normfs_PingRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->sequence != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->sequence);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_PingRequest_sequence_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->client_timestamp_ns != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, m->client_timestamp_ns);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_PingRequest_client_timestamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_PingRequest_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_PingRequest_valid(m) && normfs_PingRequest_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_PingRequest_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_PingRequest_encode(const normfs_PingRequest *m, struct gremlin_writer *w)
{
	w->offset = normfs_PingRequest_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_PingRequest_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	sequence : 1;
		unsigned	client_timestamp_ns : 1;
	} _has;
	uint64_t	sequence;
	uint64_t	client_timestamp_ns;
} normfs_PingRequest_reader;

/*@ predicate normfs_PingRequest_reader_ok{L}(normfs_PingRequest_reader *r, integer n) =
      r->src_len >= n;
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_PingRequest_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_PingRequest_reader_init(normfs_PingRequest_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_PingRequest_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_PingRequest_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->sequence = d.value;
			r->_has.sequence = true;
			continue;
		}
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->client_timestamp_ns = d.value;
			r->_has.client_timestamp_ns = true;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.sequence ==> \result == r->sequence;
*/
static inline uint64_t
normfs_PingRequest_reader_get_sequence(const normfs_PingRequest_reader *r)
{
	if (r == NULL || !r->_has.sequence) return 0;
	return r->sequence;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.client_timestamp_ns ==> \result == r->client_timestamp_ns;
*/
static inline uint64_t
normfs_PingRequest_reader_get_client_timestamp_ns(const normfs_PingRequest_reader *r)
{
	if (r == NULL || !r->_has.client_timestamp_ns) return 0;
	return r->client_timestamp_ns;
}

typedef struct normfs_WriteRequest {
	uint64_t	write_id;
	struct gremlin_bytes	queue_id;
	struct gremlin_bytes	*packets;
	size_t	packets_count;
	size_t	_size;
} normfs_WriteRequest;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_WriteRequest_cached_size(const normfs_WriteRequest *m)
{
	return m->_size;
}

/*@ predicate normfs_WriteRequest_avoid_normfs_SetupRequest{L}(normfs_WriteRequest *m, normfs_SetupRequest *p) =
      \separated(p, m) &&
      (m->packets_count == 0 || \separated(p, m->packets + (0 .. m->packets_count - 1)));
*/

/*@ predicate normfs_WriteRequest_avoid_normfs_PingRequest{L}(normfs_WriteRequest *m, normfs_PingRequest *p) =
      \separated(p, m) &&
      (m->packets_count == 0 || \separated(p, m->packets + (0 .. m->packets_count - 1)));
*/

/*@ predicate normfs_SetupRequest_navoid_normfs_WriteRequest{L}(normfs_SetupRequest *m, normfs_WriteRequest *t) =
      normfs_WriteRequest_avoid_normfs_SetupRequest(t, m);
*/

/*@ predicate normfs_PingRequest_navoid_normfs_WriteRequest{L}(normfs_PingRequest *m, normfs_WriteRequest *t) =
      normfs_WriteRequest_avoid_normfs_PingRequest(t, m);
*/

/*@ logic integer normfs_WriteRequest_packets_sum{L}(normfs_WriteRequest *m, integer n) =
      n <= 0 ? 0 :
      normfs_WriteRequest_packets_sum(m, n - 1) + 1 + varint_size(m->packets[n - 1].len) + m->packets[n - 1].len;

    lemma normfs_WriteRequest_packets_sum_frame{L1,L2}: \forall normfs_WriteRequest *m, integer n; 0 <= n ==>
      \at(m->packets, L1) == \at(m->packets, L2) ==>
      (\forall integer k; 0 <= k < n ==> \at(m->packets[k].len, L1) == \at(m->packets[k].len, L2)) ==>
      \at(normfs_WriteRequest_packets_sum(m, n), L1) == \at(normfs_WriteRequest_packets_sum(m, n), L2);
*/
/*@ \wp::strategy normfs_WriteRequest_packets_sum_ind: \tactic("Wp.induction", \pattern(0 <= n), \select(n)); */
/*@ \wp::proof normfs_WriteRequest_packets_sum_ind: normfs_WriteRequest_packets_sum_frame; */

/*@
    logic integer normfs_WriteRequest_write_id_size{L}(normfs_WriteRequest *m) =
      m->write_id != 0 ? 1 + varint_size(m->write_id) : 0;

    logic integer normfs_WriteRequest_queue_id_size{L}(normfs_WriteRequest *m) =
      m->queue_id.len > 0 ? 1 + varint_size(m->queue_id.len) + m->queue_id.len : 0;

    logic integer normfs_WriteRequest_packets_size{L}(normfs_WriteRequest *m) =
      normfs_WriteRequest_packets_sum(m, m->packets_count);

    logic integer normfs_WriteRequest_tsize{L}(normfs_WriteRequest *m) =
      normfs_WriteRequest_write_id_size(m) +
      normfs_WriteRequest_queue_id_size(m) +
      normfs_WriteRequest_packets_size(m);

    predicate normfs_WriteRequest_own{L1,L2}(normfs_WriteRequest *m) =
      \at(m->write_id, L1) == \at(m->write_id, L2) &&
      \at(m->queue_id.len, L1) == \at(m->queue_id.len, L2) &&
      \at(m->queue_id.data, L1) == \at(m->queue_id.data, L2) &&
      \at(m->packets, L1) == \at(m->packets, L2) && \at(m->packets_count, L1) == \at(m->packets_count, L2) &&
      (\forall integer k; 0 <= k < \at(m->packets_count, L1) ==>
         \at(m->packets[k].len, L1) == \at(m->packets[k].len, L2) && \at(m->packets[k].data, L1) == \at(m->packets[k].data, L2));

    predicate normfs_WriteRequest_nown{L1,L2}(normfs_WriteRequest *m) =
      \at(m->write_id, L1) == \at(m->write_id, L2) &&
      \at(m->queue_id.len, L1) == \at(m->queue_id.len, L2) &&
      \at(m->queue_id.data, L1) == \at(m->queue_id.data, L2) &&
      \at(m->packets, L1) == \at(m->packets, L2) && \at(m->packets_count, L1) == \at(m->packets_count, L2);

    predicate normfs_WriteRequest_dsame{L1,L2}(normfs_WriteRequest *m) =
      normfs_WriteRequest_own{L1,L2}(m);

    predicate normfs_WriteRequest_csame{L1,L2}(normfs_WriteRequest *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_WriteRequest_sized{L}(normfs_WriteRequest *m) =
      m->_size == normfs_WriteRequest_tsize(m);

    predicate normfs_WriteRequest_valid{L}(normfs_WriteRequest *m) =
      \valid(m) &&
      (m->queue_id.len == 0 || \valid_read(m->queue_id.data + (0 .. m->queue_id.len - 1))) &&
      (m->packets_count == 0 || \valid_read(m->packets + (0 .. m->packets_count - 1))) &&
      (\forall integer k; 0 <= k < m->packets_count ==>
         m->packets[k].len == 0 || \valid_read(m->packets[k].data + (0 .. m->packets[k].len - 1))) &&
      (m->packets_count == 0 || \separated(m, m->packets + (0 .. m->packets_count - 1)));

    predicate normfs_WriteRequest_bufsep{L}(normfs_WriteRequest *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->packets_count == 0 || \separated(b + (lo .. hi), m->packets + (0 .. m->packets_count - 1))));

    lemma normfs_WriteRequest_write_id_size_frame{L1,L2}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> \at(normfs_WriteRequest_write_id_size(m), L1) == \at(normfs_WriteRequest_write_id_size(m), L2);

    lemma normfs_WriteRequest_queue_id_size_frame{L1,L2}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> \at(normfs_WriteRequest_queue_id_size(m), L1) == \at(normfs_WriteRequest_queue_id_size(m), L2);

    lemma normfs_WriteRequest_packets_size_frame{L1,L2}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> \at(normfs_WriteRequest_packets_size(m), L1) == \at(normfs_WriteRequest_packets_size(m), L2);

    lemma normfs_WriteRequest_tsize_frame{L1,L2}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> \at(normfs_WriteRequest_tsize(m), L1) == \at(normfs_WriteRequest_tsize(m), L2);

    lemma normfs_WriteRequest_sized_frame{L1,L2}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> normfs_WriteRequest_csame{L1,L2}(m) ==> normfs_WriteRequest_sized{L1}(m) ==> normfs_WriteRequest_sized{L2}(m);

    lemma normfs_WriteRequest_dsame_trans{L1,L2,L3}: \forall normfs_WriteRequest *m;
      normfs_WriteRequest_dsame{L1,L2}(m) ==> normfs_WriteRequest_dsame{L2,L3}(m) ==> normfs_WriteRequest_dsame{L1,L3}(m);

    lemma normfs_WriteRequest_bufsep_sub{L}: \forall normfs_WriteRequest *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_WriteRequest_bufsep(m, b, lo, hi) ==> normfs_WriteRequest_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_WriteRequest_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->packets_count ==>
               0 <= normfs_WriteRequest_packets_sum(m, j) <= normfs_WriteRequest_packets_sum(m, m->packets_count);
  @/
  void normfs_WriteRequest_packets_mono(normfs_WriteRequest *m)
  {
    size_t i = 0;
    /@ loop invariant 0 <= i <= m->packets_count;
       loop invariant \forall integer j; 0 <= j <= i ==> 0 <= normfs_WriteRequest_packets_sum(m, j) <= normfs_WriteRequest_packets_sum(m, i);
       loop assigns i;
       loop variant m->packets_count - i;
    @/
    while (i < m->packets_count) {
      i++;
    }
  }
*/

/*@ ghost
  /@ requires normfs_WriteRequest_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->packets_count ==>
               0 <= normfs_WriteRequest_packets_sum(m, j) <= normfs_WriteRequest_packets_sum(m, m->packets_count);
     ensures normfs_WriteRequest_tsize(m) >= 0;
  @/
  void normfs_WriteRequest_tsize_nonneg(normfs_WriteRequest *m)
  {
    normfs_WriteRequest_packets_mono(m);
  }
*/

/*@ requires normfs_WriteRequest_valid(m);
    requires normfs_WriteRequest_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_WriteRequest_tsize(m));
    ensures normfs_WriteRequest_sized(m);
    ensures normfs_WriteRequest_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_WriteRequest_size(const normfs_WriteRequest *m)
{
	size_t s = 0;
	/*@ ghost normfs_WriteRequest_tsize_nonneg((normfs_WriteRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->write_id != 0) {
		s += 1
		   + gremlin_varint_size(m->write_id);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_WriteRequest_write_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->queue_id.len > 0) {
		s += 1
		   + gremlin_varint_size(m->queue_id.len)
		   + m->queue_id.len;
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_WriteRequest_queue_id_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ loop invariant 0 <= _i <= m->packets_count;
	    loop invariant \forall integer k; k == _i ==> s == \at(s, _A2) + \at(normfs_WriteRequest_packets_sum(m, k), Pre);
	    loop assigns _i, s;
	    loop variant m->packets_count - _i;
	*/
	for (size_t _i = 0; _i < m->packets_count; _i++) {
		/*@ assert \forall integer k; k == _i ==>
		      m->packets[k].len == \at(m->packets[k].len, Pre) &&
		      \at(normfs_WriteRequest_packets_sum(m, k + 1), Pre) == \at(normfs_WriteRequest_packets_sum(m, k), Pre) + 1 +
		        varint_size(\at(m->packets[k].len, Pre)) + \at(m->packets[k].len, Pre) &&
		      \at(normfs_WriteRequest_packets_sum(m, k + 1), Pre) <= \at(normfs_WriteRequest_packets_sum(m, m->packets_count), Pre); */
		s += 1
		   + gremlin_varint_size(m->packets[_i].len)
		   + m->packets[_i].len;
	}
	/*@ assert s == \at(s, _A2) + \at(normfs_WriteRequest_packets_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s == \at(normfs_WriteRequest_tsize(m), Pre); */
	/*@ assert normfs_WriteRequest_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_WriteRequest *)m)->_size = s;
	/*@ assert normfs_WriteRequest_own{_Aw,Here}(m); */
	/*@ assert normfs_WriteRequest_dsame{_Aw,Here}(m); */
	/*@ assert normfs_WriteRequest_dsame{Pre,Here}(m); */
	/*@ assert normfs_WriteRequest_tsize(m) == \at(normfs_WriteRequest_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_WriteRequest_valid(m) && normfs_WriteRequest_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_WriteRequest_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_WriteRequest_encode_at(const normfs_WriteRequest *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_WriteRequest_tsize_nonneg((normfs_WriteRequest *)m); */
	/*@ ghost _A0: ; */
	if (m->write_id != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->write_id);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_WriteRequest_write_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->queue_id.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 18u);
		_off = gremlin_varint_encode_at(_buf, _off, m->queue_id.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->queue_id.data, m->queue_id.len);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_WriteRequest_queue_id_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ loop invariant 0 <= _i <= m->packets_count;
	    loop invariant \forall integer k; k == _i ==> _off == \at(_off, _A2) + \at(normfs_WriteRequest_packets_sum(m, k), Pre);
	    loop assigns _i, _off, _buf[\at(_off, Pre) .. \at(_off, Pre) + \at(m->_size, Pre) - 1];
	    loop variant m->packets_count - _i;
	*/
	for (size_t _i = 0; _i < m->packets_count; _i++) {
		/*@ assert \forall integer k; k == _i ==>
		      m->packets[k].len == \at(m->packets[k].len, Pre) &&
		      \at(normfs_WriteRequest_packets_sum(m, k + 1), Pre) == \at(normfs_WriteRequest_packets_sum(m, k), Pre) + 1 +
		        varint_size(\at(m->packets[k].len, Pre)) + \at(m->packets[k].len, Pre) &&
		      \at(normfs_WriteRequest_packets_sum(m, k + 1), Pre) <= \at(normfs_WriteRequest_packets_sum(m, m->packets_count), Pre); */
		_off = gremlin_varint32_encode_at(_buf, _off, 82u);
		_off = gremlin_varint_encode_at(_buf, _off, m->packets[_i].len);
		_off = gremlin_write_bytes_at(_buf, _off, m->packets[_i].data, m->packets[_i].len);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_WriteRequest_packets_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_WriteRequest_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_WriteRequest_valid(m) && normfs_WriteRequest_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_WriteRequest_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_WriteRequest_encode(const normfs_WriteRequest *m, struct gremlin_writer *w)
{
	w->offset = normfs_WriteRequest_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_WriteRequest_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	write_id : 1;
		unsigned	queue_id : 1;
		unsigned	packets : 1;
	} _has;
	uint64_t	write_id;
	struct gremlin_bytes	queue_id;
	size_t	packets_count;
	size_t	packets_first_offset;
} normfs_WriteRequest_reader;

/*@ predicate normfs_WriteRequest_reader_ok{L}(normfs_WriteRequest_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.queue_id ==> gremlin_span_in(r->queue_id, (uint8_t *)r->src, n)) &&
      r->packets_count <= n &&
      (r->packets_count > 0 ==> r->packets_first_offset < n);
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_WriteRequest_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_WriteRequest_reader_init(normfs_WriteRequest_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_WriteRequest_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_WriteRequest_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->write_id = d.value;
			r->_has.write_id = true;
			continue;
		}
		if (t.value == 18u /* field 2, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->queue_id = d.bytes;
			r->_has.queue_id = 1;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX, repeated */) {
			if (r->packets_count == 0) {
				r->packets_first_offset = offset;
			}
			r->packets_count++;
			r->_has.packets = 1;
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.write_id ==> \result == r->write_id;
*/
static inline uint64_t
normfs_WriteRequest_reader_get_write_id(const normfs_WriteRequest_reader *r)
{
	if (r == NULL || !r->_has.write_id) return 0;
	return r->write_id;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.queue_id ==> \result == r->queue_id;
    ensures !(r != \null && r->_has.queue_id) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_WriteRequest_reader_get_queue_id(const normfs_WriteRequest_reader *r)
{
	if (r == NULL || !r->_has.queue_id) return (struct gremlin_bytes){ NULL, 0 };
	return r->queue_id;
}

typedef struct normfs_WriteRequest_reader_packets_iter {
	const uint8_t	*src;
	size_t		 src_len;
	size_t		 offset;
	size_t		 count_remaining;
} normfs_WriteRequest_reader_packets_iter;

/*@ predicate normfs_WriteRequest_reader_packets_iter_ok(normfs_WriteRequest_reader_packets_iter it) =
      it.offset <= it.src_len &&
      (it.src_len == 0 || \valid_read(it.src + (0 .. it.src_len - 1)));
*/

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null ==> \result == r->packets_count;
*/
static inline size_t
normfs_WriteRequest_reader_packets_count(const normfs_WriteRequest_reader *r)
{
	if (r == NULL) return 0;
	return r->packets_count;
}

/*@ requires r == \null || (\valid_read(r) && normfs_WriteRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))));
    assigns \nothing;
    ensures normfs_WriteRequest_reader_packets_iter_ok(\result);
    ensures r != \null && r->packets_count > 0 ==>
              \result.src == r->src && \result.src_len == r->src_len &&
              \result.count_remaining == r->packets_count;
    ensures r == \null || r->packets_count == 0 ==>
              \result.src_len == 0 && \result.count_remaining == 0;
*/
static inline normfs_WriteRequest_reader_packets_iter
normfs_WriteRequest_reader_packets_begin(const normfs_WriteRequest_reader *r)
{
	normfs_WriteRequest_reader_packets_iter it = {0};
	if (r == NULL || r->packets_count == 0) return it;
	it.src = r->src;
	it.src_len = r->src_len;
	it.offset = r->packets_first_offset;
	it.count_remaining = r->packets_count;
	return it;
}

/*@ requires \valid(it) && \valid(out);
    requires normfs_WriteRequest_reader_packets_iter_ok(*it);
    requires \separated(it, out, it->src + (0 .. it->src_len - 1));
    assigns *it, *out;
    ensures normfs_WriteRequest_reader_packets_iter_ok(*it);
    ensures it->src == \old(it->src) && it->src_len == \old(it->src_len);
    ensures \result == GREMLIN_OK ==>
              out->len == 0 || gremlin_span_in(*out, (uint8_t *)it->src, it->src_len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_WriteRequest_reader_packets_next(normfs_WriteRequest_reader_packets_iter *it, struct gremlin_bytes *out)
{
	if (it->count_remaining == 0) {
		out->data = NULL; out->len = 0;
		return GREMLIN_OK;
	}
	struct gremlin_bytes_decode_result d =
		gremlin_bytes_decode(it->src + it->offset, it->src_len - it->offset);
	if (d.error != GREMLIN_OK) return d.error;
	*out = d.bytes;
	it->offset += d.consumed;
	it->count_remaining--;
	/*@ loop invariant it->offset <= it->src_len;
	    loop assigns it->offset;
	    loop variant it->src_len - it->offset;
	*/
	while (it->count_remaining > 0 && it->offset < it->src_len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(it->src + it->offset, it->src_len - it->offset);
		if (t.error != GREMLIN_OK) return t.error;
		it->offset += t.consumed;
		if (t.value == 82u) break;
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		struct gremlin_skip_result sk =
			gremlin_skip_data(it->src + it->offset, it->src_len - it->offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		it->offset += sk.consumed;
	}
	return GREMLIN_OK;
}

typedef struct normfs_Id {
	struct gremlin_bytes	raw;
	size_t	_size;
} normfs_Id;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_Id_cached_size(const normfs_Id *m)
{
	return m->_size;
}

/*@ predicate normfs_Id_avoid_normfs_Encoding{L}(normfs_Id *m, normfs_Encoding *p, integer n) =
      n <= 0 || (\separated(p + (0 .. n - 1), m));
*/

/*@ predicate normfs_Id_avoid_normfs_Id{L}(normfs_Id *m, normfs_Id *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_PingRequest{L}(normfs_Id *m, normfs_PingRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_SetupRequest{L}(normfs_Id *m, normfs_SetupRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_WriteRequest{L}(normfs_Id *m, normfs_WriteRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_bytes{L}(normfs_Id *m, struct gremlin_bytes *p, integer n) =
      n <= 0 || (\separated(p + (0 .. n - 1), m));
*/

/*@ predicate normfs_Id_navoid_normfs_Id{L}(normfs_Id *m, normfs_Id *t) =
      normfs_Id_avoid_normfs_Id(t, m);
*/

/*@
    logic integer normfs_Id_raw_size{L}(normfs_Id *m) =
      m->raw.len > 0 ? 1 + varint_size(m->raw.len) + m->raw.len : 0;

    logic integer normfs_Id_tsize{L}(normfs_Id *m) =
      normfs_Id_raw_size(m);

    predicate normfs_Id_own{L1,L2}(normfs_Id *m) =
      \at(m->raw.len, L1) == \at(m->raw.len, L2) &&
      \at(m->raw.data, L1) == \at(m->raw.data, L2);

    predicate normfs_Id_nown{L1,L2}(normfs_Id *m) =
      \at(m->raw.len, L1) == \at(m->raw.len, L2) &&
      \at(m->raw.data, L1) == \at(m->raw.data, L2);

    predicate normfs_Id_dsame{L1,L2}(normfs_Id *m) =
      normfs_Id_own{L1,L2}(m);

    predicate normfs_Id_csame{L1,L2}(normfs_Id *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_Id_sized{L}(normfs_Id *m) =
      m->_size == normfs_Id_tsize(m);

    predicate normfs_Id_valid{L}(normfs_Id *m) =
      \valid(m) &&
      (m->raw.len == 0 || \valid_read(m->raw.data + (0 .. m->raw.len - 1)));

    predicate normfs_Id_bufsep{L}(normfs_Id *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m));

    lemma normfs_Id_raw_size_frame{L1,L2}: \forall normfs_Id *m;
      normfs_Id_dsame{L1,L2}(m) ==> \at(normfs_Id_raw_size(m), L1) == \at(normfs_Id_raw_size(m), L2);

    lemma normfs_Id_tsize_frame{L1,L2}: \forall normfs_Id *m;
      normfs_Id_dsame{L1,L2}(m) ==> \at(normfs_Id_tsize(m), L1) == \at(normfs_Id_tsize(m), L2);

    lemma normfs_Id_sized_frame{L1,L2}: \forall normfs_Id *m;
      normfs_Id_dsame{L1,L2}(m) ==> normfs_Id_csame{L1,L2}(m) ==> normfs_Id_sized{L1}(m) ==> normfs_Id_sized{L2}(m);

    lemma normfs_Id_dsame_trans{L1,L2,L3}: \forall normfs_Id *m;
      normfs_Id_dsame{L1,L2}(m) ==> normfs_Id_dsame{L2,L3}(m) ==> normfs_Id_dsame{L1,L3}(m);

    lemma normfs_Id_bufsep_sub{L}: \forall normfs_Id *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_Id_bufsep(m, b, lo, hi) ==> normfs_Id_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_Id_valid(m);
     assigns \nothing;
     ensures normfs_Id_tsize(m) >= 0;
  @/
  void normfs_Id_tsize_nonneg(normfs_Id *m)
  {
  }
*/

/*@ requires normfs_Id_valid(m);
    requires normfs_Id_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_Id_tsize(m));
    ensures normfs_Id_sized(m);
    ensures normfs_Id_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_Id_size(const normfs_Id *m)
{
	size_t s = 0;
	/*@ ghost normfs_Id_tsize_nonneg((normfs_Id *)m); */
	/*@ ghost _A0: ; */
	if (m->raw.len > 0) {
		s += 1
		   + gremlin_varint_size(m->raw.len)
		   + m->raw.len;
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_Id_raw_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert s == \at(normfs_Id_tsize(m), Pre); */
	/*@ assert normfs_Id_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_Id *)m)->_size = s;
	/*@ assert normfs_Id_own{_Aw,Here}(m); */
	/*@ assert normfs_Id_dsame{_Aw,Here}(m); */
	/*@ assert normfs_Id_dsame{Pre,Here}(m); */
	/*@ assert normfs_Id_tsize(m) == \at(normfs_Id_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_Id_valid(m) && normfs_Id_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_Id_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_Id_encode_at(const normfs_Id *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_Id_tsize_nonneg((normfs_Id *)m); */
	/*@ ghost _A0: ; */
	if (m->raw.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 10u);
		_off = gremlin_varint_encode_at(_buf, _off, m->raw.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->raw.data, m->raw.len);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_Id_raw_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_Id_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_Id_valid(m) && normfs_Id_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_Id_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_Id_encode(const normfs_Id *m, struct gremlin_writer *w)
{
	w->offset = normfs_Id_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_Id_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	raw : 1;
	} _has;
	struct gremlin_bytes	raw;
} normfs_Id_reader;

/*@ predicate normfs_Id_reader_ok{L}(normfs_Id_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.raw ==> gremlin_span_in(r->raw, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_Id_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_Id_reader_init(normfs_Id_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_Id_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_Id_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 10u /* field 1, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->raw = d.bytes;
			r->_has.raw = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.raw ==> \result == r->raw;
    ensures !(r != \null && r->_has.raw) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_Id_reader_get_raw(const normfs_Id_reader *r)
{
	if (r == NULL || !r->_has.raw) return (struct gremlin_bytes){ NULL, 0 };
	return r->raw;
}

typedef struct normfs_Offset {
	const normfs_Id *	id;
	normfs_OffsetType	type;
	size_t	_size;
} normfs_Offset;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_Offset_cached_size(const normfs_Offset *m)
{
	return m->_size;
}

/*@ predicate normfs_Id_avoid_normfs_Offset{L}(normfs_Id *m, normfs_Offset *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Offset_avoid_normfs_Encoding{L}(normfs_Offset *m, normfs_Encoding *p, integer n) =
      n <= 0 || (\separated(p + (0 .. n - 1), m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_Encoding((normfs_Id *)m->id, p, n)));
*/

/*@ predicate normfs_Offset_avoid_normfs_SetupRequest{L}(normfs_Offset *m, normfs_SetupRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_SetupRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_Offset_avoid_normfs_PingRequest{L}(normfs_Offset *m, normfs_PingRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_PingRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_Offset_avoid_normfs_WriteRequest{L}(normfs_Offset *m, normfs_WriteRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_WriteRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_Offset_avoid_bytes{L}(normfs_Offset *m, struct gremlin_bytes *p, integer n) =
      n <= 0 || (\separated(p + (0 .. n - 1), m) &&
      (m->id != \null ==> normfs_Id_avoid_bytes((normfs_Id *)m->id, p, n)));
*/

/*@
    logic integer normfs_Offset_id_size{L}(normfs_Offset *m) =
      m->id != \null ?
        1 + varint_size(normfs_Id_tsize((normfs_Id *)m->id)) + normfs_Id_tsize((normfs_Id *)m->id) : 0;

    logic integer normfs_Offset_type_size{L}(normfs_Offset *m) =
      m->type != 0 ? 1 + varint_size((uint64_t)(int64_t)m->type) : 0;

    logic integer normfs_Offset_tsize{L}(normfs_Offset *m) =
      normfs_Offset_id_size(m) +
      normfs_Offset_type_size(m);

    predicate normfs_Offset_own{L1,L2}(normfs_Offset *m) =
      \at(m->id, L1) == \at(m->id, L2) &&
      \at(m->type, L1) == \at(m->type, L2);

    predicate normfs_Offset_nown{L1,L2}(normfs_Offset *m) =
      \at(m->id, L1) == \at(m->id, L2) &&
      \at(m->type, L1) == \at(m->type, L2);

    predicate normfs_Offset_dsame{L1,L2}(normfs_Offset *m) =
      normfs_Offset_own{L1,L2}(m) &&
      (\at(m->id, L1) != \null ==> normfs_Id_dsame{L1,L2}((normfs_Id *)\at(m->id, L1)));

    predicate normfs_Offset_csame{L1,L2}(normfs_Offset *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->id, L1) != \null ==> normfs_Id_csame{L1,L2}((normfs_Id *)\at(m->id, L1)));

    predicate normfs_Offset_sized{L}(normfs_Offset *m) =
      m->_size == normfs_Offset_tsize(m) &&
      (m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id));

    predicate normfs_Offset_valid{L}(normfs_Offset *m) =
      \valid(m) &&
      (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_Offset((normfs_Id *)m->id, m));

    predicate normfs_Offset_bufsep{L}(normfs_Offset *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->id != \null ==> normfs_Id_bufsep((normfs_Id *)m->id, b, lo, hi)));

    lemma normfs_Offset_id_size_frame{L1,L2}: \forall normfs_Offset *m;
      normfs_Offset_dsame{L1,L2}(m) ==> \at(normfs_Offset_id_size(m), L1) == \at(normfs_Offset_id_size(m), L2);

    lemma normfs_Offset_type_size_frame{L1,L2}: \forall normfs_Offset *m;
      normfs_Offset_dsame{L1,L2}(m) ==> \at(normfs_Offset_type_size(m), L1) == \at(normfs_Offset_type_size(m), L2);

    lemma normfs_Offset_tsize_frame{L1,L2}: \forall normfs_Offset *m;
      normfs_Offset_dsame{L1,L2}(m) ==> \at(normfs_Offset_tsize(m), L1) == \at(normfs_Offset_tsize(m), L2);

    lemma normfs_Offset_sized_frame{L1,L2}: \forall normfs_Offset *m;
      normfs_Offset_dsame{L1,L2}(m) ==> normfs_Offset_csame{L1,L2}(m) ==> normfs_Offset_sized{L1}(m) ==> normfs_Offset_sized{L2}(m);

    lemma normfs_Offset_dsame_trans{L1,L2,L3}: \forall normfs_Offset *m;
      normfs_Offset_dsame{L1,L2}(m) ==> normfs_Offset_dsame{L2,L3}(m) ==> normfs_Offset_dsame{L1,L3}(m);

    lemma normfs_Offset_bufsep_sub{L}: \forall normfs_Offset *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_Offset_bufsep(m, b, lo, hi) ==> normfs_Offset_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_Offset_valid(m);
     assigns \nothing;
     ensures m->id != \null ==> normfs_Id_tsize((normfs_Id *)m->id) >= 0;
     ensures normfs_Offset_tsize(m) >= 0;
  @/
  void normfs_Offset_tsize_nonneg(normfs_Offset *m)
  {
    if (m->id) normfs_Id_tsize_nonneg((normfs_Id *)m->id);
  }
*/

/*@ requires normfs_Offset_valid(m);
    requires normfs_Offset_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->id->_size;
    ensures \result == \old(normfs_Offset_tsize(m));
    ensures normfs_Offset_sized(m);
    ensures normfs_Offset_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_Offset_size(const normfs_Offset *m)
{
	size_t s = 0;
	/*@ ghost normfs_Offset_tsize_nonneg((normfs_Offset *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)); */
	/*@ assert \at(normfs_Offset_type_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_Offset_id_size(m), Pre) + \at(normfs_Offset_type_size(m), Pre) == \at(normfs_Offset_tsize(m), Pre); */
	/*@ assert s + \at(normfs_Offset_id_size(m), Pre) <= \at(normfs_Offset_tsize(m), Pre); */
	/*@ assert m->id != \null ==> normfs_Id_tsize((normfs_Id *)m->id) == \at(normfs_Id_tsize((normfs_Id *)m->id), Pre); */
	/*@ assert m->id != \null ==> \at(normfs_Offset_id_size(m), Pre) == 1 + varint_size(normfs_Id_tsize((normfs_Id *)m->id)) + normfs_Id_tsize((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> &m->id->_size != &m->_size; */
	if (m->id != NULL) {
		size_t _child_size = normfs_Id_size(m->id);
		/*@ assert _child_size == \at(normfs_Id_tsize((normfs_Id *)m->id), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A0) == m->_size; */
	/*@ assert normfs_Offset_nown{_A0,Here}((normfs_Offset *)m); */
	/*@ assert normfs_Offset_own{_A0,Here}((normfs_Offset *)m); */
	/*@ assert normfs_Offset_own{Pre,Here}(m); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{Pre,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert s == \at(s, _A0) + \at(normfs_Offset_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->type != 0) {
		s += 1
		   + gremlin_varint_size((uint64_t)(int64_t)m->type);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_Offset_type_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert s == \at(normfs_Offset_tsize(m), Pre); */
	/*@ assert normfs_Offset_dsame{Pre,Here}(m); */
	/*@ assert m->id != \null ==> \separated(m, m->id); */
	/*@ ghost _Aw: ; */
	((normfs_Offset *)m)->_size = s;
	/*@ assert normfs_Offset_own{_Aw,Here}(m); */
	/*@ assert m->id != \null ==> \at(m->id->_size, _Aw) == m->id->_size; */
	/*@ assert m->id != \null ==> \at(m->id->raw.len, _Aw) == m->id->raw.len; */
	/*@ assert m->id != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{Pre,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_csame{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert normfs_Offset_dsame{_Aw,Here}(m); */
	/*@ assert normfs_Offset_dsame{Pre,Here}(m); */
	/*@ assert normfs_Offset_tsize(m) == \at(normfs_Offset_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_Offset_valid(m) && normfs_Offset_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_Offset_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_Offset_encode_at(const normfs_Offset *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_Offset_tsize_nonneg((normfs_Offset *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)); */
	/*@ assert \at(normfs_Offset_type_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_Offset_id_size(m), Pre) + \at(normfs_Offset_type_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_Offset_id_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id) && normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Offset_id_size(m) == 1 + varint_size(m->id->_size) + m->id->_size; */
	/*@ assert m->id != \null ==> normfs_Id_bufsep((normfs_Id *)m->id, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->id != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 10u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_Id_cached_size(m->id));
		/*@ assert _off == \at(_off, _A0) + 1 + varint_size(m->id->_size); */
		/*@ assert _off + m->id->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_Id_bufsep((normfs_Id *)m->id, _buf, _off, _off + m->id->_size - 1); */
		_off = normfs_Id_encode_at(m->id, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_Offset_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->type != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->type);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_Offset_type_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_Offset_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_Offset_valid(m) && normfs_Offset_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_Offset_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_Offset_encode(const normfs_Offset *m, struct gremlin_writer *w)
{
	w->offset = normfs_Offset_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_Offset_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	id : 1;
		unsigned	type : 1;
	} _has;
	struct gremlin_bytes	id;
	normfs_OffsetType	type;
} normfs_Offset_reader;

/*@ predicate normfs_Offset_reader_ok{L}(normfs_Offset_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.id ==> gremlin_span_in(r->id, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_Offset_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_Offset_reader_init(normfs_Offset_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_Offset_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_Offset_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 10u /* field 1, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->id = d.bytes;
			r->_has.id = 1;
			continue;
		}
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->type = (normfs_OffsetType)(int32_t)(uint32_t)d.value;
			r->_has.type = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_Offset_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_Id_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_Offset_reader_get_id(const normfs_Offset_reader *r, normfs_Id_reader *out)
{
	if (r == NULL || !r->_has.id) {
		return normfs_Id_reader_init(out, NULL, 0);
	}
	return normfs_Id_reader_init(out, r->id.data, r->id.len);
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.type ==> \result == r->type;
*/
static inline normfs_OffsetType
normfs_Offset_reader_get_type(const normfs_Offset_reader *r)
{
	if (r == NULL || !r->_has.type) return 0;
	return r->type;
}

typedef struct normfs_ReadRequest {
	uint64_t	read_id;
	struct gremlin_bytes	queue_id;
	uint64_t	step;
	normfs_Encoding	*accept_encodings;
	size_t	accept_encodings_count;
	const normfs_Offset *	offset;
	uint64_t	limit;
	size_t	_size;
} normfs_ReadRequest;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_ReadRequest_cached_size(const normfs_ReadRequest *m)
{
	return m->_size;
}

/*@ predicate normfs_Id_avoid_normfs_ReadRequest{L}(normfs_Id *m, normfs_ReadRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Offset_avoid_normfs_ReadRequest{L}(normfs_Offset *m, normfs_ReadRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_ReadRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadRequest_avoid_normfs_SetupRequest{L}(normfs_ReadRequest *m, normfs_SetupRequest *p) =
      \separated(p, m) &&
      (m->accept_encodings_count == 0 || \separated(p, m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_SetupRequest((normfs_Offset *)m->offset, p));
*/

/*@ predicate normfs_ReadRequest_avoid_normfs_PingRequest{L}(normfs_ReadRequest *m, normfs_PingRequest *p) =
      \separated(p, m) &&
      (m->accept_encodings_count == 0 || \separated(p, m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_PingRequest((normfs_Offset *)m->offset, p));
*/

/*@ predicate normfs_ReadRequest_avoid_normfs_WriteRequest{L}(normfs_ReadRequest *m, normfs_WriteRequest *p) =
      \separated(p, m) &&
      (m->accept_encodings_count == 0 || \separated(p, m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_WriteRequest((normfs_Offset *)m->offset, p));
*/

/*@ predicate normfs_ReadRequest_avoid_bytes{L}(normfs_ReadRequest *m, struct gremlin_bytes *p, integer n) =
      n <= 0 || (\separated(p + (0 .. n - 1), m) &&
      (m->accept_encodings_count == 0 || \separated(p + (0 .. n - 1), m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_avoid_bytes((normfs_Offset *)m->offset, p, n)));
*/

/*@ predicate normfs_SetupRequest_navoid_normfs_ReadRequest{L}(normfs_SetupRequest *m, normfs_ReadRequest *t) =
      normfs_ReadRequest_avoid_normfs_SetupRequest(t, m);
*/

/*@ predicate normfs_PingRequest_navoid_normfs_ReadRequest{L}(normfs_PingRequest *m, normfs_ReadRequest *t) =
      normfs_ReadRequest_avoid_normfs_PingRequest(t, m);
*/

/*@ predicate normfs_WriteRequest_navoid_normfs_ReadRequest{L}(normfs_WriteRequest *m, normfs_ReadRequest *t) =
      normfs_ReadRequest_avoid_normfs_WriteRequest(t, m) &&
      normfs_ReadRequest_avoid_bytes(t, m->packets, m->packets_count);
*/

/*@ logic integer normfs_ReadRequest_accept_encodings_sum{L}(normfs_ReadRequest *m, integer n) =
      n <= 0 ? 0 : normfs_ReadRequest_accept_encodings_sum(m, n - 1) + varint_size(m->accept_encodings[n - 1] < 0 ? m->accept_encodings[n - 1] + 18446744073709551616 : m->accept_encodings[n - 1]);

    lemma normfs_ReadRequest_accept_encodings_sum_frame{L1,L2}: \forall normfs_ReadRequest *m, integer n; 0 <= n ==>
      \at(m->accept_encodings, L1) == \at(m->accept_encodings, L2) ==>
      (\forall integer k; 0 <= k < n ==> \at(m->accept_encodings[k], L1) == \at(m->accept_encodings[k], L2)) ==>
      \at(normfs_ReadRequest_accept_encodings_sum(m, n), L1) == \at(normfs_ReadRequest_accept_encodings_sum(m, n), L2);
*/
/*@ \wp::strategy normfs_ReadRequest_accept_encodings_sum_ind: \tactic("Wp.induction", \pattern(0 <= n), \select(n)); */
/*@ \wp::proof normfs_ReadRequest_accept_encodings_sum_ind: normfs_ReadRequest_accept_encodings_sum_frame; */

/*@
    logic integer normfs_ReadRequest_read_id_size{L}(normfs_ReadRequest *m) =
      m->read_id != 0 ? 1 + varint_size(m->read_id) : 0;

    logic integer normfs_ReadRequest_queue_id_size{L}(normfs_ReadRequest *m) =
      m->queue_id.len > 0 ? 1 + varint_size(m->queue_id.len) + m->queue_id.len : 0;

    logic integer normfs_ReadRequest_step_size{L}(normfs_ReadRequest *m) =
      m->step != 0 ? 1 + varint_size(m->step) : 0;

    logic integer normfs_ReadRequest_accept_encodings_size{L}(normfs_ReadRequest *m) =
      m->accept_encodings_count == 1 ? 1 + varint_size(m->accept_encodings[0] < 0 ? m->accept_encodings[0] + 18446744073709551616 : m->accept_encodings[0]) :
      m->accept_encodings_count > 1 ? 1 + varint_size(normfs_ReadRequest_accept_encodings_sum(m, m->accept_encodings_count)) + normfs_ReadRequest_accept_encodings_sum(m, m->accept_encodings_count) : 0;

    logic integer normfs_ReadRequest_offset_size{L}(normfs_ReadRequest *m) =
      m->offset != \null ?
        1 + varint_size(normfs_Offset_tsize((normfs_Offset *)m->offset)) + normfs_Offset_tsize((normfs_Offset *)m->offset) : 0;

    logic integer normfs_ReadRequest_limit_size{L}(normfs_ReadRequest *m) =
      m->limit != 0 ? 1 + varint_size(m->limit) : 0;

    logic integer normfs_ReadRequest_tsize{L}(normfs_ReadRequest *m) =
      normfs_ReadRequest_read_id_size(m) +
      normfs_ReadRequest_queue_id_size(m) +
      normfs_ReadRequest_step_size(m) +
      normfs_ReadRequest_accept_encodings_size(m) +
      normfs_ReadRequest_offset_size(m) +
      normfs_ReadRequest_limit_size(m);

    predicate normfs_ReadRequest_own{L1,L2}(normfs_ReadRequest *m) =
      \at(m->read_id, L1) == \at(m->read_id, L2) &&
      \at(m->queue_id.len, L1) == \at(m->queue_id.len, L2) &&
      \at(m->queue_id.data, L1) == \at(m->queue_id.data, L2) &&
      \at(m->step, L1) == \at(m->step, L2) &&
      \at(m->accept_encodings, L1) == \at(m->accept_encodings, L2) && \at(m->accept_encodings_count, L1) == \at(m->accept_encodings_count, L2) &&
      (\forall integer k; 0 <= k < \at(m->accept_encodings_count, L1) ==>
         \at(m->accept_encodings[k], L1) == \at(m->accept_encodings[k], L2)) &&
      \at(m->offset, L1) == \at(m->offset, L2) &&
      \at(m->limit, L1) == \at(m->limit, L2);

    predicate normfs_ReadRequest_nown{L1,L2}(normfs_ReadRequest *m) =
      \at(m->read_id, L1) == \at(m->read_id, L2) &&
      \at(m->queue_id.len, L1) == \at(m->queue_id.len, L2) &&
      \at(m->queue_id.data, L1) == \at(m->queue_id.data, L2) &&
      \at(m->step, L1) == \at(m->step, L2) &&
      \at(m->accept_encodings, L1) == \at(m->accept_encodings, L2) && \at(m->accept_encodings_count, L1) == \at(m->accept_encodings_count, L2) &&
      \at(m->offset, L1) == \at(m->offset, L2) &&
      \at(m->limit, L1) == \at(m->limit, L2);

    predicate normfs_ReadRequest_dsame{L1,L2}(normfs_ReadRequest *m) =
      normfs_ReadRequest_own{L1,L2}(m) &&
      (\at(m->offset, L1) != \null ==> normfs_Offset_dsame{L1,L2}((normfs_Offset *)\at(m->offset, L1)));

    predicate normfs_ReadRequest_csame{L1,L2}(normfs_ReadRequest *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->offset, L1) != \null ==> normfs_Offset_csame{L1,L2}((normfs_Offset *)\at(m->offset, L1)));

    predicate normfs_ReadRequest_sized{L}(normfs_ReadRequest *m) =
      m->_size == normfs_ReadRequest_tsize(m) &&
      (m->offset != \null ==> normfs_Offset_sized((normfs_Offset *)m->offset));

    predicate normfs_ReadRequest_valid{L}(normfs_ReadRequest *m) =
      \valid(m) &&
      (m->queue_id.len == 0 || \valid_read(m->queue_id.data + (0 .. m->queue_id.len - 1))) &&
      (m->accept_encodings_count == 0 || \valid_read(m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->accept_encodings_count == 0 || \separated(m, m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->offset)) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_ReadRequest((normfs_Offset *)m->offset, m)) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_Encoding((normfs_Offset *)m->offset, m->accept_encodings, m->accept_encodings_count));

    predicate normfs_ReadRequest_bufsep{L}(normfs_ReadRequest *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->accept_encodings_count == 0 || \separated(b + (lo .. hi), m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_bufsep((normfs_Offset *)m->offset, b, lo, hi)));

    lemma normfs_ReadRequest_read_id_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_read_id_size(m), L1) == \at(normfs_ReadRequest_read_id_size(m), L2);

    lemma normfs_ReadRequest_queue_id_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_queue_id_size(m), L1) == \at(normfs_ReadRequest_queue_id_size(m), L2);

    lemma normfs_ReadRequest_step_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_step_size(m), L1) == \at(normfs_ReadRequest_step_size(m), L2);

    lemma normfs_ReadRequest_accept_encodings_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_accept_encodings_size(m), L1) == \at(normfs_ReadRequest_accept_encodings_size(m), L2);

    lemma normfs_ReadRequest_offset_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_offset_size(m), L1) == \at(normfs_ReadRequest_offset_size(m), L2);

    lemma normfs_ReadRequest_limit_size_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_limit_size(m), L1) == \at(normfs_ReadRequest_limit_size(m), L2);

    lemma normfs_ReadRequest_tsize_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> \at(normfs_ReadRequest_tsize(m), L1) == \at(normfs_ReadRequest_tsize(m), L2);

    lemma normfs_ReadRequest_sized_frame{L1,L2}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> normfs_ReadRequest_csame{L1,L2}(m) ==> normfs_ReadRequest_sized{L1}(m) ==> normfs_ReadRequest_sized{L2}(m);

    lemma normfs_ReadRequest_dsame_trans{L1,L2,L3}: \forall normfs_ReadRequest *m;
      normfs_ReadRequest_dsame{L1,L2}(m) ==> normfs_ReadRequest_dsame{L2,L3}(m) ==> normfs_ReadRequest_dsame{L1,L3}(m);

    lemma normfs_ReadRequest_bufsep_sub{L}: \forall normfs_ReadRequest *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_ReadRequest_bufsep(m, b, lo, hi) ==> normfs_ReadRequest_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_ReadRequest_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->accept_encodings_count ==>
               0 <= normfs_ReadRequest_accept_encodings_sum(m, j) <= normfs_ReadRequest_accept_encodings_sum(m, m->accept_encodings_count);
  @/
  void normfs_ReadRequest_accept_encodings_mono(normfs_ReadRequest *m)
  {
    size_t i = 0;
    /@ loop invariant 0 <= i <= m->accept_encodings_count;
       loop invariant \forall integer j; 0 <= j <= i ==> 0 <= normfs_ReadRequest_accept_encodings_sum(m, j) <= normfs_ReadRequest_accept_encodings_sum(m, i);
       loop assigns i;
       loop variant m->accept_encodings_count - i;
    @/
    while (i < m->accept_encodings_count) {
      i++;
    }
  }
*/

/*@ ghost
  /@ requires normfs_ReadRequest_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->accept_encodings_count ==>
               0 <= normfs_ReadRequest_accept_encodings_sum(m, j) <= normfs_ReadRequest_accept_encodings_sum(m, m->accept_encodings_count);
     ensures m->offset != \null ==> normfs_Offset_tsize((normfs_Offset *)m->offset) >= 0;
     ensures normfs_ReadRequest_tsize(m) >= 0;
  @/
  void normfs_ReadRequest_tsize_nonneg(normfs_ReadRequest *m)
  {
    normfs_ReadRequest_accept_encodings_mono(m);
    if (m->offset) normfs_Offset_tsize_nonneg((normfs_Offset *)m->offset);
  }
*/

/*@ requires normfs_ReadRequest_valid(m);
    requires normfs_ReadRequest_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->offset->_size, *(m->offset != \null ? &m->offset->id->_size : &m->_size);
    ensures \result == \old(normfs_ReadRequest_tsize(m));
    ensures normfs_ReadRequest_sized(m);
    ensures normfs_ReadRequest_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_ReadRequest_size(const normfs_ReadRequest *m)
{
	size_t s = 0;
	/*@ ghost normfs_ReadRequest_tsize_nonneg((normfs_ReadRequest *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->offset)); */
	if (m->read_id != 0) {
		s += 1
		   + gremlin_varint_size(m->read_id);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_ReadRequest_read_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->queue_id.len > 0) {
		s += 1
		   + gremlin_varint_size(m->queue_id.len)
		   + m->queue_id.len;
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_ReadRequest_queue_id_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->step != 0) {
		s += 1
		   + gremlin_varint_size(m->step);
	}
	/*@ assert s == \at(s, _A2) + \at(normfs_ReadRequest_step_size(m), Pre); */
	/*@ ghost _A3: ; */
	if (m->accept_encodings_count == 1) {
		s += 1 + gremlin_varint_size((uint64_t)(int64_t)m->accept_encodings[0]);
	} else if (m->accept_encodings_count > 1) {
		size_t _payload = 0;
		/*@ loop invariant 0 <= _i <= m->accept_encodings_count;
		    loop invariant \forall integer k; k == _i ==> _payload == \at(normfs_ReadRequest_accept_encodings_sum(m, k), Pre);
		    loop assigns _i, _payload;
		    loop variant m->accept_encodings_count - _i;
		*/
		for (size_t _i = 0; _i < m->accept_encodings_count; _i++) {
			_payload += gremlin_varint_size((uint64_t)(int64_t)m->accept_encodings[_i]);
		}
		s += 1 + gremlin_varint_size(_payload) + _payload;
	}
	/*@ assert s == \at(s, _A3) + \at(normfs_ReadRequest_accept_encodings_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert \at(normfs_ReadRequest_limit_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ReadRequest_offset_size(m), Pre) + \at(normfs_ReadRequest_limit_size(m), Pre) == \at(normfs_ReadRequest_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ReadRequest_offset_size(m), Pre) <= \at(normfs_ReadRequest_tsize(m), Pre); */
	/*@ assert m->offset != \null ==> normfs_Offset_tsize((normfs_Offset *)m->offset) == \at(normfs_Offset_tsize((normfs_Offset *)m->offset), Pre); */
	/*@ assert m->offset != \null ==> \at(normfs_ReadRequest_offset_size(m), Pre) == 1 + varint_size(normfs_Offset_tsize((normfs_Offset *)m->offset)) + normfs_Offset_tsize((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->_size; */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->read_id; */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->queue_id.len; */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->step; */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->accept_encodings_count; */
	/*@ assert m->offset != \null ==> &m->offset->_size != &m->limit; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->_size; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->read_id; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->queue_id.len; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->step; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->accept_encodings_count; */
	/*@ assert m->offset != \null ==> &m->offset->id->_size != &m->limit; */
	if (m->offset != NULL) {
		size_t _child_size = normfs_Offset_size(m->offset);
		/*@ assert _child_size == \at(normfs_Offset_tsize((normfs_Offset *)m->offset), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A4) == m->_size; */
	/*@ assert \at(m->read_id, _A4) == m->read_id; */
	/*@ assert \at(m->queue_id.len, _A4) == m->queue_id.len; */
	/*@ assert \at(m->step, _A4) == m->step; */
	/*@ assert \at(m->accept_encodings_count, _A4) == m->accept_encodings_count; */
	/*@ assert \at(m->limit, _A4) == m->limit; */
	/*@ assert normfs_ReadRequest_nown{_A4,Here}((normfs_ReadRequest *)m); */
	/*@ assert \forall integer q; 0 <= q < \at(m->accept_encodings_count, _A4) ==>
	      \at(m->accept_encodings[q], _A4) == m->accept_encodings[q]; */
	/*@ assert normfs_ReadRequest_own{_A4,Here}((normfs_ReadRequest *)m); */
	/*@ assert normfs_ReadRequest_own{Pre,Here}(m); */
	/*@ assert m->offset != \null ==> normfs_Offset_dsame{Pre,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_Offset_sized((normfs_Offset *)m->offset); */
	/*@ assert s == \at(s, _A4) + \at(normfs_ReadRequest_offset_size(m), Pre); */
	/*@ ghost _A5: ; */
	if (m->limit != 0) {
		s += 1
		   + gremlin_varint_size(m->limit);
	}
	/*@ assert s == \at(s, _A5) + \at(normfs_ReadRequest_limit_size(m), Pre); */
	/*@ ghost _A6: ; */
	/*@ assert s == \at(normfs_ReadRequest_tsize(m), Pre); */
	/*@ assert normfs_ReadRequest_dsame{Pre,Here}(m); */
	/*@ assert m->offset != \null ==> \separated(m, m->offset); */
	/*@ assert m->offset != \null && m->offset->id != \null ==> \separated(m, m->offset->id); */
	/*@ ghost _Aw: ; */
	((normfs_ReadRequest *)m)->_size = s;
	/*@ assert normfs_ReadRequest_own{_Aw,Here}(m); */
	/*@ assert m->offset != \null ==> \at(m->offset->_size, _Aw) == m->offset->_size; */
	/*@ assert m->offset != \null ==> normfs_Offset_nown{_Aw,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_Offset_own{_Aw,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null && m->offset->id != \null ==> \at(m->offset->id->_size, _Aw) == m->offset->id->_size; */
	/*@ assert m->offset != \null && m->offset->id != \null ==> \at(m->offset->id->raw.len, _Aw) == m->offset->id->raw.len; */
	/*@ assert m->offset != \null && m->offset->id != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->offset->id); */
	/*@ assert m->offset != \null && m->offset->id != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->offset->id); */
	/*@ assert m->offset != \null ==> normfs_Offset_dsame{_Aw,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_Offset_dsame{Pre,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_Offset_csame{_Aw,Here}((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_Offset_sized((normfs_Offset *)m->offset); */
	/*@ assert normfs_ReadRequest_dsame{_Aw,Here}(m); */
	/*@ assert normfs_ReadRequest_dsame{Pre,Here}(m); */
	/*@ assert normfs_ReadRequest_tsize(m) == \at(normfs_ReadRequest_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_ReadRequest_valid(m) && normfs_ReadRequest_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_ReadRequest_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_ReadRequest_encode_at(const normfs_ReadRequest *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_ReadRequest_tsize_nonneg((normfs_ReadRequest *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->offset)); */
	if (m->read_id != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->read_id);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_ReadRequest_read_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->queue_id.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 18u);
		_off = gremlin_varint_encode_at(_buf, _off, m->queue_id.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->queue_id.data, m->queue_id.len);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_ReadRequest_queue_id_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->step != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 24u);
		_off = gremlin_varint_encode_at(_buf, _off, m->step);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_ReadRequest_step_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert \at(normfs_ReadRequest_offset_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ReadRequest_limit_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ReadRequest_accept_encodings_size(m), Pre) + \at(normfs_ReadRequest_offset_size(m), Pre) + \at(normfs_ReadRequest_limit_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ReadRequest_accept_encodings_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	if (m->accept_encodings_count == 1) {
		_off = gremlin_varint32_encode_at(_buf, _off, 32u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->accept_encodings[0]);
	} else if (m->accept_encodings_count > 1) {
		size_t _payload = 0;
		/*@ loop invariant 0 <= _i <= m->accept_encodings_count;
		    loop invariant \forall integer k; k == _i ==> _payload == \at(normfs_ReadRequest_accept_encodings_sum(m, k), Pre);
		    loop assigns _i, _payload;
		    loop variant m->accept_encodings_count - _i;
		*/
		for (size_t _i = 0; _i < m->accept_encodings_count; _i++) {
			_payload += gremlin_varint_size((uint64_t)(int64_t)m->accept_encodings[_i]);
		}
		_off = gremlin_varint32_encode_at(_buf, _off, 34u);
		_off = gremlin_varint_encode_at(_buf, _off, _payload);
		/*@ ghost _P3: ; */
		/*@ loop invariant 0 <= _i <= m->accept_encodings_count;
		    loop invariant \forall integer k; k == _i ==> _off == \at(_off, _P3) + \at(normfs_ReadRequest_accept_encodings_sum(m, k), Pre);
		    loop assigns _i, _off, _buf[\at(_off, Pre) .. \at(_off, Pre) + \at(m->_size, Pre) - 1];
		    loop variant m->accept_encodings_count - _i;
		*/
		for (size_t _i = 0; _i < m->accept_encodings_count; _i++) {
			/*@ assert \forall integer k; k == _i ==>
			      \at(normfs_ReadRequest_accept_encodings_sum(m, k + 1), Pre) == \at(normfs_ReadRequest_accept_encodings_sum(m, k), Pre) + \at(varint_size(m->accept_encodings[k] < 0 ? m->accept_encodings[k] + 18446744073709551616 : m->accept_encodings[k]), Pre) &&
			      \at(normfs_ReadRequest_accept_encodings_sum(m, k + 1), Pre) <= \at(normfs_ReadRequest_accept_encodings_sum(m, m->accept_encodings_count), Pre); */
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->accept_encodings[_i]);
		}
	}
	/*@ assert _off == \at(_off, _A3) + \at(normfs_ReadRequest_accept_encodings_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert \at(normfs_ReadRequest_limit_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ReadRequest_offset_size(m), Pre) + \at(normfs_ReadRequest_limit_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ReadRequest_offset_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->offset) && normfs_Offset_sized((normfs_Offset *)m->offset); */
	/*@ assert m->offset != \null ==> normfs_ReadRequest_offset_size(m) == 1 + varint_size(m->offset->_size) + m->offset->_size; */
	/*@ assert m->offset != \null ==> normfs_Offset_bufsep((normfs_Offset *)m->offset, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->offset != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 82u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_Offset_cached_size(m->offset));
		/*@ assert _off == \at(_off, _A4) + 1 + varint_size(m->offset->_size); */
		/*@ assert _off + m->offset->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_Offset_bufsep((normfs_Offset *)m->offset, _buf, _off, _off + m->offset->_size - 1); */
		_off = normfs_Offset_encode_at(m->offset, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A4) + \at(normfs_ReadRequest_offset_size(m), Pre); */
	/*@ ghost _A5: ; */
	if (m->limit != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 88u);
		_off = gremlin_varint_encode_at(_buf, _off, m->limit);
	}
	/*@ assert _off == \at(_off, _A5) + \at(normfs_ReadRequest_limit_size(m), Pre); */
	/*@ ghost _A6: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_ReadRequest_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_ReadRequest_valid(m) && normfs_ReadRequest_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_ReadRequest_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_ReadRequest_encode(const normfs_ReadRequest *m, struct gremlin_writer *w)
{
	w->offset = normfs_ReadRequest_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_ReadRequest_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	read_id : 1;
		unsigned	queue_id : 1;
		unsigned	step : 1;
		unsigned	accept_encodings : 1;
		unsigned	offset : 1;
		unsigned	limit : 1;
	} _has;
	uint64_t	read_id;
	struct gremlin_bytes	queue_id;
	uint64_t	step;
	size_t	accept_encodings_count;
	size_t	accept_encodings_first_offset;
	bool	accept_encodings_packed;
	struct gremlin_bytes	offset;
	uint64_t	limit;
} normfs_ReadRequest_reader;

/*@ predicate normfs_ReadRequest_reader_ok{L}(normfs_ReadRequest_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.queue_id ==> gremlin_span_in(r->queue_id, (uint8_t *)r->src, n)) &&
      r->accept_encodings_count <= n &&
      (r->accept_encodings_count > 0 ==> r->accept_encodings_first_offset < n) &&
      (r->_has.offset ==> gremlin_span_in(r->offset, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_ReadRequest_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ReadRequest_reader_init(normfs_ReadRequest_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_ReadRequest_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_ReadRequest_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->read_id = d.value;
			r->_has.read_id = true;
			continue;
		}
		if (t.value == 18u /* field 2, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->queue_id = d.bytes;
			r->_has.queue_id = 1;
			continue;
		}
		if (t.value == 24u /* field 3, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->step = d.value;
			r->_has.step = true;
			continue;
		}
		if (t.value == 34u /* field 4, packed */) {
			if (r->accept_encodings_count > 0 && !r->accept_encodings_packed) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
			struct gremlin_bytes_decode_result _bd =
				gremlin_bytes_decode(src + offset, len - offset);
			if (_bd.error != GREMLIN_OK) return _bd.error;
			size_t _bcnt = 0;
			/*@ loop invariant 0 <= _bi <= _bd.bytes.len;
			    loop invariant _bcnt <= _bi;
			    loop assigns _bi, _bcnt;
			    loop variant _bd.bytes.len - _bi;
			*/
			for (size_t _bi = 0; _bi < _bd.bytes.len; _bi++) {
				if ((_bd.bytes.data[_bi] & 0x80u) == 0) _bcnt++;
			}
			if (r->accept_encodings_count == 0) {
				r->accept_encodings_first_offset = offset - t.consumed;
				r->accept_encodings_packed = true;
			}
			r->accept_encodings_count += _bcnt;
			r->_has.accept_encodings = 1;
			offset += _bd.consumed;
			continue;
		}
		if (t.value == 32u /* field 4, unpacked */) {
			if (r->accept_encodings_count > 0 && r->accept_encodings_packed) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
			struct gremlin_varint_decode_result _vd =
				gremlin_varint_decode(src + offset, len - offset);
			if (_vd.error != GREMLIN_OK) return _vd.error;
			if (r->accept_encodings_count == 0) {
				r->accept_encodings_first_offset = offset - t.consumed;
			}
			r->accept_encodings_count++;
			r->_has.accept_encodings = 1;
			offset += _vd.consumed;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->offset = d.bytes;
			r->_has.offset = 1;
			continue;
		}
		if (t.value == 88u /* field 11, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->limit = d.value;
			r->_has.limit = true;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.read_id ==> \result == r->read_id;
*/
static inline uint64_t
normfs_ReadRequest_reader_get_read_id(const normfs_ReadRequest_reader *r)
{
	if (r == NULL || !r->_has.read_id) return 0;
	return r->read_id;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.queue_id ==> \result == r->queue_id;
    ensures !(r != \null && r->_has.queue_id) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_ReadRequest_reader_get_queue_id(const normfs_ReadRequest_reader *r)
{
	if (r == NULL || !r->_has.queue_id) return (struct gremlin_bytes){ NULL, 0 };
	return r->queue_id;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.step ==> \result == r->step;
*/
static inline uint64_t
normfs_ReadRequest_reader_get_step(const normfs_ReadRequest_reader *r)
{
	if (r == NULL || !r->_has.step) return 0;
	return r->step;
}

typedef struct normfs_ReadRequest_reader_accept_encodings_iter {
	const uint8_t	*src;
	size_t		 src_len;
	size_t		 offset;
	size_t		 block_end;
	size_t		 count_remaining;
	bool		 is_packed;
} normfs_ReadRequest_reader_accept_encodings_iter;

/*@ predicate normfs_ReadRequest_reader_accept_encodings_iter_ok(normfs_ReadRequest_reader_accept_encodings_iter it) =
      it.offset <= it.src_len &&
      it.block_end <= it.src_len &&
      (it.src_len == 0 || \valid_read(it.src + (0 .. it.src_len - 1)));
*/

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null ==> \result == r->accept_encodings_count;
*/
static inline size_t
normfs_ReadRequest_reader_accept_encodings_count(const normfs_ReadRequest_reader *r)
{
	if (r == NULL) return 0;
	return r->accept_encodings_count;
}

/*@ requires r == \null || (\valid_read(r) && normfs_ReadRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))));
    assigns \nothing;
    ensures normfs_ReadRequest_reader_accept_encodings_iter_ok(\result);
    ensures r != \null && r->accept_encodings_count > 0 ==>
              \result.src == r->src && \result.src_len == r->src_len &&
              \result.count_remaining == r->accept_encodings_count;
    ensures r == \null || r->accept_encodings_count == 0 ==>
              \result.src_len == 0 && \result.count_remaining == 0;
*/
static inline normfs_ReadRequest_reader_accept_encodings_iter
normfs_ReadRequest_reader_accept_encodings_begin(const normfs_ReadRequest_reader *r)
{
	normfs_ReadRequest_reader_accept_encodings_iter it = {0};
	if (r == NULL || r->accept_encodings_count == 0) return it;
	it.src = r->src;
	it.src_len = r->src_len;
	it.offset = r->accept_encodings_first_offset;
	it.count_remaining = r->accept_encodings_count;
	it.is_packed = r->accept_encodings_packed;
	return it;
}

/*@ requires \valid(it) && \valid(out);
    requires normfs_ReadRequest_reader_accept_encodings_iter_ok(*it);
    requires \separated(it, out, it->src + (0 .. it->src_len - 1));
    assigns *it, *out;
    ensures normfs_ReadRequest_reader_accept_encodings_iter_ok(*it);
    ensures it->src == \old(it->src) && it->src_len == \old(it->src_len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ReadRequest_reader_accept_encodings_next(normfs_ReadRequest_reader_accept_encodings_iter *it, normfs_Encoding *out)
{
	*out = (normfs_Encoding)0;
	if (it->count_remaining == 0) return GREMLIN_OK;
	if (it->is_packed) {
		if (it->block_end <= it->offset) {
			/*@ loop invariant it->offset <= it->src_len;
			    loop invariant it->block_end <= it->src_len;
			    loop assigns it->offset, it->block_end;
			    loop variant it->src_len - it->offset;
			*/
			while (it->offset < it->src_len) {
				struct gremlin_varint32_decode_result _t =
					gremlin_varint32_decode(it->src + it->offset, it->src_len - it->offset);
				if (_t.error != GREMLIN_OK) return _t.error;
				it->offset += _t.consumed;
				if (_t.value == 34u) {
					struct gremlin_varint_decode_result _l =
						gremlin_varint_decode(it->src + it->offset, it->src_len - it->offset);
					if (_l.error != GREMLIN_OK) return _l.error;
					it->offset += _l.consumed;
					if (_l.value > it->src_len - it->offset) return GREMLIN_ERROR_TRUNCATED;
					it->block_end = it->offset + (size_t)_l.value;
					break;
				}
				unsigned _wt = (unsigned)(_t.value % 8u);
				if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
				struct gremlin_skip_result _sk =
					gremlin_skip_data(it->src + it->offset, it->src_len - it->offset,
					                  (enum gremlin_wire_type)_wt);
				if (_sk.error != GREMLIN_OK) return _sk.error;
				it->offset += _sk.consumed;
			}
			if (it->block_end <= it->offset) return GREMLIN_ERROR_TRUNCATED;
		}
		{
		struct gremlin_varint_decode_result _d =
			gremlin_varint_decode(it->src + it->offset, it->block_end - it->offset);
		if (_d.error != GREMLIN_OK) return _d.error;
		it->offset += _d.consumed;
		*out = (normfs_Encoding)(int32_t)(uint32_t)_d.value;
		}
		/*@ assert it->offset <= it->block_end; */
		it->count_remaining--;
		if (it->offset >= it->block_end) it->block_end = 0;
		return GREMLIN_OK;
	}
	/*@ loop invariant it->offset <= it->src_len;
	    loop assigns it->offset;
	    loop variant it->src_len - it->offset;
	*/
	while (it->offset < it->src_len) {
		struct gremlin_varint32_decode_result _t =
			gremlin_varint32_decode(it->src + it->offset, it->src_len - it->offset);
		if (_t.error != GREMLIN_OK) return _t.error;
		it->offset += _t.consumed;
		if (_t.value == 32u) {
			{
		struct gremlin_varint_decode_result _d =
			gremlin_varint_decode(it->src + it->offset, it->src_len - it->offset);
		if (_d.error != GREMLIN_OK) return _d.error;
		it->offset += _d.consumed;
		*out = (normfs_Encoding)(int32_t)(uint32_t)_d.value;
			}
			it->count_remaining--;
			return GREMLIN_OK;
		}
		unsigned _wt = (unsigned)(_t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		struct gremlin_skip_result _sk =
			gremlin_skip_data(it->src + it->offset, it->src_len - it->offset,
			                  (enum gremlin_wire_type)_wt);
		if (_sk.error != GREMLIN_OK) return _sk.error;
		it->offset += _sk.consumed;
	}
	return GREMLIN_ERROR_TRUNCATED;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ReadRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_Offset_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ReadRequest_reader_get_offset(const normfs_ReadRequest_reader *r, normfs_Offset_reader *out)
{
	if (r == NULL || !r->_has.offset) {
		return normfs_Offset_reader_init(out, NULL, 0);
	}
	return normfs_Offset_reader_init(out, r->offset.data, r->offset.len);
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.limit ==> \result == r->limit;
*/
static inline uint64_t
normfs_ReadRequest_reader_get_limit(const normfs_ReadRequest_reader *r)
{
	if (r == NULL || !r->_has.limit) return 0;
	return r->limit;
}

typedef struct normfs_ClientRequest {
	const normfs_SetupRequest *	setup;
	const normfs_PingRequest *	ping;
	const normfs_WriteRequest *	write;
	const normfs_ReadRequest *	read;
	size_t	_size;
} normfs_ClientRequest;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_ClientRequest_cached_size(const normfs_ClientRequest *m)
{
	return m->_size;
}

/*@ predicate normfs_SetupRequest_avoid_normfs_ClientRequest{L}(normfs_SetupRequest *m, normfs_ClientRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_PingRequest_avoid_normfs_ClientRequest{L}(normfs_PingRequest *m, normfs_ClientRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_WriteRequest_avoid_normfs_ClientRequest{L}(normfs_WriteRequest *m, normfs_ClientRequest *p) =
      \separated(p, m) &&
      (m->packets_count == 0 || \separated(p, m->packets + (0 .. m->packets_count - 1)));
*/

/*@ predicate normfs_Id_avoid_normfs_ClientRequest{L}(normfs_Id *m, normfs_ClientRequest *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Offset_avoid_normfs_ClientRequest{L}(normfs_Offset *m, normfs_ClientRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_ClientRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadRequest_avoid_normfs_ClientRequest{L}(normfs_ReadRequest *m, normfs_ClientRequest *p) =
      \separated(p, m) &&
      (m->accept_encodings_count == 0 || \separated(p, m->accept_encodings + (0 .. m->accept_encodings_count - 1))) &&
      (m->offset != \null ==> normfs_Offset_avoid_normfs_ClientRequest((normfs_Offset *)m->offset, p));
*/

/*@
    logic integer normfs_ClientRequest_setup_size{L}(normfs_ClientRequest *m) =
      m->setup != \null ?
        1 + varint_size(normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup)) + normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup) : 0;

    logic integer normfs_ClientRequest_ping_size{L}(normfs_ClientRequest *m) =
      m->ping != \null ?
        1 + varint_size(normfs_PingRequest_tsize((normfs_PingRequest *)m->ping)) + normfs_PingRequest_tsize((normfs_PingRequest *)m->ping) : 0;

    logic integer normfs_ClientRequest_write_size{L}(normfs_ClientRequest *m) =
      m->write != \null ?
        1 + varint_size(normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write)) + normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write) : 0;

    logic integer normfs_ClientRequest_read_size{L}(normfs_ClientRequest *m) =
      m->read != \null ?
        1 + varint_size(normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read)) + normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read) : 0;

    logic integer normfs_ClientRequest_tsize{L}(normfs_ClientRequest *m) =
      normfs_ClientRequest_setup_size(m) +
      normfs_ClientRequest_ping_size(m) +
      normfs_ClientRequest_write_size(m) +
      normfs_ClientRequest_read_size(m);

    predicate normfs_ClientRequest_own{L1,L2}(normfs_ClientRequest *m) =
      \at(m->setup, L1) == \at(m->setup, L2) &&
      \at(m->ping, L1) == \at(m->ping, L2) &&
      \at(m->write, L1) == \at(m->write, L2) &&
      \at(m->read, L1) == \at(m->read, L2);

    predicate normfs_ClientRequest_nown{L1,L2}(normfs_ClientRequest *m) =
      \at(m->setup, L1) == \at(m->setup, L2) &&
      \at(m->ping, L1) == \at(m->ping, L2) &&
      \at(m->write, L1) == \at(m->write, L2) &&
      \at(m->read, L1) == \at(m->read, L2);

    predicate normfs_ClientRequest_dsame{L1,L2}(normfs_ClientRequest *m) =
      normfs_ClientRequest_own{L1,L2}(m) &&
      (\at(m->setup, L1) != \null ==> normfs_SetupRequest_dsame{L1,L2}((normfs_SetupRequest *)\at(m->setup, L1))) &&
      (\at(m->ping, L1) != \null ==> normfs_PingRequest_dsame{L1,L2}((normfs_PingRequest *)\at(m->ping, L1))) &&
      (\at(m->write, L1) != \null ==> normfs_WriteRequest_dsame{L1,L2}((normfs_WriteRequest *)\at(m->write, L1))) &&
      (\at(m->read, L1) != \null ==> normfs_ReadRequest_dsame{L1,L2}((normfs_ReadRequest *)\at(m->read, L1)));

    predicate normfs_ClientRequest_csame{L1,L2}(normfs_ClientRequest *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->setup, L1) != \null ==> normfs_SetupRequest_csame{L1,L2}((normfs_SetupRequest *)\at(m->setup, L1))) &&
      (\at(m->ping, L1) != \null ==> normfs_PingRequest_csame{L1,L2}((normfs_PingRequest *)\at(m->ping, L1))) &&
      (\at(m->write, L1) != \null ==> normfs_WriteRequest_csame{L1,L2}((normfs_WriteRequest *)\at(m->write, L1))) &&
      (\at(m->read, L1) != \null ==> normfs_ReadRequest_csame{L1,L2}((normfs_ReadRequest *)\at(m->read, L1)));

    predicate normfs_ClientRequest_sized{L}(normfs_ClientRequest *m) =
      m->_size == normfs_ClientRequest_tsize(m) &&
      (m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup)) &&
      (m->ping != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->ping)) &&
      (m->write != \null ==> normfs_WriteRequest_sized((normfs_WriteRequest *)m->write)) &&
      (m->read != \null ==> normfs_ReadRequest_sized((normfs_ReadRequest *)m->read));

    predicate normfs_ClientRequest_valid{L}(normfs_ClientRequest *m) =
      \valid(m) &&
      (m->setup != \null ==> normfs_SetupRequest_valid((normfs_SetupRequest *)m->setup)) &&
      (m->setup != \null ==> normfs_SetupRequest_avoid_normfs_ClientRequest((normfs_SetupRequest *)m->setup, m)) &&
      (m->ping != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping)) &&
      (m->ping != \null ==> normfs_PingRequest_avoid_normfs_ClientRequest((normfs_PingRequest *)m->ping, m)) &&
      (m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write)) &&
      (m->write != \null ==> normfs_WriteRequest_avoid_normfs_ClientRequest((normfs_WriteRequest *)m->write, m)) &&
      (m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read)) &&
      (m->read != \null ==> normfs_ReadRequest_avoid_normfs_ClientRequest((normfs_ReadRequest *)m->read, m)) &&
      (m->setup != \null && m->ping != \null ==>
         normfs_SetupRequest_navoid_normfs_PingRequest((normfs_SetupRequest *)m->setup, (normfs_PingRequest *)m->ping)) &&
      (m->setup != \null && m->write != \null ==>
         normfs_SetupRequest_navoid_normfs_WriteRequest((normfs_SetupRequest *)m->setup, (normfs_WriteRequest *)m->write)) &&
      (m->setup != \null && m->read != \null ==>
         normfs_SetupRequest_navoid_normfs_ReadRequest((normfs_SetupRequest *)m->setup, (normfs_ReadRequest *)m->read)) &&
      (m->ping != \null && m->write != \null ==>
         normfs_PingRequest_navoid_normfs_WriteRequest((normfs_PingRequest *)m->ping, (normfs_WriteRequest *)m->write)) &&
      (m->ping != \null && m->read != \null ==>
         normfs_PingRequest_navoid_normfs_ReadRequest((normfs_PingRequest *)m->ping, (normfs_ReadRequest *)m->read)) &&
      (m->write != \null && m->read != \null ==>
         normfs_WriteRequest_navoid_normfs_ReadRequest((normfs_WriteRequest *)m->write, (normfs_ReadRequest *)m->read));

    predicate normfs_ClientRequest_bufsep{L}(normfs_ClientRequest *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->setup != \null ==> normfs_SetupRequest_bufsep((normfs_SetupRequest *)m->setup, b, lo, hi)) &&
      (m->ping != \null ==> normfs_PingRequest_bufsep((normfs_PingRequest *)m->ping, b, lo, hi)) &&
      (m->write != \null ==> normfs_WriteRequest_bufsep((normfs_WriteRequest *)m->write, b, lo, hi)) &&
      (m->read != \null ==> normfs_ReadRequest_bufsep((normfs_ReadRequest *)m->read, b, lo, hi)));

    lemma normfs_ClientRequest_setup_size_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> \at(normfs_ClientRequest_setup_size(m), L1) == \at(normfs_ClientRequest_setup_size(m), L2);

    lemma normfs_ClientRequest_ping_size_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> \at(normfs_ClientRequest_ping_size(m), L1) == \at(normfs_ClientRequest_ping_size(m), L2);

    lemma normfs_ClientRequest_write_size_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> \at(normfs_ClientRequest_write_size(m), L1) == \at(normfs_ClientRequest_write_size(m), L2);

    lemma normfs_ClientRequest_read_size_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> \at(normfs_ClientRequest_read_size(m), L1) == \at(normfs_ClientRequest_read_size(m), L2);

    lemma normfs_ClientRequest_tsize_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> \at(normfs_ClientRequest_tsize(m), L1) == \at(normfs_ClientRequest_tsize(m), L2);

    lemma normfs_ClientRequest_sized_frame{L1,L2}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> normfs_ClientRequest_csame{L1,L2}(m) ==> normfs_ClientRequest_sized{L1}(m) ==> normfs_ClientRequest_sized{L2}(m);

    lemma normfs_ClientRequest_dsame_trans{L1,L2,L3}: \forall normfs_ClientRequest *m;
      normfs_ClientRequest_dsame{L1,L2}(m) ==> normfs_ClientRequest_dsame{L2,L3}(m) ==> normfs_ClientRequest_dsame{L1,L3}(m);

    lemma normfs_ClientRequest_bufsep_sub{L}: \forall normfs_ClientRequest *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_ClientRequest_bufsep(m, b, lo, hi) ==> normfs_ClientRequest_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_ClientRequest_valid(m);
     assigns \nothing;
     ensures m->setup != \null ==> normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup) >= 0;
     ensures m->ping != \null ==> normfs_PingRequest_tsize((normfs_PingRequest *)m->ping) >= 0;
     ensures m->write != \null ==> normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write) >= 0;
     ensures m->read != \null ==> normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read) >= 0;
     ensures normfs_ClientRequest_tsize(m) >= 0;
  @/
  void normfs_ClientRequest_tsize_nonneg(normfs_ClientRequest *m)
  {
    if (m->setup) normfs_SetupRequest_tsize_nonneg((normfs_SetupRequest *)m->setup);
    if (m->ping) normfs_PingRequest_tsize_nonneg((normfs_PingRequest *)m->ping);
    if (m->write) normfs_WriteRequest_tsize_nonneg((normfs_WriteRequest *)m->write);
    if (m->read) normfs_ReadRequest_tsize_nonneg((normfs_ReadRequest *)m->read);
  }
*/

/*@ requires normfs_ClientRequest_valid(m);
    requires normfs_ClientRequest_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->setup->_size, m->ping->_size, m->write->_size, m->read->_size, *(m->read != \null ? &m->read->offset->_size : &m->_size), *(m->read != \null && m->read->offset != \null ? &m->read->offset->id->_size : &m->_size);
    ensures \result == \old(normfs_ClientRequest_tsize(m));
    ensures normfs_ClientRequest_sized(m);
    ensures normfs_ClientRequest_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_ClientRequest_size(const normfs_ClientRequest *m)
{
	size_t s = 0;
	/*@ ghost normfs_ClientRequest_tsize_nonneg((normfs_ClientRequest *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->setup != \null ==> normfs_SetupRequest_valid((normfs_SetupRequest *)m->setup)); */
	/*@ assert (m->ping != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping)); */
	/*@ assert (m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write)); */
	/*@ assert (m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read)); */
	/*@ assert \at(normfs_ClientRequest_ping_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ClientRequest_setup_size(m), Pre) + \at(normfs_ClientRequest_ping_size(m), Pre) + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ClientRequest_setup_size(m), Pre) <= \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup) == \at(normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup), Pre); */
	/*@ assert m->setup != \null ==> \at(normfs_ClientRequest_setup_size(m), Pre) == 1 + varint_size(normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup)) + normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null && m->ping != \null ==> \separated(m->setup, m->ping); */
	/*@ assert m->setup != \null && m->write != \null ==> \separated(m->setup, m->write); */
	/*@ assert m->setup != \null && m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m->setup); */
	/*@ assert m->setup != \null && m->read != \null ==> \separated(m->setup, m->read); */
	/*@ assert m->setup != \null && m->read != \null ==> m->read->accept_encodings_count <= 0 || \separated(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1), m->setup); */
	/*@ assert m->setup != \null && m->read != \null && m->read->offset != \null ==> \separated(m->setup, m->read->offset); */
	/*@ assert m->setup != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \separated(m->setup, m->read->offset->id); */
	/*@ assert m->setup != \null ==> &m->setup->_size != &m->_size; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->_size; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->sequence; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->client_timestamp_ns; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->_size; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->write_id; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->queue_id.len; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->packets_count; */
	/*@ assert \forall integer q; m->setup != \null && m->write != \null && 0 <= q < m->write->packets_count ==> &m->setup->_size != &m->write->packets[q].len; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->_size; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->read_id; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->queue_id.len; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->step; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->accept_encodings_count; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->limit; */
	/*@ assert m->setup != \null && m->read != \null && m->read->offset != \null ==> &m->setup->_size != &m->read->offset->_size; */
	/*@ assert m->setup != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->setup->_size != &m->read->offset->id->_size; */
	/*@ assert m->setup != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->setup->_size != &m->read->offset->id->raw.len; */
	if (m->setup != NULL) {
		size_t _child_size = normfs_SetupRequest_size(m->setup);
		/*@ assert _child_size == \at(normfs_SetupRequest_tsize((normfs_SetupRequest *)m->setup), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A0) == m->_size; */
	/*@ assert normfs_ClientRequest_nown{_A0,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{_A0,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{Pre,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A0) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->sequence, _A0) == m->ping->sequence; */
	/*@ assert m->ping != \null ==> \at(m->ping->client_timestamp_ns, _A0) == m->ping->client_timestamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingRequest_nown{_A0,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_own{_A0,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{_A0,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> \valid(m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A0) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A0) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->queue_id.len, _A0) == m->write->queue_id.len; */
	/*@ assert m->write != \null ==> \at(m->write->packets_count, _A0) == m->write->packets_count; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_nown{_A0,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->packets_count, _A0) ==>
	      \at(m->write->packets[q].len, _A0) == m->write->packets[q].len && \at(m->write->packets[q].data, _A0) == m->write->packets[q].data; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_own{_A0,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{_A0,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{Pre,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \valid(m->write); */
	/*@ assert m->write != \null ==> (m->write->queue_id.len == 0 || \valid_read(m->write->queue_id.data + (0 .. m->write->queue_id.len - 1))); */
	/*@ assert m->write != \null ==> (m->write->packets_count == 0 || \valid_read(m->write->packets + (0 .. m->write->packets_count - 1))); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->packets_count ==>
         m->write->packets[k].len == 0 || \valid_read(m->write->packets[k].data + (0 .. m->write->packets[k].len - 1))); */
	/*@ assert m->write != \null ==> (m->write->packets_count == 0 || \separated(m->write, m->write->packets + (0 .. m->write->packets_count - 1))); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A0) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A0) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->queue_id.len, _A0) == m->read->queue_id.len; */
	/*@ assert m->read != \null ==> \at(m->read->step, _A0) == m->read->step; */
	/*@ assert m->read != \null ==> \at(m->read->accept_encodings_count, _A0) == m->read->accept_encodings_count; */
	/*@ assert m->read != \null ==> \at(m->read->limit, _A0) == m->read->limit; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_nown{_A0,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \forall integer q; 0 <= q < \at(m->read->accept_encodings_count, _A0) ==>
	      \at(m->read->accept_encodings[q], _A0) == m->read->accept_encodings[q]; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_own{_A0,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null && m->read->offset != \null ==> \at(m->read->offset->_size, _A0) == m->read->offset->_size; */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_nown{_A0,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_own{_A0,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->_size, _A0) == m->read->offset->id->_size; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->raw.len, _A0) == m->read->offset->id->raw.len; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_nown{_A0,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_own{_A0,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{_A0,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{Pre,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->queue_id.len == 0 || \valid_read(m->read->queue_id.data + (0 .. m->read->queue_id.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \valid_read(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \separated(m->read, m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->read->offset)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_ReadRequest((normfs_Offset *)m->read->offset, m->read)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_Encoding((normfs_Offset *)m->read->offset, m->read->accept_encodings, m->read->accept_encodings_count)); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read); */
	/*@ assert s == \at(s, _A0) + \at(normfs_ClientRequest_setup_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert \at(normfs_ClientRequest_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ClientRequest_ping_size(m), Pre) + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ClientRequest_ping_size(m), Pre) <= \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_tsize((normfs_PingRequest *)m->ping) == \at(normfs_PingRequest_tsize((normfs_PingRequest *)m->ping), Pre); */
	/*@ assert m->ping != \null ==> \at(normfs_ClientRequest_ping_size(m), Pre) == 1 + varint_size(normfs_PingRequest_tsize((normfs_PingRequest *)m->ping)) + normfs_PingRequest_tsize((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null && m->setup != \null ==> \separated(m->ping, m->setup); */
	/*@ assert m->ping != \null && m->write != \null ==> \separated(m->ping, m->write); */
	/*@ assert m->ping != \null && m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m->ping); */
	/*@ assert m->ping != \null && m->read != \null ==> \separated(m->ping, m->read); */
	/*@ assert m->ping != \null && m->read != \null ==> m->read->accept_encodings_count <= 0 || \separated(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1), m->ping); */
	/*@ assert m->ping != \null && m->read != \null && m->read->offset != \null ==> \separated(m->ping, m->read->offset); */
	/*@ assert m->ping != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \separated(m->ping, m->read->offset->id); */
	/*@ assert m->ping != \null ==> &m->ping->_size != &m->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->version; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->_size; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->write_id; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->queue_id.len; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->packets_count; */
	/*@ assert \forall integer q; m->ping != \null && m->write != \null && 0 <= q < m->write->packets_count ==> &m->ping->_size != &m->write->packets[q].len; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->_size; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->read_id; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->queue_id.len; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->step; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->accept_encodings_count; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->limit; */
	/*@ assert m->ping != \null && m->read != \null && m->read->offset != \null ==> &m->ping->_size != &m->read->offset->_size; */
	/*@ assert m->ping != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->ping->_size != &m->read->offset->id->_size; */
	/*@ assert m->ping != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->ping->_size != &m->read->offset->id->raw.len; */
	if (m->ping != NULL) {
		size_t _child_size = normfs_PingRequest_size(m->ping);
		/*@ assert _child_size == \at(normfs_PingRequest_tsize((normfs_PingRequest *)m->ping), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A1) == m->_size; */
	/*@ assert normfs_ClientRequest_nown{_A1,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{_A1,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A1) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A1) == m->setup->version; */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_nown{_A1,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_own{_A1,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{_A1,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{Pre,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_csame{_A1,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A1) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A1) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->queue_id.len, _A1) == m->write->queue_id.len; */
	/*@ assert m->write != \null ==> \at(m->write->packets_count, _A1) == m->write->packets_count; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_nown{_A1,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->packets_count, _A1) ==>
	      \at(m->write->packets[q].len, _A1) == m->write->packets[q].len && \at(m->write->packets[q].data, _A1) == m->write->packets[q].data; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_own{_A1,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{_A1,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{Pre,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \valid(m->write); */
	/*@ assert m->write != \null ==> (m->write->queue_id.len == 0 || \valid_read(m->write->queue_id.data + (0 .. m->write->queue_id.len - 1))); */
	/*@ assert m->write != \null ==> (m->write->packets_count == 0 || \valid_read(m->write->packets + (0 .. m->write->packets_count - 1))); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->packets_count ==>
         m->write->packets[k].len == 0 || \valid_read(m->write->packets[k].data + (0 .. m->write->packets[k].len - 1))); */
	/*@ assert m->write != \null ==> (m->write->packets_count == 0 || \separated(m->write, m->write->packets + (0 .. m->write->packets_count - 1))); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A1) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A1) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->queue_id.len, _A1) == m->read->queue_id.len; */
	/*@ assert m->read != \null ==> \at(m->read->step, _A1) == m->read->step; */
	/*@ assert m->read != \null ==> \at(m->read->accept_encodings_count, _A1) == m->read->accept_encodings_count; */
	/*@ assert m->read != \null ==> \at(m->read->limit, _A1) == m->read->limit; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_nown{_A1,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \forall integer q; 0 <= q < \at(m->read->accept_encodings_count, _A1) ==>
	      \at(m->read->accept_encodings[q], _A1) == m->read->accept_encodings[q]; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_own{_A1,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null && m->read->offset != \null ==> \at(m->read->offset->_size, _A1) == m->read->offset->_size; */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_nown{_A1,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_own{_A1,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->_size, _A1) == m->read->offset->id->_size; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->raw.len, _A1) == m->read->offset->id->raw.len; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_nown{_A1,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_own{_A1,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{_A1,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{Pre,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->queue_id.len == 0 || \valid_read(m->read->queue_id.data + (0 .. m->read->queue_id.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \valid_read(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \separated(m->read, m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->read->offset)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_ReadRequest((normfs_Offset *)m->read->offset, m->read)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_Encoding((normfs_Offset *)m->read->offset, m->read->accept_encodings, m->read->accept_encodings_count)); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read); */
	/*@ assert s == \at(s, _A1) + \at(normfs_ClientRequest_ping_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ClientRequest_write_size(m), Pre) <= \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write) == \at(normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write), Pre); */
	/*@ assert m->write != \null ==> \at(normfs_ClientRequest_write_size(m), Pre) == 1 + varint_size(normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write)) + normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null && m->setup != \null ==> \separated(m->write, m->setup); */
	/*@ assert m->write != \null && m->ping != \null ==> \separated(m->write, m->ping); */
	/*@ assert m->write != \null && m->read != \null ==> \separated(m->write, m->read); */
	/*@ assert m->write != \null && m->read != \null ==> m->read->accept_encodings_count <= 0 || \separated(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1), m->write); */
	/*@ assert m->write != \null && m->read != \null && m->read->offset != \null ==> \separated(m->write, m->read->offset); */
	/*@ assert m->write != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \separated(m->write, m->read->offset->id); */
	/*@ assert m->write != \null ==> &m->write->_size != &m->_size; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->_size; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->version; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->_size; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->sequence; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->client_timestamp_ns; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->_size; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->read_id; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->queue_id.len; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->step; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->accept_encodings_count; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->limit; */
	/*@ assert m->write != \null && m->read != \null && m->read->offset != \null ==> &m->write->_size != &m->read->offset->_size; */
	/*@ assert m->write != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->write->_size != &m->read->offset->id->_size; */
	/*@ assert m->write != \null && m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> &m->write->_size != &m->read->offset->id->raw.len; */
	if (m->write != NULL) {
		size_t _child_size = normfs_WriteRequest_size(m->write);
		/*@ assert _child_size == \at(normfs_WriteRequest_tsize((normfs_WriteRequest *)m->write), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A2) == m->_size; */
	/*@ assert normfs_ClientRequest_nown{_A2,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{_A2,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A2) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A2) == m->setup->version; */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_nown{_A2,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_own{_A2,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{_A2,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{Pre,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_csame{_A2,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A2) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->sequence, _A2) == m->ping->sequence; */
	/*@ assert m->ping != \null ==> \at(m->ping->client_timestamp_ns, _A2) == m->ping->client_timestamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingRequest_nown{_A2,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_own{_A2,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{_A2,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_csame{_A2,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->ping); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{Pre,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_sized((normfs_WriteRequest *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A2) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A2) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->queue_id.len, _A2) == m->read->queue_id.len; */
	/*@ assert m->read != \null ==> \at(m->read->step, _A2) == m->read->step; */
	/*@ assert m->read != \null ==> \at(m->read->accept_encodings_count, _A2) == m->read->accept_encodings_count; */
	/*@ assert m->read != \null ==> \at(m->read->limit, _A2) == m->read->limit; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_nown{_A2,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \forall integer q; 0 <= q < \at(m->read->accept_encodings_count, _A2) ==>
	      \at(m->read->accept_encodings[q], _A2) == m->read->accept_encodings[q]; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_own{_A2,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null && m->read->offset != \null ==> \at(m->read->offset->_size, _A2) == m->read->offset->_size; */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_nown{_A2,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_own{_A2,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->_size, _A2) == m->read->offset->id->_size; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->raw.len, _A2) == m->read->offset->id->raw.len; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_nown{_A2,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_own{_A2,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{_A2,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{Pre,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->queue_id.len == 0 || \valid_read(m->read->queue_id.data + (0 .. m->read->queue_id.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \valid_read(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->accept_encodings_count == 0 || \separated(m->read, m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1))); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_valid((normfs_Offset *)m->read->offset)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_ReadRequest((normfs_Offset *)m->read->offset, m->read)); */
	/*@ assert m->read != \null ==> (m->read->offset != \null ==> normfs_Offset_avoid_normfs_Encoding((normfs_Offset *)m->read->offset, m->read->accept_encodings, m->read->accept_encodings_count)); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read); */
	/*@ assert s == \at(s, _A2) + \at(normfs_ClientRequest_write_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s + \at(normfs_ClientRequest_read_size(m), Pre) == \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ClientRequest_read_size(m), Pre) <= \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read) == \at(normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read), Pre); */
	/*@ assert m->read != \null ==> \at(normfs_ClientRequest_read_size(m), Pre) == 1 + varint_size(normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read)) + normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null && m->setup != \null ==> \separated(m->read, m->setup); */
	/*@ assert m->read != \null && m->read->offset != \null && m->setup != \null ==> \separated(m->read->offset, m->setup); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null && m->setup != \null ==> \separated(m->read->offset->id, m->setup); */
	/*@ assert m->read != \null && m->ping != \null ==> \separated(m->read, m->ping); */
	/*@ assert m->read != \null && m->read->offset != \null && m->ping != \null ==> \separated(m->read->offset, m->ping); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null && m->ping != \null ==> \separated(m->read->offset->id, m->ping); */
	/*@ assert m->read != \null && m->write != \null ==> \separated(m->read, m->write); */
	/*@ assert m->read != \null && m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m->read); */
	/*@ assert m->read != \null && m->read->offset != \null && m->write != \null ==> \separated(m->read->offset, m->write); */
	/*@ assert m->read != \null && m->read->offset != \null && m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null && m->write != \null ==> \separated(m->read->offset->id, m->write); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null && m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m->read->offset->id); */
	/*@ assert m->read != \null ==> &m->read->_size != &m->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->version; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->_size; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->sequence; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->client_timestamp_ns; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->_size; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->write_id; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->queue_id.len; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->packets_count; */
	/*@ assert \forall integer q; m->read != \null && m->write != \null && 0 <= q < m->write->packets_count ==> &m->read->_size != &m->write->packets[q].len; */
	/*@ assert m->read != \null ==> &m->read->offset->_size != &m->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->offset->_size != &m->setup->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->offset->_size != &m->setup->version; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->offset->_size != &m->ping->_size; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->offset->_size != &m->ping->sequence; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->offset->_size != &m->ping->client_timestamp_ns; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->offset->_size != &m->write->_size; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->offset->_size != &m->write->write_id; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->offset->_size != &m->write->queue_id.len; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->offset->_size != &m->write->packets_count; */
	/*@ assert \forall integer q; m->read != \null && m->write != \null && 0 <= q < m->write->packets_count ==> &m->read->offset->_size != &m->write->packets[q].len; */
	/*@ assert m->read != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->setup->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->setup->version; */
	/*@ assert m->read != \null && m->ping != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->ping->_size; */
	/*@ assert m->read != \null && m->ping != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->ping->sequence; */
	/*@ assert m->read != \null && m->ping != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->ping->client_timestamp_ns; */
	/*@ assert m->read != \null && m->write != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->write->_size; */
	/*@ assert m->read != \null && m->write != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->write->write_id; */
	/*@ assert m->read != \null && m->write != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->write->queue_id.len; */
	/*@ assert m->read != \null && m->write != \null ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->write->packets_count; */
	/*@ assert \forall integer q; m->read != \null && m->write != \null && 0 <= q < m->write->packets_count ==> (m->read->offset != \null ? &m->read->offset->id->_size : &m->read->_size) != &m->write->packets[q].len; */
	if (m->read != NULL) {
		size_t _child_size = normfs_ReadRequest_size(m->read);
		/*@ assert _child_size == \at(normfs_ReadRequest_tsize((normfs_ReadRequest *)m->read), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A3) == m->_size; */
	/*@ assert normfs_ClientRequest_nown{_A3,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{_A3,Here}((normfs_ClientRequest *)m); */
	/*@ assert normfs_ClientRequest_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A3) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A3) == m->setup->version; */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_nown{_A3,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_own{_A3,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{_A3,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{Pre,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_csame{_A3,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A3) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->sequence, _A3) == m->ping->sequence; */
	/*@ assert m->ping != \null ==> \at(m->ping->client_timestamp_ns, _A3) == m->ping->client_timestamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingRequest_nown{_A3,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_own{_A3,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{_A3,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_csame{_A3,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A3) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A3) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->queue_id.len, _A3) == m->write->queue_id.len; */
	/*@ assert m->write != \null ==> \at(m->write->packets_count, _A3) == m->write->packets_count; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_nown{_A3,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->packets_count, _A3) ==>
	      \at(m->write->packets[q].len, _A3) == m->write->packets[q].len && \at(m->write->packets[q].data, _A3) == m->write->packets[q].data; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_own{_A3,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{_A3,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{Pre,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_csame{_A3,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_sized((normfs_WriteRequest *)m->write); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{Pre,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_sized((normfs_ReadRequest *)m->read); */
	/*@ assert s == \at(s, _A3) + \at(normfs_ClientRequest_read_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert s == \at(normfs_ClientRequest_tsize(m), Pre); */
	/*@ assert normfs_ClientRequest_dsame{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \separated(m, m->setup); */
	/*@ assert m->ping != \null ==> \separated(m, m->ping); */
	/*@ assert m->write != \null ==> \separated(m, m->write); */
	/*@ assert m->write != \null ==> m->write->packets_count <= 0 || \separated(m->write->packets + (0 .. m->write->packets_count - 1), m); */
	/*@ assert m->read != \null ==> \separated(m, m->read); */
	/*@ assert m->read != \null ==> m->read->accept_encodings_count <= 0 || \separated(m->read->accept_encodings + (0 .. m->read->accept_encodings_count - 1), m); */
	/*@ assert m->read != \null && m->read->offset != \null ==> \separated(m, m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \separated(m, m->read->offset->id); */
	/*@ ghost _Aw: ; */
	((normfs_ClientRequest *)m)->_size = s;
	/*@ assert normfs_ClientRequest_own{_Aw,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _Aw) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _Aw) == m->setup->version; */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_nown{_Aw,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_own{_Aw,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{_Aw,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_dsame{Pre,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_csame{_Aw,Here}((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _Aw) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->sequence, _Aw) == m->ping->sequence; */
	/*@ assert m->ping != \null ==> \at(m->ping->client_timestamp_ns, _Aw) == m->ping->client_timestamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingRequest_nown{_Aw,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_own{_Aw,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{_Aw,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_csame{_Aw,Here}((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _Aw) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _Aw) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->queue_id.len, _Aw) == m->write->queue_id.len; */
	/*@ assert m->write != \null ==> \at(m->write->packets_count, _Aw) == m->write->packets_count; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_nown{_Aw,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->packets_count, _Aw) ==>
	      \at(m->write->packets[q].len, _Aw) == m->write->packets[q].len && \at(m->write->packets[q].data, _Aw) == m->write->packets[q].data; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_own{_Aw,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{_Aw,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_dsame{Pre,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_csame{_Aw,Here}((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_sized((normfs_WriteRequest *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _Aw) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _Aw) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->queue_id.len, _Aw) == m->read->queue_id.len; */
	/*@ assert m->read != \null ==> \at(m->read->step, _Aw) == m->read->step; */
	/*@ assert m->read != \null ==> \at(m->read->accept_encodings_count, _Aw) == m->read->accept_encodings_count; */
	/*@ assert m->read != \null ==> \at(m->read->limit, _Aw) == m->read->limit; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_nown{_Aw,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> \forall integer q; 0 <= q < \at(m->read->accept_encodings_count, _Aw) ==>
	      \at(m->read->accept_encodings[q], _Aw) == m->read->accept_encodings[q]; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_own{_Aw,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null && m->read->offset != \null ==> \at(m->read->offset->_size, _Aw) == m->read->offset->_size; */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_nown{_Aw,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null ==> normfs_Offset_own{_Aw,Here}((normfs_Offset *)m->read->offset); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->_size, _Aw) == m->read->offset->id->_size; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> \at(m->read->offset->id->raw.len, _Aw) == m->read->offset->id->raw.len; */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null && m->read->offset != \null && m->read->offset->id != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->read->offset->id); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{_Aw,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_dsame{Pre,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_csame{_Aw,Here}((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_sized((normfs_ReadRequest *)m->read); */
	/*@ assert normfs_ClientRequest_dsame{_Aw,Here}(m); */
	/*@ assert normfs_ClientRequest_dsame{Pre,Here}(m); */
	/*@ assert normfs_ClientRequest_tsize(m) == \at(normfs_ClientRequest_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_ClientRequest_valid(m) && normfs_ClientRequest_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_ClientRequest_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_ClientRequest_encode_at(const normfs_ClientRequest *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_ClientRequest_tsize_nonneg((normfs_ClientRequest *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->setup != \null ==> normfs_SetupRequest_valid((normfs_SetupRequest *)m->setup)); */
	/*@ assert (m->ping != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping)); */
	/*@ assert (m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write)); */
	/*@ assert (m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read)); */
	/*@ assert \at(normfs_ClientRequest_ping_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ClientRequest_setup_size(m), Pre) + \at(normfs_ClientRequest_ping_size(m), Pre) + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ClientRequest_setup_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_valid((normfs_SetupRequest *)m->setup) && normfs_SetupRequest_sized((normfs_SetupRequest *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_ClientRequest_setup_size(m) == 1 + varint_size(m->setup->_size) + m->setup->_size; */
	/*@ assert m->setup != \null ==> normfs_SetupRequest_bufsep((normfs_SetupRequest *)m->setup, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->setup != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 10u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_SetupRequest_cached_size(m->setup));
		/*@ assert _off == \at(_off, _A0) + 1 + varint_size(m->setup->_size); */
		/*@ assert _off + m->setup->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_SetupRequest_bufsep((normfs_SetupRequest *)m->setup, _buf, _off, _off + m->setup->_size - 1); */
		_off = normfs_SetupRequest_encode_at(m->setup, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_ClientRequest_setup_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert \at(normfs_ClientRequest_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ClientRequest_ping_size(m), Pre) + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ClientRequest_ping_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->ping != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping) && normfs_PingRequest_sized((normfs_PingRequest *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_ClientRequest_ping_size(m) == 1 + varint_size(m->ping->_size) + m->ping->_size; */
	/*@ assert m->ping != \null ==> normfs_PingRequest_bufsep((normfs_PingRequest *)m->ping, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->ping != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 18u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_PingRequest_cached_size(m->ping));
		/*@ assert _off == \at(_off, _A1) + 1 + varint_size(m->ping->_size); */
		/*@ assert _off + m->ping->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_PingRequest_bufsep((normfs_PingRequest *)m->ping, _buf, _off, _off + m->ping->_size - 1); */
		_off = normfs_PingRequest_encode_at(m->ping, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_ClientRequest_ping_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ClientRequest_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ClientRequest_write_size(m), Pre) + \at(normfs_ClientRequest_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ClientRequest_write_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->write != \null ==> normfs_WriteRequest_valid((normfs_WriteRequest *)m->write) && normfs_WriteRequest_sized((normfs_WriteRequest *)m->write); */
	/*@ assert m->write != \null ==> normfs_ClientRequest_write_size(m) == 1 + varint_size(m->write->_size) + m->write->_size; */
	/*@ assert m->write != \null ==> normfs_WriteRequest_bufsep((normfs_WriteRequest *)m->write, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->write != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 82u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_WriteRequest_cached_size(m->write));
		/*@ assert _off == \at(_off, _A2) + 1 + varint_size(m->write->_size); */
		/*@ assert _off + m->write->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_WriteRequest_bufsep((normfs_WriteRequest *)m->write, _buf, _off, _off + m->write->_size - 1); */
		_off = normfs_WriteRequest_encode_at(m->write, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_ClientRequest_write_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off + \at(normfs_ClientRequest_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ClientRequest_read_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->read != \null ==> normfs_ReadRequest_valid((normfs_ReadRequest *)m->read) && normfs_ReadRequest_sized((normfs_ReadRequest *)m->read); */
	/*@ assert m->read != \null ==> normfs_ClientRequest_read_size(m) == 1 + varint_size(m->read->_size) + m->read->_size; */
	/*@ assert m->read != \null ==> normfs_ReadRequest_bufsep((normfs_ReadRequest *)m->read, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->read != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 90u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_ReadRequest_cached_size(m->read));
		/*@ assert _off == \at(_off, _A3) + 1 + varint_size(m->read->_size); */
		/*@ assert _off + m->read->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_ReadRequest_bufsep((normfs_ReadRequest *)m->read, _buf, _off, _off + m->read->_size - 1); */
		_off = normfs_ReadRequest_encode_at(m->read, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A3) + \at(normfs_ClientRequest_read_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_ClientRequest_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_ClientRequest_valid(m) && normfs_ClientRequest_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_ClientRequest_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_ClientRequest_encode(const normfs_ClientRequest *m, struct gremlin_writer *w)
{
	w->offset = normfs_ClientRequest_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_ClientRequest_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	setup : 1;
		unsigned	ping : 1;
		unsigned	write : 1;
		unsigned	read : 1;
	} _has;
	struct gremlin_bytes	setup;
	struct gremlin_bytes	ping;
	struct gremlin_bytes	write;
	struct gremlin_bytes	read;
} normfs_ClientRequest_reader;

/*@ predicate normfs_ClientRequest_reader_ok{L}(normfs_ClientRequest_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.setup ==> gremlin_span_in(r->setup, (uint8_t *)r->src, n)) &&
      (r->_has.ping ==> gremlin_span_in(r->ping, (uint8_t *)r->src, n)) &&
      (r->_has.write ==> gremlin_span_in(r->write, (uint8_t *)r->src, n)) &&
      (r->_has.read ==> gremlin_span_in(r->read, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_ClientRequest_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ClientRequest_reader_init(normfs_ClientRequest_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_ClientRequest_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_ClientRequest_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 10u /* field 1, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->setup = d.bytes;
			r->_has.setup = 1;
			continue;
		}
		if (t.value == 18u /* field 2, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->ping = d.bytes;
			r->_has.ping = 1;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->write = d.bytes;
			r->_has.write = 1;
			continue;
		}
		if (t.value == 90u /* field 11, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->read = d.bytes;
			r->_has.read = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ClientRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_SetupRequest_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ClientRequest_reader_get_setup(const normfs_ClientRequest_reader *r, normfs_SetupRequest_reader *out)
{
	if (r == NULL || !r->_has.setup) {
		return normfs_SetupRequest_reader_init(out, NULL, 0);
	}
	return normfs_SetupRequest_reader_init(out, r->setup.data, r->setup.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ClientRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_PingRequest_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ClientRequest_reader_get_ping(const normfs_ClientRequest_reader *r, normfs_PingRequest_reader *out)
{
	if (r == NULL || !r->_has.ping) {
		return normfs_PingRequest_reader_init(out, NULL, 0);
	}
	return normfs_PingRequest_reader_init(out, r->ping.data, r->ping.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ClientRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_WriteRequest_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ClientRequest_reader_get_write(const normfs_ClientRequest_reader *r, normfs_WriteRequest_reader *out)
{
	if (r == NULL || !r->_has.write) {
		return normfs_WriteRequest_reader_init(out, NULL, 0);
	}
	return normfs_WriteRequest_reader_init(out, r->write.data, r->write.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ClientRequest_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_ReadRequest_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ClientRequest_reader_get_read(const normfs_ClientRequest_reader *r, normfs_ReadRequest_reader *out)
{
	if (r == NULL || !r->_has.read) {
		return normfs_ReadRequest_reader_init(out, NULL, 0);
	}
	return normfs_ReadRequest_reader_init(out, r->read.data, r->read.len);
}

typedef struct normfs_SetupResponse {
	uint64_t	version;
	struct gremlin_bytes	instance_id_bytes;
	struct gremlin_bytes	instance_id;
	size_t	_size;
} normfs_SetupResponse;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_SetupResponse_cached_size(const normfs_SetupResponse *m)
{
	return m->_size;
}

/*@ predicate normfs_PingRequest_avoid_normfs_SetupResponse{L}(normfs_PingRequest *m, normfs_SetupResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_SetupResponse{L}(normfs_Id *m, normfs_SetupResponse *p) =
      \separated(p, m);
*/

/*@
    logic integer normfs_SetupResponse_version_size{L}(normfs_SetupResponse *m) =
      m->version != 0 ? 1 + varint_size(m->version) : 0;

    logic integer normfs_SetupResponse_instance_id_bytes_size{L}(normfs_SetupResponse *m) =
      m->instance_id_bytes.len > 0 ? 1 + varint_size(m->instance_id_bytes.len) + m->instance_id_bytes.len : 0;

    logic integer normfs_SetupResponse_instance_id_size{L}(normfs_SetupResponse *m) =
      m->instance_id.len > 0 ? 1 + varint_size(m->instance_id.len) + m->instance_id.len : 0;

    logic integer normfs_SetupResponse_tsize{L}(normfs_SetupResponse *m) =
      normfs_SetupResponse_version_size(m) +
      normfs_SetupResponse_instance_id_bytes_size(m) +
      normfs_SetupResponse_instance_id_size(m);

    predicate normfs_SetupResponse_own{L1,L2}(normfs_SetupResponse *m) =
      \at(m->version, L1) == \at(m->version, L2) &&
      \at(m->instance_id_bytes.len, L1) == \at(m->instance_id_bytes.len, L2) &&
      \at(m->instance_id_bytes.data, L1) == \at(m->instance_id_bytes.data, L2) &&
      \at(m->instance_id.len, L1) == \at(m->instance_id.len, L2) &&
      \at(m->instance_id.data, L1) == \at(m->instance_id.data, L2);

    predicate normfs_SetupResponse_nown{L1,L2}(normfs_SetupResponse *m) =
      \at(m->version, L1) == \at(m->version, L2) &&
      \at(m->instance_id_bytes.len, L1) == \at(m->instance_id_bytes.len, L2) &&
      \at(m->instance_id_bytes.data, L1) == \at(m->instance_id_bytes.data, L2) &&
      \at(m->instance_id.len, L1) == \at(m->instance_id.len, L2) &&
      \at(m->instance_id.data, L1) == \at(m->instance_id.data, L2);

    predicate normfs_SetupResponse_dsame{L1,L2}(normfs_SetupResponse *m) =
      normfs_SetupResponse_own{L1,L2}(m);

    predicate normfs_SetupResponse_csame{L1,L2}(normfs_SetupResponse *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_SetupResponse_sized{L}(normfs_SetupResponse *m) =
      m->_size == normfs_SetupResponse_tsize(m);

    predicate normfs_SetupResponse_valid{L}(normfs_SetupResponse *m) =
      \valid(m) &&
      (m->instance_id_bytes.len == 0 || \valid_read(m->instance_id_bytes.data + (0 .. m->instance_id_bytes.len - 1))) &&
      (m->instance_id.len == 0 || \valid_read(m->instance_id.data + (0 .. m->instance_id.len - 1)));

    predicate normfs_SetupResponse_bufsep{L}(normfs_SetupResponse *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m));

    lemma normfs_SetupResponse_version_size_frame{L1,L2}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> \at(normfs_SetupResponse_version_size(m), L1) == \at(normfs_SetupResponse_version_size(m), L2);

    lemma normfs_SetupResponse_instance_id_bytes_size_frame{L1,L2}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> \at(normfs_SetupResponse_instance_id_bytes_size(m), L1) == \at(normfs_SetupResponse_instance_id_bytes_size(m), L2);

    lemma normfs_SetupResponse_instance_id_size_frame{L1,L2}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> \at(normfs_SetupResponse_instance_id_size(m), L1) == \at(normfs_SetupResponse_instance_id_size(m), L2);

    lemma normfs_SetupResponse_tsize_frame{L1,L2}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> \at(normfs_SetupResponse_tsize(m), L1) == \at(normfs_SetupResponse_tsize(m), L2);

    lemma normfs_SetupResponse_sized_frame{L1,L2}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> normfs_SetupResponse_csame{L1,L2}(m) ==> normfs_SetupResponse_sized{L1}(m) ==> normfs_SetupResponse_sized{L2}(m);

    lemma normfs_SetupResponse_dsame_trans{L1,L2,L3}: \forall normfs_SetupResponse *m;
      normfs_SetupResponse_dsame{L1,L2}(m) ==> normfs_SetupResponse_dsame{L2,L3}(m) ==> normfs_SetupResponse_dsame{L1,L3}(m);

    lemma normfs_SetupResponse_bufsep_sub{L}: \forall normfs_SetupResponse *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_SetupResponse_bufsep(m, b, lo, hi) ==> normfs_SetupResponse_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_SetupResponse_valid(m);
     assigns \nothing;
     ensures normfs_SetupResponse_tsize(m) >= 0;
  @/
  void normfs_SetupResponse_tsize_nonneg(normfs_SetupResponse *m)
  {
  }
*/

/*@ requires normfs_SetupResponse_valid(m);
    requires normfs_SetupResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_SetupResponse_tsize(m));
    ensures normfs_SetupResponse_sized(m);
    ensures normfs_SetupResponse_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_SetupResponse_size(const normfs_SetupResponse *m)
{
	size_t s = 0;
	/*@ ghost normfs_SetupResponse_tsize_nonneg((normfs_SetupResponse *)m); */
	/*@ ghost _A0: ; */
	if (m->version != 0) {
		s += 1
		   + gremlin_varint_size(m->version);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_SetupResponse_version_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->instance_id_bytes.len > 0) {
		s += 1
		   + gremlin_varint_size(m->instance_id_bytes.len)
		   + m->instance_id_bytes.len;
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_SetupResponse_instance_id_bytes_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->instance_id.len > 0) {
		s += 1
		   + gremlin_varint_size(m->instance_id.len)
		   + m->instance_id.len;
	}
	/*@ assert s == \at(s, _A2) + \at(normfs_SetupResponse_instance_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s == \at(normfs_SetupResponse_tsize(m), Pre); */
	/*@ assert normfs_SetupResponse_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_SetupResponse *)m)->_size = s;
	/*@ assert normfs_SetupResponse_own{_Aw,Here}(m); */
	/*@ assert normfs_SetupResponse_dsame{_Aw,Here}(m); */
	/*@ assert normfs_SetupResponse_dsame{Pre,Here}(m); */
	/*@ assert normfs_SetupResponse_tsize(m) == \at(normfs_SetupResponse_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_SetupResponse_valid(m) && normfs_SetupResponse_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_SetupResponse_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_SetupResponse_encode_at(const normfs_SetupResponse *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_SetupResponse_tsize_nonneg((normfs_SetupResponse *)m); */
	/*@ ghost _A0: ; */
	if (m->version != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->version);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_SetupResponse_version_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->instance_id_bytes.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 18u);
		_off = gremlin_varint_encode_at(_buf, _off, m->instance_id_bytes.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->instance_id_bytes.data, m->instance_id_bytes.len);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_SetupResponse_instance_id_bytes_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->instance_id.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 26u);
		_off = gremlin_varint_encode_at(_buf, _off, m->instance_id.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->instance_id.data, m->instance_id.len);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_SetupResponse_instance_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_SetupResponse_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_SetupResponse_valid(m) && normfs_SetupResponse_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_SetupResponse_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_SetupResponse_encode(const normfs_SetupResponse *m, struct gremlin_writer *w)
{
	w->offset = normfs_SetupResponse_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_SetupResponse_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	version : 1;
		unsigned	instance_id_bytes : 1;
		unsigned	instance_id : 1;
	} _has;
	uint64_t	version;
	struct gremlin_bytes	instance_id_bytes;
	struct gremlin_bytes	instance_id;
} normfs_SetupResponse_reader;

/*@ predicate normfs_SetupResponse_reader_ok{L}(normfs_SetupResponse_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.instance_id_bytes ==> gremlin_span_in(r->instance_id_bytes, (uint8_t *)r->src, n)) &&
      (r->_has.instance_id ==> gremlin_span_in(r->instance_id, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_SetupResponse_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_SetupResponse_reader_init(normfs_SetupResponse_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_SetupResponse_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_SetupResponse_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->version = d.value;
			r->_has.version = true;
			continue;
		}
		if (t.value == 18u /* field 2, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->instance_id_bytes = d.bytes;
			r->_has.instance_id_bytes = 1;
			continue;
		}
		if (t.value == 26u /* field 3, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->instance_id = d.bytes;
			r->_has.instance_id = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.version ==> \result == r->version;
*/
static inline uint64_t
normfs_SetupResponse_reader_get_version(const normfs_SetupResponse_reader *r)
{
	if (r == NULL || !r->_has.version) return 0;
	return r->version;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.instance_id_bytes ==> \result == r->instance_id_bytes;
    ensures !(r != \null && r->_has.instance_id_bytes) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_SetupResponse_reader_get_instance_id_bytes(const normfs_SetupResponse_reader *r)
{
	if (r == NULL || !r->_has.instance_id_bytes) return (struct gremlin_bytes){ NULL, 0 };
	return r->instance_id_bytes;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.instance_id ==> \result == r->instance_id;
    ensures !(r != \null && r->_has.instance_id) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_SetupResponse_reader_get_instance_id(const normfs_SetupResponse_reader *r)
{
	if (r == NULL || !r->_has.instance_id) return (struct gremlin_bytes){ NULL, 0 };
	return r->instance_id;
}

typedef struct normfs_PingResponse {
	uint64_t	local_stamp_ns;
	uint64_t	monotonic_stamp_ns;
	const normfs_PingRequest *	request;
	size_t	_size;
} normfs_PingResponse;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_PingResponse_cached_size(const normfs_PingResponse *m)
{
	return m->_size;
}

/*@ predicate normfs_PingRequest_avoid_normfs_PingResponse{L}(normfs_PingRequest *m, normfs_PingResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_PingResponse{L}(normfs_Id *m, normfs_PingResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_PingResponse_avoid_normfs_SetupResponse{L}(normfs_PingResponse *m, normfs_SetupResponse *p) =
      \separated(p, m) &&
      (m->request != \null ==> normfs_PingRequest_avoid_normfs_SetupResponse((normfs_PingRequest *)m->request, p));
*/

/*@ predicate normfs_SetupResponse_navoid_normfs_PingResponse{L}(normfs_SetupResponse *m, normfs_PingResponse *t) =
      normfs_PingResponse_avoid_normfs_SetupResponse(t, m);
*/

/*@
    logic integer normfs_PingResponse_local_stamp_ns_size{L}(normfs_PingResponse *m) =
      m->local_stamp_ns != 0 ? 1 + varint_size(m->local_stamp_ns) : 0;

    logic integer normfs_PingResponse_monotonic_stamp_ns_size{L}(normfs_PingResponse *m) =
      m->monotonic_stamp_ns != 0 ? 1 + varint_size(m->monotonic_stamp_ns) : 0;

    logic integer normfs_PingResponse_request_size{L}(normfs_PingResponse *m) =
      m->request != \null ?
        1 + varint_size(normfs_PingRequest_tsize((normfs_PingRequest *)m->request)) + normfs_PingRequest_tsize((normfs_PingRequest *)m->request) : 0;

    logic integer normfs_PingResponse_tsize{L}(normfs_PingResponse *m) =
      normfs_PingResponse_local_stamp_ns_size(m) +
      normfs_PingResponse_monotonic_stamp_ns_size(m) +
      normfs_PingResponse_request_size(m);

    predicate normfs_PingResponse_own{L1,L2}(normfs_PingResponse *m) =
      \at(m->local_stamp_ns, L1) == \at(m->local_stamp_ns, L2) &&
      \at(m->monotonic_stamp_ns, L1) == \at(m->monotonic_stamp_ns, L2) &&
      \at(m->request, L1) == \at(m->request, L2);

    predicate normfs_PingResponse_nown{L1,L2}(normfs_PingResponse *m) =
      \at(m->local_stamp_ns, L1) == \at(m->local_stamp_ns, L2) &&
      \at(m->monotonic_stamp_ns, L1) == \at(m->monotonic_stamp_ns, L2) &&
      \at(m->request, L1) == \at(m->request, L2);

    predicate normfs_PingResponse_dsame{L1,L2}(normfs_PingResponse *m) =
      normfs_PingResponse_own{L1,L2}(m) &&
      (\at(m->request, L1) != \null ==> normfs_PingRequest_dsame{L1,L2}((normfs_PingRequest *)\at(m->request, L1)));

    predicate normfs_PingResponse_csame{L1,L2}(normfs_PingResponse *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->request, L1) != \null ==> normfs_PingRequest_csame{L1,L2}((normfs_PingRequest *)\at(m->request, L1)));

    predicate normfs_PingResponse_sized{L}(normfs_PingResponse *m) =
      m->_size == normfs_PingResponse_tsize(m) &&
      (m->request != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->request));

    predicate normfs_PingResponse_valid{L}(normfs_PingResponse *m) =
      \valid(m) &&
      (m->request != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->request)) &&
      (m->request != \null ==> normfs_PingRequest_avoid_normfs_PingResponse((normfs_PingRequest *)m->request, m));

    predicate normfs_PingResponse_bufsep{L}(normfs_PingResponse *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->request != \null ==> normfs_PingRequest_bufsep((normfs_PingRequest *)m->request, b, lo, hi)));

    lemma normfs_PingResponse_local_stamp_ns_size_frame{L1,L2}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> \at(normfs_PingResponse_local_stamp_ns_size(m), L1) == \at(normfs_PingResponse_local_stamp_ns_size(m), L2);

    lemma normfs_PingResponse_monotonic_stamp_ns_size_frame{L1,L2}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> \at(normfs_PingResponse_monotonic_stamp_ns_size(m), L1) == \at(normfs_PingResponse_monotonic_stamp_ns_size(m), L2);

    lemma normfs_PingResponse_request_size_frame{L1,L2}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> \at(normfs_PingResponse_request_size(m), L1) == \at(normfs_PingResponse_request_size(m), L2);

    lemma normfs_PingResponse_tsize_frame{L1,L2}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> \at(normfs_PingResponse_tsize(m), L1) == \at(normfs_PingResponse_tsize(m), L2);

    lemma normfs_PingResponse_sized_frame{L1,L2}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> normfs_PingResponse_csame{L1,L2}(m) ==> normfs_PingResponse_sized{L1}(m) ==> normfs_PingResponse_sized{L2}(m);

    lemma normfs_PingResponse_dsame_trans{L1,L2,L3}: \forall normfs_PingResponse *m;
      normfs_PingResponse_dsame{L1,L2}(m) ==> normfs_PingResponse_dsame{L2,L3}(m) ==> normfs_PingResponse_dsame{L1,L3}(m);

    lemma normfs_PingResponse_bufsep_sub{L}: \forall normfs_PingResponse *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_PingResponse_bufsep(m, b, lo, hi) ==> normfs_PingResponse_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_PingResponse_valid(m);
     assigns \nothing;
     ensures m->request != \null ==> normfs_PingRequest_tsize((normfs_PingRequest *)m->request) >= 0;
     ensures normfs_PingResponse_tsize(m) >= 0;
  @/
  void normfs_PingResponse_tsize_nonneg(normfs_PingResponse *m)
  {
    if (m->request) normfs_PingRequest_tsize_nonneg((normfs_PingRequest *)m->request);
  }
*/

/*@ requires normfs_PingResponse_valid(m);
    requires normfs_PingResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->request->_size;
    ensures \result == \old(normfs_PingResponse_tsize(m));
    ensures normfs_PingResponse_sized(m);
    ensures normfs_PingResponse_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_PingResponse_size(const normfs_PingResponse *m)
{
	size_t s = 0;
	/*@ ghost normfs_PingResponse_tsize_nonneg((normfs_PingResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->request != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->request)); */
	if (m->local_stamp_ns != 0) {
		s += 1
		   + gremlin_varint_size(m->local_stamp_ns);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_PingResponse_local_stamp_ns_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->monotonic_stamp_ns != 0) {
		s += 1
		   + gremlin_varint_size(m->monotonic_stamp_ns);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_PingResponse_monotonic_stamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert s + \at(normfs_PingResponse_request_size(m), Pre) == \at(normfs_PingResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_PingResponse_request_size(m), Pre) <= \at(normfs_PingResponse_tsize(m), Pre); */
	/*@ assert m->request != \null ==> normfs_PingRequest_tsize((normfs_PingRequest *)m->request) == \at(normfs_PingRequest_tsize((normfs_PingRequest *)m->request), Pre); */
	/*@ assert m->request != \null ==> \at(normfs_PingResponse_request_size(m), Pre) == 1 + varint_size(normfs_PingRequest_tsize((normfs_PingRequest *)m->request)) + normfs_PingRequest_tsize((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> &m->request->_size != &m->_size; */
	/*@ assert m->request != \null ==> &m->request->_size != &m->local_stamp_ns; */
	/*@ assert m->request != \null ==> &m->request->_size != &m->monotonic_stamp_ns; */
	if (m->request != NULL) {
		size_t _child_size = normfs_PingRequest_size(m->request);
		/*@ assert _child_size == \at(normfs_PingRequest_tsize((normfs_PingRequest *)m->request), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A2) == m->_size; */
	/*@ assert \at(m->local_stamp_ns, _A2) == m->local_stamp_ns; */
	/*@ assert \at(m->monotonic_stamp_ns, _A2) == m->monotonic_stamp_ns; */
	/*@ assert normfs_PingResponse_nown{_A2,Here}((normfs_PingResponse *)m); */
	/*@ assert normfs_PingResponse_own{_A2,Here}((normfs_PingResponse *)m); */
	/*@ assert normfs_PingResponse_own{Pre,Here}(m); */
	/*@ assert m->request != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->request); */
	/*@ assert s == \at(s, _A2) + \at(normfs_PingResponse_request_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s == \at(normfs_PingResponse_tsize(m), Pre); */
	/*@ assert normfs_PingResponse_dsame{Pre,Here}(m); */
	/*@ assert m->request != \null ==> \separated(m, m->request); */
	/*@ ghost _Aw: ; */
	((normfs_PingResponse *)m)->_size = s;
	/*@ assert normfs_PingResponse_own{_Aw,Here}(m); */
	/*@ assert m->request != \null ==> \at(m->request->_size, _Aw) == m->request->_size; */
	/*@ assert m->request != \null ==> \at(m->request->sequence, _Aw) == m->request->sequence; */
	/*@ assert m->request != \null ==> \at(m->request->client_timestamp_ns, _Aw) == m->request->client_timestamp_ns; */
	/*@ assert m->request != \null ==> normfs_PingRequest_nown{_Aw,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_own{_Aw,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_dsame{_Aw,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_dsame{Pre,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_csame{_Aw,Here}((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingRequest_sized((normfs_PingRequest *)m->request); */
	/*@ assert normfs_PingResponse_dsame{_Aw,Here}(m); */
	/*@ assert normfs_PingResponse_dsame{Pre,Here}(m); */
	/*@ assert normfs_PingResponse_tsize(m) == \at(normfs_PingResponse_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_PingResponse_valid(m) && normfs_PingResponse_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_PingResponse_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_PingResponse_encode_at(const normfs_PingResponse *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_PingResponse_tsize_nonneg((normfs_PingResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->request != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->request)); */
	if (m->local_stamp_ns != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, m->local_stamp_ns);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_PingResponse_local_stamp_ns_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->monotonic_stamp_ns != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 24u);
		_off = gremlin_varint_encode_at(_buf, _off, m->monotonic_stamp_ns);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_PingResponse_monotonic_stamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert _off + \at(normfs_PingResponse_request_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_PingResponse_request_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->request != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->request) && normfs_PingRequest_sized((normfs_PingRequest *)m->request); */
	/*@ assert m->request != \null ==> normfs_PingResponse_request_size(m) == 1 + varint_size(m->request->_size) + m->request->_size; */
	/*@ assert m->request != \null ==> normfs_PingRequest_bufsep((normfs_PingRequest *)m->request, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->request != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 34u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_PingRequest_cached_size(m->request));
		/*@ assert _off == \at(_off, _A2) + 1 + varint_size(m->request->_size); */
		/*@ assert _off + m->request->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_PingRequest_bufsep((normfs_PingRequest *)m->request, _buf, _off, _off + m->request->_size - 1); */
		_off = normfs_PingRequest_encode_at(m->request, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_PingResponse_request_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_PingResponse_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_PingResponse_valid(m) && normfs_PingResponse_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_PingResponse_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_PingResponse_encode(const normfs_PingResponse *m, struct gremlin_writer *w)
{
	w->offset = normfs_PingResponse_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_PingResponse_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	local_stamp_ns : 1;
		unsigned	monotonic_stamp_ns : 1;
		unsigned	request : 1;
	} _has;
	uint64_t	local_stamp_ns;
	uint64_t	monotonic_stamp_ns;
	struct gremlin_bytes	request;
} normfs_PingResponse_reader;

/*@ predicate normfs_PingResponse_reader_ok{L}(normfs_PingResponse_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.request ==> gremlin_span_in(r->request, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_PingResponse_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_PingResponse_reader_init(normfs_PingResponse_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_PingResponse_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_PingResponse_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->local_stamp_ns = d.value;
			r->_has.local_stamp_ns = true;
			continue;
		}
		if (t.value == 24u /* field 3, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->monotonic_stamp_ns = d.value;
			r->_has.monotonic_stamp_ns = true;
			continue;
		}
		if (t.value == 34u /* field 4, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->request = d.bytes;
			r->_has.request = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.local_stamp_ns ==> \result == r->local_stamp_ns;
*/
static inline uint64_t
normfs_PingResponse_reader_get_local_stamp_ns(const normfs_PingResponse_reader *r)
{
	if (r == NULL || !r->_has.local_stamp_ns) return 0;
	return r->local_stamp_ns;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.monotonic_stamp_ns ==> \result == r->monotonic_stamp_ns;
*/
static inline uint64_t
normfs_PingResponse_reader_get_monotonic_stamp_ns(const normfs_PingResponse_reader *r)
{
	if (r == NULL || !r->_has.monotonic_stamp_ns) return 0;
	return r->monotonic_stamp_ns;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_PingResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_PingRequest_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_PingResponse_reader_get_request(const normfs_PingResponse_reader *r, normfs_PingRequest_reader *out)
{
	if (r == NULL || !r->_has.request) {
		return normfs_PingRequest_reader_init(out, NULL, 0);
	}
	return normfs_PingRequest_reader_init(out, r->request.data, r->request.len);
}

typedef struct normfs_WriteResponse {
	uint64_t	write_id;
	normfs_WriteResponse_Result	result;
	const normfs_Id * const	*ids;
	size_t	ids_count;
	size_t	_size;
} normfs_WriteResponse;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_WriteResponse_cached_size(const normfs_WriteResponse *m)
{
	return m->_size;
}

/*@ predicate normfs_Id_avoid_normfs_WriteResponse{L}(normfs_Id *m, normfs_WriteResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_WriteResponse_avoid_normfs_SetupResponse{L}(normfs_WriteResponse *m, normfs_SetupResponse *p) =
      \separated(p, m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_avoid_normfs_SetupResponse((normfs_Id *)m->ids[k], p));
*/

/*@ predicate normfs_WriteResponse_avoid_normfs_PingResponse{L}(normfs_WriteResponse *m, normfs_PingResponse *p) =
      \separated(p, m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_avoid_normfs_PingResponse((normfs_Id *)m->ids[k], p));
*/

/*@ predicate normfs_WriteResponse_avoid_normfs_PingRequest{L}(normfs_WriteResponse *m, normfs_PingRequest *p) =
      \separated(p, m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_avoid_normfs_PingRequest((normfs_Id *)m->ids[k], p));
*/

/*@ predicate normfs_PingRequest_navoid_normfs_WriteResponse{L}(normfs_PingRequest *m, normfs_WriteResponse *t) =
      normfs_WriteResponse_avoid_normfs_PingRequest(t, m);
*/

/*@ predicate normfs_SetupResponse_navoid_normfs_WriteResponse{L}(normfs_SetupResponse *m, normfs_WriteResponse *t) =
      normfs_WriteResponse_avoid_normfs_SetupResponse(t, m);
*/

/*@ predicate normfs_PingResponse_navoid_normfs_WriteResponse{L}(normfs_PingResponse *m, normfs_WriteResponse *t) =
      normfs_WriteResponse_avoid_normfs_PingResponse(t, m) &&
      (m->request != \null ==> normfs_PingRequest_navoid_normfs_WriteResponse((normfs_PingRequest *)m->request, t));
*/

/*@ logic integer normfs_WriteResponse_ids_term{L}(normfs_Id *e) =
      e != \null ? 1 + varint_size(normfs_Id_tsize(e)) + normfs_Id_tsize(e) : 2;

    lemma normfs_WriteResponse_ids_term_frame{L1,L2}: \forall normfs_Id *e;
      (e != \null ==> normfs_Id_dsame{L1,L2}(e)) ==> \at(normfs_WriteResponse_ids_term(e), L1) == \at(normfs_WriteResponse_ids_term(e), L2);

    logic integer normfs_WriteResponse_ids_sum{L}(normfs_WriteResponse *m, integer n) =
      n <= 0 ? 0 : normfs_WriteResponse_ids_sum(m, n - 1) + normfs_WriteResponse_ids_term((normfs_Id *)m->ids[n - 1]);

    lemma normfs_WriteResponse_ids_sum_frame{L1,L2}: \forall normfs_WriteResponse *m, integer n; 0 <= n ==>
      \at(m->ids, L1) == \at(m->ids, L2) ==>
      (\forall integer k; 0 <= k < n ==>
         \at(m->ids[k], L1) == \at(m->ids[k], L2) &&
         \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), L1) == \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), L2)) ==>
      \at(normfs_WriteResponse_ids_sum(m, n), L1) == \at(normfs_WriteResponse_ids_sum(m, n), L2);
*/
/*@ \wp::strategy normfs_WriteResponse_ids_sum_ind: \tactic("Wp.induction", \pattern(0 <= n), \select(n)); */
/*@ \wp::proof normfs_WriteResponse_ids_sum_ind: normfs_WriteResponse_ids_sum_frame; */

/*@
    logic integer normfs_WriteResponse_write_id_size{L}(normfs_WriteResponse *m) =
      m->write_id != 0 ? 1 + varint_size(m->write_id) : 0;

    logic integer normfs_WriteResponse_result_size{L}(normfs_WriteResponse *m) =
      m->result != 0 ? 1 + varint_size((uint64_t)(int64_t)m->result) : 0;

    logic integer normfs_WriteResponse_ids_size{L}(normfs_WriteResponse *m) =
      normfs_WriteResponse_ids_sum(m, m->ids_count);

    logic integer normfs_WriteResponse_tsize{L}(normfs_WriteResponse *m) =
      normfs_WriteResponse_write_id_size(m) +
      normfs_WriteResponse_result_size(m) +
      normfs_WriteResponse_ids_size(m);

    predicate normfs_WriteResponse_own{L1,L2}(normfs_WriteResponse *m) =
      \at(m->write_id, L1) == \at(m->write_id, L2) &&
      \at(m->result, L1) == \at(m->result, L2) &&
      \at(m->ids, L1) == \at(m->ids, L2) && \at(m->ids_count, L1) == \at(m->ids_count, L2) &&
      (\forall integer k; 0 <= k < \at(m->ids_count, L1) ==>
         \at(m->ids[k], L1) == \at(m->ids[k], L2));

    predicate normfs_WriteResponse_nown{L1,L2}(normfs_WriteResponse *m) =
      \at(m->write_id, L1) == \at(m->write_id, L2) &&
      \at(m->result, L1) == \at(m->result, L2) &&
      \at(m->ids, L1) == \at(m->ids, L2) && \at(m->ids_count, L1) == \at(m->ids_count, L2);

    predicate normfs_WriteResponse_dsame{L1,L2}(normfs_WriteResponse *m) =
      normfs_WriteResponse_own{L1,L2}(m) &&
      (\forall integer k; 0 <= k < \at(m->ids_count, L1) &&
         \at(m->ids[k], L1) != \null ==> normfs_Id_dsame{L1,L2}((normfs_Id *)\at(m->ids[k], L1)));

    predicate normfs_WriteResponse_csame{L1,L2}(normfs_WriteResponse *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\forall integer k; 0 <= k < \at(m->ids_count, L1) &&
         \at(m->ids[k], L1) != \null ==> normfs_Id_csame{L1,L2}((normfs_Id *)\at(m->ids[k], L1)));

    predicate normfs_WriteResponse_sized{L}(normfs_WriteResponse *m) =
      m->_size == normfs_WriteResponse_tsize(m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_sized((normfs_Id *)m->ids[k]));

    predicate normfs_WriteResponse_valid{L}(normfs_WriteResponse *m) =
      \valid(m) &&
      (m->ids_count == 0 || \valid_read(m->ids + (0 .. m->ids_count - 1))) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_valid((normfs_Id *)m->ids[k])) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_avoid_normfs_WriteResponse((normfs_Id *)m->ids[k], m)) &&
      (\forall integer i, j; 0 <= i < m->ids_count && 0 <= j < m->ids_count &&
         i != j && m->ids[i] != \null && m->ids[j] != \null ==>
         normfs_Id_navoid_normfs_Id((normfs_Id *)m->ids[i], (normfs_Id *)m->ids[j]));

    predicate normfs_WriteResponse_bufsep{L}(normfs_WriteResponse *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->ids_count == 0 || \separated(b + (lo .. hi), m->ids + (0 .. m->ids_count - 1))) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_bufsep((normfs_Id *)m->ids[k], b, lo, hi)));

    lemma normfs_WriteResponse_write_id_size_frame{L1,L2}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> \at(normfs_WriteResponse_write_id_size(m), L1) == \at(normfs_WriteResponse_write_id_size(m), L2);

    lemma normfs_WriteResponse_result_size_frame{L1,L2}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> \at(normfs_WriteResponse_result_size(m), L1) == \at(normfs_WriteResponse_result_size(m), L2);

    lemma normfs_WriteResponse_ids_size_frame{L1,L2}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> \at(normfs_WriteResponse_ids_size(m), L1) == \at(normfs_WriteResponse_ids_size(m), L2);

    lemma normfs_WriteResponse_tsize_frame{L1,L2}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> \at(normfs_WriteResponse_tsize(m), L1) == \at(normfs_WriteResponse_tsize(m), L2);

    lemma normfs_WriteResponse_sized_frame{L1,L2}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> normfs_WriteResponse_csame{L1,L2}(m) ==> normfs_WriteResponse_sized{L1}(m) ==> normfs_WriteResponse_sized{L2}(m);

    lemma normfs_WriteResponse_dsame_trans{L1,L2,L3}: \forall normfs_WriteResponse *m;
      normfs_WriteResponse_dsame{L1,L2}(m) ==> normfs_WriteResponse_dsame{L2,L3}(m) ==> normfs_WriteResponse_dsame{L1,L3}(m);

    lemma normfs_WriteResponse_bufsep_sub{L}: \forall normfs_WriteResponse *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_WriteResponse_bufsep(m, b, lo, hi) ==> normfs_WriteResponse_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ assigns \nothing;
     ensures normfs_WriteResponse_ids_term((normfs_Id *)\null) == 2;
  @/
  void normfs_WriteResponse_ids_term_null(void) { }
*/

/*@ ghost
  /@ requires normfs_WriteResponse_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->ids_count ==>
               0 <= normfs_WriteResponse_ids_sum(m, j) <= normfs_WriteResponse_ids_sum(m, m->ids_count);
  @/
  void normfs_WriteResponse_ids_mono(normfs_WriteResponse *m)
  {
    size_t i = 0;
    /@ loop invariant 0 <= i <= m->ids_count;
       loop invariant \forall integer j; 0 <= j <= i ==> 0 <= normfs_WriteResponse_ids_sum(m, j) <= normfs_WriteResponse_ids_sum(m, i);
       loop assigns i;
       loop variant m->ids_count - i;
    @/
    while (i < m->ids_count) {
      if (m->ids[i]) normfs_Id_tsize_nonneg((normfs_Id *)m->ids[i]);
      i++;
    }
  }
*/

/*@ ghost
  /@ requires normfs_WriteResponse_valid(m);
     assigns \nothing;
     ensures \forall integer j; 0 <= j <= m->ids_count ==>
               0 <= normfs_WriteResponse_ids_sum(m, j) <= normfs_WriteResponse_ids_sum(m, m->ids_count);
     ensures normfs_WriteResponse_tsize(m) >= 0;
  @/
  void normfs_WriteResponse_tsize_nonneg(normfs_WriteResponse *m)
  {
    normfs_WriteResponse_ids_mono(m);
  }
*/

/*@ requires normfs_WriteResponse_valid(m);
    requires normfs_WriteResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->ids[0 .. m->ids_count - 1]->_size;
    ensures \result == \old(normfs_WriteResponse_tsize(m));
    ensures normfs_WriteResponse_sized(m);
    ensures normfs_WriteResponse_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_WriteResponse_size(const normfs_WriteResponse *m)
{
	size_t s = 0;
	/*@ ghost normfs_WriteResponse_tsize_nonneg((normfs_WriteResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_valid((normfs_Id *)m->ids[k])); */
	if (m->write_id != 0) {
		s += 1
		   + gremlin_varint_size(m->write_id);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_WriteResponse_write_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->result != 0) {
		s += 1
		   + gremlin_varint_size((uint64_t)(int64_t)m->result);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_WriteResponse_result_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ loop invariant 0 <= _i <= m->ids_count;
	    loop invariant normfs_WriteResponse_own{Pre,Here}(m);
	    loop invariant \forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
	                     normfs_Id_dsame{Pre,Here}((normfs_Id *)m->ids[k]);
	    loop invariant \forall integer k; 0 <= k < _i && m->ids[k] != \null ==>
	                     normfs_Id_sized((normfs_Id *)m->ids[k]);
	    loop invariant \forall integer k; k == _i ==> s == \at(s, _A2) + \at(normfs_WriteResponse_ids_sum(m, k), Pre);
	    loop assigns _i, s, \at(m->ids, Pre)[0 .. \at(m->ids_count, Pre) - 1]->_size;
	    loop variant m->ids_count - _i;
	*/
	for (size_t _i = 0; _i < m->ids_count; _i++) {
		const normfs_Id *_el = m->ids[_i];
		/*@ assert \forall integer k; k == _i ==>
		      \at(normfs_WriteResponse_ids_sum(m, k + 1), Pre) == \at(normfs_WriteResponse_ids_sum(m, k), Pre) + \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) &&
		      \at(normfs_WriteResponse_ids_sum(m, k + 1), Pre) <= \at(normfs_WriteResponse_ids_sum(m, m->ids_count), Pre); */
		if (_el != NULL) {
			size_t _child_size = normfs_Id_size(_el);
			/*@ assert \forall integer k; k == _i ==>
			      _child_size == \at(normfs_Id_tsize((normfs_Id *)m->ids[k]), Pre) &&
			      \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) == 1 + varint_size(_child_size) + _child_size; */
			s += 1
			   + gremlin_varint_size(_child_size)
			   + _child_size;
		} else {
			/*@ ghost normfs_WriteResponse_ids_term_null(); */
			/*@ assert \forall integer k; k == _i ==> \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) == 2; */
			s += 1 + 1;
		}
	}
	/*@ assert normfs_WriteResponse_own{Pre,Here}(m); */
	/*@ assert s == \at(s, _A2) + \at(normfs_WriteResponse_ids_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s == \at(normfs_WriteResponse_tsize(m), Pre); */
	/*@ assert normfs_WriteResponse_dsame{Pre,Here}(m); */
	/*@ assert \forall integer j; 0 <= j < m->ids_count && m->ids[j] != \null ==> \separated(m, m->ids[j]); */
	/*@ ghost _Aw: ; */
	((normfs_WriteResponse *)m)->_size = s;
	/*@ assert normfs_WriteResponse_own{_Aw,Here}(m); */
	/*@ assert \forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==> normfs_Id_dsame{_Aw,Here}((normfs_Id *)m->ids[k]); */
	/*@ assert \forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==> normfs_Id_dsame{Pre,Here}((normfs_Id *)m->ids[k]); */
	/*@ assert \forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==> normfs_Id_csame{_Aw,Here}((normfs_Id *)m->ids[k]); */
	/*@ assert \forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==> normfs_Id_sized((normfs_Id *)m->ids[k]); */
	/*@ assert normfs_WriteResponse_dsame{_Aw,Here}(m); */
	/*@ assert normfs_WriteResponse_dsame{Pre,Here}(m); */
	/*@ assert normfs_WriteResponse_tsize(m) == \at(normfs_WriteResponse_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_WriteResponse_valid(m) && normfs_WriteResponse_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_WriteResponse_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_WriteResponse_encode_at(const normfs_WriteResponse *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_WriteResponse_tsize_nonneg((normfs_WriteResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_valid((normfs_Id *)m->ids[k])); */
	if (m->write_id != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->write_id);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_WriteResponse_write_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->result != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->result);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_WriteResponse_result_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ loop invariant 0 <= _i <= m->ids_count;
	    loop invariant \forall integer k; k == _i ==> _off == \at(_off, _A2) + \at(normfs_WriteResponse_ids_sum(m, k), Pre);
	    loop assigns _i, _off, _buf[\at(_off, Pre) .. \at(_off, Pre) + \at(m->_size, Pre) - 1];
	    loop variant m->ids_count - _i;
	*/
	for (size_t _i = 0; _i < m->ids_count; _i++) {
		const normfs_Id *_el = m->ids[_i];
		/*@ assert \forall integer k; k == _i ==>
		      \at(normfs_WriteResponse_ids_sum(m, k + 1), Pre) == \at(normfs_WriteResponse_ids_sum(m, k), Pre) + \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) &&
		      \at(normfs_WriteResponse_ids_sum(m, k + 1), Pre) <= \at(normfs_WriteResponse_ids_sum(m, m->ids_count), Pre); */
		if (_el != NULL) {
			/*@ assert normfs_Id_sized((normfs_Id *)_el); */
			/*@ assert \forall integer k; k == _i ==>
			      \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) == 1 + varint_size(_el->_size) + _el->_size; */
			_off = gremlin_varint32_encode_at(_buf, _off, 82u);
			_off = gremlin_varint_encode_at(_buf, _off, normfs_Id_cached_size(_el));
			_off = normfs_Id_encode_at(_el, _buf, _off);
		} else {
			/*@ ghost normfs_WriteResponse_ids_term_null(); */
			/*@ assert \forall integer k; k == _i ==> \at(normfs_WriteResponse_ids_term((normfs_Id *)m->ids[k]), Pre) == 2; */
			_off = gremlin_varint32_encode_at(_buf, _off, 82u);
			_off = gremlin_varint_encode_at(_buf, _off, 0);
		}
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_WriteResponse_ids_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_WriteResponse_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_WriteResponse_valid(m) && normfs_WriteResponse_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_WriteResponse_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_WriteResponse_encode(const normfs_WriteResponse *m, struct gremlin_writer *w)
{
	w->offset = normfs_WriteResponse_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_WriteResponse_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	write_id : 1;
		unsigned	result : 1;
		unsigned	ids : 1;
	} _has;
	uint64_t	write_id;
	normfs_WriteResponse_Result	result;
	size_t	ids_count;
	size_t	ids_first_offset;
} normfs_WriteResponse_reader;

/*@ predicate normfs_WriteResponse_reader_ok{L}(normfs_WriteResponse_reader *r, integer n) =
      r->src_len >= n &&
      r->ids_count <= n &&
      (r->ids_count > 0 ==> r->ids_first_offset < n);
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_WriteResponse_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_WriteResponse_reader_init(normfs_WriteResponse_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_WriteResponse_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_WriteResponse_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->write_id = d.value;
			r->_has.write_id = true;
			continue;
		}
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->result = (normfs_WriteResponse_Result)(int32_t)(uint32_t)d.value;
			r->_has.result = 1;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX, repeated msg */) {
			if (r->ids_count == 0) {
				r->ids_first_offset = offset;
			}
			r->ids_count++;
			r->_has.ids = 1;
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.write_id ==> \result == r->write_id;
*/
static inline uint64_t
normfs_WriteResponse_reader_get_write_id(const normfs_WriteResponse_reader *r)
{
	if (r == NULL || !r->_has.write_id) return 0;
	return r->write_id;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.result ==> \result == r->result;
*/
static inline normfs_WriteResponse_Result
normfs_WriteResponse_reader_get_result(const normfs_WriteResponse_reader *r)
{
	if (r == NULL || !r->_has.result) return 0;
	return r->result;
}

typedef struct normfs_WriteResponse_reader_ids_iter {
	const uint8_t	*src;
	size_t		 src_len;
	size_t		 offset;
	size_t		 count_remaining;
} normfs_WriteResponse_reader_ids_iter;

/*@ predicate normfs_WriteResponse_reader_ids_iter_ok(normfs_WriteResponse_reader_ids_iter it) =
      it.offset <= it.src_len &&
      (it.src_len == 0 || \valid_read(it.src + (0 .. it.src_len - 1)));
*/

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null ==> \result == r->ids_count;
*/
static inline size_t
normfs_WriteResponse_reader_ids_count(const normfs_WriteResponse_reader *r)
{
	if (r == NULL) return 0;
	return r->ids_count;
}

/*@ requires r == \null || (\valid_read(r) && normfs_WriteResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))));
    assigns \nothing;
    ensures normfs_WriteResponse_reader_ids_iter_ok(\result);
    ensures r != \null && r->ids_count > 0 ==>
              \result.src == r->src && \result.src_len == r->src_len &&
              \result.count_remaining == r->ids_count;
    ensures r == \null || r->ids_count == 0 ==>
              \result.src_len == 0 && \result.count_remaining == 0;
*/
static inline normfs_WriteResponse_reader_ids_iter
normfs_WriteResponse_reader_ids_begin(const normfs_WriteResponse_reader *r)
{
	normfs_WriteResponse_reader_ids_iter it = {0};
	if (r == NULL || r->ids_count == 0) return it;
	it.src = r->src;
	it.src_len = r->src_len;
	it.offset = r->ids_first_offset;
	it.count_remaining = r->ids_count;
	return it;
}

/*@ requires \valid(it) && \valid(out);
    requires normfs_WriteResponse_reader_ids_iter_ok(*it);
    requires \separated(it, out, it->src + (0 .. it->src_len - 1));
    assigns *it, *out;
    ensures normfs_WriteResponse_reader_ids_iter_ok(*it);
    ensures it->src == \old(it->src) && it->src_len == \old(it->src_len);
    ensures \result == GREMLIN_OK ==>
              normfs_Id_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_WriteResponse_reader_ids_next(normfs_WriteResponse_reader_ids_iter *it, normfs_Id_reader *out)
{
	if (it->count_remaining == 0) {
		return normfs_Id_reader_init(out, NULL, 0);
	}
	struct gremlin_bytes_decode_result d =
		gremlin_bytes_decode(it->src + it->offset, it->src_len - it->offset);
	if (d.error != GREMLIN_OK) return d.error;
	it->offset += d.consumed;
	it->count_remaining--;
	enum gremlin_error _ie = normfs_Id_reader_init(out, d.bytes.data, d.bytes.len);
	if (_ie != GREMLIN_OK) return _ie;
	/*@ loop invariant it->offset <= it->src_len;
	    loop assigns it->offset;
	    loop variant it->src_len - it->offset;
	*/
	while (it->count_remaining > 0 && it->offset < it->src_len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(it->src + it->offset, it->src_len - it->offset);
		if (t.error != GREMLIN_OK) return t.error;
		it->offset += t.consumed;
		if (t.value == 82u) break;
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		struct gremlin_skip_result sk =
			gremlin_skip_data(it->src + it->offset, it->src_len - it->offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		it->offset += sk.consumed;
	}
	return GREMLIN_OK;
}

typedef struct normfs_ReadResponse {
	uint64_t	read_id;
	normfs_ReadResponse_Result	result;
	const normfs_Id *	id;
	struct gremlin_bytes	data;
	normfs_ReadResponse_DataSource	data_source;
	normfs_Encoding	encoding;
	size_t	_size;
} normfs_ReadResponse;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_ReadResponse_cached_size(const normfs_ReadResponse *m)
{
	return m->_size;
}

/*@ predicate normfs_Id_avoid_normfs_ReadResponse{L}(normfs_Id *m, normfs_ReadResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_SetupResponse{L}(normfs_ReadResponse *m, normfs_SetupResponse *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_SetupResponse((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_PingResponse{L}(normfs_ReadResponse *m, normfs_PingResponse *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_PingResponse((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_WriteResponse{L}(normfs_ReadResponse *m, normfs_WriteResponse *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_WriteResponse((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_PingRequest{L}(normfs_ReadResponse *m, normfs_PingRequest *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_PingRequest((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_Id{L}(normfs_ReadResponse *m, normfs_Id *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_Id((normfs_Id *)m->id, p));
*/

/*@ predicate normfs_PingRequest_navoid_normfs_ReadResponse{L}(normfs_PingRequest *m, normfs_ReadResponse *t) =
      normfs_ReadResponse_avoid_normfs_PingRequest(t, m);
*/

/*@ predicate normfs_Id_navoid_normfs_ReadResponse{L}(normfs_Id *m, normfs_ReadResponse *t) =
      normfs_ReadResponse_avoid_normfs_Id(t, m);
*/

/*@ predicate normfs_SetupResponse_navoid_normfs_ReadResponse{L}(normfs_SetupResponse *m, normfs_ReadResponse *t) =
      normfs_ReadResponse_avoid_normfs_SetupResponse(t, m);
*/

/*@ predicate normfs_PingResponse_navoid_normfs_ReadResponse{L}(normfs_PingResponse *m, normfs_ReadResponse *t) =
      normfs_ReadResponse_avoid_normfs_PingResponse(t, m) &&
      (m->request != \null ==> normfs_PingRequest_navoid_normfs_ReadResponse((normfs_PingRequest *)m->request, t));
*/

/*@ predicate normfs_WriteResponse_navoid_normfs_ReadResponse{L}(normfs_WriteResponse *m, normfs_ReadResponse *t) =
      normfs_ReadResponse_avoid_normfs_WriteResponse(t, m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_navoid_normfs_ReadResponse((normfs_Id *)m->ids[k], t));
*/

/*@
    logic integer normfs_ReadResponse_read_id_size{L}(normfs_ReadResponse *m) =
      m->read_id != 0 ? 1 + varint_size(m->read_id) : 0;

    logic integer normfs_ReadResponse_result_size{L}(normfs_ReadResponse *m) =
      m->result != 0 ? 1 + varint_size((uint64_t)(int64_t)m->result) : 0;

    logic integer normfs_ReadResponse_id_size{L}(normfs_ReadResponse *m) =
      m->id != \null ?
        1 + varint_size(normfs_Id_tsize((normfs_Id *)m->id)) + normfs_Id_tsize((normfs_Id *)m->id) : 0;

    logic integer normfs_ReadResponse_data_size{L}(normfs_ReadResponse *m) =
      m->data.len > 0 ? 1 + varint_size(m->data.len) + m->data.len : 0;

    logic integer normfs_ReadResponse_data_source_size{L}(normfs_ReadResponse *m) =
      m->data_source != 0 ? 1 + varint_size((uint64_t)(int64_t)m->data_source) : 0;

    logic integer normfs_ReadResponse_encoding_size{L}(normfs_ReadResponse *m) =
      m->encoding != 0 ? 1 + varint_size((uint64_t)(int64_t)m->encoding) : 0;

    logic integer normfs_ReadResponse_tsize{L}(normfs_ReadResponse *m) =
      normfs_ReadResponse_read_id_size(m) +
      normfs_ReadResponse_result_size(m) +
      normfs_ReadResponse_id_size(m) +
      normfs_ReadResponse_data_size(m) +
      normfs_ReadResponse_data_source_size(m) +
      normfs_ReadResponse_encoding_size(m);

    predicate normfs_ReadResponse_own{L1,L2}(normfs_ReadResponse *m) =
      \at(m->read_id, L1) == \at(m->read_id, L2) &&
      \at(m->result, L1) == \at(m->result, L2) &&
      \at(m->id, L1) == \at(m->id, L2) &&
      \at(m->data.len, L1) == \at(m->data.len, L2) &&
      \at(m->data.data, L1) == \at(m->data.data, L2) &&
      \at(m->data_source, L1) == \at(m->data_source, L2) &&
      \at(m->encoding, L1) == \at(m->encoding, L2);

    predicate normfs_ReadResponse_nown{L1,L2}(normfs_ReadResponse *m) =
      \at(m->read_id, L1) == \at(m->read_id, L2) &&
      \at(m->result, L1) == \at(m->result, L2) &&
      \at(m->id, L1) == \at(m->id, L2) &&
      \at(m->data.len, L1) == \at(m->data.len, L2) &&
      \at(m->data.data, L1) == \at(m->data.data, L2) &&
      \at(m->data_source, L1) == \at(m->data_source, L2) &&
      \at(m->encoding, L1) == \at(m->encoding, L2);

    predicate normfs_ReadResponse_dsame{L1,L2}(normfs_ReadResponse *m) =
      normfs_ReadResponse_own{L1,L2}(m) &&
      (\at(m->id, L1) != \null ==> normfs_Id_dsame{L1,L2}((normfs_Id *)\at(m->id, L1)));

    predicate normfs_ReadResponse_csame{L1,L2}(normfs_ReadResponse *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->id, L1) != \null ==> normfs_Id_csame{L1,L2}((normfs_Id *)\at(m->id, L1)));

    predicate normfs_ReadResponse_sized{L}(normfs_ReadResponse *m) =
      m->_size == normfs_ReadResponse_tsize(m) &&
      (m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id));

    predicate normfs_ReadResponse_valid{L}(normfs_ReadResponse *m) =
      \valid(m) &&
      (m->data.len == 0 || \valid_read(m->data.data + (0 .. m->data.len - 1))) &&
      (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_ReadResponse((normfs_Id *)m->id, m));

    predicate normfs_ReadResponse_bufsep{L}(normfs_ReadResponse *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->id != \null ==> normfs_Id_bufsep((normfs_Id *)m->id, b, lo, hi)));

    lemma normfs_ReadResponse_read_id_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_read_id_size(m), L1) == \at(normfs_ReadResponse_read_id_size(m), L2);

    lemma normfs_ReadResponse_result_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_result_size(m), L1) == \at(normfs_ReadResponse_result_size(m), L2);

    lemma normfs_ReadResponse_id_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_id_size(m), L1) == \at(normfs_ReadResponse_id_size(m), L2);

    lemma normfs_ReadResponse_data_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_data_size(m), L1) == \at(normfs_ReadResponse_data_size(m), L2);

    lemma normfs_ReadResponse_data_source_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_data_source_size(m), L1) == \at(normfs_ReadResponse_data_source_size(m), L2);

    lemma normfs_ReadResponse_encoding_size_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_encoding_size(m), L1) == \at(normfs_ReadResponse_encoding_size(m), L2);

    lemma normfs_ReadResponse_tsize_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> \at(normfs_ReadResponse_tsize(m), L1) == \at(normfs_ReadResponse_tsize(m), L2);

    lemma normfs_ReadResponse_sized_frame{L1,L2}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> normfs_ReadResponse_csame{L1,L2}(m) ==> normfs_ReadResponse_sized{L1}(m) ==> normfs_ReadResponse_sized{L2}(m);

    lemma normfs_ReadResponse_dsame_trans{L1,L2,L3}: \forall normfs_ReadResponse *m;
      normfs_ReadResponse_dsame{L1,L2}(m) ==> normfs_ReadResponse_dsame{L2,L3}(m) ==> normfs_ReadResponse_dsame{L1,L3}(m);

    lemma normfs_ReadResponse_bufsep_sub{L}: \forall normfs_ReadResponse *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_ReadResponse_bufsep(m, b, lo, hi) ==> normfs_ReadResponse_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_ReadResponse_valid(m);
     assigns \nothing;
     ensures m->id != \null ==> normfs_Id_tsize((normfs_Id *)m->id) >= 0;
     ensures normfs_ReadResponse_tsize(m) >= 0;
  @/
  void normfs_ReadResponse_tsize_nonneg(normfs_ReadResponse *m)
  {
    if (m->id) normfs_Id_tsize_nonneg((normfs_Id *)m->id);
  }
*/

/*@ requires normfs_ReadResponse_valid(m);
    requires normfs_ReadResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->id->_size;
    ensures \result == \old(normfs_ReadResponse_tsize(m));
    ensures normfs_ReadResponse_sized(m);
    ensures normfs_ReadResponse_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_ReadResponse_size(const normfs_ReadResponse *m)
{
	size_t s = 0;
	/*@ ghost normfs_ReadResponse_tsize_nonneg((normfs_ReadResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)); */
	if (m->read_id != 0) {
		s += 1
		   + gremlin_varint_size(m->read_id);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_ReadResponse_read_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->result != 0) {
		s += 1
		   + gremlin_varint_size((uint64_t)(int64_t)m->result);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_ReadResponse_result_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ReadResponse_data_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ReadResponse_data_source_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ReadResponse_encoding_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ReadResponse_id_size(m), Pre) + \at(normfs_ReadResponse_data_size(m), Pre) + \at(normfs_ReadResponse_data_source_size(m), Pre) + \at(normfs_ReadResponse_encoding_size(m), Pre) == \at(normfs_ReadResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ReadResponse_id_size(m), Pre) <= \at(normfs_ReadResponse_tsize(m), Pre); */
	/*@ assert m->id != \null ==> normfs_Id_tsize((normfs_Id *)m->id) == \at(normfs_Id_tsize((normfs_Id *)m->id), Pre); */
	/*@ assert m->id != \null ==> \at(normfs_ReadResponse_id_size(m), Pre) == 1 + varint_size(normfs_Id_tsize((normfs_Id *)m->id)) + normfs_Id_tsize((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> &m->id->_size != &m->_size; */
	/*@ assert m->id != \null ==> &m->id->_size != &m->read_id; */
	/*@ assert m->id != \null ==> &m->id->_size != &m->data.len; */
	if (m->id != NULL) {
		size_t _child_size = normfs_Id_size(m->id);
		/*@ assert _child_size == \at(normfs_Id_tsize((normfs_Id *)m->id), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A2) == m->_size; */
	/*@ assert \at(m->read_id, _A2) == m->read_id; */
	/*@ assert \at(m->data.len, _A2) == m->data.len; */
	/*@ assert normfs_ReadResponse_nown{_A2,Here}((normfs_ReadResponse *)m); */
	/*@ assert normfs_ReadResponse_own{_A2,Here}((normfs_ReadResponse *)m); */
	/*@ assert normfs_ReadResponse_own{Pre,Here}(m); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{Pre,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert s == \at(s, _A2) + \at(normfs_ReadResponse_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	if (m->data.len > 0) {
		s += 1
		   + gremlin_varint_size(m->data.len)
		   + m->data.len;
	}
	/*@ assert s == \at(s, _A3) + \at(normfs_ReadResponse_data_size(m), Pre); */
	/*@ ghost _A4: ; */
	if (m->data_source != 0) {
		s += 1
		   + gremlin_varint_size((uint64_t)(int64_t)m->data_source);
	}
	/*@ assert s == \at(s, _A4) + \at(normfs_ReadResponse_data_source_size(m), Pre); */
	/*@ ghost _A5: ; */
	if (m->encoding != 0) {
		s += 1
		   + gremlin_varint_size((uint64_t)(int64_t)m->encoding);
	}
	/*@ assert s == \at(s, _A5) + \at(normfs_ReadResponse_encoding_size(m), Pre); */
	/*@ ghost _A6: ; */
	/*@ assert s == \at(normfs_ReadResponse_tsize(m), Pre); */
	/*@ assert normfs_ReadResponse_dsame{Pre,Here}(m); */
	/*@ assert m->id != \null ==> \separated(m, m->id); */
	/*@ ghost _Aw: ; */
	((normfs_ReadResponse *)m)->_size = s;
	/*@ assert normfs_ReadResponse_own{_Aw,Here}(m); */
	/*@ assert m->id != \null ==> \at(m->id->_size, _Aw) == m->id->_size; */
	/*@ assert m->id != \null ==> \at(m->id->raw.len, _Aw) == m->id->raw.len; */
	/*@ assert m->id != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_dsame{Pre,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_csame{_Aw,Here}((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert normfs_ReadResponse_dsame{_Aw,Here}(m); */
	/*@ assert normfs_ReadResponse_dsame{Pre,Here}(m); */
	/*@ assert normfs_ReadResponse_tsize(m) == \at(normfs_ReadResponse_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_ReadResponse_valid(m) && normfs_ReadResponse_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_ReadResponse_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_ReadResponse_encode_at(const normfs_ReadResponse *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_ReadResponse_tsize_nonneg((normfs_ReadResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id)); */
	if (m->read_id != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->read_id);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_ReadResponse_read_id_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->result != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->result);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_ReadResponse_result_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ReadResponse_data_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ReadResponse_data_source_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ReadResponse_encoding_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ReadResponse_id_size(m), Pre) + \at(normfs_ReadResponse_data_size(m), Pre) + \at(normfs_ReadResponse_data_source_size(m), Pre) + \at(normfs_ReadResponse_encoding_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ReadResponse_id_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->id != \null ==> normfs_Id_valid((normfs_Id *)m->id) && normfs_Id_sized((normfs_Id *)m->id); */
	/*@ assert m->id != \null ==> normfs_ReadResponse_id_size(m) == 1 + varint_size(m->id->_size) + m->id->_size; */
	/*@ assert m->id != \null ==> normfs_Id_bufsep((normfs_Id *)m->id, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->id != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 82u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_Id_cached_size(m->id));
		/*@ assert _off == \at(_off, _A2) + 1 + varint_size(m->id->_size); */
		/*@ assert _off + m->id->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_Id_bufsep((normfs_Id *)m->id, _buf, _off, _off + m->id->_size - 1); */
		_off = normfs_Id_encode_at(m->id, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_ReadResponse_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	if (m->data.len > 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 90u);
		_off = gremlin_varint_encode_at(_buf, _off, m->data.len);
		_off = gremlin_write_bytes_at(_buf, _off, m->data.data, m->data.len);
	}
	/*@ assert _off == \at(_off, _A3) + \at(normfs_ReadResponse_data_size(m), Pre); */
	/*@ ghost _A4: ; */
	if (m->data_source != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 96u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->data_source);
	}
	/*@ assert _off == \at(_off, _A4) + \at(normfs_ReadResponse_data_source_size(m), Pre); */
	/*@ ghost _A5: ; */
	if (m->encoding != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 104u);
		_off = gremlin_varint_encode_at(_buf, _off, (uint64_t)(int64_t)m->encoding);
	}
	/*@ assert _off == \at(_off, _A5) + \at(normfs_ReadResponse_encoding_size(m), Pre); */
	/*@ ghost _A6: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_ReadResponse_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_ReadResponse_valid(m) && normfs_ReadResponse_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_ReadResponse_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_ReadResponse_encode(const normfs_ReadResponse *m, struct gremlin_writer *w)
{
	w->offset = normfs_ReadResponse_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_ReadResponse_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	read_id : 1;
		unsigned	result : 1;
		unsigned	id : 1;
		unsigned	data : 1;
		unsigned	data_source : 1;
		unsigned	encoding : 1;
	} _has;
	uint64_t	read_id;
	normfs_ReadResponse_Result	result;
	struct gremlin_bytes	id;
	struct gremlin_bytes	data;
	normfs_ReadResponse_DataSource	data_source;
	normfs_Encoding	encoding;
} normfs_ReadResponse_reader;

/*@ predicate normfs_ReadResponse_reader_ok{L}(normfs_ReadResponse_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.id ==> gremlin_span_in(r->id, (uint8_t *)r->src, n)) &&
      (r->_has.data ==> gremlin_span_in(r->data, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_ReadResponse_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ReadResponse_reader_init(normfs_ReadResponse_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_ReadResponse_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_ReadResponse_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->read_id = d.value;
			r->_has.read_id = true;
			continue;
		}
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->result = (normfs_ReadResponse_Result)(int32_t)(uint32_t)d.value;
			r->_has.result = 1;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->id = d.bytes;
			r->_has.id = 1;
			continue;
		}
		if (t.value == 90u /* field 11, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->data = d.bytes;
			r->_has.data = 1;
			continue;
		}
		if (t.value == 96u /* field 12, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->data_source = (normfs_ReadResponse_DataSource)(int32_t)(uint32_t)d.value;
			r->_has.data_source = 1;
			continue;
		}
		if (t.value == 104u /* field 13, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->encoding = (normfs_Encoding)(int32_t)(uint32_t)d.value;
			r->_has.encoding = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.read_id ==> \result == r->read_id;
*/
static inline uint64_t
normfs_ReadResponse_reader_get_read_id(const normfs_ReadResponse_reader *r)
{
	if (r == NULL || !r->_has.read_id) return 0;
	return r->read_id;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.result ==> \result == r->result;
*/
static inline normfs_ReadResponse_Result
normfs_ReadResponse_reader_get_result(const normfs_ReadResponse_reader *r)
{
	if (r == NULL || !r->_has.result) return 0;
	return r->result;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ReadResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_Id_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ReadResponse_reader_get_id(const normfs_ReadResponse_reader *r, normfs_Id_reader *out)
{
	if (r == NULL || !r->_has.id) {
		return normfs_Id_reader_init(out, NULL, 0);
	}
	return normfs_Id_reader_init(out, r->id.data, r->id.len);
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.data ==> \result == r->data;
    ensures !(r != \null && r->_has.data) ==> \result.data == \null && \result.len == 0;
*/
static inline struct gremlin_bytes
normfs_ReadResponse_reader_get_data(const normfs_ReadResponse_reader *r)
{
	if (r == NULL || !r->_has.data) return (struct gremlin_bytes){ NULL, 0 };
	return r->data;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.data_source ==> \result == r->data_source;
*/
static inline normfs_ReadResponse_DataSource
normfs_ReadResponse_reader_get_data_source(const normfs_ReadResponse_reader *r)
{
	if (r == NULL || !r->_has.data_source) return 0;
	return r->data_source;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.encoding ==> \result == r->encoding;
*/
static inline normfs_Encoding
normfs_ReadResponse_reader_get_encoding(const normfs_ReadResponse_reader *r)
{
	if (r == NULL || !r->_has.encoding) return 0;
	return r->encoding;
}

typedef struct normfs_ServerResponse {
	const normfs_SetupResponse *	setup;
	const normfs_PingResponse *	ping;
	const normfs_WriteResponse *	write;
	const normfs_ReadResponse *	read;
	size_t	_size;
} normfs_ServerResponse;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_ServerResponse_cached_size(const normfs_ServerResponse *m)
{
	return m->_size;
}

/*@ predicate normfs_PingRequest_avoid_normfs_ServerResponse{L}(normfs_PingRequest *m, normfs_ServerResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_Id_avoid_normfs_ServerResponse{L}(normfs_Id *m, normfs_ServerResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_SetupResponse_avoid_normfs_ServerResponse{L}(normfs_SetupResponse *m, normfs_ServerResponse *p) =
      \separated(p, m);
*/

/*@ predicate normfs_PingResponse_avoid_normfs_ServerResponse{L}(normfs_PingResponse *m, normfs_ServerResponse *p) =
      \separated(p, m) &&
      (m->request != \null ==> normfs_PingRequest_avoid_normfs_ServerResponse((normfs_PingRequest *)m->request, p));
*/

/*@ predicate normfs_WriteResponse_avoid_normfs_ServerResponse{L}(normfs_WriteResponse *m, normfs_ServerResponse *p) =
      \separated(p, m) &&
      (\forall integer k; 0 <= k < m->ids_count && m->ids[k] != \null ==>
         normfs_Id_avoid_normfs_ServerResponse((normfs_Id *)m->ids[k], p));
*/

/*@ predicate normfs_ReadResponse_avoid_normfs_ServerResponse{L}(normfs_ReadResponse *m, normfs_ServerResponse *p) =
      \separated(p, m) &&
      (m->id != \null ==> normfs_Id_avoid_normfs_ServerResponse((normfs_Id *)m->id, p));
*/

/*@
    logic integer normfs_ServerResponse_setup_size{L}(normfs_ServerResponse *m) =
      m->setup != \null ?
        1 + varint_size(normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup)) + normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup) : 0;

    logic integer normfs_ServerResponse_ping_size{L}(normfs_ServerResponse *m) =
      m->ping != \null ?
        1 + varint_size(normfs_PingResponse_tsize((normfs_PingResponse *)m->ping)) + normfs_PingResponse_tsize((normfs_PingResponse *)m->ping) : 0;

    logic integer normfs_ServerResponse_write_size{L}(normfs_ServerResponse *m) =
      m->write != \null ?
        1 + varint_size(normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write)) + normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write) : 0;

    logic integer normfs_ServerResponse_read_size{L}(normfs_ServerResponse *m) =
      m->read != \null ?
        1 + varint_size(normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read)) + normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read) : 0;

    logic integer normfs_ServerResponse_tsize{L}(normfs_ServerResponse *m) =
      normfs_ServerResponse_setup_size(m) +
      normfs_ServerResponse_ping_size(m) +
      normfs_ServerResponse_write_size(m) +
      normfs_ServerResponse_read_size(m);

    predicate normfs_ServerResponse_own{L1,L2}(normfs_ServerResponse *m) =
      \at(m->setup, L1) == \at(m->setup, L2) &&
      \at(m->ping, L1) == \at(m->ping, L2) &&
      \at(m->write, L1) == \at(m->write, L2) &&
      \at(m->read, L1) == \at(m->read, L2);

    predicate normfs_ServerResponse_nown{L1,L2}(normfs_ServerResponse *m) =
      \at(m->setup, L1) == \at(m->setup, L2) &&
      \at(m->ping, L1) == \at(m->ping, L2) &&
      \at(m->write, L1) == \at(m->write, L2) &&
      \at(m->read, L1) == \at(m->read, L2);

    predicate normfs_ServerResponse_dsame{L1,L2}(normfs_ServerResponse *m) =
      normfs_ServerResponse_own{L1,L2}(m) &&
      (\at(m->setup, L1) != \null ==> normfs_SetupResponse_dsame{L1,L2}((normfs_SetupResponse *)\at(m->setup, L1))) &&
      (\at(m->ping, L1) != \null ==> normfs_PingResponse_dsame{L1,L2}((normfs_PingResponse *)\at(m->ping, L1))) &&
      (\at(m->write, L1) != \null ==> normfs_WriteResponse_dsame{L1,L2}((normfs_WriteResponse *)\at(m->write, L1))) &&
      (\at(m->read, L1) != \null ==> normfs_ReadResponse_dsame{L1,L2}((normfs_ReadResponse *)\at(m->read, L1)));

    predicate normfs_ServerResponse_csame{L1,L2}(normfs_ServerResponse *m) =
      \at(m->_size, L1) == \at(m->_size, L2) &&
      (\at(m->setup, L1) != \null ==> normfs_SetupResponse_csame{L1,L2}((normfs_SetupResponse *)\at(m->setup, L1))) &&
      (\at(m->ping, L1) != \null ==> normfs_PingResponse_csame{L1,L2}((normfs_PingResponse *)\at(m->ping, L1))) &&
      (\at(m->write, L1) != \null ==> normfs_WriteResponse_csame{L1,L2}((normfs_WriteResponse *)\at(m->write, L1))) &&
      (\at(m->read, L1) != \null ==> normfs_ReadResponse_csame{L1,L2}((normfs_ReadResponse *)\at(m->read, L1)));

    predicate normfs_ServerResponse_sized{L}(normfs_ServerResponse *m) =
      m->_size == normfs_ServerResponse_tsize(m) &&
      (m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup)) &&
      (m->ping != \null ==> normfs_PingResponse_sized((normfs_PingResponse *)m->ping)) &&
      (m->write != \null ==> normfs_WriteResponse_sized((normfs_WriteResponse *)m->write)) &&
      (m->read != \null ==> normfs_ReadResponse_sized((normfs_ReadResponse *)m->read));

    predicate normfs_ServerResponse_valid{L}(normfs_ServerResponse *m) =
      \valid(m) &&
      (m->setup != \null ==> normfs_SetupResponse_valid((normfs_SetupResponse *)m->setup)) &&
      (m->setup != \null ==> normfs_SetupResponse_avoid_normfs_ServerResponse((normfs_SetupResponse *)m->setup, m)) &&
      (m->ping != \null ==> normfs_PingResponse_valid((normfs_PingResponse *)m->ping)) &&
      (m->ping != \null ==> normfs_PingResponse_avoid_normfs_ServerResponse((normfs_PingResponse *)m->ping, m)) &&
      (m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write)) &&
      (m->write != \null ==> normfs_WriteResponse_avoid_normfs_ServerResponse((normfs_WriteResponse *)m->write, m)) &&
      (m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read)) &&
      (m->read != \null ==> normfs_ReadResponse_avoid_normfs_ServerResponse((normfs_ReadResponse *)m->read, m)) &&
      (m->setup != \null && m->ping != \null ==>
         normfs_SetupResponse_navoid_normfs_PingResponse((normfs_SetupResponse *)m->setup, (normfs_PingResponse *)m->ping)) &&
      (m->setup != \null && m->write != \null ==>
         normfs_SetupResponse_navoid_normfs_WriteResponse((normfs_SetupResponse *)m->setup, (normfs_WriteResponse *)m->write)) &&
      (m->setup != \null && m->read != \null ==>
         normfs_SetupResponse_navoid_normfs_ReadResponse((normfs_SetupResponse *)m->setup, (normfs_ReadResponse *)m->read)) &&
      (m->ping != \null && m->write != \null ==>
         normfs_PingResponse_navoid_normfs_WriteResponse((normfs_PingResponse *)m->ping, (normfs_WriteResponse *)m->write)) &&
      (m->ping != \null && m->read != \null ==>
         normfs_PingResponse_navoid_normfs_ReadResponse((normfs_PingResponse *)m->ping, (normfs_ReadResponse *)m->read)) &&
      (m->write != \null && m->read != \null ==>
         normfs_WriteResponse_navoid_normfs_ReadResponse((normfs_WriteResponse *)m->write, (normfs_ReadResponse *)m->read));

    predicate normfs_ServerResponse_bufsep{L}(normfs_ServerResponse *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m) &&
      (m->setup != \null ==> normfs_SetupResponse_bufsep((normfs_SetupResponse *)m->setup, b, lo, hi)) &&
      (m->ping != \null ==> normfs_PingResponse_bufsep((normfs_PingResponse *)m->ping, b, lo, hi)) &&
      (m->write != \null ==> normfs_WriteResponse_bufsep((normfs_WriteResponse *)m->write, b, lo, hi)) &&
      (m->read != \null ==> normfs_ReadResponse_bufsep((normfs_ReadResponse *)m->read, b, lo, hi)));

    lemma normfs_ServerResponse_setup_size_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> \at(normfs_ServerResponse_setup_size(m), L1) == \at(normfs_ServerResponse_setup_size(m), L2);

    lemma normfs_ServerResponse_ping_size_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> \at(normfs_ServerResponse_ping_size(m), L1) == \at(normfs_ServerResponse_ping_size(m), L2);

    lemma normfs_ServerResponse_write_size_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> \at(normfs_ServerResponse_write_size(m), L1) == \at(normfs_ServerResponse_write_size(m), L2);

    lemma normfs_ServerResponse_read_size_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> \at(normfs_ServerResponse_read_size(m), L1) == \at(normfs_ServerResponse_read_size(m), L2);

    lemma normfs_ServerResponse_tsize_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> \at(normfs_ServerResponse_tsize(m), L1) == \at(normfs_ServerResponse_tsize(m), L2);

    lemma normfs_ServerResponse_sized_frame{L1,L2}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> normfs_ServerResponse_csame{L1,L2}(m) ==> normfs_ServerResponse_sized{L1}(m) ==> normfs_ServerResponse_sized{L2}(m);

    lemma normfs_ServerResponse_dsame_trans{L1,L2,L3}: \forall normfs_ServerResponse *m;
      normfs_ServerResponse_dsame{L1,L2}(m) ==> normfs_ServerResponse_dsame{L2,L3}(m) ==> normfs_ServerResponse_dsame{L1,L3}(m);

    lemma normfs_ServerResponse_bufsep_sub{L}: \forall normfs_ServerResponse *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_ServerResponse_bufsep(m, b, lo, hi) ==> normfs_ServerResponse_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_ServerResponse_valid(m);
     assigns \nothing;
     ensures m->setup != \null ==> normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup) >= 0;
     ensures m->ping != \null ==> normfs_PingResponse_tsize((normfs_PingResponse *)m->ping) >= 0;
     ensures m->write != \null ==> normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write) >= 0;
     ensures m->read != \null ==> normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read) >= 0;
     ensures normfs_ServerResponse_tsize(m) >= 0;
  @/
  void normfs_ServerResponse_tsize_nonneg(normfs_ServerResponse *m)
  {
    if (m->setup) normfs_SetupResponse_tsize_nonneg((normfs_SetupResponse *)m->setup);
    if (m->ping) normfs_PingResponse_tsize_nonneg((normfs_PingResponse *)m->ping);
    if (m->write) normfs_WriteResponse_tsize_nonneg((normfs_WriteResponse *)m->write);
    if (m->read) normfs_ReadResponse_tsize_nonneg((normfs_ReadResponse *)m->read);
  }
*/

/*@ requires normfs_ServerResponse_valid(m);
    requires normfs_ServerResponse_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size, m->setup->_size, m->ping->_size, *(m->ping != \null ? &m->ping->request->_size : &m->_size), m->write->_size, m->write->ids[0 .. (m->write != \null ? m->write->ids_count : 0) - 1]->_size, m->read->_size, *(m->read != \null ? &m->read->id->_size : &m->_size);
    ensures \result == \old(normfs_ServerResponse_tsize(m));
    ensures normfs_ServerResponse_sized(m);
    ensures normfs_ServerResponse_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_ServerResponse_size(const normfs_ServerResponse *m)
{
	size_t s = 0;
	/*@ ghost normfs_ServerResponse_tsize_nonneg((normfs_ServerResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->setup != \null ==> normfs_SetupResponse_valid((normfs_SetupResponse *)m->setup)); */
	/*@ assert (m->ping != \null ==> normfs_PingResponse_valid((normfs_PingResponse *)m->ping)); */
	/*@ assert (m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write)); */
	/*@ assert (m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read)); */
	/*@ assert \at(normfs_ServerResponse_ping_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ServerResponse_setup_size(m), Pre) + \at(normfs_ServerResponse_ping_size(m), Pre) + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ServerResponse_setup_size(m), Pre) <= \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup) == \at(normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup), Pre); */
	/*@ assert m->setup != \null ==> \at(normfs_ServerResponse_setup_size(m), Pre) == 1 + varint_size(normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup)) + normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null && m->ping != \null ==> \separated(m->setup, m->ping); */
	/*@ assert m->setup != \null && m->ping != \null && m->ping->request != \null ==> \separated(m->setup, m->ping->request); */
	/*@ assert m->setup != \null && m->write != \null ==> \separated(m->setup, m->write); */
	/*@ assert \forall integer j; m->setup != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m->setup, m->write->ids[j]); */
	/*@ assert m->setup != \null && m->read != \null ==> \separated(m->setup, m->read); */
	/*@ assert m->setup != \null && m->read != \null && m->read->id != \null ==> \separated(m->setup, m->read->id); */
	/*@ assert m->setup != \null ==> &m->setup->_size != &m->_size; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->_size; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->local_stamp_ns; */
	/*@ assert m->setup != \null && m->ping != \null ==> &m->setup->_size != &m->ping->monotonic_stamp_ns; */
	/*@ assert m->setup != \null && m->ping != \null && m->ping->request != \null ==> &m->setup->_size != &m->ping->request->_size; */
	/*@ assert m->setup != \null && m->ping != \null && m->ping->request != \null ==> &m->setup->_size != &m->ping->request->sequence; */
	/*@ assert m->setup != \null && m->ping != \null && m->ping->request != \null ==> &m->setup->_size != &m->ping->request->client_timestamp_ns; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->_size; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->write_id; */
	/*@ assert m->setup != \null && m->write != \null ==> &m->setup->_size != &m->write->ids_count; */
	/*@ assert \forall integer j; m->setup != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->setup->_size != &m->write->ids[j]->_size; */
	/*@ assert \forall integer j; m->setup != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->setup->_size != &m->write->ids[j]->raw.len; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->_size; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->read_id; */
	/*@ assert m->setup != \null && m->read != \null ==> &m->setup->_size != &m->read->data.len; */
	/*@ assert m->setup != \null && m->read != \null && m->read->id != \null ==> &m->setup->_size != &m->read->id->_size; */
	/*@ assert m->setup != \null && m->read != \null && m->read->id != \null ==> &m->setup->_size != &m->read->id->raw.len; */
	if (m->setup != NULL) {
		size_t _child_size = normfs_SetupResponse_size(m->setup);
		/*@ assert _child_size == \at(normfs_SetupResponse_tsize((normfs_SetupResponse *)m->setup), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A0) == m->_size; */
	/*@ assert normfs_ServerResponse_nown{_A0,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{_A0,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{Pre,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A0) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->local_stamp_ns, _A0) == m->ping->local_stamp_ns; */
	/*@ assert m->ping != \null ==> \at(m->ping->monotonic_stamp_ns, _A0) == m->ping->monotonic_stamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingResponse_nown{_A0,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_own{_A0,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->_size, _A0) == m->ping->request->_size; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->sequence, _A0) == m->ping->request->sequence; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->client_timestamp_ns, _A0) == m->ping->request->client_timestamp_ns; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_nown{_A0,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_own{_A0,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{_A0,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{Pre,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> \valid(m->ping); */
	/*@ assert m->ping != \null ==> (m->ping->request != \null ==> normfs_PingRequest_valid((normfs_PingRequest *)m->ping->request)); */
	/*@ assert m->ping != \null ==> (m->ping->request != \null ==> normfs_PingRequest_avoid_normfs_PingResponse((normfs_PingRequest *)m->ping->request, m->ping)); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_valid((normfs_PingResponse *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A0) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A0) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->ids_count, _A0) == m->write->ids_count; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_nown{_A0,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->ids_count, _A0) ==>
	      \at(m->write->ids[q], _A0) == m->write->ids[q]; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_own{_A0,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->_size, _A0) == m->write->ids[k]->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->raw.len, _A0) == m->write->ids[k]->raw.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_nown{_A0,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_own{_A0,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{_A0,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{Pre,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \valid(m->write); */
	/*@ assert m->write != \null ==> (m->write->ids_count == 0 || \valid_read(m->write->ids + (0 .. m->write->ids_count - 1))); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==>
         normfs_Id_valid((normfs_Id *)m->write->ids[k])); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==>
         normfs_Id_avoid_normfs_WriteResponse((normfs_Id *)m->write->ids[k], m->write)); */
	/*@ assert m->write != \null ==> (\forall integer i, j; 0 <= i < m->write->ids_count && 0 <= j < m->write->ids_count &&
         i != j && m->write->ids[i] != \null && m->write->ids[j] != \null ==>
         normfs_Id_navoid_normfs_Id((normfs_Id *)m->write->ids[i], (normfs_Id *)m->write->ids[j])); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A0) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A0) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->data.len, _A0) == m->read->data.len; */
	/*@ assert m->read != \null ==> normfs_ReadResponse_nown{_A0,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_own{_A0,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->_size, _A0) == m->read->id->_size; */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->raw.len, _A0) == m->read->id->raw.len; */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_nown{_A0,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_own{_A0,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{_A0,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{Pre,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->data.len == 0 || \valid_read(m->read->data.data + (0 .. m->read->data.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_valid((normfs_Id *)m->read->id)); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_avoid_normfs_ReadResponse((normfs_Id *)m->read->id, m->read)); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read); */
	/*@ assert s == \at(s, _A0) + \at(normfs_ServerResponse_setup_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert \at(normfs_ServerResponse_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ServerResponse_ping_size(m), Pre) + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ServerResponse_ping_size(m), Pre) <= \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_tsize((normfs_PingResponse *)m->ping) == \at(normfs_PingResponse_tsize((normfs_PingResponse *)m->ping), Pre); */
	/*@ assert m->ping != \null ==> \at(normfs_ServerResponse_ping_size(m), Pre) == 1 + varint_size(normfs_PingResponse_tsize((normfs_PingResponse *)m->ping)) + normfs_PingResponse_tsize((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null && m->setup != \null ==> \separated(m->ping, m->setup); */
	/*@ assert m->ping != \null && m->ping->request != \null && m->setup != \null ==> \separated(m->ping->request, m->setup); */
	/*@ assert m->ping != \null && m->write != \null ==> \separated(m->ping, m->write); */
	/*@ assert \forall integer j; m->ping != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m->ping, m->write->ids[j]); */
	/*@ assert m->ping != \null && m->ping->request != \null && m->write != \null ==> \separated(m->ping->request, m->write); */
	/*@ assert \forall integer j; m->ping != \null && m->ping->request != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m->ping->request, m->write->ids[j]); */
	/*@ assert m->ping != \null && m->read != \null ==> \separated(m->ping, m->read); */
	/*@ assert m->ping != \null && m->read != \null && m->read->id != \null ==> \separated(m->ping, m->read->id); */
	/*@ assert m->ping != \null && m->ping->request != \null && m->read != \null ==> \separated(m->ping->request, m->read); */
	/*@ assert m->ping != \null && m->ping->request != \null && m->read != \null && m->read->id != \null ==> \separated(m->ping->request, m->read->id); */
	/*@ assert m->ping != \null ==> &m->ping->_size != &m->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->version; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->_size != &m->setup->instance_id.len; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->_size; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->write_id; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->_size != &m->write->ids_count; */
	/*@ assert \forall integer j; m->ping != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->ping->_size != &m->write->ids[j]->_size; */
	/*@ assert \forall integer j; m->ping != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->ping->_size != &m->write->ids[j]->raw.len; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->_size; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->read_id; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->_size != &m->read->data.len; */
	/*@ assert m->ping != \null && m->read != \null && m->read->id != \null ==> &m->ping->_size != &m->read->id->_size; */
	/*@ assert m->ping != \null && m->read != \null && m->read->id != \null ==> &m->ping->_size != &m->read->id->raw.len; */
	/*@ assert m->ping != \null ==> &m->ping->request->_size != &m->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->request->_size != &m->setup->_size; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->request->_size != &m->setup->version; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->request->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert m->ping != \null && m->setup != \null ==> &m->ping->request->_size != &m->setup->instance_id.len; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->request->_size != &m->write->_size; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->request->_size != &m->write->write_id; */
	/*@ assert m->ping != \null && m->write != \null ==> &m->ping->request->_size != &m->write->ids_count; */
	/*@ assert \forall integer j; m->ping != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->ping->request->_size != &m->write->ids[j]->_size; */
	/*@ assert \forall integer j; m->ping != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->ping->request->_size != &m->write->ids[j]->raw.len; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->request->_size != &m->read->_size; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->request->_size != &m->read->read_id; */
	/*@ assert m->ping != \null && m->read != \null ==> &m->ping->request->_size != &m->read->data.len; */
	/*@ assert m->ping != \null && m->read != \null && m->read->id != \null ==> &m->ping->request->_size != &m->read->id->_size; */
	/*@ assert m->ping != \null && m->read != \null && m->read->id != \null ==> &m->ping->request->_size != &m->read->id->raw.len; */
	if (m->ping != NULL) {
		size_t _child_size = normfs_PingResponse_size(m->ping);
		/*@ assert _child_size == \at(normfs_PingResponse_tsize((normfs_PingResponse *)m->ping), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A1) == m->_size; */
	/*@ assert normfs_ServerResponse_nown{_A1,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{_A1,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A1) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A1) == m->setup->version; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id_bytes.len, _A1) == m->setup->instance_id_bytes.len; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id.len, _A1) == m->setup->instance_id.len; */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_nown{_A1,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_own{_A1,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{_A1,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{Pre,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_csame{_A1,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{Pre,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_sized((normfs_PingResponse *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A1) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A1) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->ids_count, _A1) == m->write->ids_count; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_nown{_A1,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->ids_count, _A1) ==>
	      \at(m->write->ids[q], _A1) == m->write->ids[q]; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_own{_A1,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->_size, _A1) == m->write->ids[k]->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->raw.len, _A1) == m->write->ids[k]->raw.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_nown{_A1,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_own{_A1,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{_A1,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{Pre,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \valid(m->write); */
	/*@ assert m->write != \null ==> (m->write->ids_count == 0 || \valid_read(m->write->ids + (0 .. m->write->ids_count - 1))); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==>
         normfs_Id_valid((normfs_Id *)m->write->ids[k])); */
	/*@ assert m->write != \null ==> (\forall integer k; 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==>
         normfs_Id_avoid_normfs_WriteResponse((normfs_Id *)m->write->ids[k], m->write)); */
	/*@ assert m->write != \null ==> (\forall integer i, j; 0 <= i < m->write->ids_count && 0 <= j < m->write->ids_count &&
         i != j && m->write->ids[i] != \null && m->write->ids[j] != \null ==>
         normfs_Id_navoid_normfs_Id((normfs_Id *)m->write->ids[i], (normfs_Id *)m->write->ids[j])); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A1) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A1) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->data.len, _A1) == m->read->data.len; */
	/*@ assert m->read != \null ==> normfs_ReadResponse_nown{_A1,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_own{_A1,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->_size, _A1) == m->read->id->_size; */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->raw.len, _A1) == m->read->id->raw.len; */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_nown{_A1,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_own{_A1,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{_A1,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{Pre,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->data.len == 0 || \valid_read(m->read->data.data + (0 .. m->read->data.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_valid((normfs_Id *)m->read->id)); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_avoid_normfs_ReadResponse((normfs_Id *)m->read->id, m->read)); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read); */
	/*@ assert s == \at(s, _A1) + \at(normfs_ServerResponse_ping_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert s + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ServerResponse_write_size(m), Pre) <= \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write) == \at(normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write), Pre); */
	/*@ assert m->write != \null ==> \at(normfs_ServerResponse_write_size(m), Pre) == 1 + varint_size(normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write)) + normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null && m->setup != \null ==> \separated(m->write, m->setup); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->setup != \null ==> \separated(m->write->ids[k], m->setup); */
	/*@ assert m->write != \null && m->ping != \null ==> \separated(m->write, m->ping); */
	/*@ assert m->write != \null && m->ping != \null && m->ping->request != \null ==> \separated(m->write, m->ping->request); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null ==> \separated(m->write->ids[k], m->ping); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null && m->ping->request != \null ==> \separated(m->write->ids[k], m->ping->request); */
	/*@ assert m->write != \null && m->read != \null ==> \separated(m->write, m->read); */
	/*@ assert m->write != \null && m->read != \null && m->read->id != \null ==> \separated(m->write, m->read->id); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null ==> \separated(m->write->ids[k], m->read); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null && m->read->id != \null ==> \separated(m->write->ids[k], m->read->id); */
	/*@ assert m->write != \null ==> &m->write->_size != &m->_size; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->_size; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->version; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert m->write != \null && m->setup != \null ==> &m->write->_size != &m->setup->instance_id.len; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->_size; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->local_stamp_ns; */
	/*@ assert m->write != \null && m->ping != \null ==> &m->write->_size != &m->ping->monotonic_stamp_ns; */
	/*@ assert m->write != \null && m->ping != \null && m->ping->request != \null ==> &m->write->_size != &m->ping->request->_size; */
	/*@ assert m->write != \null && m->ping != \null && m->ping->request != \null ==> &m->write->_size != &m->ping->request->sequence; */
	/*@ assert m->write != \null && m->ping != \null && m->ping->request != \null ==> &m->write->_size != &m->ping->request->client_timestamp_ns; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->_size; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->read_id; */
	/*@ assert m->write != \null && m->read != \null ==> &m->write->_size != &m->read->data.len; */
	/*@ assert m->write != \null && m->read != \null && m->read->id != \null ==> &m->write->_size != &m->read->id->_size; */
	/*@ assert m->write != \null && m->read != \null && m->read->id != \null ==> &m->write->_size != &m->read->id->raw.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count ==> &m->write->ids[k]->_size != &m->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->setup != \null ==> &m->write->ids[k]->_size != &m->setup->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->setup != \null ==> &m->write->ids[k]->_size != &m->setup->version; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->setup != \null ==> &m->write->ids[k]->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->setup != \null ==> &m->write->ids[k]->_size != &m->setup->instance_id.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null ==> &m->write->ids[k]->_size != &m->ping->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null ==> &m->write->ids[k]->_size != &m->ping->local_stamp_ns; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null ==> &m->write->ids[k]->_size != &m->ping->monotonic_stamp_ns; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null && m->ping->request != \null ==> &m->write->ids[k]->_size != &m->ping->request->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null && m->ping->request != \null ==> &m->write->ids[k]->_size != &m->ping->request->sequence; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->ping != \null && m->ping->request != \null ==> &m->write->ids[k]->_size != &m->ping->request->client_timestamp_ns; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null ==> &m->write->ids[k]->_size != &m->read->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null ==> &m->write->ids[k]->_size != &m->read->read_id; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null ==> &m->write->ids[k]->_size != &m->read->data.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null && m->read->id != \null ==> &m->write->ids[k]->_size != &m->read->id->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->read != \null && m->read->id != \null ==> &m->write->ids[k]->_size != &m->read->id->raw.len; */
	if (m->write != NULL) {
		size_t _child_size = normfs_WriteResponse_size(m->write);
		/*@ assert _child_size == \at(normfs_WriteResponse_tsize((normfs_WriteResponse *)m->write), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A2) == m->_size; */
	/*@ assert normfs_ServerResponse_nown{_A2,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{_A2,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A2) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A2) == m->setup->version; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id_bytes.len, _A2) == m->setup->instance_id_bytes.len; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id.len, _A2) == m->setup->instance_id.len; */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_nown{_A2,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_own{_A2,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{_A2,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{Pre,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_csame{_A2,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A2) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->local_stamp_ns, _A2) == m->ping->local_stamp_ns; */
	/*@ assert m->ping != \null ==> \at(m->ping->monotonic_stamp_ns, _A2) == m->ping->monotonic_stamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingResponse_nown{_A2,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_own{_A2,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->_size, _A2) == m->ping->request->_size; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->sequence, _A2) == m->ping->request->sequence; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->client_timestamp_ns, _A2) == m->ping->request->client_timestamp_ns; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_nown{_A2,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_own{_A2,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{_A2,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{Pre,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_csame{_A2,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_sized((normfs_PingResponse *)m->ping); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{Pre,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_sized((normfs_WriteResponse *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _A2) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _A2) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->data.len, _A2) == m->read->data.len; */
	/*@ assert m->read != \null ==> normfs_ReadResponse_nown{_A2,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_own{_A2,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->_size, _A2) == m->read->id->_size; */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->raw.len, _A2) == m->read->id->raw.len; */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_nown{_A2,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_own{_A2,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{_A2,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{Pre,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> \valid(m->read); */
	/*@ assert m->read != \null ==> (m->read->data.len == 0 || \valid_read(m->read->data.data + (0 .. m->read->data.len - 1))); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_valid((normfs_Id *)m->read->id)); */
	/*@ assert m->read != \null ==> (m->read->id != \null ==> normfs_Id_avoid_normfs_ReadResponse((normfs_Id *)m->read->id, m->read)); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read); */
	/*@ assert s == \at(s, _A2) + \at(normfs_ServerResponse_write_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s + \at(normfs_ServerResponse_read_size(m), Pre) == \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert s + \at(normfs_ServerResponse_read_size(m), Pre) <= \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read) == \at(normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read), Pre); */
	/*@ assert m->read != \null ==> \at(normfs_ServerResponse_read_size(m), Pre) == 1 + varint_size(normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read)) + normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null && m->setup != \null ==> \separated(m->read, m->setup); */
	/*@ assert m->read != \null && m->read->id != \null && m->setup != \null ==> \separated(m->read->id, m->setup); */
	/*@ assert m->read != \null && m->ping != \null ==> \separated(m->read, m->ping); */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> \separated(m->read, m->ping->request); */
	/*@ assert m->read != \null && m->read->id != \null && m->ping != \null ==> \separated(m->read->id, m->ping); */
	/*@ assert m->read != \null && m->read->id != \null && m->ping != \null && m->ping->request != \null ==> \separated(m->read->id, m->ping->request); */
	/*@ assert m->read != \null && m->write != \null ==> \separated(m->read, m->write); */
	/*@ assert \forall integer j; m->read != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m->read, m->write->ids[j]); */
	/*@ assert m->read != \null && m->read->id != \null && m->write != \null ==> \separated(m->read->id, m->write); */
	/*@ assert \forall integer j; m->read != \null && m->read->id != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m->read->id, m->write->ids[j]); */
	/*@ assert m->read != \null ==> &m->read->_size != &m->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->version; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->_size != &m->setup->instance_id.len; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->_size; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->local_stamp_ns; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->_size != &m->ping->monotonic_stamp_ns; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->_size != &m->ping->request->_size; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->_size != &m->ping->request->sequence; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->_size != &m->ping->request->client_timestamp_ns; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->_size; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->write_id; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->_size != &m->write->ids_count; */
	/*@ assert \forall integer j; m->read != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->read->_size != &m->write->ids[j]->_size; */
	/*@ assert \forall integer j; m->read != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->read->_size != &m->write->ids[j]->raw.len; */
	/*@ assert m->read != \null ==> &m->read->id->_size != &m->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->id->_size != &m->setup->_size; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->id->_size != &m->setup->version; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->id->_size != &m->setup->instance_id_bytes.len; */
	/*@ assert m->read != \null && m->setup != \null ==> &m->read->id->_size != &m->setup->instance_id.len; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->id->_size != &m->ping->_size; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->id->_size != &m->ping->local_stamp_ns; */
	/*@ assert m->read != \null && m->ping != \null ==> &m->read->id->_size != &m->ping->monotonic_stamp_ns; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->id->_size != &m->ping->request->_size; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->id->_size != &m->ping->request->sequence; */
	/*@ assert m->read != \null && m->ping != \null && m->ping->request != \null ==> &m->read->id->_size != &m->ping->request->client_timestamp_ns; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->id->_size != &m->write->_size; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->id->_size != &m->write->write_id; */
	/*@ assert m->read != \null && m->write != \null ==> &m->read->id->_size != &m->write->ids_count; */
	/*@ assert \forall integer j; m->read != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->read->id->_size != &m->write->ids[j]->_size; */
	/*@ assert \forall integer j; m->read != \null && m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> &m->read->id->_size != &m->write->ids[j]->raw.len; */
	if (m->read != NULL) {
		size_t _child_size = normfs_ReadResponse_size(m->read);
		/*@ assert _child_size == \at(normfs_ReadResponse_tsize((normfs_ReadResponse *)m->read), Pre); */
		s += 1
		   + gremlin_varint_size(_child_size)
		   + _child_size;
	}
	/*@ assert \at(m->_size, _A3) == m->_size; */
	/*@ assert normfs_ServerResponse_nown{_A3,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{_A3,Here}((normfs_ServerResponse *)m); */
	/*@ assert normfs_ServerResponse_own{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _A3) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _A3) == m->setup->version; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id_bytes.len, _A3) == m->setup->instance_id_bytes.len; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id.len, _A3) == m->setup->instance_id.len; */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_nown{_A3,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_own{_A3,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{_A3,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{Pre,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_csame{_A3,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _A3) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->local_stamp_ns, _A3) == m->ping->local_stamp_ns; */
	/*@ assert m->ping != \null ==> \at(m->ping->monotonic_stamp_ns, _A3) == m->ping->monotonic_stamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingResponse_nown{_A3,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_own{_A3,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->_size, _A3) == m->ping->request->_size; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->sequence, _A3) == m->ping->request->sequence; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->client_timestamp_ns, _A3) == m->ping->request->client_timestamp_ns; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_nown{_A3,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_own{_A3,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{_A3,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{Pre,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_csame{_A3,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_sized((normfs_PingResponse *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _A3) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _A3) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->ids_count, _A3) == m->write->ids_count; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_nown{_A3,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->ids_count, _A3) ==>
	      \at(m->write->ids[q], _A3) == m->write->ids[q]; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_own{_A3,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->_size, _A3) == m->write->ids[k]->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->raw.len, _A3) == m->write->ids[k]->raw.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_nown{_A3,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_own{_A3,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{_A3,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{Pre,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_csame{_A3,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_sized((normfs_WriteResponse *)m->write); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{Pre,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_sized((normfs_ReadResponse *)m->read); */
	/*@ assert s == \at(s, _A3) + \at(normfs_ServerResponse_read_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert s == \at(normfs_ServerResponse_tsize(m), Pre); */
	/*@ assert normfs_ServerResponse_dsame{Pre,Here}(m); */
	/*@ assert m->setup != \null ==> \separated(m, m->setup); */
	/*@ assert m->ping != \null ==> \separated(m, m->ping); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \separated(m, m->ping->request); */
	/*@ assert m->write != \null ==> \separated(m, m->write); */
	/*@ assert \forall integer j; m->write != \null && 0 <= j < m->write->ids_count && m->write->ids[j] != \null ==> \separated(m, m->write->ids[j]); */
	/*@ assert m->read != \null ==> \separated(m, m->read); */
	/*@ assert m->read != \null && m->read->id != \null ==> \separated(m, m->read->id); */
	/*@ ghost _Aw: ; */
	((normfs_ServerResponse *)m)->_size = s;
	/*@ assert normfs_ServerResponse_own{_Aw,Here}(m); */
	/*@ assert m->setup != \null ==> \at(m->setup->_size, _Aw) == m->setup->_size; */
	/*@ assert m->setup != \null ==> \at(m->setup->version, _Aw) == m->setup->version; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id_bytes.len, _Aw) == m->setup->instance_id_bytes.len; */
	/*@ assert m->setup != \null ==> \at(m->setup->instance_id.len, _Aw) == m->setup->instance_id.len; */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_nown{_Aw,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_own{_Aw,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{_Aw,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_dsame{Pre,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_csame{_Aw,Here}((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->ping != \null ==> \at(m->ping->_size, _Aw) == m->ping->_size; */
	/*@ assert m->ping != \null ==> \at(m->ping->local_stamp_ns, _Aw) == m->ping->local_stamp_ns; */
	/*@ assert m->ping != \null ==> \at(m->ping->monotonic_stamp_ns, _Aw) == m->ping->monotonic_stamp_ns; */
	/*@ assert m->ping != \null ==> normfs_PingResponse_nown{_Aw,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_own{_Aw,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->_size, _Aw) == m->ping->request->_size; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->sequence, _Aw) == m->ping->request->sequence; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> \at(m->ping->request->client_timestamp_ns, _Aw) == m->ping->request->client_timestamp_ns; */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_nown{_Aw,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null && m->ping->request != \null ==> normfs_PingRequest_own{_Aw,Here}((normfs_PingRequest *)m->ping->request); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{_Aw,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_dsame{Pre,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_csame{_Aw,Here}((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_sized((normfs_PingResponse *)m->ping); */
	/*@ assert m->write != \null ==> \at(m->write->_size, _Aw) == m->write->_size; */
	/*@ assert m->write != \null ==> \at(m->write->write_id, _Aw) == m->write->write_id; */
	/*@ assert m->write != \null ==> \at(m->write->ids_count, _Aw) == m->write->ids_count; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_nown{_Aw,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> \forall integer q; 0 <= q < \at(m->write->ids_count, _Aw) ==>
	      \at(m->write->ids[q], _Aw) == m->write->ids[q]; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_own{_Aw,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->_size, _Aw) == m->write->ids[k]->_size; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> \at(m->write->ids[k]->raw.len, _Aw) == m->write->ids[k]->raw.len; */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert \forall integer k; m->write != \null && 0 <= k < m->write->ids_count && m->write->ids[k] != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->write->ids[k]); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{_Aw,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_dsame{Pre,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_csame{_Aw,Here}((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_sized((normfs_WriteResponse *)m->write); */
	/*@ assert m->read != \null ==> \at(m->read->_size, _Aw) == m->read->_size; */
	/*@ assert m->read != \null ==> \at(m->read->read_id, _Aw) == m->read->read_id; */
	/*@ assert m->read != \null ==> \at(m->read->data.len, _Aw) == m->read->data.len; */
	/*@ assert m->read != \null ==> normfs_ReadResponse_nown{_Aw,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_own{_Aw,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->_size, _Aw) == m->read->id->_size; */
	/*@ assert m->read != \null && m->read->id != \null ==> \at(m->read->id->raw.len, _Aw) == m->read->id->raw.len; */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_nown{_Aw,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null && m->read->id != \null ==> normfs_Id_own{_Aw,Here}((normfs_Id *)m->read->id); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{_Aw,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_dsame{Pre,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_csame{_Aw,Here}((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_sized((normfs_ReadResponse *)m->read); */
	/*@ assert normfs_ServerResponse_dsame{_Aw,Here}(m); */
	/*@ assert normfs_ServerResponse_dsame{Pre,Here}(m); */
	/*@ assert normfs_ServerResponse_tsize(m) == \at(normfs_ServerResponse_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_ServerResponse_valid(m) && normfs_ServerResponse_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_ServerResponse_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_ServerResponse_encode_at(const normfs_ServerResponse *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_ServerResponse_tsize_nonneg((normfs_ServerResponse *)m); */
	/*@ ghost _A0: ; */
	/*@ assert (m->setup != \null ==> normfs_SetupResponse_valid((normfs_SetupResponse *)m->setup)); */
	/*@ assert (m->ping != \null ==> normfs_PingResponse_valid((normfs_PingResponse *)m->ping)); */
	/*@ assert (m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write)); */
	/*@ assert (m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read)); */
	/*@ assert \at(normfs_ServerResponse_ping_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ServerResponse_setup_size(m), Pre) + \at(normfs_ServerResponse_ping_size(m), Pre) + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ServerResponse_setup_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_valid((normfs_SetupResponse *)m->setup) && normfs_SetupResponse_sized((normfs_SetupResponse *)m->setup); */
	/*@ assert m->setup != \null ==> normfs_ServerResponse_setup_size(m) == 1 + varint_size(m->setup->_size) + m->setup->_size; */
	/*@ assert m->setup != \null ==> normfs_SetupResponse_bufsep((normfs_SetupResponse *)m->setup, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->setup != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 10u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_SetupResponse_cached_size(m->setup));
		/*@ assert _off == \at(_off, _A0) + 1 + varint_size(m->setup->_size); */
		/*@ assert _off + m->setup->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_SetupResponse_bufsep((normfs_SetupResponse *)m->setup, _buf, _off, _off + m->setup->_size - 1); */
		_off = normfs_SetupResponse_encode_at(m->setup, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_ServerResponse_setup_size(m), Pre); */
	/*@ ghost _A1: ; */
	/*@ assert \at(normfs_ServerResponse_write_size(m), Pre) >= 0; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ServerResponse_ping_size(m), Pre) + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ServerResponse_ping_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->ping != \null ==> normfs_PingResponse_valid((normfs_PingResponse *)m->ping) && normfs_PingResponse_sized((normfs_PingResponse *)m->ping); */
	/*@ assert m->ping != \null ==> normfs_ServerResponse_ping_size(m) == 1 + varint_size(m->ping->_size) + m->ping->_size; */
	/*@ assert m->ping != \null ==> normfs_PingResponse_bufsep((normfs_PingResponse *)m->ping, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->ping != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 18u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_PingResponse_cached_size(m->ping));
		/*@ assert _off == \at(_off, _A1) + 1 + varint_size(m->ping->_size); */
		/*@ assert _off + m->ping->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_PingResponse_bufsep((normfs_PingResponse *)m->ping, _buf, _off, _off + m->ping->_size - 1); */
		_off = normfs_PingResponse_encode_at(m->ping, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_ServerResponse_ping_size(m), Pre); */
	/*@ ghost _A2: ; */
	/*@ assert \at(normfs_ServerResponse_read_size(m), Pre) >= 0; */
	/*@ assert _off + \at(normfs_ServerResponse_write_size(m), Pre) + \at(normfs_ServerResponse_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ServerResponse_write_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->write != \null ==> normfs_WriteResponse_valid((normfs_WriteResponse *)m->write) && normfs_WriteResponse_sized((normfs_WriteResponse *)m->write); */
	/*@ assert m->write != \null ==> normfs_ServerResponse_write_size(m) == 1 + varint_size(m->write->_size) + m->write->_size; */
	/*@ assert m->write != \null ==> normfs_WriteResponse_bufsep((normfs_WriteResponse *)m->write, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->write != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 82u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_WriteResponse_cached_size(m->write));
		/*@ assert _off == \at(_off, _A2) + 1 + varint_size(m->write->_size); */
		/*@ assert _off + m->write->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_WriteResponse_bufsep((normfs_WriteResponse *)m->write, _buf, _off, _off + m->write->_size - 1); */
		_off = normfs_WriteResponse_encode_at(m->write, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_ServerResponse_write_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off + \at(normfs_ServerResponse_read_size(m), Pre) == \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert _off + \at(normfs_ServerResponse_read_size(m), Pre) <= \at(_off, Pre) + \at(m->_size, Pre); */
	/*@ assert m->read != \null ==> normfs_ReadResponse_valid((normfs_ReadResponse *)m->read) && normfs_ReadResponse_sized((normfs_ReadResponse *)m->read); */
	/*@ assert m->read != \null ==> normfs_ServerResponse_read_size(m) == 1 + varint_size(m->read->_size) + m->read->_size; */
	/*@ assert m->read != \null ==> normfs_ReadResponse_bufsep((normfs_ReadResponse *)m->read, _buf, _off, \at(_off, Pre) + \at(m->_size, Pre) - 1); */
	if (m->read != NULL) {
		_off = gremlin_varint32_encode_at(_buf, _off, 90u);
		_off = gremlin_varint_encode_at(_buf, _off, normfs_ReadResponse_cached_size(m->read));
		/*@ assert _off == \at(_off, _A3) + 1 + varint_size(m->read->_size); */
		/*@ assert _off + m->read->_size <= \at(_off, Pre) + \at(m->_size, Pre); */
		/*@ assert normfs_ReadResponse_bufsep((normfs_ReadResponse *)m->read, _buf, _off, _off + m->read->_size - 1); */
		_off = normfs_ReadResponse_encode_at(m->read, _buf, _off);
	}
	/*@ assert _off == \at(_off, _A3) + \at(normfs_ServerResponse_read_size(m), Pre); */
	/*@ ghost _A4: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_ServerResponse_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_ServerResponse_valid(m) && normfs_ServerResponse_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_ServerResponse_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_ServerResponse_encode(const normfs_ServerResponse *m, struct gremlin_writer *w)
{
	w->offset = normfs_ServerResponse_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_ServerResponse_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	setup : 1;
		unsigned	ping : 1;
		unsigned	write : 1;
		unsigned	read : 1;
	} _has;
	struct gremlin_bytes	setup;
	struct gremlin_bytes	ping;
	struct gremlin_bytes	write;
	struct gremlin_bytes	read;
} normfs_ServerResponse_reader;

/*@ predicate normfs_ServerResponse_reader_ok{L}(normfs_ServerResponse_reader *r, integer n) =
      r->src_len >= n &&
      (r->_has.setup ==> gremlin_span_in(r->setup, (uint8_t *)r->src, n)) &&
      (r->_has.ping ==> gremlin_span_in(r->ping, (uint8_t *)r->src, n)) &&
      (r->_has.write ==> gremlin_span_in(r->write, (uint8_t *)r->src, n)) &&
      (r->_has.read ==> gremlin_span_in(r->read, (uint8_t *)r->src, n));
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_ServerResponse_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ServerResponse_reader_init(normfs_ServerResponse_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_ServerResponse_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_ServerResponse_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 10u /* field 1, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->setup = d.bytes;
			r->_has.setup = 1;
			continue;
		}
		if (t.value == 18u /* field 2, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->ping = d.bytes;
			r->_has.ping = 1;
			continue;
		}
		if (t.value == 82u /* field 10, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->write = d.bytes;
			r->_has.write = 1;
			continue;
		}
		if (t.value == 90u /* field 11, GREMLIN_WIRE_LEN_PREFIX */) {
			struct gremlin_bytes_decode_result d =
				gremlin_bytes_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->read = d.bytes;
			r->_has.read = 1;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ServerResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_SetupResponse_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ServerResponse_reader_get_setup(const normfs_ServerResponse_reader *r, normfs_SetupResponse_reader *out)
{
	if (r == NULL || !r->_has.setup) {
		return normfs_SetupResponse_reader_init(out, NULL, 0);
	}
	return normfs_SetupResponse_reader_init(out, r->setup.data, r->setup.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ServerResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_PingResponse_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ServerResponse_reader_get_ping(const normfs_ServerResponse_reader *r, normfs_PingResponse_reader *out)
{
	if (r == NULL || !r->_has.ping) {
		return normfs_PingResponse_reader_init(out, NULL, 0);
	}
	return normfs_PingResponse_reader_init(out, r->ping.data, r->ping.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ServerResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_WriteResponse_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ServerResponse_reader_get_write(const normfs_ServerResponse_reader *r, normfs_WriteResponse_reader *out)
{
	if (r == NULL || !r->_has.write) {
		return normfs_WriteResponse_reader_init(out, NULL, 0);
	}
	return normfs_WriteResponse_reader_init(out, r->write.data, r->write.len);
}

/*@ requires \valid(out);
    requires r == \null || (\valid_read(r) && normfs_ServerResponse_reader_ok(r, r->src_len) &&
               (r->src_len == 0 ||
                \valid_read(r->src + (0 .. r->src_len - 1))) &&
               \separated(out, r, r->src + (0 .. r->src_len - 1)));
    assigns *out;
    ensures \result == GREMLIN_OK ==>
              normfs_ReadResponse_reader_ok(out, out->src_len) &&
              (out->src_len == 0 ||
               \valid_read(out->src + (0 .. out->src_len - 1)));
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_ServerResponse_reader_get_read(const normfs_ServerResponse_reader *r, normfs_ReadResponse_reader *out)
{
	if (r == NULL || !r->_has.read) {
		return normfs_ReadResponse_reader_init(out, NULL, 0);
	}
	return normfs_ReadResponse_reader_init(out, r->read.data, r->read.len);
}

typedef struct normfs_Stamp {
	uint64_t	monotonic_stamp_ns;
	uint64_t	local_stamp_ns;
	uint64_t	app_start_id;
	size_t	_size;
} normfs_Stamp;

/*@ requires \valid_read(m);
    assigns \nothing;
    ensures \result == m->_size;
*/
static inline size_t
normfs_Stamp_cached_size(const normfs_Stamp *m)
{
	return m->_size;
}

/*@
    logic integer normfs_Stamp_monotonic_stamp_ns_size{L}(normfs_Stamp *m) =
      m->monotonic_stamp_ns != 0 ? 1 + varint_size(m->monotonic_stamp_ns) : 0;

    logic integer normfs_Stamp_local_stamp_ns_size{L}(normfs_Stamp *m) =
      m->local_stamp_ns != 0 ? 1 + varint_size(m->local_stamp_ns) : 0;

    logic integer normfs_Stamp_app_start_id_size{L}(normfs_Stamp *m) =
      m->app_start_id != 0 ? 1 + varint_size(m->app_start_id) : 0;

    logic integer normfs_Stamp_tsize{L}(normfs_Stamp *m) =
      normfs_Stamp_monotonic_stamp_ns_size(m) +
      normfs_Stamp_local_stamp_ns_size(m) +
      normfs_Stamp_app_start_id_size(m);

    predicate normfs_Stamp_own{L1,L2}(normfs_Stamp *m) =
      \at(m->monotonic_stamp_ns, L1) == \at(m->monotonic_stamp_ns, L2) &&
      \at(m->local_stamp_ns, L1) == \at(m->local_stamp_ns, L2) &&
      \at(m->app_start_id, L1) == \at(m->app_start_id, L2);

    predicate normfs_Stamp_nown{L1,L2}(normfs_Stamp *m) =
      \at(m->monotonic_stamp_ns, L1) == \at(m->monotonic_stamp_ns, L2) &&
      \at(m->local_stamp_ns, L1) == \at(m->local_stamp_ns, L2) &&
      \at(m->app_start_id, L1) == \at(m->app_start_id, L2);

    predicate normfs_Stamp_dsame{L1,L2}(normfs_Stamp *m) =
      normfs_Stamp_own{L1,L2}(m);

    predicate normfs_Stamp_csame{L1,L2}(normfs_Stamp *m) =
      \at(m->_size, L1) == \at(m->_size, L2);

    predicate normfs_Stamp_sized{L}(normfs_Stamp *m) =
      m->_size == normfs_Stamp_tsize(m);

    predicate normfs_Stamp_valid{L}(normfs_Stamp *m) =
      \valid(m);

    predicate normfs_Stamp_bufsep{L}(normfs_Stamp *m, uint8_t *b, integer lo, integer hi) =
      hi < lo || (\separated(b + (lo .. hi), m));

    lemma normfs_Stamp_monotonic_stamp_ns_size_frame{L1,L2}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> \at(normfs_Stamp_monotonic_stamp_ns_size(m), L1) == \at(normfs_Stamp_monotonic_stamp_ns_size(m), L2);

    lemma normfs_Stamp_local_stamp_ns_size_frame{L1,L2}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> \at(normfs_Stamp_local_stamp_ns_size(m), L1) == \at(normfs_Stamp_local_stamp_ns_size(m), L2);

    lemma normfs_Stamp_app_start_id_size_frame{L1,L2}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> \at(normfs_Stamp_app_start_id_size(m), L1) == \at(normfs_Stamp_app_start_id_size(m), L2);

    lemma normfs_Stamp_tsize_frame{L1,L2}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> \at(normfs_Stamp_tsize(m), L1) == \at(normfs_Stamp_tsize(m), L2);

    lemma normfs_Stamp_sized_frame{L1,L2}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> normfs_Stamp_csame{L1,L2}(m) ==> normfs_Stamp_sized{L1}(m) ==> normfs_Stamp_sized{L2}(m);

    lemma normfs_Stamp_dsame_trans{L1,L2,L3}: \forall normfs_Stamp *m;
      normfs_Stamp_dsame{L1,L2}(m) ==> normfs_Stamp_dsame{L2,L3}(m) ==> normfs_Stamp_dsame{L1,L3}(m);

    lemma normfs_Stamp_bufsep_sub{L}: \forall normfs_Stamp *m, uint8_t *b, integer lo, hi, lo2, hi2;
      lo <= lo2 ==> hi2 <= hi ==> normfs_Stamp_bufsep(m, b, lo, hi) ==> normfs_Stamp_bufsep(m, b, lo2, hi2);
*/

/*@ ghost
  /@ requires normfs_Stamp_valid(m);
     assigns \nothing;
     ensures normfs_Stamp_tsize(m) >= 0;
  @/
  void normfs_Stamp_tsize_nonneg(normfs_Stamp *m)
  {
  }
*/

/*@ requires normfs_Stamp_valid(m);
    requires normfs_Stamp_tsize(m) <= 0x7fffffffffffffff;
    assigns m->_size;
    ensures \result == \old(normfs_Stamp_tsize(m));
    ensures normfs_Stamp_sized(m);
    ensures normfs_Stamp_dsame{Pre,Post}(m);
*/
__attribute__((noinline, unused)) static size_t
normfs_Stamp_size(const normfs_Stamp *m)
{
	size_t s = 0;
	/*@ ghost normfs_Stamp_tsize_nonneg((normfs_Stamp *)m); */
	/*@ ghost _A0: ; */
	if (m->monotonic_stamp_ns != 0) {
		s += 1
		   + gremlin_varint_size(m->monotonic_stamp_ns);
	}
	/*@ assert s == \at(s, _A0) + \at(normfs_Stamp_monotonic_stamp_ns_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->local_stamp_ns != 0) {
		s += 1
		   + gremlin_varint_size(m->local_stamp_ns);
	}
	/*@ assert s == \at(s, _A1) + \at(normfs_Stamp_local_stamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->app_start_id != 0) {
		s += 1
		   + gremlin_varint_size(m->app_start_id);
	}
	/*@ assert s == \at(s, _A2) + \at(normfs_Stamp_app_start_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert s == \at(normfs_Stamp_tsize(m), Pre); */
	/*@ assert normfs_Stamp_dsame{Pre,Here}(m); */
	/*@ ghost _Aw: ; */
	((normfs_Stamp *)m)->_size = s;
	/*@ assert normfs_Stamp_own{_Aw,Here}(m); */
	/*@ assert normfs_Stamp_dsame{_Aw,Here}(m); */
	/*@ assert normfs_Stamp_dsame{Pre,Here}(m); */
	/*@ assert normfs_Stamp_tsize(m) == \at(normfs_Stamp_tsize(m), Pre); */
	return s;
}

/*@ requires normfs_Stamp_valid(m) && normfs_Stamp_sized(m);
    requires _off + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(_buf + (_off .. _off + m->_size - 1));
    requires normfs_Stamp_bufsep(m, _buf, _off, _off + m->_size - 1);
    assigns _buf[_off .. _off + m->_size - 1];
    ensures \result == \old(_off + m->_size);
*/
__attribute__((unused)) static size_t
normfs_Stamp_encode_at(const normfs_Stamp *m, uint8_t * __restrict__ _buf, size_t _off)
{
	/*@ ghost normfs_Stamp_tsize_nonneg((normfs_Stamp *)m); */
	/*@ ghost _A0: ; */
	if (m->monotonic_stamp_ns != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 8u);
		_off = gremlin_varint_encode_at(_buf, _off, m->monotonic_stamp_ns);
	}
	/*@ assert _off == \at(_off, _A0) + \at(normfs_Stamp_monotonic_stamp_ns_size(m), Pre); */
	/*@ ghost _A1: ; */
	if (m->local_stamp_ns != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 16u);
		_off = gremlin_varint_encode_at(_buf, _off, m->local_stamp_ns);
	}
	/*@ assert _off == \at(_off, _A1) + \at(normfs_Stamp_local_stamp_ns_size(m), Pre); */
	/*@ ghost _A2: ; */
	if (m->app_start_id != 0) {
		_off = gremlin_varint32_encode_at(_buf, _off, 24u);
		_off = gremlin_varint_encode_at(_buf, _off, m->app_start_id);
	}
	/*@ assert _off == \at(_off, _A2) + \at(normfs_Stamp_app_start_id_size(m), Pre); */
	/*@ ghost _A3: ; */
	/*@ assert _off == \at(_off, Pre) + \at(normfs_Stamp_tsize(m), Pre); */
	return _off;
}

/*@ requires normfs_Stamp_valid(m) && normfs_Stamp_sized(m);
    requires \valid(w);
    requires w->offset + m->_size <= 0x7fffffffffffffff;
    requires m->_size == 0 || \valid(w->buf + (w->offset .. w->offset + m->_size - 1));
    requires normfs_Stamp_bufsep(m, w->buf, w->offset, w->offset + m->_size - 1);
    requires \separated(w, w->buf + (w->offset .. w->offset + m->_size - 1));
    assigns w->offset, w->buf[w->offset .. w->offset + m->_size - 1];
    ensures w->offset == \old(w->offset + m->_size);
*/
__attribute__((unused)) static void
normfs_Stamp_encode(const normfs_Stamp *m, struct gremlin_writer *w)
{
	w->offset = normfs_Stamp_encode_at(m, w->buf, w->offset);
}

typedef struct normfs_Stamp_reader {
	const uint8_t	*src;
	size_t		 src_len;
	struct {
		unsigned	monotonic_stamp_ns : 1;
		unsigned	local_stamp_ns : 1;
		unsigned	app_start_id : 1;
	} _has;
	uint64_t	monotonic_stamp_ns;
	uint64_t	local_stamp_ns;
	uint64_t	app_start_id;
} normfs_Stamp_reader;

/*@ predicate normfs_Stamp_reader_ok{L}(normfs_Stamp_reader *r, integer n) =
      r->src_len >= n;
*/

/*@ requires \valid(r);
    requires len == 0 || \valid_read(src + (0 .. len - 1));
    requires \separated(r, src + (0 .. len - 1));
    assigns *r;
    ensures r->src == src && r->src_len == len;
    ensures \result == GREMLIN_OK ==> normfs_Stamp_reader_ok(r, len);
    ensures \result == GREMLIN_OK || \result == GREMLIN_ERROR_TRUNCATED ||
            \result == GREMLIN_ERROR_OVERFLOW ||
            \result == GREMLIN_ERROR_INVALID_WIRE_TYPE ||
            \result == GREMLIN_ERROR_INVALID_FIELD_NUM;
*/
static inline enum gremlin_error
normfs_Stamp_reader_init(normfs_Stamp_reader *r, const uint8_t *src, size_t len)
{
	*r = (normfs_Stamp_reader){ .src = src, .src_len = len };
	size_t offset = 0;
	/*@ loop invariant 0 <= offset <= len;
	    loop invariant r->src == src && r->src_len == len;
	    loop invariant normfs_Stamp_reader_ok(r, offset);
	    loop assigns offset, *r;
	    loop variant len - offset;
	*/
	while (offset < len) {
		struct gremlin_varint32_decode_result t =
			gremlin_varint32_decode(src + offset, len - offset);
		if (t.error != GREMLIN_OK) return t.error;
		offset += t.consumed;
		if (t.value == 8u /* field 1, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->monotonic_stamp_ns = d.value;
			r->_has.monotonic_stamp_ns = true;
			continue;
		}
		if (t.value == 16u /* field 2, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->local_stamp_ns = d.value;
			r->_has.local_stamp_ns = true;
			continue;
		}
		if (t.value == 24u /* field 3, GREMLIN_WIRE_VARINT */) {
			struct gremlin_varint_decode_result d =
				gremlin_varint_decode(src + offset, len - offset);
			if (d.error != GREMLIN_OK) return d.error;
			offset += d.consumed;
			r->app_start_id = d.value;
			r->_has.app_start_id = true;
			continue;
		}
		unsigned _wt = (unsigned)(t.value % 8u);
		if (_wt == 6u || _wt == 7u) return GREMLIN_ERROR_INVALID_WIRE_TYPE;
		if (t.value / 8u == 0u) return GREMLIN_ERROR_INVALID_FIELD_NUM;
		struct gremlin_skip_result sk =
			gremlin_skip_data(src + offset, len - offset,
			                  (enum gremlin_wire_type)_wt);
		if (sk.error != GREMLIN_OK) return sk.error;
		offset += sk.consumed;
	}
	return GREMLIN_OK;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.monotonic_stamp_ns ==> \result == r->monotonic_stamp_ns;
*/
static inline uint64_t
normfs_Stamp_reader_get_monotonic_stamp_ns(const normfs_Stamp_reader *r)
{
	if (r == NULL || !r->_has.monotonic_stamp_ns) return 0;
	return r->monotonic_stamp_ns;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.local_stamp_ns ==> \result == r->local_stamp_ns;
*/
static inline uint64_t
normfs_Stamp_reader_get_local_stamp_ns(const normfs_Stamp_reader *r)
{
	if (r == NULL || !r->_has.local_stamp_ns) return 0;
	return r->local_stamp_ns;
}

/*@ requires r == \null || \valid_read(r);
    assigns \nothing;
    ensures r != \null && r->_has.app_start_id ==> \result == r->app_start_id;
*/
static inline uint64_t
normfs_Stamp_reader_get_app_start_id(const normfs_Stamp_reader *r)
{
	if (r == NULL || !r->_has.app_start_id) return 0;
	return r->app_start_id;
}

#endif
