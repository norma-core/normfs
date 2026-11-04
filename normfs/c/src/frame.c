#include "normfs/frame.h"

/*@ requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_FRAME_OK ||
            \result.status == NORMFS_FRAME_ERR_BAD_LENGTH ||
            \result.status == NORMFS_FRAME_ERR_TOO_LARGE;
    ensures \result.status == NORMFS_FRAME_ERR_BAD_LENGTH <==>
              out_len != NORMFS_FRAME_HEADER_SIZE;
    ensures \result.status == NORMFS_FRAME_OK <==>
              (out_len == NORMFS_FRAME_HEADER_SIZE &&
               payload_len <= NORMFS_FRAME_MAX_PAYLOAD);
    ensures \result.status != NORMFS_FRAME_OK ==>
              \result.written == 0 &&
              \forall integer i; 0 <= i < out_len ==> out[i] == \old(out[i]);
    ensures \result.status == NORMFS_FRAME_OK ==>
              \result.written == NORMFS_FRAME_HEADER_SIZE &&
              normfs_uintn_le64_logic(out) == payload_len;
*/
struct normfs_frame_encode_result
normfs_frame_header_encode(uint64_t payload_len, uint8_t *out, size_t out_len)
{
	struct normfs_frame_encode_result r = {0u, NORMFS_FRAME_ERR_BAD_LENGTH};

	if (out_len != NORMFS_FRAME_HEADER_SIZE) return r;
	if (payload_len > NORMFS_FRAME_MAX_PAYLOAD) {
		r.status = NORMFS_FRAME_ERR_TOO_LARGE;
		return r;
	}

	normfs_uintn_le64_write(out, payload_len);
	r.written = NORMFS_FRAME_HEADER_SIZE;
	r.status = NORMFS_FRAME_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_FRAME_OK ||
            \result.status == NORMFS_FRAME_ERR_BAD_LENGTH ||
            \result.status == NORMFS_FRAME_ERR_TOO_LARGE;
    ensures len != NORMFS_FRAME_HEADER_SIZE ==>
              \result.status == NORMFS_FRAME_ERR_BAD_LENGTH &&
              \result.payload_len == 0;
    // filled in for a too large header too, so the caller can report it
    ensures len == NORMFS_FRAME_HEADER_SIZE ==>
              \result.payload_len == normfs_uintn_le64_logic(buf);
    ensures \result.status == NORMFS_FRAME_OK <==>
              (len == NORMFS_FRAME_HEADER_SIZE &&
               normfs_uintn_le64_logic(buf) <= NORMFS_FRAME_MAX_PAYLOAD);
    ensures \result.status == NORMFS_FRAME_OK ==>
              \result.consumed == NORMFS_FRAME_HEADER_SIZE;
    ensures \result.status != NORMFS_FRAME_OK ==> \result.consumed == 0;
*/
struct normfs_frame_decode_result
normfs_frame_header_decode(const uint8_t *buf, size_t len)
{
	struct normfs_frame_decode_result r = {0u, 0u, NORMFS_FRAME_ERR_BAD_LENGTH};

	if (len != NORMFS_FRAME_HEADER_SIZE) return r;

	r.payload_len = normfs_uintn_le64_read(buf);
	if (r.payload_len > NORMFS_FRAME_MAX_PAYLOAD) {
		r.status = NORMFS_FRAME_ERR_TOO_LARGE;
		return r;
	}

	r.consumed = NORMFS_FRAME_HEADER_SIZE;
	r.status = NORMFS_FRAME_OK;
	return r;
}

/*@ assigns \nothing;
    ensures \result == 1 <==> payload_len <= NORMFS_FRAME_MAX_PAYLOAD;
*/
int
normfs_frame_header_roundtrip_holds(uint64_t payload_len)
{
	uint8_t buf[NORMFS_FRAME_HEADER_SIZE];
	struct normfs_frame_encode_result enc;
	struct normfs_frame_decode_result dec;

	enc = normfs_frame_header_encode(payload_len, buf, sizeof(buf));
	if (enc.status != NORMFS_FRAME_OK) return 0;

	dec = normfs_frame_header_decode(buf, enc.written);

	int ok = (dec.status == NORMFS_FRAME_OK);
	ok &= (dec.consumed == enc.written);
	ok &= (dec.payload_len == payload_len);
	return ok;
}
