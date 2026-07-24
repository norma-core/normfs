#ifndef NORMFS_UINTN_VARINT_H
#define NORMFS_UINTN_VARINT_H

#include <stddef.h>
#include <stdint.h>

#if defined(__GNUC__) || defined(__clang__)
#define NORMFS_UINTN_INLINE static inline __attribute__((always_inline))
#else
#define NORMFS_UINTN_INLINE static inline
#endif

enum normfs_uintn_varint_status {
	NORMFS_UINTN_VARINT_OK = 0,
	NORMFS_UINTN_VARINT_ERR_TRUNCATED = 1,
	NORMFS_UINTN_VARINT_ERR_OVERFLOW = 2,
	NORMFS_UINTN_VARINT_ERR_NON_CANONICAL = 3,
	NORMFS_UINTN_VARINT_ERR_NO_SPACE = 4
};

struct normfs_uintn_varint_encode_result {
	size_t written;
	int status;
};

struct normfs_uintn_varint32_decode_result {
	uint32_t value;
	size_t consumed;
	int status;
};

struct normfs_uintn_varint64_decode_result {
	uint64_t value;
	size_t consumed;
	int status;
};

/*@ axiomatic NormfsUintnVarintSize {
      logic integer normfs_uintn_varint32_size_logic(integer v) =
        v < 0x80 ? 1 :
        v < 0x4000 ? 2 :
        v < 0x200000 ? 3 :
        v < 0x10000000 ? 4 :
        5;

      logic integer normfs_uintn_varint64_size_logic(integer v) =
        v < 0x80 ? 1 :
        v < 0x4000 ? 2 :
        v < 0x200000 ? 3 :
        v < 0x10000000 ? 4 :
        v < 0x800000000 ? 5 :
        v < 0x40000000000 ? 6 :
        v < 0x2000000000000 ? 7 :
        v < 0x100000000000000 ? 8 :
        v < 0x8000000000000000 ? 9 :
        10;
    }
*/

/*@ assigns \nothing;
    ensures \result == normfs_uintn_varint32_size_logic(value);
    ensures 1 <= \result <= 5;
*/
NORMFS_UINTN_INLINE size_t
normfs_uintn_varint32_size(uint32_t value)
{
	if (value < 0x80u) return 1u;
	if (value < 0x4000u) return 2u;
	if (value < 0x200000u) return 3u;
	if (value < 0x10000000u) return 4u;
	return 5u;
}

/*@ assigns \nothing;
    ensures \result == normfs_uintn_varint64_size_logic(value);
    ensures 1 <= \result <= 10;
*/
NORMFS_UINTN_INLINE size_t
normfs_uintn_varint64_size(uint64_t value)
{
	if (value < 0x80ull) return 1u;
	if (value < 0x4000ull) return 2u;
	if (value < 0x200000ull) return 3u;
	if (value < 0x10000000ull) return 4u;
	if (value < 0x800000000ull) return 5u;
	if (value < 0x40000000000ull) return 6u;
	if (value < 0x2000000000000ull) return 7u;
	if (value < 0x100000000000000ull) return 8u;
	if (value < 0x8000000000000000ull) return 9u;
	return 10u;
}

/*@ requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.written == normfs_uintn_varint32_size_logic(value);
    ensures \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE ==>
              \result.written == 0;
    ensures \result.written <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            value < 0x80 ==>
              out[0] == (uint8_t)value;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x80 <= value < 0x4000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(value / 128);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x4000 <= value < 0x200000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(value / 0x4000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x200000 <= value < 0x10000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(value / 0x200000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x10000000 <= value ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(value / 0x10000000);
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint_encode_result
normfs_uintn_varint32_encode(uint32_t value, uint8_t *out, size_t out_len)
{
	struct normfs_uintn_varint_encode_result r = {
	    0u,
	    NORMFS_UINTN_VARINT_ERR_NO_SPACE
	};
	size_t n = normfs_uintn_varint32_size(value);
	if (out_len < n) return r;

	if (value < 0x80u) {
		out[0] = (uint8_t)value;
	} else if (value < 0x4000u) {
		out[0] = (uint8_t)(128u + value % 128u);
		out[1] = (uint8_t)(value / 128u);
	} else if (value < 0x200000u) {
		out[0] = (uint8_t)(128u + value % 128u);
		out[1] = (uint8_t)(128u + (value / 128u) % 128u);
		out[2] = (uint8_t)(value / 0x4000u);
	} else if (value < 0x10000000u) {
		out[0] = (uint8_t)(128u + value % 128u);
		out[1] = (uint8_t)(128u + (value / 128u) % 128u);
		out[2] = (uint8_t)(128u + (value / 0x4000u) % 128u);
		out[3] = (uint8_t)(value / 0x200000u);
	} else {
		out[0] = (uint8_t)(128u + value % 128u);
		out[1] = (uint8_t)(128u + (value / 128u) % 128u);
		out[2] = (uint8_t)(128u + (value / 0x4000u) % 128u);
		out[3] = (uint8_t)(128u + (value / 0x200000u) % 128u);
		out[4] = (uint8_t)(value / 0x10000000u);
	}

	r.written = n;
	r.status = NORMFS_UINTN_VARINT_OK;
	return r;
}

/*@ requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    assigns out[0 .. out_len - 1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.written == normfs_uintn_varint64_size_logic(value);
    ensures \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE ==>
              \result.written == 0;
    ensures \result.written <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            value < 0x80 ==>
              out[0] == (uint8_t)value;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x80 <= value < 0x4000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(value / 128);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x4000 <= value < 0x200000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(value / 0x4000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x200000 <= value < 0x10000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(value / 0x200000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x10000000 <= value < 0x800000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(value / 0x10000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x800000000 <= value < 0x40000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(value / 0x800000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x40000000000 <= value < 0x2000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(value / 0x40000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x2000000000000 <= value < 0x100000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(value / 0x2000000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x100000000000000 <= value < 0x8000000000000000 ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(128 + (value / 0x2000000000000) % 128) &&
              out[8] == (uint8_t)(value / 0x100000000000000);
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            0x8000000000000000 <= value ==>
              out[0] == (uint8_t)(128 + value % 128) &&
              out[1] == (uint8_t)(128 + (value / 128) % 128) &&
              out[2] == (uint8_t)(128 + (value / 0x4000) % 128) &&
              out[3] == (uint8_t)(128 + (value / 0x200000) % 128) &&
              out[4] == (uint8_t)(128 + (value / 0x10000000) % 128) &&
              out[5] == (uint8_t)(128 + (value / 0x800000000) % 128) &&
              out[6] == (uint8_t)(128 + (value / 0x40000000000) % 128) &&
              out[7] == (uint8_t)(128 + (value / 0x2000000000000) % 128) &&
              out[8] == (uint8_t)(128 + (value / 0x100000000000000) % 128) &&
              out[9] == (uint8_t)(value / 0x8000000000000000);
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint_encode_result
normfs_uintn_varint64_encode(uint64_t value, uint8_t *out, size_t out_len)
{
	struct normfs_uintn_varint_encode_result r = {
	    0u,
	    NORMFS_UINTN_VARINT_ERR_NO_SPACE
	};
	size_t n = normfs_uintn_varint64_size(value);
	if (out_len < n) return r;

	if (value < 0x80ull) {
		out[0] = (uint8_t)value;
	} else if (value < 0x4000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(value / 128ull);
	} else if (value < 0x200000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(value / 0x4000ull);
	} else if (value < 0x10000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(value / 0x200000ull);
	} else if (value < 0x800000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(value / 0x10000000ull);
	} else if (value < 0x40000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(value / 0x800000000ull);
	} else if (value < 0x2000000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(value / 0x40000000000ull);
	} else if (value < 0x100000000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(128ull + (value / 0x40000000000ull) % 128ull);
		out[7] = (uint8_t)(value / 0x2000000000000ull);
	} else if (value < 0x8000000000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(128ull + (value / 0x40000000000ull) % 128ull);
		out[7] = (uint8_t)(128ull + (value / 0x2000000000000ull) % 128ull);
		out[8] = (uint8_t)(value / 0x100000000000000ull);
	} else {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(128ull + (value / 0x40000000000ull) % 128ull);
		out[7] = (uint8_t)(128ull + (value / 0x2000000000000ull) % 128ull);
		out[8] = (uint8_t)(128ull + (value / 0x100000000000000ull) % 128ull);
		out[9] = (uint8_t)(value / 0x8000000000000000ull);
	}

	r.written = n;
	r.status = NORMFS_UINTN_VARINT_OK;
	return r;
}

/*@ assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.value == value &&
              \result.consumed == consumed &&
              \result.consumed == normfs_uintn_varint32_size_logic(value) &&
              1 <= \result.consumed <= 5;
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint32_decode_result
normfs_uintn_varint32_decode_ok(uint32_t value, size_t consumed)
{
	struct normfs_uintn_varint32_decode_result r;
	size_t canonical = normfs_uintn_varint32_size(value);
	if (canonical != consumed) {
		r.value = 0u;
		r.consumed = 0u;
		r.status = NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
		return r;
	}
	r.value = value;
	r.consumed = consumed;
	r.status = NORMFS_UINTN_VARINT_OK;
	return r;
}

/*@ assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.value == value &&
              \result.consumed == consumed &&
              \result.consumed == normfs_uintn_varint64_size_logic(value) &&
              1 <= \result.consumed <= 10;
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint64_decode_result
normfs_uintn_varint64_decode_ok(uint64_t value, size_t consumed)
{
	struct normfs_uintn_varint64_decode_result r;
	size_t canonical = normfs_uintn_varint64_size(value);
	if (canonical != consumed) {
		r.value = 0u;
		r.consumed = 0u;
		r.status = NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
		return r;
	}
	r.value = value;
	r.consumed = consumed;
	r.status = NORMFS_UINTN_VARINT_OK;
	return r;
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_TRUNCATED ||
            \result.status == NORMFS_UINTN_VARINT_ERR_OVERFLOW ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              1 <= \result.consumed <= 5 && \result.consumed <= len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.consumed == normfs_uintn_varint32_size_logic(\result.value);
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint32_decode_result
normfs_uintn_varint32_decode(const uint8_t *buf, size_t len)
{
	struct normfs_uintn_varint32_decode_result r = {
	    0u,
	    0u,
	    NORMFS_UINTN_VARINT_ERR_TRUNCATED
	};

	if (len < 1u) return r;
	uint8_t b0 = buf[0];
	if (b0 < 128u) return normfs_uintn_varint32_decode_ok((uint32_t)b0, 1u);
	uint32_t value = (uint32_t)b0 - 128u;

	if (len < 2u) return r;
	uint8_t b1 = buf[1];
	if (b1 < 128u) {
		value += (uint32_t)b1 * 128u;
		return normfs_uintn_varint32_decode_ok(value, 2u);
	}
	value += ((uint32_t)b1 - 128u) * 128u;

	if (len < 3u) return r;
	uint8_t b2 = buf[2];
	if (b2 < 128u) {
		value += (uint32_t)b2 * 0x4000u;
		return normfs_uintn_varint32_decode_ok(value, 3u);
	}
	value += ((uint32_t)b2 - 128u) * 0x4000u;

	if (len < 4u) return r;
	uint8_t b3 = buf[3];
	if (b3 < 128u) {
		value += (uint32_t)b3 * 0x200000u;
		return normfs_uintn_varint32_decode_ok(value, 4u);
	}
	value += ((uint32_t)b3 - 128u) * 0x200000u;

	if (len < 5u) return r;
	uint8_t b4 = buf[4];
	if (b4 >= 16u) {
		r.status = NORMFS_UINTN_VARINT_ERR_OVERFLOW;
		return r;
	}
	value += (uint32_t)b4 * 0x10000000u;
	return normfs_uintn_varint32_decode_ok(value, 5u);
}

/*@ requires len == 0 || \valid_read(buf + (0 .. len - 1));
    assigns \nothing;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ||
            \result.status == NORMFS_UINTN_VARINT_ERR_TRUNCATED ||
            \result.status == NORMFS_UINTN_VARINT_ERR_OVERFLOW ||
            \result.status == NORMFS_UINTN_VARINT_ERR_NON_CANONICAL;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              1 <= \result.consumed <= 10 && \result.consumed <= len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.consumed == normfs_uintn_varint64_size_logic(\result.value);
    ensures \result.status != NORMFS_UINTN_VARINT_OK ==>
              \result.value == 0 && \result.consumed == 0;
*/
NORMFS_UINTN_INLINE struct normfs_uintn_varint64_decode_result
normfs_uintn_varint64_decode(const uint8_t *buf, size_t len)
{
	struct normfs_uintn_varint64_decode_result r = {
	    0u,
	    0u,
	    NORMFS_UINTN_VARINT_ERR_TRUNCATED
	};
	uint64_t value = 0u;
	uint32_t shift = 0u;

	/*@ loop invariant 0 <= i <= 10;
	    loop invariant shift == 7 * i;
	    loop invariant shift <= 70;
	    loop assigns i, shift, value;
	    loop variant 10 - i;
	*/
	for (size_t i = 0u; i < 10u; i++) {
		if (i >= len) return r;
		uint8_t b = buf[i];

		if (i == 9u) {
			if (b >= 2u) {
				r.status = NORMFS_UINTN_VARINT_ERR_OVERFLOW;
				return r;
			}
			value += (uint64_t)b << shift;
			return normfs_uintn_varint64_decode_ok(value, 10u);
		}

		if (b < 128u) {
			value += (uint64_t)b << shift;
			return normfs_uintn_varint64_decode_ok(value, i + 1u);
		}

		value += ((uint64_t)b - 128ull) << shift;
		shift += 7u;
	}

	r.status = NORMFS_UINTN_VARINT_ERR_OVERFLOW;
	return r;
}

size_t normfs_uintn_varint32_size_ffi(uint32_t value);
size_t normfs_uintn_varint64_size_ffi(uint64_t value);

struct normfs_uintn_varint_encode_result
normfs_uintn_varint32_encode_ffi(uint32_t value, uint8_t *out, size_t out_len);
struct normfs_uintn_varint_encode_result
normfs_uintn_varint64_encode_ffi(uint64_t value, uint8_t *out, size_t out_len);

struct normfs_uintn_varint32_decode_result
normfs_uintn_varint32_decode_ffi(const uint8_t *buf, size_t len);
struct normfs_uintn_varint64_decode_result
normfs_uintn_varint64_decode_ffi(const uint8_t *buf, size_t len);

#endif /* NORMFS_UINTN_VARINT_H */
