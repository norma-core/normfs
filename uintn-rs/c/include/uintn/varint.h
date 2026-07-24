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

/*
 * Closed form for the bytes of an encoded varint, so a caller can pin the
 * encoding of a field in one clause instead of enumerating every width. All
 * of these are definitions, not axioms.
 */
/*@ axiomatic NormfsUintnVarintPow {
      logic integer normfs_uintn_pow128(integer k) =
        k <= 0 ? 1 :
        k == 1 ? 128 :
        k == 2 ? 0x4000 :
        k == 3 ? 0x200000 :
        k == 4 ? 0x10000000 :
        k == 5 ? 0x800000000 :
        k == 6 ? 0x40000000000 :
        k == 7 ? 0x2000000000000 :
        k == 8 ? 0x100000000000000 :
        0x8000000000000000;

      logic integer normfs_uintn_varint64_byte(integer v, integer k) =
        k == normfs_uintn_varint64_size_logic(v) - 1
          ? v / normfs_uintn_pow128(k)
          : 128 + (v / normfs_uintn_pow128(k)) % 128;
    }
*/

/*
 * Byte level readings of an encoded varint. These let a caller state what a
 * decoded field is in terms of the bytes it came from, without enumerating
 * every combination of widths when several varints sit back to back. Both are
 * definitions, not axioms.
 */
/*@ axiomatic NormfsUintnVarintBytes {
      logic integer normfs_uintn_varint64_len{L}(uint8_t *p) =
        p[0] < 128 ? 1 :
        p[1] < 128 ? 2 :
        p[2] < 128 ? 3 :
        p[3] < 128 ? 4 :
        p[4] < 128 ? 5 :
        p[5] < 128 ? 6 :
        p[6] < 128 ? 7 :
        p[7] < 128 ? 8 :
        p[8] < 128 ? 9 :
        10;

      logic integer normfs_uintn_varint64_value{L}(uint8_t *p) =
        p[0] < 128 ? p[0] :
        p[1] < 128 ? (p[0] - 128) + 128 * p[1] :
        p[2] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) + 16384 * p[2] :
        p[3] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * p[3] :
        p[4] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * p[4] :
        p[5] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * (p[4] - 128) + 34359738368 * p[5] :
        p[6] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * (p[4] - 128) + 34359738368 * (p[5] - 128) +
                     4398046511104 * p[6] :
        p[7] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * (p[4] - 128) + 34359738368 * (p[5] - 128) +
                     4398046511104 * (p[6] - 128) + 562949953421312 * p[7] :
        p[8] < 128 ? (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * (p[4] - 128) + 34359738368 * (p[5] - 128) +
                     4398046511104 * (p[6] - 128) +
                     562949953421312 * (p[7] - 128) +
                     72057594037927936 * p[8] :
                     (p[0] - 128) + 128 * (p[1] - 128) +
                     16384 * (p[2] - 128) + 2097152 * (p[3] - 128) +
                     268435456 * (p[4] - 128) + 34359738368 * (p[5] - 128) +
                     4398046511104 * (p[6] - 128) +
                     562949953421312 * (p[7] - 128) +
                     72057594037927936 * (p[8] - 128) +
                     9223372036854775808 * p[9];

      // p holds the canonical encoding of some u64: its byte length is the
      // canonical size of the value it spells, and it does not overflow u64.
      predicate normfs_uintn_varint64_canonical{L}(uint8_t *p) =
        normfs_uintn_varint64_len(p) ==
          normfs_uintn_varint64_size_logic(normfs_uintn_varint64_value(p)) &&
        (normfs_uintn_varint64_len(p) < 10 || p[9] < 2);
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
    ensures \result.status == NORMFS_UINTN_VARINT_OK <==>
              normfs_uintn_varint64_size_logic(value) <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.written == normfs_uintn_varint64_size_logic(value);
    ensures \result.status == NORMFS_UINTN_VARINT_ERR_NO_SPACE ==>
              \result.written == 0;
    ensures \result.written <= out_len;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              normfs_uintn_varint64_len(out) == \result.written &&
              normfs_uintn_varint64_value(out) == value;
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \forall integer k; 0 <= k < \result.written ==>
                out[k] == normfs_uintn_varint64_byte(value, k);
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

	/*
	 * The successive base 128 digits of value, spelled out so the provers
	 * can reassemble value from the bytes written below. Each pair is the
	 * Euclidean identity a == a % 128 + 128 * (a / 128) together with the
	 * division composition (a / 128^k) / 128 == a / 128^(k+1).
	 */
	/*@ assert value == value % 128 + 128 * (value / 128); */
	/*@ assert (value / 128) / 128 == value / 0x4000; */
	/*@ assert value / 128 == (value / 128) % 128 + 128 * (value / 0x4000); */
	/*@ assert (value / 0x4000) / 128 == value / 0x200000; */
	/*@ assert value / 0x4000 ==
	           (value / 0x4000) % 128 + 128 * (value / 0x200000); */
	/*@ assert (value / 0x200000) / 128 == value / 0x10000000; */
	/*@ assert value / 0x200000 ==
	           (value / 0x200000) % 128 + 128 * (value / 0x10000000); */
	/*@ assert (value / 0x10000000) / 128 == value / 0x800000000; */
	/*@ assert value / 0x10000000 ==
	           (value / 0x10000000) % 128 + 128 * (value / 0x800000000); */
	/*@ assert (value / 0x800000000) / 128 == value / 0x40000000000; */
	/*@ assert value / 0x800000000 ==
	           (value / 0x800000000) % 128 + 128 * (value / 0x40000000000); */
	/*@ assert (value / 0x40000000000) / 128 == value / 0x2000000000000; */
	/*@ assert value / 0x40000000000 ==
	           (value / 0x40000000000) % 128 +
	           128 * (value / 0x2000000000000); */
	/*@ assert (value / 0x2000000000000) / 128 == value / 0x100000000000000; */
	/*@ assert value / 0x2000000000000 ==
	           (value / 0x2000000000000) % 128 +
	           128 * (value / 0x100000000000000); */
	/*@ assert (value / 0x100000000000000) / 128 ==
	           value / 0x8000000000000000; */
	/*@ assert value / 0x100000000000000 ==
	           (value / 0x100000000000000) % 128 +
	           128 * (value / 0x8000000000000000); */

	if (value < 0x80ull) {
		out[0] = (uint8_t)value;
		/*@ assert normfs_uintn_varint64_len(out) == 1; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x4000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(value / 128ull);
		/*@ assert normfs_uintn_varint64_len(out) == 2; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x200000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(value / 0x4000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 3; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x10000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(value / 0x200000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 4; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x800000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(value / 0x10000000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 5; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x40000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(value / 0x800000000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 6; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x2000000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(value / 0x40000000000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 7; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
	} else if (value < 0x100000000000000ull) {
		out[0] = (uint8_t)(128ull + value % 128ull);
		out[1] = (uint8_t)(128ull + (value / 128ull) % 128ull);
		out[2] = (uint8_t)(128ull + (value / 0x4000ull) % 128ull);
		out[3] = (uint8_t)(128ull + (value / 0x200000ull) % 128ull);
		out[4] = (uint8_t)(128ull + (value / 0x10000000ull) % 128ull);
		out[5] = (uint8_t)(128ull + (value / 0x800000000ull) % 128ull);
		out[6] = (uint8_t)(128ull + (value / 0x40000000000ull) % 128ull);
		out[7] = (uint8_t)(value / 0x2000000000000ull);
		/*@ assert normfs_uintn_varint64_len(out) == 8; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
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
		/*@ assert normfs_uintn_varint64_len(out) == 9; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
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
		/*@ assert normfs_uintn_varint64_len(out) == 10; */
		/*@ assert normfs_uintn_varint64_value(out) == value; */
		/*@ assert \forall integer k; 0 <= k < n ==>
		             out[k] == normfs_uintn_varint64_byte(value, k); */
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
    ensures \result.status == NORMFS_UINTN_VARINT_OK <==>
              consumed == normfs_uintn_varint64_size_logic(value);
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
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 1 ==>
              buf[0] < 128 &&
              \result.value == buf[0];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 2 ==>
              buf[0] >= 128 && buf[1] < 128 &&
              \result.value == (buf[0] - 128) + 128 * buf[1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 3 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * buf[2];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 4 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 && buf[3] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * buf[3];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 5 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] < 16 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * buf[4];
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
    ensures \result.status == NORMFS_UINTN_VARINT_OK ==>
              \result.consumed == normfs_uintn_varint64_len(buf) &&
              \result.value == normfs_uintn_varint64_value(buf);
    // completeness: a canonical encoding with enough bytes always decodes
    ensures (normfs_uintn_varint64_len(buf) <= len &&
             normfs_uintn_varint64_canonical(buf)) ==>
              \result.status == NORMFS_UINTN_VARINT_OK;
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 1 ==>
              buf[0] < 128 &&
              \result.value == buf[0];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 2 ==>
              buf[0] >= 128 && buf[1] < 128 &&
              \result.value == (buf[0] - 128) + 128 * buf[1];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 3 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * buf[2];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 4 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 && buf[3] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * buf[3];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 5 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * buf[4];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 6 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] >= 128 && buf[5] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * (buf[4] - 128) + 34359738368 * buf[5];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 7 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] >= 128 && buf[5] >= 128 && buf[6] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * (buf[4] - 128) +
                               34359738368 * (buf[5] - 128) +
                               4398046511104 * buf[6];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 8 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] >= 128 && buf[5] >= 128 &&
              buf[6] >= 128 && buf[7] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * (buf[4] - 128) +
                               34359738368 * (buf[5] - 128) +
                               4398046511104 * (buf[6] - 128) +
                               562949953421312 * buf[7];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 9 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] >= 128 && buf[5] >= 128 &&
              buf[6] >= 128 && buf[7] >= 128 && buf[8] < 128 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * (buf[4] - 128) +
                               34359738368 * (buf[5] - 128) +
                               4398046511104 * (buf[6] - 128) +
                               562949953421312 * (buf[7] - 128) +
                               72057594037927936 * buf[8];
    ensures \result.status == NORMFS_UINTN_VARINT_OK &&
            \result.consumed == 10 ==>
              buf[0] >= 128 && buf[1] >= 128 && buf[2] >= 128 &&
              buf[3] >= 128 && buf[4] >= 128 && buf[5] >= 128 &&
              buf[6] >= 128 && buf[7] >= 128 && buf[8] >= 128 && buf[9] < 2 &&
              \result.value == (buf[0] - 128) + 128 * (buf[1] - 128) +
                               16384 * (buf[2] - 128) + 2097152 * (buf[3] - 128) +
                               268435456 * (buf[4] - 128) +
                               34359738368 * (buf[5] - 128) +
                               4398046511104 * (buf[6] - 128) +
                               562949953421312 * (buf[7] - 128) +
                               72057594037927936 * (buf[8] - 128) +
                               9223372036854775808 * buf[9];
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

	uint64_t value;

	if (len < 1u) return r;
	uint8_t b0 = buf[0];
	if (b0 < 128u) {
		/*@ assert (uint64_t)b0 == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 1; */
		return normfs_uintn_varint64_decode_ok((uint64_t)b0, 1u);
	}
	value = (uint64_t)b0 - 128ull;

	if (len < 2u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 2; */
		return r;
	}
	uint8_t b1 = buf[1];
	if (b1 < 128u) {
		value += (uint64_t)b1 * 128ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 2; */
		return normfs_uintn_varint64_decode_ok(value, 2u);
	}
	value += ((uint64_t)b1 - 128ull) * 128ull;

	if (len < 3u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 3; */
		return r;
	}
	uint8_t b2 = buf[2];
	if (b2 < 128u) {
		value += (uint64_t)b2 * 16384ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 3; */
		return normfs_uintn_varint64_decode_ok(value, 3u);
	}
	value += ((uint64_t)b2 - 128ull) * 16384ull;

	if (len < 4u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 4; */
		return r;
	}
	uint8_t b3 = buf[3];
	if (b3 < 128u) {
		value += (uint64_t)b3 * 2097152ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 4; */
		return normfs_uintn_varint64_decode_ok(value, 4u);
	}
	value += ((uint64_t)b3 - 128ull) * 2097152ull;

	if (len < 5u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 5; */
		return r;
	}
	uint8_t b4 = buf[4];
	if (b4 < 128u) {
		value += (uint64_t)b4 * 268435456ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 5; */
		return normfs_uintn_varint64_decode_ok(value, 5u);
	}
	value += ((uint64_t)b4 - 128ull) * 268435456ull;

	if (len < 6u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 6; */
		return r;
	}
	uint8_t b5 = buf[5];
	if (b5 < 128u) {
		value += (uint64_t)b5 * 34359738368ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 6; */
		return normfs_uintn_varint64_decode_ok(value, 6u);
	}
	value += ((uint64_t)b5 - 128ull) * 34359738368ull;

	if (len < 7u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 7; */
		return r;
	}
	uint8_t b6 = buf[6];
	if (b6 < 128u) {
		value += (uint64_t)b6 * 4398046511104ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 7; */
		return normfs_uintn_varint64_decode_ok(value, 7u);
	}
	value += ((uint64_t)b6 - 128ull) * 4398046511104ull;

	if (len < 8u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 8; */
		return r;
	}
	uint8_t b7 = buf[7];
	if (b7 < 128u) {
		value += (uint64_t)b7 * 562949953421312ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 8; */
		return normfs_uintn_varint64_decode_ok(value, 8u);
	}
	value += ((uint64_t)b7 - 128ull) * 562949953421312ull;

	if (len < 9u) {
		/*@ assert normfs_uintn_varint64_len(buf) >= 9; */
		return r;
	}
	uint8_t b8 = buf[8];
	if (b8 < 128u) {
		value += (uint64_t)b8 * 72057594037927936ull;
		/*@ assert value == normfs_uintn_varint64_value(buf); */
		/*@ assert normfs_uintn_varint64_len(buf) == 9; */
		return normfs_uintn_varint64_decode_ok(value, 9u);
	}
	value += ((uint64_t)b8 - 128ull) * 72057594037927936ull;

	if (len < 10u) return r;
	uint8_t b9 = buf[9];
	/*@ assert normfs_uintn_varint64_len(buf) == 10; */
	if (b9 >= 2u) {
		r.status = NORMFS_UINTN_VARINT_ERR_OVERFLOW;
		return r;
	}
	value += (uint64_t)b9 * 9223372036854775808ull;
	/*@ assert value == normfs_uintn_varint64_value(buf); */
	return normfs_uintn_varint64_decode_ok(value, 10u);
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
