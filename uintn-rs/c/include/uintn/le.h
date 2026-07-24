#ifndef NORMFS_UINTN_LE_H
#define NORMFS_UINTN_LE_H

#include <stddef.h>
#include <stdint.h>

#if defined(__GNUC__) || defined(__clang__)
#define NORMFS_UINTN_LE_INLINE static inline __attribute__((always_inline))
#else
#define NORMFS_UINTN_LE_INLINE static inline
#endif

/*
 * Fixed width little-endian integers. Used for on-disk fields whose encoding
 * predates the varints and therefore cannot change, such as the WAL and store
 * header version words.
 *
 * These are written with *, / and % rather than <<, >> and |: the provers
 * cannot relate bitwise operators to arithmetic, which is why varint.h avoids
 * them as well.
 */

/*@ axiomatic NormfsUintnLe {
      logic integer normfs_uintn_le64_logic{L}(uint8_t *p) =
        p[0] +
        256 * p[1] +
        65536 * p[2] +
        16777216 * p[3] +
        4294967296 * p[4] +
        1099511627776 * p[5] +
        281474976710656 * p[6] +
        72057594037927936 * p[7];
    }
*/

/*@ requires \valid_read(p + (0 .. 7));
    assigns \nothing;
    ensures \result == normfs_uintn_le64_logic(p);
*/
NORMFS_UINTN_LE_INLINE uint64_t
normfs_uintn_le64_read(const uint8_t *p)
{
	return (uint64_t)p[0] +
	    (uint64_t)p[1] * 256ull +
	    (uint64_t)p[2] * 65536ull +
	    (uint64_t)p[3] * 16777216ull +
	    (uint64_t)p[4] * 4294967296ull +
	    (uint64_t)p[5] * 1099511627776ull +
	    (uint64_t)p[6] * 281474976710656ull +
	    (uint64_t)p[7] * 72057594037927936ull;
}

/*@ requires \valid(p + (0 .. 7));
    assigns p[0 .. 7];
    ensures normfs_uintn_le64_logic(p) == value;
    ensures p[0] == (uint8_t)(value % 256);
    ensures p[1] == (uint8_t)((value / 256) % 256);
    ensures p[2] == (uint8_t)((value / 65536) % 256);
    ensures p[3] == (uint8_t)((value / 16777216) % 256);
    ensures p[4] == (uint8_t)((value / 4294967296) % 256);
    ensures p[5] == (uint8_t)((value / 1099511627776) % 256);
    ensures p[6] == (uint8_t)((value / 281474976710656) % 256);
    ensures p[7] == (uint8_t)(value / 72057594037927936);
*/
NORMFS_UINTN_LE_INLINE void
normfs_uintn_le64_write(uint8_t *p, uint64_t value)
{
	uint64_t b0 = value % 256ull;
	uint64_t r0 = value / 256ull;
	uint64_t b1 = r0 % 256ull;
	uint64_t r1 = r0 / 256ull;
	uint64_t b2 = r1 % 256ull;
	uint64_t r2 = r1 / 256ull;
	uint64_t b3 = r2 % 256ull;
	uint64_t r3 = r2 / 256ull;
	uint64_t b4 = r3 % 256ull;
	uint64_t r4 = r3 / 256ull;
	uint64_t b5 = r4 % 256ull;
	uint64_t r5 = r4 / 256ull;
	uint64_t b6 = r5 % 256ull;
	uint64_t b7 = r5 / 256ull;

	/*@ assert value == b0 + 256 * r0; */
	/*@ assert r0 == b1 + 256 * r1; */
	/*@ assert r1 == b2 + 256 * r2; */
	/*@ assert r2 == b3 + 256 * r3; */
	/*@ assert r3 == b4 + 256 * r4; */
	/*@ assert r4 == b5 + 256 * r5; */
	/*@ assert r5 == b6 + 256 * b7; */
	/*@ assert b7 < 256; */
	/*@ assert value == b0 + 256 * b1 + 65536 * b2 + 16777216 * b3 +
	                    4294967296 * b4 + 1099511627776 * b5 +
	                    281474976710656 * b6 + 72057594037927936 * b7; */

	p[0] = (uint8_t)b0;
	p[1] = (uint8_t)b1;
	p[2] = (uint8_t)b2;
	p[3] = (uint8_t)b3;
	p[4] = (uint8_t)b4;
	p[5] = (uint8_t)b5;
	p[6] = (uint8_t)b6;
	p[7] = (uint8_t)b7;
}

#endif /* NORMFS_UINTN_LE_H */
