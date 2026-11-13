#include "normfs/chacha20.h"

const uint32_t normfs_chacha20_sigma[4] = {
	0x61707865U, 0x3320646EU, 0x79622D32U, 0x6B206574U
};

/*@ requires 0 < n < 32;
    assigns \nothing;
    ensures \result == normfs_chacha20_rotl(x, n);
*/
static uint32_t
normfs_chacha20_rotl_fn(uint32_t x, unsigned int n)
{
	return (uint32_t)(x << n) | (x >> (32u - n));
}

/*
 * Written with *, / and % so what is proved is what the bytes mean rather than
 * that the C echoes the spec, and so it holds on any host byte order.
 * uintn/le.h has no le32; wal_entry.c carries a local pair for the same reason.
 */
/*@ requires \valid_read(p + (0 .. 3));
    assigns \nothing;
    ensures \result == (uint32_t)normfs_chacha20_le32(p);
*/
static uint32_t
normfs_chacha20_le32_read(const uint8_t *p)
{
	return (uint32_t)p[0] + (uint32_t)p[1] * 256u +
	    (uint32_t)p[2] * 65536u + (uint32_t)p[3] * 16777216u;
}

/*@ requires \valid(p + (0 .. 3));
    assigns p[0 .. 3];
    ensures normfs_chacha20_le32(p) == value;
    ensures p[0] == (uint8_t)(value % 256);
    ensures p[1] == (uint8_t)((value / 256) % 256);
    ensures p[2] == (uint8_t)((value / 65536) % 256);
    ensures p[3] == (uint8_t)(value / 16777216);
*/
static void
normfs_chacha20_le32_write(uint8_t *p, uint32_t value)
{
	uint32_t b0 = value % 256u;
	uint32_t r1 = value / 256u;
	uint32_t b1 = r1 % 256u;
	uint32_t r2 = r1 / 256u;
	uint32_t b2 = r2 % 256u;
	uint32_t b3 = r2 / 256u;

	/*@ assert value == b0 + 256 * r1; */
	/*@ assert r1 == b1 + 256 * r2; */
	/*@ assert r2 == b2 + 256 * b3; */
	/*@ assert b3 < 256; */
	/*@ assert value == b0 + 256 * b1 + 65536 * b2 + 16777216 * b3; */

	p[0] = (uint8_t)b0;
	p[1] = (uint8_t)b1;
	p[2] = (uint8_t)b2;
	p[3] = (uint8_t)b3;
}

/*
 * Four values in and out rather than the state array and four indices: that
 * form would put a pairwise separation precondition and four symbolic offsets
 * into every goal. Every call site passes literals; opt_level(3) inlines it.
 */
struct normfs_chacha20_qr {
	uint32_t a;
	uint32_t b;
	uint32_t c;
	uint32_t d;
};

/*@ assigns \nothing;
    ensures \result.a == (uint32_t)(
      (uint32_t)(a + b) + normfs_chacha20_rotl((uint32_t)(b ^ (uint32_t)(
        (uint32_t)(c + normfs_chacha20_rotl((uint32_t)(d ^ (uint32_t)(a + b)),
          16)))), 12));
*/
static struct normfs_chacha20_qr
normfs_chacha20_quarter(uint32_t a, uint32_t b, uint32_t c, uint32_t d)
{
	struct normfs_chacha20_qr r;
	uint32_t a1 = (uint32_t)(a + b);
	uint32_t d1 = normfs_chacha20_rotl_fn((uint32_t)(d ^ a1), 16u);
	uint32_t c1 = (uint32_t)(c + d1);
	uint32_t b1 = normfs_chacha20_rotl_fn((uint32_t)(b ^ c1), 12u);
	uint32_t a2 = (uint32_t)(a1 + b1);
	uint32_t d2 = normfs_chacha20_rotl_fn((uint32_t)(d1 ^ a2), 8u);
	uint32_t c2 = (uint32_t)(c1 + d2);
	uint32_t b2 = normfs_chacha20_rotl_fn((uint32_t)(b1 ^ c2), 7u);

	r.a = a2;
	r.b = b2;
	r.c = c2;
	r.d = d2;
	return r;
}

/*@ requires \valid(x + (0 .. 15));
    requires 0 <= ia < 16 && 0 <= ib < 16 && 0 <= ic < 16 && 0 <= id < 16;
    assigns x[ia], x[ib], x[ic], x[id];
*/
static void
normfs_chacha20_qr_at(uint32_t *x, size_t ia, size_t ib, size_t ic, size_t id)
{
	struct normfs_chacha20_qr r =
	    normfs_chacha20_quarter(x[ia], x[ib], x[ic], x[id]);

	x[ia] = r.a;
	x[ib] = r.b;
	x[ic] = r.c;
	x[id] = r.d;
}

void
normfs_chacha20_block(const uint8_t *key, const uint8_t *nonce,
    uint32_t counter, uint8_t *out)
{
	uint32_t st[16];
	uint32_t x[16];
	size_t i;

	/*@ loop invariant 0 <= i <= 4;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     st[j] == normfs_chacha20_sigma[j];
	    loop assigns i, st[0 .. 3];
	    loop variant 4 - i;
	*/
	for (i = 0u; i < 4u; i++)
		st[i] = normfs_chacha20_sigma[i];

	/*@ loop invariant 0 <= i <= 8;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     st[4 + j] ==
	                       (uint32_t)normfs_chacha20_le32(key + 4 * j);
	    loop assigns i, st[4 .. 11];
	    loop variant 8 - i;
	*/
	for (i = 0u; i < 8u; i++)
		st[4u + i] = normfs_chacha20_le32_read(key + 4u * i);

	st[12] = counter;

	/*@ loop invariant 0 <= i <= 3;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     st[13 + j] ==
	                       (uint32_t)normfs_chacha20_le32(nonce + 4 * j);
	    loop assigns i, st[13 .. 15];
	    loop variant 3 - i;
	*/
	for (i = 0u; i < 3u; i++)
		st[13u + i] = normfs_chacha20_le32_read(nonce + 4u * i);

	/*@ loop invariant 0 <= i <= 16;
	    loop invariant \forall integer j; 0 <= j < i ==> x[j] == st[j];
	    loop assigns i, x[0 .. 15];
	    loop variant 16 - i;
	*/
	for (i = 0u; i < 16u; i++)
		x[i] = st[i];

	/* Ten double rounds, not two hundred unrolled quarter rounds: smoke
	 * test goals scale with statement count. */
	/*@ loop invariant 0 <= i <= 10;
	    loop assigns i, x[0 .. 15];
	    loop variant 10 - i;
	*/
	for (i = 0u; i < 10u; i++) {
		normfs_chacha20_qr_at(x, 0u, 4u, 8u, 12u);
		normfs_chacha20_qr_at(x, 1u, 5u, 9u, 13u);
		normfs_chacha20_qr_at(x, 2u, 6u, 10u, 14u);
		normfs_chacha20_qr_at(x, 3u, 7u, 11u, 15u);
		normfs_chacha20_qr_at(x, 0u, 5u, 10u, 15u);
		normfs_chacha20_qr_at(x, 1u, 6u, 11u, 12u);
		normfs_chacha20_qr_at(x, 2u, 7u, 8u, 13u);
		normfs_chacha20_qr_at(x, 3u, 4u, 9u, 14u);
	}

	/*@ loop invariant 0 <= i <= 16;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     normfs_chacha20_le32(out + 4 * j) ==
	                       (uint32_t)(x[j] + st[j]);
	    loop assigns i, out[0 .. NORMFS_CHACHA20_BLOCK - 1];
	    loop variant 16 - i;
	*/
	for (i = 0u; i < 16u; i++)
		normfs_chacha20_le32_write(out + 4u * i,
		    (uint32_t)(x[i] + st[i]));
}
