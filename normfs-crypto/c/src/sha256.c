#include "normfs/sha256.h"

/* Proved by verify-sha256. Must not include a system header. */

const uint32_t normfs_sha256_k[64] = {
	0x428A2F98U, 0x71374491U, 0xB5C0FBCFU, 0xE9B5DBA5U,
	0x3956C25BU, 0x59F111F1U, 0x923F82A4U, 0xAB1C5ED5U,
	0xD807AA98U, 0x12835B01U, 0x243185BEU, 0x550C7DC3U,
	0x72BE5D74U, 0x80DEB1FEU, 0x9BDC06A7U, 0xC19BF174U,
	0xE49B69C1U, 0xEFBE4786U, 0x0FC19DC6U, 0x240CA1CCU,
	0x2DE92C6FU, 0x4A7484AAU, 0x5CB0A9DCU, 0x76F988DAU,
	0x983E5152U, 0xA831C66DU, 0xB00327C8U, 0xBF597FC7U,
	0xC6E00BF3U, 0xD5A79147U, 0x06CA6351U, 0x14292967U,
	0x27B70A85U, 0x2E1B2138U, 0x4D2C6DFCU, 0x53380D13U,
	0x650A7354U, 0x766A0ABBU, 0x81C2C92EU, 0x92722C85U,
	0xA2BFE8A1U, 0xA81A664BU, 0xC24B8B70U, 0xC76C51A3U,
	0xD192E819U, 0xD6990624U, 0xF40E3585U, 0x106AA070U,
	0x19A4C116U, 0x1E376C08U, 0x2748774CU, 0x34B0BCB5U,
	0x391C0CB3U, 0x4ED8AA4AU, 0x5B9CCA4FU, 0x682E6FF3U,
	0x748F82EEU, 0x78A5636FU, 0x84C87814U, 0x8CC70208U,
	0x90BEFFFAU, 0xA4506CEBU, 0xBEF9A3F7U, 0xC67178F2U
};

const uint32_t normfs_sha256_h0[8] = {
	0x6A09E667U, 0xBB67AE85U, 0x3C6EF372U, 0xA54FF53AU,
	0x510E527FU, 0x9B05688CU, 0x1F83D9ABU, 0x5BE0CD19U
};

/*@ requires 0 < n < 32;
    assigns \nothing;
    ensures \result == normfs_sha256_rotr(x, n);
*/
static uint32_t
normfs_sha256_rotr_fn(uint32_t x, unsigned int n)
{
	return (x >> n) | (uint32_t)(x << (32u - n));
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_ch(x, y, z);
*/
static uint32_t
normfs_sha256_ch_fn(uint32_t x, uint32_t y, uint32_t z)
{
	return z ^ (x & (y ^ z));
}

/*
 * The one expression here the provers do not close in a single step: three ands
 * under two xors. Naming each and gives them one land per goal. The FIPS form
 * is kept rather than the cheaper (x & y) ^ (z & (x ^ y)) because the two
 * cannot be proved equal -- that needs the bit level algebra this file is
 * arranged to avoid -- and a header a reviewer can diff against FIPS 180-4 is
 * worth three statements.
 */
/*@ assigns \nothing;
    ensures \result == normfs_sha256_maj(x, y, z);
*/
static uint32_t
normfs_sha256_maj_fn(uint32_t x, uint32_t y, uint32_t z)
{
	uint32_t xy = x & y;
	uint32_t xz = x & z;
	uint32_t yz = y & z;

	/*@ assert xy == normfs_sha256_and(x, y); */
	/*@ assert xz == normfs_sha256_and(x, z); */
	/*@ assert yz == normfs_sha256_and(y, z); */
	return xy ^ xz ^ yz;
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_bsig0(x);
*/
static uint32_t
normfs_sha256_bsig0_fn(uint32_t x)
{
	return normfs_sha256_rotr_fn(x, 2u) ^ normfs_sha256_rotr_fn(x, 13u) ^
	    normfs_sha256_rotr_fn(x, 22u);
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_bsig1(x);
*/
static uint32_t
normfs_sha256_bsig1_fn(uint32_t x)
{
	return normfs_sha256_rotr_fn(x, 6u) ^ normfs_sha256_rotr_fn(x, 11u) ^
	    normfs_sha256_rotr_fn(x, 25u);
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_ssig0(x);
*/
static uint32_t
normfs_sha256_ssig0_fn(uint32_t x)
{
	return normfs_sha256_rotr_fn(x, 7u) ^ normfs_sha256_rotr_fn(x, 18u) ^
	    (x >> 3);
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_ssig1(x);
*/
static uint32_t
normfs_sha256_ssig1_fn(uint32_t x)
{
	return normfs_sha256_rotr_fn(x, 17u) ^ normfs_sha256_rotr_fn(x, 19u) ^
	    (x >> 10);
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_t1(e, f, g, h, k, w);
*/
static uint32_t
normfs_sha256_t1_fn(uint32_t e, uint32_t f, uint32_t g, uint32_t h,
    uint32_t k, uint32_t w)
{
	return (uint32_t)((uint32_t)((uint32_t)((uint32_t)(h +
	    normfs_sha256_bsig1_fn(e)) + normfs_sha256_ch_fn(e, f, g)) + k) + w);
}

/*@ assigns \nothing;
    ensures \result == normfs_sha256_t2(a, b, c);
*/
static uint32_t
normfs_sha256_t2_fn(uint32_t a, uint32_t b, uint32_t c)
{
	return (uint32_t)(normfs_sha256_bsig0_fn(a) +
	    normfs_sha256_maj_fn(a, b, c));
}

/*
 * Written with *, / and % so what is proved is what the bytes mean rather than
 * that the C echoes the spec, and so it holds on any host byte order. Never
 * memcpy a uint32_t or cast uint8_t* to uint32_t* here.
 */
/*@ requires \valid_read(p + (0 .. 3));
    assigns \nothing;
    ensures \result == (uint32_t)normfs_sha256_be32(p);
*/
static uint32_t
normfs_sha256_be32_read(const uint8_t *p)
{
	return (uint32_t)p[0] * 16777216u + (uint32_t)p[1] * 65536u +
	    (uint32_t)p[2] * 256u + (uint32_t)p[3];
}

/*@ requires \valid(p + (0 .. 3));
    assigns p[0 .. 3];
    ensures normfs_sha256_be32(p) == value;
    ensures p[0] == (uint8_t)(value / 16777216);
    ensures p[1] == (uint8_t)((value / 65536) % 256);
    ensures p[2] == (uint8_t)((value / 256) % 256);
    ensures p[3] == (uint8_t)(value % 256);
*/
static void
normfs_sha256_be32_write(uint8_t *p, uint32_t value)
{
	uint32_t b3 = value % 256u;
	uint32_t r2 = value / 256u;
	uint32_t b2 = r2 % 256u;
	uint32_t r1 = r2 / 256u;
	uint32_t b1 = r1 % 256u;
	uint32_t b0 = r1 / 256u;

	/*@ assert value == b3 + 256 * r2; */
	/*@ assert r2 == b2 + 256 * r1; */
	/*@ assert r1 == b1 + 256 * b0; */
	/*@ assert b0 < 256; */
	/*@ assert value == 16777216 * b0 + 65536 * b1 + 256 * b2 + b3; */

	p[0] = (uint8_t)b0;
	p[1] = (uint8_t)b1;
	p[2] = (uint8_t)b2;
	p[3] = (uint8_t)b3;
}

/* value * 8 rather than value << 3 keeps it arithmetic; the precondition is
 * what stops the multiply overflowing. */
/*@ requires \valid(p + (0 .. 7));
    requires value <= NORMFS_SHA256_MAX_INPUT;
    assigns p[0 .. 7];
    ensures p[7] == (uint8_t)((8 * value) % 256);
    ensures p[6] == (uint8_t)(((8 * value) / 256) % 256);
    ensures p[5] == (uint8_t)(((8 * value) / 65536) % 256);
    ensures p[4] == (uint8_t)(((8 * value) / 16777216) % 256);
    ensures p[3] == (uint8_t)(((8 * value) / 4294967296) % 256);
    ensures p[2] == (uint8_t)(((8 * value) / 1099511627776) % 256);
    ensures p[1] == (uint8_t)(((8 * value) / 281474976710656) % 256);
    ensures p[0] == (uint8_t)((8 * value) / 72057594037927936);
*/
static void
normfs_sha256_be64_write_bits(uint8_t *p, uint64_t value)
{
	uint64_t bits = value * 8u;
	uint64_t b7 = bits % 256u;
	uint64_t r6 = bits / 256u;
	uint64_t b6 = r6 % 256u;
	uint64_t r5 = r6 / 256u;
	uint64_t b5 = r5 % 256u;
	uint64_t r4 = r5 / 256u;
	uint64_t b4 = r4 % 256u;
	uint64_t r3 = r4 / 256u;
	uint64_t b3 = r3 % 256u;
	uint64_t r2 = r3 / 256u;
	uint64_t b2 = r2 % 256u;
	uint64_t r1 = r2 / 256u;
	uint64_t b1 = r1 % 256u;
	uint64_t b0 = r1 / 256u;

	/*@ assert bits == b7 + 256 * r6; */
	/*@ assert r6 == b6 + 256 * r5; */
	/*@ assert r5 == b5 + 256 * r4; */
	/*@ assert r4 == b4 + 256 * r3; */
	/*@ assert r3 == b3 + 256 * r2; */
	/*@ assert r2 == b2 + 256 * r1; */
	/*@ assert r1 == b1 + 256 * b0; */

	p[0] = (uint8_t)b0;
	p[1] = (uint8_t)b1;
	p[2] = (uint8_t)b2;
	p[3] = (uint8_t)b3;
	p[4] = (uint8_t)b4;
	p[5] = (uint8_t)b5;
	p[6] = (uint8_t)b6;
	p[7] = (uint8_t)b7;
}

/*@ requires \valid_read(blk + (0 .. NORMFS_SHA256_BLOCK - 1));
    requires \valid(w + (0 .. 63));
    requires \separated(w + (0 .. 63),
                        blk + (0 .. NORMFS_SHA256_BLOCK - 1));
    assigns w[0 .. 63];
    ensures \forall integer j; 0 <= j < 64 ==>
              w[j] == normfs_sha256_w{Pre}(blk, j);
*/
static void
normfs_sha256_schedule(const uint8_t *blk, uint32_t *w)
{
	size_t t;

	/*@ loop invariant 0 <= t <= 16;
	    loop invariant \forall integer j; 0 <= j < t ==>
	                     w[j] == normfs_sha256_w{Pre}(blk, j);
	    loop assigns t, w[0 .. 15];
	    loop variant 16 - t;
	*/
	for (t = 0u; t < 16u; t++)
		w[t] = normfs_sha256_be32_read(blk + 4u * t);

	/*@ loop invariant 16 <= t <= 64;
	    loop invariant \forall integer j; 0 <= j < t ==>
	                     w[j] == normfs_sha256_w{Pre}(blk, j);
	    loop assigns t, w[16 .. 63];
	    loop variant 64 - t;
	*/
	for (t = 16u; t < 64u; t++)
		w[t] = (uint32_t)((uint32_t)((uint32_t)(
		    normfs_sha256_ssig1_fn(w[t - 2u]) + w[t - 7u]) +
		    normfs_sha256_ssig0_fn(w[t - 15u])) + w[t - 16u]);
}

void
normfs_sha256_compress(uint32_t *st, const uint8_t *blk)
{
	uint32_t w[64];
	uint32_t a = st[0];
	uint32_t b = st[1];
	uint32_t c = st[2];
	uint32_t d = st[3];
	uint32_t e = st[4];
	uint32_t f = st[5];
	uint32_t g = st[6];
	uint32_t h = st[7];
	size_t t;

	normfs_sha256_schedule(blk, w);

	/*@ loop invariant 0 <= t <= 64;
	    loop invariant a == normfs_sha256_rst{Pre}(st, blk, t, 0);
	    loop invariant b == normfs_sha256_rst{Pre}(st, blk, t, 1);
	    loop invariant c == normfs_sha256_rst{Pre}(st, blk, t, 2);
	    loop invariant d == normfs_sha256_rst{Pre}(st, blk, t, 3);
	    loop invariant e == normfs_sha256_rst{Pre}(st, blk, t, 4);
	    loop invariant f == normfs_sha256_rst{Pre}(st, blk, t, 5);
	    loop invariant g == normfs_sha256_rst{Pre}(st, blk, t, 6);
	    loop invariant h == normfs_sha256_rst{Pre}(st, blk, t, 7);
	    loop invariant \forall integer j; 0 <= j < 64 ==>
	                     w[j] == normfs_sha256_w{Pre}(blk, j);
	    loop assigns t, a, b, c, d, e, f, g, h;
	    loop variant 64 - t;
	*/
	for (t = 0u; t < 64u; t++) {
		uint32_t x1 = normfs_sha256_t1_fn(e, f, g, h,
		    normfs_sha256_k[t], w[t]);
		uint32_t x2 = normfs_sha256_t2_fn(a, b, c);

		h = g;
		g = f;
		f = e;
		e = (uint32_t)(d + x1);
		d = c;
		c = b;
		b = a;
		a = (uint32_t)(x1 + x2);
	}

	st[0] = (uint32_t)(st[0] + a);
	st[1] = (uint32_t)(st[1] + b);
	st[2] = (uint32_t)(st[2] + c);
	st[3] = (uint32_t)(st[3] + d);
	st[4] = (uint32_t)(st[4] + e);
	st[5] = (uint32_t)(st[5] + f);
	st[6] = (uint32_t)(st[6] + g);
	st[7] = (uint32_t)(st[7] + h);
}

size_t
normfs_sha256_absorb(uint32_t *st, const uint8_t *data, size_t len)
{
	size_t off = 0u;

	/*@ loop invariant 0 <= off <= len;
	    loop invariant off % NORMFS_SHA256_BLOCK == 0;
	    loop assigns off, st[0 .. 7];
	    loop variant len - off;
	*/
	while (len - off >= (size_t)NORMFS_SHA256_BLOCK) {
		normfs_sha256_compress(st, data + off);
		off += (size_t)NORMFS_SHA256_BLOCK;
	}

	return off;
}

void
normfs_sha256_finish(uint32_t *st, const uint8_t *tail, size_t tail_len,
    uint64_t total_len, uint8_t *out)
{
	/* tail (<= 128) + 0x80 + zero run + 8 length bytes, rounded up to a
	 * block. */
	uint8_t pad[3 * NORMFS_SHA256_BLOCK];
	size_t padded;
	size_t i;

	padded = (tail_len % (size_t)NORMFS_SHA256_BLOCK <
	    (size_t)NORMFS_SHA256_BLOCK - 8u)
	    ? tail_len - tail_len % (size_t)NORMFS_SHA256_BLOCK +
	      (size_t)NORMFS_SHA256_BLOCK
	    : tail_len - tail_len % (size_t)NORMFS_SHA256_BLOCK +
	      2u * (size_t)NORMFS_SHA256_BLOCK;

	/*@ loop invariant 0 <= i <= tail_len;
	    loop invariant \forall integer j; 0 <= j < i ==> pad[j] == tail[j];
	    loop assigns i, pad[0 .. tail_len - 1];
	    loop variant tail_len - i;
	*/
	for (i = 0u; i < tail_len; i++)
		pad[i] = tail[i];

	pad[tail_len] = 0x80u;

	/*@ loop invariant tail_len + 1 <= i <= padded - 8;
	    loop invariant \forall integer j; tail_len + 1 <= j < i ==>
	                     pad[j] == 0;
	    loop assigns i, pad[tail_len + 1 .. padded - 9];
	    loop variant padded - 8 - i;
	*/
	for (i = tail_len + 1u; i < padded - 8u; i++)
		pad[i] = 0u;

	normfs_sha256_be64_write_bits(pad + padded - 8u, total_len);

	/*@ loop invariant 0 <= i <= padded;
	    loop invariant i % NORMFS_SHA256_BLOCK == 0;
	    loop assigns i, st[0 .. 7];
	    loop variant padded - i;
	*/
	for (i = 0u; i < padded; i += (size_t)NORMFS_SHA256_BLOCK)
		normfs_sha256_compress(st, pad + i);

	/*@ loop invariant 0 <= i <= 8;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     normfs_sha256_be32(out + 4 * j) == st[j];
	    loop assigns i, out[0 .. NORMFS_SHA256_DIGEST - 1];
	    loop variant 8 - i;
	*/
	for (i = 0u; i < 8u; i++)
		normfs_sha256_be32_write(out + 4u * i, st[i]);
}

void
normfs_sha256(const uint8_t *data, size_t len, uint8_t *out)
{
	uint32_t st[8];
	size_t taken;
	size_t i;

	/*@ loop invariant 0 <= i <= 8;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     st[j] == normfs_sha256_h0[j];
	    loop assigns i, st[0 .. 7];
	    loop variant 8 - i;
	*/
	for (i = 0u; i < 8u; i++)
		st[i] = normfs_sha256_h0[i];

	taken = normfs_sha256_absorb(st, data, len);
	normfs_sha256_finish(st, data + taken, len - taken, (uint64_t)len, out);
}
