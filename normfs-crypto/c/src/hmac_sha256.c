#include "normfs/hmac_sha256.h"

#define NORMFS_HMAC_IPAD 0x36u
#define NORMFS_HMAC_OPAD 0x5Cu

/*
 * K0: the key padded to a block with zeros, or its digest so padded when the
 * key is longer than a block. The two branches are separate loops rather than
 * one, because the hashed branch writes 32 bytes and the direct branch
 * key_len, and a single loop would need a conditional bound the invariant
 * cannot carry.
 */
/*@ requires \valid(k0 + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1));
    requires key_len == 0 || \valid_read(key + (0 .. key_len - 1));
    requires key_len <= NORMFS_SHA256_MAX_INPUT;
    requires key_len == 0 ||
             \separated(k0 + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1),
                        key + (0 .. key_len - 1));
    assigns k0[0 .. NORMFS_HMAC_SHA256_BLOCK - 1];
    ensures key_len <= NORMFS_HMAC_SHA256_BLOCK ==>
              (\forall integer i; 0 <= i < key_len ==> k0[i] == key[i]);
    ensures key_len <= NORMFS_HMAC_SHA256_BLOCK ==>
              (\forall integer i; key_len <= i < NORMFS_HMAC_SHA256_BLOCK ==>
                 k0[i] == 0);
    ensures key_len > NORMFS_HMAC_SHA256_BLOCK ==>
              (\forall integer i;
                 NORMFS_SHA256_DIGEST <= i < NORMFS_HMAC_SHA256_BLOCK ==>
                   k0[i] == 0);
*/
static void
normfs_hmac_sha256_k0(const uint8_t *key, size_t key_len, uint8_t *k0)
{
	size_t i;

	if (key_len > (size_t)NORMFS_HMAC_SHA256_BLOCK) {
		normfs_sha256(key, key_len, k0);

		/*@ loop invariant NORMFS_SHA256_DIGEST <= i <=
		                     NORMFS_HMAC_SHA256_BLOCK;
		    loop invariant \forall integer j;
		                     NORMFS_SHA256_DIGEST <= j < i ==> k0[j] == 0;
		    loop assigns i,
		      k0[NORMFS_SHA256_DIGEST .. NORMFS_HMAC_SHA256_BLOCK - 1];
		    loop variant NORMFS_HMAC_SHA256_BLOCK - i;
		*/
		for (i = (size_t)NORMFS_SHA256_DIGEST;
		    i < (size_t)NORMFS_HMAC_SHA256_BLOCK; i++)
			k0[i] = 0u;

		return;
	}

	/*@ loop invariant 0 <= i <= key_len;
	    loop invariant \forall integer j; 0 <= j < i ==> k0[j] == key[j];
	    loop assigns i, k0[0 .. key_len - 1];
	    loop variant key_len - i;
	*/
	for (i = 0u; i < key_len; i++)
		k0[i] = key[i];

	/*@ loop invariant key_len <= i <= NORMFS_HMAC_SHA256_BLOCK;
	    loop invariant \forall integer j; 0 <= j < key_len ==>
	                     k0[j] == key[j];
	    loop invariant \forall integer j; key_len <= j < i ==> k0[j] == 0;
	    loop assigns i, k0[key_len .. NORMFS_HMAC_SHA256_BLOCK - 1];
	    loop variant NORMFS_HMAC_SHA256_BLOCK - i;
	*/
	for (i = key_len; i < (size_t)NORMFS_HMAC_SHA256_BLOCK; i++)
		k0[i] = 0u;
}

/* 0x36 and 0x5C are xored into bytes already in range, so these goals carry no
 * truncation and close without the split maj needed. */
/*@ requires \valid(out + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1));
    requires \valid_read(k0 + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1));
    requires \separated(out + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1),
                        k0 + (0 .. NORMFS_HMAC_SHA256_BLOCK - 1));
    assigns out[0 .. NORMFS_HMAC_SHA256_BLOCK - 1];
    ensures \forall integer i; 0 <= i < NORMFS_HMAC_SHA256_BLOCK ==>
              out[i] == (uint8_t)(k0[i] ^ b);
*/
static void
normfs_hmac_sha256_xor_block(const uint8_t *k0, uint8_t b, uint8_t *out)
{
	size_t i;

	/*@ loop invariant 0 <= i <= NORMFS_HMAC_SHA256_BLOCK;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     out[j] == (uint8_t)(k0[j] ^ b);
	    loop assigns i, out[0 .. NORMFS_HMAC_SHA256_BLOCK - 1];
	    loop variant NORMFS_HMAC_SHA256_BLOCK - i;
	*/
	for (i = 0u; i < (size_t)NORMFS_HMAC_SHA256_BLOCK; i++)
		out[i] = (uint8_t)(k0[i] ^ b);
}

void
normfs_hmac_sha256_2(const uint8_t *key, size_t key_len,
    const uint8_t *m1, size_t m1_len, const uint8_t *m2, size_t m2_len,
    uint8_t *out)
{
	uint8_t k0[NORMFS_HMAC_SHA256_BLOCK];
	uint8_t pad[NORMFS_HMAC_SHA256_BLOCK];
	/* m1's remainder is under a block and m2 is at most one, so the join
	 * never exceeds what normfs_sha256_finish accepts. */
	uint8_t tail[2 * NORMFS_SHA256_BLOCK];
	uint8_t inner[NORMFS_SHA256_DIGEST];
	uint32_t st[8];
	size_t taken;
	size_t tail_len;
	size_t i;

	normfs_hmac_sha256_k0(key, key_len, k0);

	normfs_hmac_sha256_xor_block(k0, NORMFS_HMAC_IPAD, pad);

	/*@ loop invariant 0 <= i <= 8;
	    loop assigns i, st[0 .. 7];
	    loop variant 8 - i;
	*/
	for (i = 0u; i < 8u; i++)
		st[i] = normfs_sha256_h0[i];

	normfs_sha256_compress(st, pad);
	taken = normfs_sha256_absorb(st, m1, m1_len);

	tail_len = m1_len - taken;

	/*@ loop invariant 0 <= i <= tail_len;
	    loop assigns i, tail[0 .. tail_len - 1];
	    loop variant tail_len - i;
	*/
	for (i = 0u; i < tail_len; i++)
		tail[i] = m1[taken + i];

	/*@ loop invariant 0 <= i <= m2_len;
	    loop assigns i, tail[tail_len .. tail_len + m2_len - 1];
	    loop variant m2_len - i;
	*/
	for (i = 0u; i < m2_len; i++)
		tail[tail_len + i] = m2[i];

	normfs_sha256_finish(st, tail, tail_len + m2_len,
	    (uint64_t)NORMFS_HMAC_SHA256_BLOCK + (uint64_t)m1_len +
	    (uint64_t)m2_len, inner);

	normfs_hmac_sha256_xor_block(k0, NORMFS_HMAC_OPAD, pad);

	/*@ loop invariant 0 <= i <= 8;
	    loop assigns i, st[0 .. 7];
	    loop variant 8 - i;
	*/
	for (i = 0u; i < 8u; i++)
		st[i] = normfs_sha256_h0[i];

	normfs_sha256_compress(st, pad);
	normfs_sha256_finish(st, inner, (size_t)NORMFS_SHA256_DIGEST,
	    (uint64_t)NORMFS_HMAC_SHA256_BLOCK + (uint64_t)NORMFS_SHA256_DIGEST,
	    out);
}

void
normfs_hmac_sha256(const uint8_t *key, size_t key_len,
    const uint8_t *msg, size_t msg_len, uint8_t *out)
{
	normfs_hmac_sha256_2(key, key_len, msg, msg_len, msg, 0u, out);
}
