/*
 * What WP cannot reach: the round transform is echoed by a contract with the
 * same expression tree as the C, and K and H0 are extern arrays no contract
 * pins. These vectors are what says it is SHA-256 -- the job test_crc32c.c does
 * by re-deriving the CRC32C table from its polynomial.
 */
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "normfs/sha256.h"

/* assert() is a no-op under NDEBUG, which the Release build defines, so the
 * checks report and return failure themselves instead. */
#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "sha256: FAIL %s:%d: %s\n",     \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static int
hex_eq(const uint8_t *got, const char *want)
{
	char buf[2 * NORMFS_SHA256_DIGEST + 1];
	size_t i;

	for (i = 0u; i < (size_t)NORMFS_SHA256_DIGEST; i++)
		(void)snprintf(buf + 2u * i, 3u, "%02x", got[i]);

	return strcmp(buf, want) == 0;
}

static int
digest_eq(const char *msg, size_t len, const char *want)
{
	uint8_t out[NORMFS_SHA256_DIGEST];

	normfs_sha256((const uint8_t *)msg, len, out);
	return hex_eq(out, want);
}

/* FIPS 180-4 appendix B. */
static int
test_fips_vectors(void)
{
	CHECK(digest_eq("", 0u,
	    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
	CHECK(digest_eq("abc", 3u,
	    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
	CHECK(digest_eq(
	    "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56u,
	    "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"));
	CHECK(digest_eq(
	    "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn"
	    "hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu", 112u,
	    "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"));
	return 0;
}

/* The lengths where the padding decision changes. */
static int
test_padding_boundaries(void)
{
	static const struct {
		size_t len;
		const char *want;
	} cases[] = {
		{ 55u, "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318" },
		{ 56u, "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a" },
		{ 63u, "7d3e74a05d7db15bce4ad9ec0658ea98e3f06eeecf16b4c6fff2da457ddc2f34" },
		{ 64u, "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb" },
		{ 65u, "635361c48bb9eab14198e76ea8ab7f1a41685d6ad62aa9146d301d4f17eb0ae0" }
	};
	uint8_t buf[128];
	size_t i;

	memset(buf, 'a', sizeof(buf));

	for (i = 0u; i < sizeof(cases) / sizeof(cases[0]); i++) {
		uint8_t out[NORMFS_SHA256_DIGEST];

		normfs_sha256(buf, cases[i].len, out);
		CHECK(hex_eq(out, cases[i].want));
	}
	return 0;
}

/* Driven through absorb rather than one buffer, so the multi-block path and the
 * 64-bit length field are both exercised. */
static int
test_million_a(void)
{
	uint8_t blk[NORMFS_SHA256_BLOCK];
	uint8_t out[NORMFS_SHA256_DIGEST];
	uint32_t st[8];
	size_t i;

	memset(blk, 'a', sizeof(blk));
	for (i = 0u; i < 8u; i++)
		st[i] = normfs_sha256_h0[i];

	for (i = 0u; i < 15625u; i++)
		(void)normfs_sha256_absorb(st, blk, sizeof(blk));

	normfs_sha256_finish(st, blk, 0u, 1000000u, out);
	CHECK(hex_eq(out,
	    "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"));
	return 0;
}

/* absorb + finish must agree with the one-shot at every split point; this is
 * what catches a buffer-boundary bug. */
static int
test_absorb_finish_equivalence(void)
{
	uint8_t buf[200];
	size_t len;
	size_t i;

	for (i = 0u; i < sizeof(buf); i++)
		buf[i] = (uint8_t)(i * 7u + 3u);

	for (len = 0u; len <= sizeof(buf); len++) {
		uint8_t one[NORMFS_SHA256_DIGEST];
		uint8_t split[NORMFS_SHA256_DIGEST];
		uint32_t st[8];
		size_t taken;

		normfs_sha256(buf, len, one);

		for (i = 0u; i < 8u; i++)
			st[i] = normfs_sha256_h0[i];
		taken = normfs_sha256_absorb(st, buf, len);
		CHECK(taken == len - len % NORMFS_SHA256_BLOCK);
		normfs_sha256_finish(st, buf + taken, len - taken,
		    (uint64_t)len, split);

		CHECK(memcmp(one, split, sizeof(one)) == 0);
	}
	return 0;
}

/*
 * K and H0 are the fractional parts of the cube and square roots of the first 64
 * and 8 primes. double carries 52 bits after the point where 32 are needed, so
 * a mistyped constant misses by far more than the rounding.
 */
static int
test_constants_from_primes(void)
{
	unsigned int primes[64];
	unsigned int n = 0u;
	unsigned int cand = 2u;
	unsigned int i;

	while (n < 64u) {
		unsigned int d;
		int prime = 1;

		for (d = 2u; d * d <= cand; d++) {
			if (cand % d == 0u) {
				prime = 0;
				break;
			}
		}
		if (prime != 0)
			primes[n++] = cand;
		cand++;
	}

	for (i = 0u; i < 8u; i++) {
		double frac = sqrt((double)primes[i]);

		frac -= floor(frac);
		CHECK(normfs_sha256_h0[i] ==
		    (uint32_t)floor(frac * 4294967296.0));
	}

	for (i = 0u; i < 64u; i++) {
		double frac = cbrt((double)primes[i]);

		frac -= floor(frac);
		CHECK(normfs_sha256_k[i] ==
		    (uint32_t)floor(frac * 4294967296.0));
	}
	return 0;
}

int
main(void)
{
	if (test_fips_vectors() != 0)
		return 1;
	if (test_padding_boundaries() != 0)
		return 1;
	if (test_million_a() != 0)
		return 1;
	if (test_absorb_finish_equivalence() != 0)
		return 1;
	if (test_constants_from_primes() != 0)
		return 1;

	printf("sha256: all tests passed\n");
	return 0;
}
