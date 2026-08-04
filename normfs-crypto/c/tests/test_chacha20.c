/*
 * RFC 8439. The quarter round in src/chacha20.c is echoed rather than proved,
 * so these vectors are what says the expression is ChaCha20. They catch a
 * wrong index with probability 1: one wrong word changes all 64 output bytes.
 */
#include <stdio.h>
#include <string.h>

#include "normfs/chacha20.h"

#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "chacha20: FAIL %s:%d: %s\n",   \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static int
hex_eq(const uint8_t *got, size_t len, const char *want)
{
	char buf[2 * NORMFS_CHACHA20_BLOCK + 1];
	size_t i;

	for (i = 0u; i < len; i++)
		(void)snprintf(buf + 2u * i, 3u, "%02x", got[i]);

	return strcmp(buf, want) == 0;
}

/* RFC 8439 section 2.3.2. */
static int
test_rfc8439_block(void)
{
	uint8_t key[NORMFS_CHACHA20_KEY];
	uint8_t nonce[NORMFS_CHACHA20_NONCE];
	uint8_t out[NORMFS_CHACHA20_BLOCK];
	size_t i;

	for (i = 0u; i < sizeof(key); i++)
		key[i] = (uint8_t)i;

	memset(nonce, 0, sizeof(nonce));
	nonce[3] = 0x09u;
	nonce[7] = 0x4au;

	normfs_chacha20_block(key, nonce, 1u, out);

	CHECK(hex_eq(out, sizeof(out),
	    "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4e"
	    "d2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e"));
	return 0;
}

/* RFC 8439 appendix A.1 test vector 1: all-zero key and nonce, counter 0.
 * This is exactly the shape the key derivation uses. */
static int
test_rfc8439_zero(void)
{
	uint8_t key[NORMFS_CHACHA20_KEY];
	uint8_t nonce[NORMFS_CHACHA20_NONCE];
	uint8_t out[NORMFS_CHACHA20_BLOCK];

	memset(key, 0, sizeof(key));
	memset(nonce, 0, sizeof(nonce));

	normfs_chacha20_block(key, nonce, 0u, out);

	CHECK(hex_eq(out, sizeof(out),
	    "76b8e0ada0f13d90405d6ae55386bd28bdd219b8a08ded1aa836efcc8b770dc7"
	    "da41597c5157488d7724e03fb8d84a376a43b8f41518a11cc387b669b2ee6586"));

	/* Appendix A.1 vector 2: same key and nonce, counter 1. */
	normfs_chacha20_block(key, nonce, 1u, out);
	CHECK(hex_eq(out, sizeof(out),
	    "9f07e7be5551387a98ba977c732d080dcb0f29a048e3656912c6533e32ee7aed"
	    "29b721769ce64e43d57133b074d839d531ed1f28510afb45ace10a1f4b794d6f"));
	return 0;
}

/* The sigma words are "expand 32-byte k" read little endian. */
static int
test_sigma_from_string(void)
{
	static const char s[] = "expand 32-byte k";
	size_t i;

	for (i = 0u; i < 4u; i++) {
		uint32_t want = (uint32_t)(unsigned char)s[4u * i] +
		    (uint32_t)(unsigned char)s[4u * i + 1u] * 256u +
		    (uint32_t)(unsigned char)s[4u * i + 2u] * 65536u +
		    (uint32_t)(unsigned char)s[4u * i + 3u] * 16777216u;

		CHECK(normfs_chacha20_sigma[i] == want);
	}
	return 0;
}

int
main(void)
{
	if (test_rfc8439_block() != 0)
		return 1;
	if (test_rfc8439_zero() != 0)
		return 1;
	if (test_sigma_from_string() != 0)
		return 1;

	printf("chacha20: all tests passed\n");
	return 0;
}
