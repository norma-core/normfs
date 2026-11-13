/*
 * RFC 4231. The C proves its key schedule and framing but says nothing about
 * what H(opad ‖ H(ipad ‖ m)) computes; these vectors do.
 *
 * Cases 6 and 7 use a 131-byte key, the only thing that reaches the
 * hash-the-long-key branch.
 */
#include <stdio.h>
#include <string.h>

#include "normfs/hmac_sha256.h"

#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "hmac_sha256: FAIL %s:%d: %s\n", \
			    __FILE__, __LINE__, #cond);                 \
			return 1;                                       \
		}                                                       \
	} while (0)

static int
hex_eq(const uint8_t *got, const char *want)
{
	char buf[2 * NORMFS_HMAC_SHA256_TAG + 1];
	size_t i;

	for (i = 0u; i < (size_t)NORMFS_HMAC_SHA256_TAG; i++)
		(void)snprintf(buf + 2u * i, 3u, "%02x", got[i]);

	return strcmp(buf, want) == 0;
}

static int
check_case(const uint8_t *key, size_t key_len, const uint8_t *msg,
    size_t msg_len, const char *want)
{
	uint8_t out[NORMFS_HMAC_SHA256_TAG];

	normfs_hmac_sha256(key, key_len, msg, msg_len, out);
	return hex_eq(out, want) ? 0 : 1;
}

static int
test_rfc4231(void)
{
	uint8_t key[131];
	uint8_t msg[152];

	/* 1 */
	memset(key, 0x0b, 20u);
	CHECK(check_case(key, 20u, (const uint8_t *)"Hi There", 8u,
	    "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7") == 0);

	/* 2 */
	CHECK(check_case((const uint8_t *)"Jefe", 4u,
	    (const uint8_t *)"what do ya want for nothing?", 28u,
	    "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843") == 0);

	/* 3 */
	memset(key, 0xaa, 20u);
	memset(msg, 0xdd, 50u);
	CHECK(check_case(key, 20u, msg, 50u,
	    "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe") == 0);

	/* 4 */
	{
		size_t i;

		for (i = 0u; i < 25u; i++)
			key[i] = (uint8_t)(i + 1u);
	}
	memset(msg, 0xcd, 50u);
	CHECK(check_case(key, 25u, msg, 50u,
	    "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b") == 0);

	/* 6: 131-byte key, hashed first */
	memset(key, 0xaa, 131u);
	CHECK(check_case(key, 131u,
	    (const uint8_t *)"Test Using Larger Than Block-Size Key - Hash Key First",
	    54u,
	    "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54") == 0);

	/* 7: 131-byte key and a 152-byte message */
	memset(key, 0xaa, 131u);
	CHECK(check_case(key, 131u, (const uint8_t *)
	    "This is a test using a larger than block-size key and a larger "
	    "than block-size data. The key needs to be hashed before being "
	    "used by the HMAC algorithm.", 152u,
	    "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2") == 0);
	return 0;
}

/* HKDF-Expand relies on this when it passes (info, counter byte). */
static int
test_two_part_message(void)
{
	uint8_t key[40];
	uint8_t msg[200];
	size_t split;
	size_t i;

	for (i = 0u; i < sizeof(key); i++)
		key[i] = (uint8_t)(i * 3u + 1u);
	for (i = 0u; i < sizeof(msg); i++)
		msg[i] = (uint8_t)(i * 5u + 7u);

	for (split = 0u; split + NORMFS_HMAC_SHA256_BLOCK <= sizeof(msg);
	    split++) {
		uint8_t one[NORMFS_HMAC_SHA256_TAG];
		uint8_t two[NORMFS_HMAC_SHA256_TAG];
		size_t rest = sizeof(msg) - split;

		if (rest > (size_t)NORMFS_HMAC_SHA256_BLOCK)
			rest = (size_t)NORMFS_HMAC_SHA256_BLOCK;

		normfs_hmac_sha256(key, sizeof(key), msg, split + rest, one);
		normfs_hmac_sha256_2(key, sizeof(key), msg, split,
		    msg + split, rest, two);
		CHECK(memcmp(one, two, sizeof(one)) == 0);
	}
	return 0;
}

int
main(void)
{
	if (test_rfc4231() != 0)
		return 1;
	if (test_two_part_message() != 0)
		return 1;

	printf("hmac_sha256: all tests passed\n");
	return 0;
}
