/*
 * RFC 5869, plus the argument handling of normfs_kdf_derive_file_key. Whether
 * the composition matches the data already on disk is settled in kdf_test.rs,
 * against the derivation this replaces; here it is only the published vectors.
 */
#include <stdio.h>
#include <string.h>

#include "normfs/kdf.h"

#define CHECK(cond)                                                     \
	do {                                                            \
		if (!(cond)) {                                          \
			fprintf(stderr, "kdf: FAIL %s:%d: %s\n",        \
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

/* Only the first 32 bytes of OKM are compared: expand is restricted to one
 * block, and T(1) is exactly those bytes in all three cases. */
static int
test_rfc5869(void)
{
	uint8_t ikm[80];
	uint8_t salt[80];
	uint8_t info[80];
	uint8_t prk[NORMFS_KDF_PRK];
	uint8_t okm[NORMFS_SHA256_DIGEST];
	size_t i;

	/* A.1 */
	memset(ikm, 0x0b, 22u);
	for (i = 0u; i < 13u; i++)
		salt[i] = (uint8_t)i;
	for (i = 0u; i < 10u; i++)
		info[i] = (uint8_t)(0xf0u + i);

	normfs_hkdf_sha256_extract(salt, 13u, ikm, 22u, prk);
	CHECK(hex_eq(prk, sizeof(prk),
	    "077709362c2e32df0ddc3f0dc47bba6390b6c73bb50f9c3122ec844ad7c2b3e5"));
	CHECK(normfs_hkdf_sha256_expand(prk, info, 10u, okm, sizeof(okm)) ==
	    NORMFS_KDF_OK);
	CHECK(hex_eq(okm, sizeof(okm),
	    "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf"));

	/* A.2: 80-byte inputs, so the long-key path and a multi-block message
	 * both run. */
	for (i = 0u; i < 80u; i++) {
		ikm[i] = (uint8_t)i;
		salt[i] = (uint8_t)(0x60u + i);
		info[i] = (uint8_t)(0xb0u + i);
	}
	normfs_hkdf_sha256_extract(salt, 80u, ikm, 80u, prk);
	CHECK(hex_eq(prk, sizeof(prk),
	    "06a6b88c5853361a06104c9ceb35b45cef760014904671014a193f40c15fc244"));
	CHECK(normfs_hkdf_sha256_expand(prk, info, 80u, okm, sizeof(okm)) ==
	    NORMFS_KDF_OK);
	CHECK(hex_eq(okm, sizeof(okm),
	    "b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c"));

	/* A.3: the "salt not provided" case, which the file key derivation
	 * uses. */
	memset(ikm, 0x0b, 22u);
	normfs_hkdf_sha256_extract(NULL, 0u, ikm, 22u, prk);
	CHECK(hex_eq(prk, sizeof(prk),
	    "19ef24a32c717b167f33a91d6f648bdf96596776afdb6377ac434c1c293ccb04"));
	CHECK(normfs_hkdf_sha256_expand(prk, NULL, 0u, okm, sizeof(okm)) ==
	    NORMFS_KDF_OK);
	CHECK(hex_eq(okm, sizeof(okm),
	    "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d"));
	return 0;
}

/* Getting this wrong would change every key on disk, and A.3 above only covers
 * it indirectly. */
static int
test_empty_salt_is_zero_block(void)
{
	uint8_t zeros[NORMFS_KDF_PRK];
	uint8_t ikm[16];
	uint8_t a[NORMFS_KDF_PRK];
	uint8_t b[NORMFS_KDF_PRK];

	memset(zeros, 0, sizeof(zeros));
	memset(ikm, 0x5a, sizeof(ikm));

	normfs_hkdf_sha256_extract(NULL, 0u, ikm, sizeof(ikm), a);
	normfs_hkdf_sha256_extract(zeros, sizeof(zeros), ikm, sizeof(ikm), b);
	CHECK(memcmp(a, b, sizeof(a)) == 0);
	return 0;
}

static int
test_derive_argument_checks(void)
{
	uint8_t seed[NORMFS_SEED_SIZE];
	uint8_t key[NORMFS_KDF_KEY];
	uint8_t nonce[NORMFS_KDF_NONCE];
	size_t i;

	memset(seed, 0x11, sizeof(seed));

	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed) - 1u, NULL, 0u,
	    key, sizeof(key), nonce, sizeof(nonce)) ==
	    NORMFS_KDF_ERR_INVALID_ARG);
	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed), NULL, 0u,
	    key, sizeof(key) - 1u, nonce, sizeof(nonce)) ==
	    NORMFS_KDF_ERR_INVALID_ARG);
	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed), NULL, 0u,
	    key, sizeof(key), nonce, sizeof(nonce) - 1u) ==
	    NORMFS_KDF_ERR_INVALID_ARG);
	/* okm_len > 32 is a precondition of expand rather than a runtime check,
	 * so there is nothing to test for it here: an over-long draw is a
	 * caller bug the contract forbids, not a status. */

	/* Nothing partial escapes a rejected call. */
	for (i = 0u; i < sizeof(key); i++)
		CHECK(key[i] == 0u);
	for (i = 0u; i < sizeof(nonce); i++)
		CHECK(nonce[i] == 0u);

	/* info_len == 0 is not reachable through QueueId, whose resolver always
	 * yields at least a slash, so it is covered here instead. */
	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed), NULL, 0u,
	    key, sizeof(key), nonce, sizeof(nonce)) == NORMFS_KDF_OK);
	return 0;
}

static int
test_derive_is_a_function_of_info(void)
{
	uint8_t seed[NORMFS_SEED_SIZE];
	uint8_t k1[NORMFS_KDF_KEY];
	uint8_t k2[NORMFS_KDF_KEY];
	uint8_t n1[NORMFS_KDF_NONCE];
	uint8_t n2[NORMFS_KDF_NONCE];

	memset(seed, 0x22, sizeof(seed));

	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed),
	    (const uint8_t *)"/q\x2a", 3u, k1, sizeof(k1), n1, sizeof(n1)) ==
	    NORMFS_KDF_OK);
	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed),
	    (const uint8_t *)"/q\x2a", 3u, k2, sizeof(k2), n2, sizeof(n2)) ==
	    NORMFS_KDF_OK);
	CHECK(memcmp(k1, k2, sizeof(k1)) == 0);
	CHECK(memcmp(n1, n2, sizeof(n1)) == 0);

	/* Same numeric file id, one byte wider: different info, different key.
	 * This is the case that makes UintN's width load bearing. */
	CHECK(normfs_kdf_derive_file_key(seed, sizeof(seed),
	    (const uint8_t *)"/q\x2a\x00", 4u, k2, sizeof(k2), n2,
	    sizeof(n2)) == NORMFS_KDF_OK);
	CHECK(memcmp(k1, k2, sizeof(k1)) != 0);
	CHECK(memcmp(n1, n2, sizeof(n1)) != 0);
	return 0;
}

int
main(void)
{
	if (test_rfc5869() != 0)
		return 1;
	if (test_empty_salt_is_zero_block() != 0)
		return 1;
	if (test_derive_argument_checks() != 0)
		return 1;
	if (test_derive_is_a_function_of_info() != 0)
		return 1;

	printf("kdf: all tests passed\n");
	return 0;
}
