#include "normfs/kdf.h"

void
normfs_hkdf_sha256_extract(const uint8_t *salt, size_t salt_len,
    const uint8_t *ikm, size_t ikm_len, uint8_t *prk)
{
	normfs_hmac_sha256(salt, salt_len, ikm, ikm_len, prk);
}

int
normfs_hkdf_sha256_expand(const uint8_t *prk, const uint8_t *info,
    size_t info_len, uint8_t *okm, size_t okm_len)
{
	uint8_t t[NORMFS_SHA256_DIGEST];
	uint8_t counter = 1u;
	size_t i;

	if (info_len > NORMFS_KDF_INFO_MAX)
		return NORMFS_KDF_ERR_INFO_TOO_LONG;

	/* L <= 32 means one block, so T(0) is empty and the message is just
	 * info followed by the counter byte -- which is why the HMAC takes its
	 * message in two parts and no buffer here is sized by info_len. */
	normfs_hmac_sha256_2(prk, (size_t)NORMFS_KDF_PRK, info, info_len,
	    &counter, 1u, t);

	/*@ loop invariant 0 <= i <= okm_len;
	    loop assigns i, okm[0 .. okm_len - 1];
	    loop variant okm_len - i;
	*/
	for (i = 0u; i < okm_len; i++)
		okm[i] = t[i];

	normfs_seed_zero(t, sizeof(t));
	return NORMFS_KDF_OK;
}

int
normfs_kdf_derive_file_key(const uint8_t *seed, size_t seed_len,
    const uint8_t *info, size_t info_len,
    uint8_t *out_key, size_t out_key_len,
    uint8_t *out_nonce, size_t out_nonce_len)
{
	uint8_t salt[NORMFS_KDF_PRK];
	uint8_t prk[NORMFS_KDF_PRK];
	uint8_t rng_seed[NORMFS_KDF_KEY];
	uint8_t nonce[NORMFS_CHACHA20_NONCE];
	uint8_t ks[NORMFS_CHACHA20_BLOCK];
	int status;
	size_t i;

	normfs_seed_zero(out_key, (size_t)NORMFS_KDF_KEY);
	normfs_seed_zero(out_nonce, (size_t)NORMFS_KDF_NONCE);

	if (seed_len != (size_t)NORMFS_SEED_SIZE ||
	    out_key_len != (size_t)NORMFS_KDF_KEY ||
	    out_nonce_len != (size_t)NORMFS_KDF_NONCE)
		return NORMFS_KDF_ERR_INVALID_ARG;
	/* info_len is checked by expand below rather than here: a second check
	 * would make expand's failure path dead code, which -wp-smoke-tests
	 * reports as a reachable contradiction. */

	/* Not an absent salt: RFC 5869 with salt omitted uses HashLen zero
	 * bytes, and that is what produced every key currently on disk. */
	normfs_seed_zero(salt, sizeof(salt));
	normfs_hkdf_sha256_extract(salt, sizeof(salt), seed, seed_len, prk);

	status = normfs_hkdf_sha256_expand(prk, info, info_len, rng_seed,
	    sizeof(rng_seed));
	if (status != NORMFS_KDF_OK) {
		normfs_seed_zero(prk, sizeof(prk));
		return status;
	}

	normfs_seed_zero(nonce, sizeof(nonce));
	normfs_chacha20_block(rng_seed, nonce, 0u, ks);

	/*@ loop invariant 0 <= i <= NORMFS_KDF_KEY;
	    loop assigns i, out_key[0 .. NORMFS_KDF_KEY - 1];
	    loop variant NORMFS_KDF_KEY - i;
	*/
	for (i = 0u; i < (size_t)NORMFS_KDF_KEY; i++)
		out_key[i] = ks[i];

	/*@ loop invariant 0 <= i <= NORMFS_KDF_NONCE;
	    loop assigns i, out_nonce[0 .. NORMFS_KDF_NONCE - 1];
	    loop variant NORMFS_KDF_NONCE - i;
	*/
	for (i = 0u; i < (size_t)NORMFS_KDF_NONCE; i++)
		out_nonce[i] = ks[(size_t)NORMFS_KDF_KEY + i];

	normfs_seed_zero(prk, sizeof(prk));
	normfs_seed_zero(rng_seed, sizeof(rng_seed));
	normfs_seed_zero(ks, sizeof(ks));
	return NORMFS_KDF_OK;
}
