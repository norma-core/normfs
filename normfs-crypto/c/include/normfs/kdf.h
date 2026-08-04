#ifndef NORMFS_KDF_H
#define NORMFS_KDF_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/chacha20.h"
#include "normfs/hmac_sha256.h"
#include "normfs/seed.h"

/*
 * HKDF-SHA256 (RFC 5869) and the per-file key derivation built on it.
 *
 * There is encrypted data on disk behind every byte of this, so the
 * composition is fixed rather than parameterised:
 *
 *   prk       = HMAC-SHA256(key = 32 zero bytes, msg = seed)   Extract, salt None
 *   rng_seed  = HMAC-SHA256(key = prk, msg = info || 0x01)     Expand, L = 32
 *   ks        = ChaCha20(key = rng_seed, nonce = 12 zero bytes, counter = 0)
 *   aes_key   = ks[0  .. 32)
 *   gcm_nonce = ks[32 .. 44)
 *
 * The key is drawn before the nonce because the Rust this replaces drew them
 * in that order from one ChaCha20Rng, and the two draws were contiguous.
 *
 * info is built by the caller and passed whole. Its tail is UintN's
 * value_to_bytes, whose width the enum variant chooses -- U8(42) and U16(42)
 * are different info and so different keys -- and reproducing that narrowing
 * here would be a second definition of an encoding already on disk.
 */

#define NORMFS_KDF_KEY 32
#define NORMFS_KDF_NONCE 12
#define NORMFS_KDF_PRK 32

/*
 * Generous on purpose. The bound exists only so info_len + 1 + a block
 * provably cannot wrap size_t; it is not a policy. A tight limit would newly
 * reject a queue path that works today, which is the one thing this change
 * must not do.
 */
#define NORMFS_KDF_INFO_MAX ((size_t)0x100000)

enum normfs_kdf_status {
	NORMFS_KDF_OK = 0,
	NORMFS_KDF_ERR_INVALID_ARG = 1,
	NORMFS_KDF_ERR_INFO_TOO_LONG = 2
};

/*
 * HKDF-Extract. salt_len == 0 is the RFC's "salt not provided", which is a
 * block of zero bytes rather than an absent key -- the distinction the Rust
 * Hkdf::new(None, ...) made, and the one that decides every key on disk.
 */
/*@ requires salt_len == 0 || \valid_read(salt + (0 .. salt_len - 1));
    requires ikm_len == 0 || \valid_read(ikm + (0 .. ikm_len - 1));
    requires \valid(prk + (0 .. NORMFS_KDF_PRK - 1));
    requires salt_len <= NORMFS_SHA256_MAX_INPUT;
    requires ikm_len <= NORMFS_SHA256_MAX_INPUT - 2 * NORMFS_SHA256_BLOCK;
    requires salt_len == 0 ||
             \separated(prk + (0 .. NORMFS_KDF_PRK - 1),
                        salt + (0 .. salt_len - 1));
    requires ikm_len == 0 ||
             \separated(prk + (0 .. NORMFS_KDF_PRK - 1),
                        ikm + (0 .. ikm_len - 1));
    assigns prk[0 .. NORMFS_KDF_PRK - 1];
*/
void normfs_hkdf_sha256_extract(const uint8_t *salt, size_t salt_len,
    const uint8_t *ikm, size_t ikm_len, uint8_t *prk);

/*
 * HKDF-Expand restricted to L <= 32, one T(1) block. The multi-block loop is
 * deliberately absent: the only caller draws 32 bytes, and its absence is a
 * proof obligation removed rather than a feature missing.
 *
 * The L bound is a precondition rather than a runtime check because a function
 * that accepts any okm_len has an unbounded assigns region, and WP cannot
 * place an unbounded region when the destination is a caller's pointer -- the
 * goals stop converging rather than fail. Bounding it here costs the caller
 * nothing: every call site passes a compile-time constant.
 */
/*@ requires \valid_read(prk + (0 .. NORMFS_KDF_PRK - 1));
    requires info_len == 0 || \valid_read(info + (0 .. info_len - 1));
    requires 0 < okm_len <= NORMFS_SHA256_DIGEST;
    requires \valid(okm + (0 .. okm_len - 1));
    requires \separated(okm + (0 .. okm_len - 1),
                        prk + (0 .. NORMFS_KDF_PRK - 1));
    requires info_len == 0 ||
             \separated(okm + (0 .. okm_len - 1), info + (0 .. info_len - 1));
    assigns okm[0 .. okm_len - 1];
    ensures \result == NORMFS_KDF_OK ||
            \result == NORMFS_KDF_ERR_INFO_TOO_LONG;
    ensures \result == NORMFS_KDF_ERR_INFO_TOO_LONG <==>
              info_len > NORMFS_KDF_INFO_MAX;
*/
int normfs_hkdf_sha256_expand(const uint8_t *prk, const uint8_t *info,
    size_t info_len, uint8_t *okm, size_t okm_len);

/*
 * The whole derivation in one call, so the hot path crosses the FFI once per
 * encrypt or decrypt rather than four times.
 *
 * The lengths are cross checks against Rust side constants that have drifted,
 * not a way to ask for fewer bytes. Every intermediate -- prk, rng_seed and
 * the keystream block -- is wiped before return through normfs_seed_zero,
 * whose fallback chain is already the arbiter for the root secret.
 */
/*@ requires seed_len == 0 || \valid_read(seed + (0 .. seed_len - 1));
    requires info_len == 0 || \valid_read(info + (0 .. info_len - 1));
    requires \valid(out_key + (0 .. NORMFS_KDF_KEY - 1));
    requires \valid(out_nonce + (0 .. NORMFS_KDF_NONCE - 1));
    requires \separated(out_key + (0 .. NORMFS_KDF_KEY - 1),
                        out_nonce + (0 .. NORMFS_KDF_NONCE - 1));
    requires seed_len == 0 ||
             \separated(out_key + (0 .. NORMFS_KDF_KEY - 1),
                        seed + (0 .. seed_len - 1));
    requires seed_len == 0 ||
             \separated(out_nonce + (0 .. NORMFS_KDF_NONCE - 1),
                        seed + (0 .. seed_len - 1));
    requires info_len == 0 ||
             \separated(out_key + (0 .. NORMFS_KDF_KEY - 1),
                        info + (0 .. info_len - 1));
    requires info_len == 0 ||
             \separated(out_nonce + (0 .. NORMFS_KDF_NONCE - 1),
                        info + (0 .. info_len - 1));
    assigns out_key[0 .. NORMFS_KDF_KEY - 1],
            out_nonce[0 .. NORMFS_KDF_NONCE - 1];
    ensures \result == NORMFS_KDF_OK ||
            \result == NORMFS_KDF_ERR_INVALID_ARG ||
            \result == NORMFS_KDF_ERR_INFO_TOO_LONG;
    ensures \result == NORMFS_KDF_ERR_INVALID_ARG <==>
              (seed_len != NORMFS_SEED_SIZE ||
               out_key_len != NORMFS_KDF_KEY ||
               out_nonce_len != NORMFS_KDF_NONCE);
    // Completeness: with well formed arguments only the info bound is left.
    ensures (seed_len == NORMFS_SEED_SIZE &&
             out_key_len == NORMFS_KDF_KEY &&
             out_nonce_len == NORMFS_KDF_NONCE) ==>
              (\result == NORMFS_KDF_OK <==> info_len <= NORMFS_KDF_INFO_MAX);
    // Nothing partial reaches the caller, as in normfs_seed_load.
    ensures \result != NORMFS_KDF_OK ==>
              (\forall integer i; 0 <= i < NORMFS_KDF_KEY ==>
                 out_key[i] == 0) &&
              (\forall integer i; 0 <= i < NORMFS_KDF_NONCE ==>
                 out_nonce[i] == 0);
*/
int normfs_kdf_derive_file_key(const uint8_t *seed, size_t seed_len,
    const uint8_t *info, size_t info_len,
    uint8_t *out_key, size_t out_key_len,
    uint8_t *out_nonce, size_t out_nonce_len);

#endif /* NORMFS_KDF_H */
