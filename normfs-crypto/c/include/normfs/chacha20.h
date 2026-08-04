#ifndef NORMFS_CHACHA20_H
#define NORMFS_CHACHA20_H

#include <stddef.h>
#include <stdint.h>

/*
 * ChaCha20, RFC 8439.
 *
 * Only the block function: the key derivation draws 44 bytes, so a streaming
 * keystream API would be surface nothing calls. The general signature exists so
 * the RFC 8439 vector runs against the code that ships rather than a twin.
 *
 * The quarter round is echoed rather than proved, as in sha256.h, and
 * tests/test_chacha20.c is what says the expression is ChaCha20 -- it catches a
 * wrong index with probability 1, since one wrong word changes all 64 output
 * bytes. The state setup and the little endian words are proved outright.
 */

#define NORMFS_CHACHA20_KEY 32
#define NORMFS_CHACHA20_NONCE 12
#define NORMFS_CHACHA20_BLOCK 64
#define NORMFS_CHACHA20_ROUNDS 20

/* "expand 32-byte k", little endian; test_chacha20.c derives them from the
 * string. */
extern const uint32_t normfs_chacha20_sigma[4];

/*@ axiomatic NormfsChaCha20 {
      logic uint32_t normfs_chacha20_rotl(uint32_t x, integer n) =
        (uint32_t)((uint32_t)(x << n) | (x >> (32 - n)));

      logic integer normfs_chacha20_le32{L}(uint8_t *p) =
        p[0] + 256 * p[1] + 65536 * p[2] + 16777216 * p[3];
    }
*/

/*@ requires \valid_read(key + (0 .. NORMFS_CHACHA20_KEY - 1));
    requires \valid_read(nonce + (0 .. NORMFS_CHACHA20_NONCE - 1));
    requires \valid(out + (0 .. NORMFS_CHACHA20_BLOCK - 1));
    requires \separated(out + (0 .. NORMFS_CHACHA20_BLOCK - 1),
                        key + (0 .. NORMFS_CHACHA20_KEY - 1));
    requires \separated(out + (0 .. NORMFS_CHACHA20_BLOCK - 1),
                        nonce + (0 .. NORMFS_CHACHA20_NONCE - 1));
    assigns out[0 .. NORMFS_CHACHA20_BLOCK - 1];
*/
void normfs_chacha20_block(const uint8_t *key, const uint8_t *nonce,
    uint32_t counter, uint8_t *out);

#endif /* NORMFS_CHACHA20_H */
