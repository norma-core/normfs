#ifndef NORMFS_HMAC_SHA256_H
#define NORMFS_HMAC_SHA256_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/sha256.h"

/*
 * HMAC-SHA256, RFC 2104.
 *
 * What is proved here is the key schedule and the framing: K0's padding and
 * the hash-the-long-key branch, the ipad and opad bytes, the block counts, and
 * memory safety on every path. What the composition H(opad ‖ H(ipad ‖ m))
 * computes is not stated -- it is built by calling functions that are
 * themselves proved, and RFC 4231 in tests/test_hmac_sha256.c is what says the
 * result is HMAC.
 */

#define NORMFS_HMAC_SHA256_BLOCK NORMFS_SHA256_BLOCK
#define NORMFS_HMAC_SHA256_TAG NORMFS_SHA256_DIGEST

/*
 * The message is two parts because HKDF-Expand's message is info followed by a
 * single counter byte, and taking them separately is what keeps this module
 * free of any buffer whose size depends on info_len. m2 is bounded by a block
 * so the staging area is fixed.
 *
 * A key longer than a block is hashed first, per RFC 2104, so RFC 4231 cases 6
 * and 7 run unmodified.
 */
/*@ requires key_len == 0 || \valid_read(key + (0 .. key_len - 1));
    requires m1_len == 0 || \valid_read(m1 + (0 .. m1_len - 1));
    requires m2_len <= NORMFS_HMAC_SHA256_BLOCK;
    requires m2_len == 0 || \valid_read(m2 + (0 .. m2_len - 1));
    requires \valid(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1));
    requires key_len <= NORMFS_SHA256_MAX_INPUT;
    requires m1_len <= NORMFS_SHA256_MAX_INPUT - 2 * NORMFS_SHA256_BLOCK;
    requires key_len == 0 ||
             \separated(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1),
                        key + (0 .. key_len - 1));
    requires m1_len == 0 ||
             \separated(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1),
                        m1 + (0 .. m1_len - 1));
    requires m2_len == 0 ||
             \separated(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1),
                        m2 + (0 .. m2_len - 1));
    assigns out[0 .. NORMFS_HMAC_SHA256_TAG - 1];
*/
void normfs_hmac_sha256_2(const uint8_t *key, size_t key_len,
    const uint8_t *m1, size_t m1_len, const uint8_t *m2, size_t m2_len,
    uint8_t *out);

/* m2_len == 0, so RFC 4231 reads like RFC 4231. */
/*@ requires key_len == 0 || \valid_read(key + (0 .. key_len - 1));
    requires msg_len == 0 || \valid_read(msg + (0 .. msg_len - 1));
    requires \valid(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1));
    requires key_len <= NORMFS_SHA256_MAX_INPUT;
    requires msg_len <= NORMFS_SHA256_MAX_INPUT - 2 * NORMFS_SHA256_BLOCK;
    requires key_len == 0 ||
             \separated(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1),
                        key + (0 .. key_len - 1));
    requires msg_len == 0 ||
             \separated(out + (0 .. NORMFS_HMAC_SHA256_TAG - 1),
                        msg + (0 .. msg_len - 1));
    assigns out[0 .. NORMFS_HMAC_SHA256_TAG - 1];
*/
void normfs_hmac_sha256(const uint8_t *key, size_t key_len,
    const uint8_t *msg, size_t msg_len, uint8_t *out);

#endif /* NORMFS_HMAC_SHA256_H */
