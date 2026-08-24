#ifndef NORMFS_SHA256_H
#define NORMFS_SHA256_H

#include <stddef.h>
#include <stdint.h>

/*
 * SHA-256, FIPS 180-4.
 *
 * The round transform and the sigma/ch/maj functions are echoed, not proved:
 * the ACSL below is the same expression tree as the C, so the goals close by
 * congruence and say nothing about the expression being SHA-256 -- the trade
 * crc32c.c makes for its table. tests/test_sha256.c is what says it. The
 * schedule's indices, the padding at every length, and the big endian words
 * are proved outright.
 *
 * The logic is typed uint32_t rather than integer, with a cast on every body.
 * That is load bearing: under integer typing the provers cannot relate a land
 * result to an lxor argument, and even (x & y) ^ z fails against a
 * syntactically identical body. crc32c.h gets away with integer because its
 * one land is against a literal mask.
 */

#define NORMFS_SHA256_BLOCK 64
#define NORMFS_SHA256_DIGEST 32

/* The length field counts bits, so this is the longest expressible message. */
#define NORMFS_SHA256_MAX_INPUT ((uint64_t)0x1FFFFFFFFFFFFFFFu)

/* Exported so the ACSL can index them and test_sha256.c can re-derive them
 * from the primes: the contracts fix how they are used, not what they are. */
extern const uint32_t normfs_sha256_k[64];
extern const uint32_t normfs_sha256_h0[8];

/*@ axiomatic NormfsSha256Fn {
      logic uint32_t normfs_sha256_rotr(uint32_t x, integer n) =
        (uint32_t)((x >> n) | (uint32_t)(x << (32 - n)));

      logic uint32_t normfs_sha256_and(uint32_t x, uint32_t y) =
        (uint32_t)(x & y);

      // Complement free so no goal carries a truncation; the textbook
      // (~x & z) form would put one in every round.
      logic uint32_t normfs_sha256_ch(uint32_t x, uint32_t y, uint32_t z) =
        (uint32_t)(z ^ (x & (y ^ z)));

      logic uint32_t normfs_sha256_maj(uint32_t x, uint32_t y, uint32_t z) =
        (uint32_t)((x & y) ^ (x & z) ^ (y & z));

      logic uint32_t normfs_sha256_bsig0(uint32_t x) =
        (uint32_t)(normfs_sha256_rotr(x, 2) ^ normfs_sha256_rotr(x, 13) ^
                   normfs_sha256_rotr(x, 22));
      logic uint32_t normfs_sha256_bsig1(uint32_t x) =
        (uint32_t)(normfs_sha256_rotr(x, 6) ^ normfs_sha256_rotr(x, 11) ^
                   normfs_sha256_rotr(x, 25));
      logic uint32_t normfs_sha256_ssig0(uint32_t x) =
        (uint32_t)(normfs_sha256_rotr(x, 7) ^ normfs_sha256_rotr(x, 18) ^
                   (x >> 3));
      logic uint32_t normfs_sha256_ssig1(uint32_t x) =
        (uint32_t)(normfs_sha256_rotr(x, 17) ^ normfs_sha256_rotr(x, 19) ^
                   (x >> 10));

      // The casts nest the way C associates + left to right. A tidier
      // (uint32_t)(a + b + c + d) is equal but not syntactically equal, and
      // congruence would give way to modular arithmetic.
      logic uint32_t normfs_sha256_t1(uint32_t e, uint32_t f, uint32_t g,
                                      uint32_t h, uint32_t k, uint32_t w) =
        (uint32_t)((uint32_t)((uint32_t)((uint32_t)(h +
          normfs_sha256_bsig1(e)) + normfs_sha256_ch(e, f, g)) + k) + w);

      logic uint32_t normfs_sha256_t2(uint32_t a, uint32_t b, uint32_t c) =
        (uint32_t)(normfs_sha256_bsig0(a) + normfs_sha256_maj(a, b, c));
    }
*/

/*@ axiomatic NormfsSha256Be {
      logic integer normfs_sha256_be32{L}(uint8_t *p) =
        16777216 * p[0] + 65536 * p[1] + 256 * p[2] + p[3];
    }
*/

/*@ axiomatic NormfsSha256Schedule {
      logic uint32_t normfs_sha256_w{L}(uint8_t *blk, integer t)
        reads blk[0 .. NORMFS_SHA256_BLOCK - 1];

      axiom normfs_sha256_w_load{L}:
        \forall uint8_t *blk, integer t; 0 <= t < 16 ==>
          normfs_sha256_w(blk, t) ==
            (uint32_t)normfs_sha256_be32(blk + 4 * t);

      axiom normfs_sha256_w_step{L}:
        \forall uint8_t *blk, integer t; 16 <= t < 64 ==>
          normfs_sha256_w(blk, t) ==
            (uint32_t)((uint32_t)((uint32_t)(
              normfs_sha256_ssig1(normfs_sha256_w(blk, t - 2)) +
              normfs_sha256_w(blk, t - 7)) +
              normfs_sha256_ssig0(normfs_sha256_w(blk, t - 15))) +
              normfs_sha256_w(blk, t - 16));
    }
*/

/* The eight working words after t rounds, selected by j. The recursion is on t
 * alone so it stays structurally decreasing; the j split is a conditional
 * inside one body rather than eight mutually recursive functions. */
/*@ axiomatic NormfsSha256Round {
      logic uint32_t normfs_sha256_rst{L}(uint32_t *st, uint8_t *blk,
                                          integer t, integer j)
        reads st[0 .. 7], blk[0 .. NORMFS_SHA256_BLOCK - 1];

      axiom normfs_sha256_rst_init{L}:
        \forall uint32_t *st, uint8_t *blk, integer j;
          0 <= j < 8 ==> normfs_sha256_rst(st, blk, 0, j) == st[j];

      axiom normfs_sha256_rst_a{L}:
        \forall uint32_t *st, uint8_t *blk, integer t;
          0 <= t < 64 ==>
            normfs_sha256_rst(st, blk, t + 1, 0) ==
              (uint32_t)(normfs_sha256_t1(
                  normfs_sha256_rst(st, blk, t, 4),
                  normfs_sha256_rst(st, blk, t, 5),
                  normfs_sha256_rst(st, blk, t, 6),
                  normfs_sha256_rst(st, blk, t, 7),
                  normfs_sha256_k[t], normfs_sha256_w(blk, t)) +
                normfs_sha256_t2(
                  normfs_sha256_rst(st, blk, t, 0),
                  normfs_sha256_rst(st, blk, t, 1),
                  normfs_sha256_rst(st, blk, t, 2)));

      axiom normfs_sha256_rst_e{L}:
        \forall uint32_t *st, uint8_t *blk, integer t;
          0 <= t < 64 ==>
            normfs_sha256_rst(st, blk, t + 1, 4) ==
              (uint32_t)(normfs_sha256_rst(st, blk, t, 3) +
                normfs_sha256_t1(
                  normfs_sha256_rst(st, blk, t, 4),
                  normfs_sha256_rst(st, blk, t, 5),
                  normfs_sha256_rst(st, blk, t, 6),
                  normfs_sha256_rst(st, blk, t, 7),
                  normfs_sha256_k[t], normfs_sha256_w(blk, t)));

      axiom normfs_sha256_rst_shift{L}:
        \forall uint32_t *st, uint8_t *blk, integer t, j;
          0 <= t < 64 && 1 <= j <= 7 && j != 4 ==>
            normfs_sha256_rst(st, blk, t + 1, j) ==
              normfs_sha256_rst(st, blk, t, j - 1);
    }
*/

/*@ requires \valid(st + (0 .. 7));
    requires \valid_read(blk + (0 .. NORMFS_SHA256_BLOCK - 1));
    requires \separated(st + (0 .. 7), blk + (0 .. NORMFS_SHA256_BLOCK - 1));
    assigns st[0 .. 7];
    ensures \forall integer j; 0 <= j < 8 ==>
              st[j] == (uint32_t)(\old(st[j]) +
                normfs_sha256_rst{Old}(\old(st), blk, 64, j));
*/
void normfs_sha256_compress(uint32_t *st, const uint8_t *blk);

/*
 * absorb/finish rather than init/update/final: a streaming context's
 * postcondition is "the digest of everything you passed to update", and stating
 * that in ACSL needs those bytes in memory, which is what a streaming context
 * does not keep. Splitting at the block boundary lets every function below
 * carry a real value contract, for one % 64 at the caller.
 */
/*@ requires \valid(st + (0 .. 7));
    requires len == 0 || \valid_read(data + (0 .. len - 1));
    requires len == 0 || \separated(st + (0 .. 7), data + (0 .. len - 1));
    assigns st[0 .. 7];
    ensures \result == len - len % NORMFS_SHA256_BLOCK;
    ensures \result <= len;
    ensures \result % NORMFS_SHA256_BLOCK == 0;
*/
size_t normfs_sha256_absorb(uint32_t *st, const uint8_t *data, size_t len);

/*
 * tail_len may exceed one block because HMAC stages a partial block followed by
 * a second message and joins them here. total_len covers the whole message,
 * absorbed part included.
 */
/*@ requires \valid(st + (0 .. 7));
    requires tail_len <= 2 * NORMFS_SHA256_BLOCK;
    requires tail_len == 0 || \valid_read(tail + (0 .. tail_len - 1));
    requires \valid(out + (0 .. NORMFS_SHA256_DIGEST - 1));
    requires total_len <= NORMFS_SHA256_MAX_INPUT;
    requires total_len >= tail_len;
    requires tail_len == 0 ||
             \separated(out + (0 .. NORMFS_SHA256_DIGEST - 1),
                        tail + (0 .. tail_len - 1));
    requires \separated(out + (0 .. NORMFS_SHA256_DIGEST - 1), st + (0 .. 7));
    requires tail_len == 0 ||
             \separated(st + (0 .. 7), tail + (0 .. tail_len - 1));
    assigns st[0 .. 7], out[0 .. NORMFS_SHA256_DIGEST - 1];
    ensures \forall integer j; 0 <= j < 8 ==>
              normfs_sha256_be32(out + 4 * j) == st[j];
*/
void normfs_sha256_finish(uint32_t *st, const uint8_t *tail, size_t tail_len,
    uint64_t total_len, uint8_t *out);

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    requires \valid(out + (0 .. NORMFS_SHA256_DIGEST - 1));
    requires len <= NORMFS_SHA256_MAX_INPUT;
    requires len == 0 ||
             \separated(out + (0 .. NORMFS_SHA256_DIGEST - 1),
                        data + (0 .. len - 1));
    assigns out[0 .. NORMFS_SHA256_DIGEST - 1];
*/
void normfs_sha256(const uint8_t *data, size_t len, uint8_t *out);

#endif /* NORMFS_SHA256_H */
