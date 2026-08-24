#ifndef NORMFS_SEED_H
#define NORMFS_SEED_H

#include <stddef.h>
#include <stdint.h>

/*
 * The root secret: 32 bytes of OS entropy at <data_dir>/.crypto_seed.
 *
 * The path join happens here rather than in Rust because it is the part a
 * prover can rule on. The syscalls around it cannot be proved; they are assumed
 * shims in normfs/seed_sys.h, and tests/test_seed.c discharges them.
 */

#define NORMFS_SEED_SIZE 32
#define NORMFS_SEED_FILE_NAME ".crypto_seed"
/* Pinned against the literal in seed.c. */
#define NORMFS_SEED_FILE_NAME_LEN 12
#define NORMFS_SEED_PATH_MAX 4096

/*
 * A ceiling on what one wipe may cover, not a policy: WP cannot place an
 * unbounded assigns region when the destination is a caller's pointer, and
 * without this bound a caller's own goals stop converging rather than fail.
 * Every buffer wiped in this tree is a key, a digest or a keystream block.
 */
#define NORMFS_SEED_ZERO_MAX 4096

enum normfs_seed_status {
	NORMFS_SEED_OK = 0,
	NORMFS_SEED_ERR_INVALID_ARG = 1,
	NORMFS_SEED_ERR_PATH_TOO_LONG = 2,
	NORMFS_SEED_ERR_OS_RNG = 3,
	NORMFS_SEED_ERR_INVALID_SEED = 4,
	NORMFS_SEED_ERR_IO = 5
};

/*
 * os_error is the errno of the first failing syscall, 0 when the failure was
 * decided before any syscall ran. It exists so Rust can rebuild the io::Error
 * with from_raw_os_error and keep NotFound and AlreadyExists classifying.
 *
 * Two 4 byte members, so the pair packs with no padding and the Rust #[repr(C)]
 * mirror cannot disagree about the layout.
 */
struct normfs_seed_result {
	int os_error;
	int status;
};

/*
 * The data directory arrives as a NUL terminated string plus its length. Both
 * are needed: the length lets the contracts talk about the bytes without
 * Frama-C's string axiomatics, the NUL lets the shims call open(2) directly.
 *
 * seed_len is a cross check against a Rust side constant that has drifted, not
 * a way to pass a shorter buffer.
 */

/* Exported rather than static so test_seed.c can pin the literal file name. */
int normfs_seed_path(const char *data_dir, size_t data_dir_len,
    char *out, size_t out_len, size_t *used);

struct normfs_seed_result
normfs_seed_generate(uint8_t *seed, size_t seed_len);

struct normfs_seed_result
normfs_seed_load(const char *data_dir, size_t data_dir_len,
    uint8_t *seed, size_t seed_len);

struct normfs_seed_result
normfs_seed_save(const char *data_dir, size_t data_dir_len,
    const uint8_t *seed, size_t seed_len);

/* Every error is 0, mirroring Rust's Path::exists, which swallows them too. */
int normfs_seed_exists(const char *data_dir, size_t data_dir_len);

/* The contract lives here rather than on the definition because other proved
 * units call it, and a spec-less declaration makes WP assume the callee
 * assigns everything. */
/*@ requires seed_len == 0 || \valid(seed + (0 .. seed_len - 1));
    requires seed_len <= NORMFS_SEED_ZERO_MAX;
    assigns seed[0 .. seed_len - 1];
    ensures \forall integer k; 0 <= k < seed_len ==> seed[k] == 0;
*/
void normfs_seed_zero(uint8_t *seed, size_t seed_len);

#endif /* NORMFS_SEED_H */
