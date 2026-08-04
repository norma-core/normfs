#include "normfs/seed.h"
#include "normfs/seed_sys.h"

/*
 * Proved by Frama-C WP (the verify-seed target). Must never include a system
 * header; normfs/seed_sys.h explains why.
 */

static const char normfs_seed_file_name[] = NORMFS_SEED_FILE_NAME;

/* c99 has no _Static_assert, so a negative array size is the check. */
typedef char normfs_seed_file_name_len_check[
    (sizeof(normfs_seed_file_name) == NORMFS_SEED_FILE_NAME_LEN + 1) ? 1 : -1];

/*@ axiomatic NormfsSeedPath {
      // No separator after a trailing '/': a leading "//" is implementation
      // defined in POSIX.
      logic integer normfs_seed_sep{L}(char *dir, integer dir_len) =
        dir_len <= 0 ? 0 : (dir[dir_len - 1] == '/' ? 0 : 1);

      logic integer normfs_seed_path_len{L}(char *dir, integer dir_len) =
        dir_len + normfs_seed_sep(dir, dir_len) + NORMFS_SEED_FILE_NAME_LEN;
    }
*/

/*@ requires \valid_read(data_dir + (0 .. data_dir_len));
    requires data_dir[data_dir_len] == 0;
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    requires \valid(used);
    requires out_len == 0 ||
             \separated(out + (0 .. out_len - 1),
                        data_dir + (0 .. data_dir_len));
    requires out_len == 0 || \separated(used, out + (0 .. out_len - 1));
    requires \separated(used, data_dir + (0 .. data_dir_len));
    assigns out[0 .. out_len - 1], *used;

    ensures \result == NORMFS_SEED_OK ||
            \result == NORMFS_SEED_ERR_PATH_TOO_LONG;
    ensures \result == NORMFS_SEED_ERR_PATH_TOO_LONG <==>
              out_len < normfs_seed_path_len(data_dir, data_dir_len) + 1;
    // Completeness. Without this the clause above is satisfied by a function
    // that returns PATH_TOO_LONG for everything.
    ensures \result == NORMFS_SEED_OK <==>
              out_len >= normfs_seed_path_len(data_dir, data_dir_len) + 1;

    ensures \result == NORMFS_SEED_OK ==>
              *used == normfs_seed_path_len(data_dir, data_dir_len);
    ensures \result == NORMFS_SEED_OK ==> *used < out_len;
    ensures \result == NORMFS_SEED_OK ==> out[*used] == 0;
    ensures \result == NORMFS_SEED_OK ==>
              \forall integer k; 0 <= k < data_dir_len ==>
                out[k] == data_dir[k];
    ensures \result == NORMFS_SEED_OK &&
            normfs_seed_sep(data_dir, data_dir_len) == 1 ==>
              out[data_dir_len] == '/';
    ensures \result == NORMFS_SEED_OK ==>
              \forall integer k; 0 <= k <= NORMFS_SEED_FILE_NAME_LEN ==>
                out[data_dir_len + normfs_seed_sep(data_dir, data_dir_len) + k]
                  == normfs_seed_file_name[k];
    ensures \result != NORMFS_SEED_OK ==> *used == 0;
*/
int
normfs_seed_path(const char *data_dir, size_t data_dir_len, char *out,
    size_t out_len, size_t *used)
{
	size_t sep;
	size_t need;
	size_t i;

	*used = 0u;

	sep = (data_dir_len > 0u && data_dir[data_dir_len - 1u] != '/') ? 1u : 0u;

	/* Subtractions rather than one addition: data_dir_len + sep + 13 is the
	 * one place that could wrap size_t. */
	if (out_len <= data_dir_len)
		return NORMFS_SEED_ERR_PATH_TOO_LONG;
	if (out_len - data_dir_len <= sep)
		return NORMFS_SEED_ERR_PATH_TOO_LONG;
	if (out_len - data_dir_len - sep <= (size_t)NORMFS_SEED_FILE_NAME_LEN)
		return NORMFS_SEED_ERR_PATH_TOO_LONG;

	/*@ loop invariant 0 <= i <= data_dir_len;
	    loop invariant \forall integer k; 0 <= k < i ==>
	                     out[k] == data_dir[k];
	    loop assigns i, out[0 .. data_dir_len - 1];
	    loop variant data_dir_len - i;
	*/
	for (i = 0u; i < data_dir_len; i++)
		out[i] = data_dir[i];

	if (sep == 1u)
		out[data_dir_len] = '/';

	need = data_dir_len + sep;

	/* The two carry-forward invariants are load bearing: without them WP
	 * loses the first loop's byte facts across this loop's writes. */
	/*@ loop invariant 0 <= i <= NORMFS_SEED_FILE_NAME_LEN + 1;
	    loop invariant \forall integer k; 0 <= k < i ==>
	                     out[need + k] == normfs_seed_file_name[k];
	    loop invariant \forall integer k; 0 <= k < data_dir_len ==>
	                     out[k] == data_dir[k];
	    loop invariant sep == 1 ==> out[data_dir_len] == '/';
	    loop assigns i, out[need .. need + NORMFS_SEED_FILE_NAME_LEN];
	    loop variant NORMFS_SEED_FILE_NAME_LEN + 1 - i;
	*/
	for (i = 0u; i <= (size_t)NORMFS_SEED_FILE_NAME_LEN; i++)
		out[need + i] = normfs_seed_file_name[i];

	*used = need + (size_t)NORMFS_SEED_FILE_NAME_LEN;
	return NORMFS_SEED_OK;
}

void
normfs_seed_zero(uint8_t *seed, size_t seed_len)
{
	normfs_seed_sys_zero(seed, seed_len);
}

/*@ requires 0 < len <= NORMFS_SEED_SYS_IO_MAX;
    requires \valid(seed + (0 .. len - 1));
    requires \valid(os_error);
    requires \separated(os_error, seed + (0 .. len - 1));
    assigns seed[0 .. len - 1], *os_error;
    ensures \result == NORMFS_SEED_OK ||
            \result == NORMFS_SEED_ERR_INVALID_SEED ||
            \result == NORMFS_SEED_ERR_IO;
    ensures \result == NORMFS_SEED_ERR_IO ==> *os_error > 0;
*/
static int
normfs_seed_read_exact(int fd, uint8_t *seed, size_t len, int *os_error)
{
	size_t total = 0u;
	long n;

	/*@ loop invariant 0 <= total <= len;
	    loop assigns total, n, seed[0 .. len - 1], *os_error;
	    loop variant len - total;
	*/
	while (total < len) {
		n = normfs_seed_sys_read(fd, seed + total, len - total,
		    os_error);
		if (n < 0)
			return NORMFS_SEED_ERR_IO;
		if (n == 0)
			return NORMFS_SEED_ERR_INVALID_SEED;
		total += (size_t)n;
	}

	return NORMFS_SEED_OK;
}

/*@ requires 0 < len <= NORMFS_SEED_SYS_IO_MAX;
    requires \valid_read(seed + (0 .. len - 1));
    requires \valid(os_error);
    requires \separated(os_error, seed + (0 .. len - 1));
    assigns *os_error;
    ensures \result == NORMFS_SEED_OK || \result == NORMFS_SEED_ERR_IO;
    ensures \result == NORMFS_SEED_ERR_IO ==> *os_error > 0;
*/
static int
normfs_seed_write_all(int fd, const uint8_t *seed, size_t len, int *os_error)
{
	size_t total = 0u;
	long n;

	/*@ loop invariant 0 <= total <= len;
	    loop assigns total, n, *os_error;
	    loop variant len - total;
	*/
	while (total < len) {
		n = normfs_seed_sys_write(fd, seed + total, len - total,
		    os_error);
		if (n < 0)
			return NORMFS_SEED_ERR_IO;
		total += (size_t)n;
	}

	return NORMFS_SEED_OK;
}

/*
 * Wiping first makes "nothing partial reaches the caller" fall out of the early
 * returns. getentropy leaves its buffer unspecified on failure, so that path
 * wipes again.
 */
/*@ requires \valid(seed + (0 .. NORMFS_SEED_SIZE - 1));
    assigns seed[0 .. NORMFS_SEED_SIZE - 1];
    ensures \result.status == NORMFS_SEED_OK ||
            \result.status == NORMFS_SEED_ERR_INVALID_ARG ||
            \result.status == NORMFS_SEED_ERR_OS_RNG;
    ensures \result.status == NORMFS_SEED_ERR_INVALID_ARG <==>
              seed_len != NORMFS_SEED_SIZE;
    ensures seed_len == NORMFS_SEED_SIZE ==>
              (\result.status == NORMFS_SEED_OK ||
               \result.status == NORMFS_SEED_ERR_OS_RNG);
    ensures \result.status == NORMFS_SEED_OK ==> \result.os_error == 0;
    ensures \result.status == NORMFS_SEED_ERR_INVALID_ARG ==>
              \result.os_error == 0;
    ensures \result.status == NORMFS_SEED_ERR_OS_RNG ==> \result.os_error > 0;
    ensures \result.status != NORMFS_SEED_OK ==>
              \forall integer k; 0 <= k < NORMFS_SEED_SIZE ==> seed[k] == 0;
*/
struct normfs_seed_result
normfs_seed_generate(uint8_t *seed, size_t seed_len)
{
	struct normfs_seed_result r;
	int e = 0;

	r.os_error = 0;
	r.status = NORMFS_SEED_OK;

	normfs_seed_zero(seed, (size_t)NORMFS_SEED_SIZE);

	if (seed_len != (size_t)NORMFS_SEED_SIZE) {
		r.status = NORMFS_SEED_ERR_INVALID_ARG;
		return r;
	}

	if (normfs_seed_sys_entropy(seed, (size_t)NORMFS_SEED_SIZE, &e) != 0) {
		normfs_seed_zero(seed, (size_t)NORMFS_SEED_SIZE);
		r.os_error = e;
		r.status = NORMFS_SEED_ERR_OS_RNG;
		return r;
	}

	return r;
}

/*@ requires \valid_read(data_dir + (0 .. data_dir_len));
    requires data_dir[data_dir_len] == 0;
    requires \valid(seed + (0 .. NORMFS_SEED_SIZE - 1));
    requires \separated(seed + (0 .. NORMFS_SEED_SIZE - 1),
                        data_dir + (0 .. data_dir_len));
    assigns seed[0 .. NORMFS_SEED_SIZE - 1];

    ensures \result.status == NORMFS_SEED_OK ||
            \result.status == NORMFS_SEED_ERR_INVALID_ARG ||
            \result.status == NORMFS_SEED_ERR_PATH_TOO_LONG ||
            \result.status == NORMFS_SEED_ERR_INVALID_SEED ||
            \result.status == NORMFS_SEED_ERR_IO;
    ensures \result.status == NORMFS_SEED_ERR_INVALID_ARG <==>
              seed_len != NORMFS_SEED_SIZE;
    ensures \result.status == NORMFS_SEED_ERR_PATH_TOO_LONG <==>
              (seed_len == NORMFS_SEED_SIZE &&
               NORMFS_SEED_PATH_MAX <
                 normfs_seed_path_len(data_dir, data_dir_len) + 1);
    ensures (seed_len == NORMFS_SEED_SIZE &&
             NORMFS_SEED_PATH_MAX >=
               normfs_seed_path_len(data_dir, data_dir_len) + 1) ==>
              (\result.status == NORMFS_SEED_OK ||
               \result.status == NORMFS_SEED_ERR_INVALID_SEED ||
               \result.status == NORMFS_SEED_ERR_IO);
    ensures \result.status == NORMFS_SEED_OK ==> \result.os_error == 0;
    ensures \result.status == NORMFS_SEED_ERR_IO ==> \result.os_error > 0;
    ensures \result.status != NORMFS_SEED_OK ==>
              \forall integer k; 0 <= k < NORMFS_SEED_SIZE ==> seed[k] == 0;
*/
struct normfs_seed_result
normfs_seed_load(const char *data_dir, size_t data_dir_len, uint8_t *seed,
    size_t seed_len)
{
	struct normfs_seed_result r;
	char path[NORMFS_SEED_PATH_MAX];
	size_t path_len = 0u;
	int e = 0;
	int e_close = 0;
	int status;
	int fd;

	r.os_error = 0;
	r.status = NORMFS_SEED_OK;

	normfs_seed_zero(seed, (size_t)NORMFS_SEED_SIZE);

	if (seed_len != (size_t)NORMFS_SEED_SIZE) {
		r.status = NORMFS_SEED_ERR_INVALID_ARG;
		return r;
	}

	status = normfs_seed_path(data_dir, data_dir_len, path, sizeof(path),
	    &path_len);
	if (status != NORMFS_SEED_OK) {
		r.status = status;
		return r;
	}

	fd = normfs_seed_sys_open_read(path, path_len, &e);
	if (fd < 0) {
		r.os_error = e;
		r.status = NORMFS_SEED_ERR_IO;
		return r;
	}

	status = normfs_seed_read_exact(fd, seed, (size_t)NORMFS_SEED_SIZE, &e);
	if (status == NORMFS_SEED_ERR_IO)
		r.os_error = e;

	/* The first error wins, which is why the cleanup errno gets its own
	 * local. */
	if (normfs_seed_sys_close(fd, &e_close) != 0 &&
	    status == NORMFS_SEED_OK) {
		status = NORMFS_SEED_ERR_IO;
		r.os_error = e_close;
	}

	if (status != NORMFS_SEED_OK)
		normfs_seed_zero(seed, (size_t)NORMFS_SEED_SIZE);

	r.status = status;
	return r;
}

/*
 * O_CREAT|O_EXCL rather than a temp file and rename: a rename would clobber a
 * concurrent winner's seed, and every byte already written under it would
 * become undecryptable. First writer wins, everyone else gets EEXIST.
 */
/*@ requires \valid_read(data_dir + (0 .. data_dir_len));
    requires data_dir[data_dir_len] == 0;
    requires \valid_read(seed + (0 .. NORMFS_SEED_SIZE - 1));
    requires \separated(seed + (0 .. NORMFS_SEED_SIZE - 1),
                        data_dir + (0 .. data_dir_len));
    assigns \nothing;

    ensures \result.status == NORMFS_SEED_OK ||
            \result.status == NORMFS_SEED_ERR_INVALID_ARG ||
            \result.status == NORMFS_SEED_ERR_PATH_TOO_LONG ||
            \result.status == NORMFS_SEED_ERR_IO;
    ensures \result.status == NORMFS_SEED_ERR_INVALID_ARG <==>
              seed_len != NORMFS_SEED_SIZE;
    ensures \result.status == NORMFS_SEED_ERR_PATH_TOO_LONG <==>
              (seed_len == NORMFS_SEED_SIZE &&
               NORMFS_SEED_PATH_MAX <
                 normfs_seed_path_len(data_dir, data_dir_len) + 1);
    ensures (seed_len == NORMFS_SEED_SIZE &&
             NORMFS_SEED_PATH_MAX >=
               normfs_seed_path_len(data_dir, data_dir_len) + 1) ==>
              (\result.status == NORMFS_SEED_OK ||
               \result.status == NORMFS_SEED_ERR_IO);
    ensures \result.status == NORMFS_SEED_OK ==> \result.os_error == 0;
    ensures \result.status == NORMFS_SEED_ERR_IO ==> \result.os_error > 0;
    ensures (\result.status == NORMFS_SEED_ERR_INVALID_ARG ||
             \result.status == NORMFS_SEED_ERR_PATH_TOO_LONG) ==>
              \result.os_error == 0;
*/
struct normfs_seed_result
normfs_seed_save(const char *data_dir, size_t data_dir_len,
    const uint8_t *seed, size_t seed_len)
{
	struct normfs_seed_result r;
	char path[NORMFS_SEED_PATH_MAX];
	size_t path_len = 0u;
	int e = 0;
	int e_close = 0;
	int status;
	int fd;

	r.os_error = 0;
	r.status = NORMFS_SEED_OK;

	if (seed_len != (size_t)NORMFS_SEED_SIZE) {
		r.status = NORMFS_SEED_ERR_INVALID_ARG;
		return r;
	}

	status = normfs_seed_path(data_dir, data_dir_len, path, sizeof(path),
	    &path_len);
	if (status != NORMFS_SEED_OK) {
		r.status = status;
		return r;
	}

	fd = normfs_seed_sys_create_excl(path, path_len, &e);
	if (fd < 0) {
		r.os_error = e;
		r.status = NORMFS_SEED_ERR_IO;
		return r;
	}

	status = normfs_seed_write_all(fd, seed, (size_t)NORMFS_SEED_SIZE, &e);
	if (status != NORMFS_SEED_OK)
		r.os_error = e;

	if (status == NORMFS_SEED_OK &&
	    normfs_seed_sys_fsync(fd, &e) != 0) {
		status = NORMFS_SEED_ERR_IO;
		r.os_error = e;
	}

	if (normfs_seed_sys_close(fd, &e_close) != 0 &&
	    status == NORMFS_SEED_OK) {
		status = NORMFS_SEED_ERR_IO;
		r.os_error = e_close;
	}

	r.status = status;
	return r;
}

/*@ requires \valid_read(data_dir + (0 .. data_dir_len));
    requires data_dir[data_dir_len] == 0;
    assigns \nothing;
    ensures \result == 0 || \result == 1;
*/
int
normfs_seed_exists(const char *data_dir, size_t data_dir_len)
{
	char path[NORMFS_SEED_PATH_MAX];
	size_t path_len = 0u;

	if (normfs_seed_path(data_dir, data_dir_len, path, sizeof(path),
	    &path_len) != NORMFS_SEED_OK)
		return 0;

	return normfs_seed_sys_exists(path, path_len);
}
