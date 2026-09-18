#include "normfs/disk_monitor.h"
#include "normfs/disk_monitor_sys.h"

/* Proved by Frama-C WP (verify-disk-monitor). Must never include a system
 * header; normfs/disk_monitor_sys.h explains why. */

static const char normfs_disk_ext_store[] = "store";
static const char normfs_disk_ext_wal[] = "wal";
#define NORMFS_DISK_EXT_STORE_LEN 5
#define NORMFS_DISK_EXT_WAL_LEN 3

/* c99 has no _Static_assert, so a negative array size is the check. */
typedef char normfs_disk_ext_store_len_check[
    (sizeof(normfs_disk_ext_store) == NORMFS_DISK_EXT_STORE_LEN + 1) ? 1 : -1];
typedef char normfs_disk_ext_wal_len_check[
    (sizeof(normfs_disk_ext_wal) == NORMFS_DISK_EXT_WAL_LEN + 1) ? 1 : -1];

struct normfs_disk_level {
	uint64_t dirs[NORMFS_DISK_CHUNKS / 64];
	size_t next;
	size_t path_len;
};

/*@ axiomatic NormfsDisk {
      predicate is_hex(integer c) =
        ('0' <= c <= '9') || ('a' <= c <= 'f');
      logic integer hex_val(integer c) =
        ('0' <= c <= '9') ? c - '0' : c - 'a' + 10;
      logic integer hex_digit(integer v) =
        v < 10 ? '0' + v : 'a' + (v - 10);
      logic integer next_hex(integer c) = c == '9' ? 'a' : c + 1;

      predicate disk_id_wf{L}(struct normfs_disk_id *id) =
        1 <= id->len <= NORMFS_DISK_ID_MAX &&
        (\forall integer k; 0 <= k < id->len ==> is_hex(id->hex[k])) &&
        (id->len == 1 || id->hex[0] != '0');

      predicate disk_event_wf{L}(struct normfs_disk_event *e) =
        disk_id_wf(&e->id) &&
        (e->kind == NORMFS_DISK_STORE || e->kind == NORMFS_DISK_WAL) &&
        (e->deleted == 0 || e->deleted == 1) &&
        (e->deleted == 1 ==> e->os_error == 0) &&
        (e->deleted == 0 ==> e->os_error > 0);

      predicate disk_kind_ok(integer kind) =
        kind == NORMFS_DISK_STORE || kind == NORMFS_DISK_WAL;
      logic integer disk_ext_len(integer kind) =
        kind == NORMFS_DISK_STORE ? NORMFS_DISK_EXT_STORE_LEN
                                  : NORMFS_DISK_EXT_WAL_LEN;
      logic integer disk_ext_byte{L}(integer kind, integer k) =
        kind == NORMFS_DISK_STORE ? normfs_disk_ext_store[k]
                                  : normfs_disk_ext_wal[k];

      // No separator after a trailing '/': a leading "//" is implementation
      // defined in POSIX.
      logic integer disk_sep{L}(char *dir, integer dir_len) =
        dir_len <= 0 ? 0 : (dir[dir_len - 1] == '/' ? 0 : 1);

      // Zero padded on the left to whole chunks of three digits; chunk g is
      // out[4g .. 4g+2], its separator out[4g+3]: '/' between chunks, '.'
      // before the extension. Same layout as UintN::to_file_path.
      logic integer disk_groups(integer len) = (len + 2) / 3;
      logic integer disk_pad(integer len) = 3 * disk_groups(len) - len;
      logic integer disk_digit{L}(struct normfs_disk_id *id, integer p) =
        p < disk_pad(id->len) ? '0' : id->hex[p - disk_pad(id->len)];
      // One quantifier per slot, and the id read at its own label: a
      // two-variable form, or a digit read from the same char memory that
      // the later writes havoc, times the provers out.
      predicate disk_chunks_at{Lid, Lout}(struct normfs_disk_id *id,
                                          char *out, integer base) =
        (\forall integer g; 0 <= g < disk_groups(\at(id->len, Lid)) ==>
           \at(out[base + 4 * g], Lout) == disk_digit{Lid}(id, 3 * g)) &&
        (\forall integer g; 0 <= g < disk_groups(\at(id->len, Lid)) ==>
           \at(out[base + 4 * g + 1], Lout) == disk_digit{Lid}(id, 3 * g + 1)) &&
        (\forall integer g; 0 <= g < disk_groups(\at(id->len, Lid)) ==>
           \at(out[base + 4 * g + 2], Lout) == disk_digit{Lid}(id, 3 * g + 2)) &&
        (\forall integer g; 0 <= g < disk_groups(\at(id->len, Lid)) ==>
           \at(out[base + 4 * g + 3], Lout) ==
             (g == disk_groups(\at(id->len, Lid)) - 1 ? '.' : '/'));
      logic integer disk_path_len{L}(char *dir, integer dir_len,
                                     integer id_len, integer kind) =
        dir_len + disk_sep(dir, dir_len) + 4 * disk_groups(id_len) +
        disk_ext_len(kind);

      logic integer disk_chunk_val(integer c, integer r) =
        r == 0 ? c / 256 : (r == 1 ? (c / 16) % 16 : c % 16);
      logic integer disk_chunk_digit{L}(uint16_t *chunks, integer p) =
        hex_digit(disk_chunk_val(chunks[p / 3], p % 3));

    }
*/

/*@ assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==> is_hex(c);
*/
static int
normfs_disk_is_hex(char c)
{
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

/*@ requires is_hex(c);
    assigns \nothing;
    ensures \result == hex_val(c);
    ensures 0 <= \result < 16;
*/
static unsigned
normfs_disk_hex_val(char c)
{
	if (c >= '0' && c <= '9')
		return (unsigned)(c - '0');
	return (unsigned)(c - 'a') + 10u;
}

/*@ requires v < 16;
    assigns \nothing;
    ensures \result == hex_digit(v);
    ensures is_hex(\result);
*/
static char
normfs_disk_hex_digit(unsigned v)
{
	if (v < 10u)
		return (char)('0' + (int)v);
	return (char)('a' + (int)(v - 10u));
}

/*@ requires \valid_read(id);
    assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==> disk_id_wf(id);
*/
static int
normfs_disk_id_ok(const struct normfs_disk_id *id)
{
	size_t k;

	if (id->len < 1u || id->len > (size_t)NORMFS_DISK_ID_MAX)
		return 0;

	/*@ loop invariant 0 <= k <= id->len;
	    loop invariant \forall integer q; 0 <= q < k ==> is_hex(id->hex[q]);
	    loop assigns k;
	    loop variant id->len - k;
	*/
	for (k = 0u; k < id->len; k++) {
		if (!normfs_disk_is_hex(id->hex[k]))
			return 0;
	}

	if (id->len > 1u && id->hex[0] == '0')
		return 0;

	return 1;
}

/* Numeric order, given no leading zeros: length first, then bytewise. */
/*@ requires \valid_read(a);
    requires \valid_read(b);
    requires a->len <= NORMFS_DISK_ID_MAX;
    requires b->len <= NORMFS_DISK_ID_MAX;
    assigns \nothing;
    ensures \result == -1 || \result == 0 || \result == 1;
    ensures a->len < b->len ==> \result == -1;
    ensures a->len > b->len ==> \result == 1;
    ensures \result == 0 ==> a->len == b->len;
    ensures \result == 0 ==>
              \forall integer k; 0 <= k < a->len ==> a->hex[k] == b->hex[k];
    ensures a->len == b->len &&
            (\forall integer k; 0 <= k < a->len ==> a->hex[k] == b->hex[k])
              ==> \result == 0;
    ensures a->len == b->len && \result == -1 ==>
              \exists integer j; 0 <= j < a->len && a->hex[j] < b->hex[j] &&
                (\forall integer k; 0 <= k < j ==> a->hex[k] == b->hex[k]);
    ensures a->len == b->len && \result == 1 ==>
              \exists integer j; 0 <= j < a->len && a->hex[j] > b->hex[j] &&
                (\forall integer k; 0 <= k < j ==> a->hex[k] == b->hex[k]);
*/
static int
normfs_disk_id_cmp(const struct normfs_disk_id *a,
    const struct normfs_disk_id *b)
{
	size_t k;

	if (a->len < b->len)
		return -1;
	if (a->len > b->len)
		return 1;

	/*@ loop invariant 0 <= k <= a->len;
	    loop invariant \forall integer q; 0 <= q < k ==> a->hex[q] == b->hex[q];
	    loop assigns k;
	    loop variant a->len - k;
	*/
	for (k = 0u; k < a->len; k++) {
		if (a->hex[k] < b->hex[k])
			return -1;
		if (a->hex[k] > b->hex[k])
			return 1;
	}

	return 0;
}

/* Field by field: a struct copy is a memcpy over both memories to the
 * provers, and disk_id_wf does not survive it in a large context. */
/*@ requires \valid_read(src);
    requires disk_id_wf(src);
    requires \valid(dst);
    requires \separated(dst, src);
    assigns dst->hex[0 .. NORMFS_DISK_ID_MAX - 1], dst->len;
    ensures disk_id_wf(dst);
    ensures dst->len == src->len;
    ensures \forall integer k; 0 <= k < src->len ==> dst->hex[k] == src->hex[k];
*/
static void
normfs_disk_id_copy(struct normfs_disk_id *dst,
    const struct normfs_disk_id *src)
{
	size_t k;

	/*@ loop invariant 0 <= k <= src->len;
	    loop invariant \forall integer q; 0 <= q < k ==> dst->hex[q] == src->hex[q];
	    loop assigns k, dst->hex[0 .. NORMFS_DISK_ID_MAX - 1];
	    loop variant src->len - k;
	*/
	for (k = 0u; k < src->len; k++)
		dst->hex[k] = src->hex[k];
	dst->len = src->len;
}

/*@ requires \valid(id);
    requires disk_id_wf(id);
    assigns id->hex[0 .. NORMFS_DISK_ID_MAX - 1], id->len;
    ensures \result == NORMFS_DISK_OK ||
            \result == NORMFS_DISK_ERR_ID_OVERFLOW;
    ensures \result == NORMFS_DISK_ERR_ID_OVERFLOW <==>
              (\old(id->len) == NORMFS_DISK_ID_MAX &&
               \forall integer k; 0 <= k < \old(id->len) ==>
                 \old(id->hex[k]) == 'f');
    ensures \result == NORMFS_DISK_ERR_ID_OVERFLOW ==>
              id->len == \old(id->len) &&
              \forall integer k; 0 <= k < NORMFS_DISK_ID_MAX ==>
                id->hex[k] == \old(id->hex[k]);
    ensures \result == NORMFS_DISK_OK ==> disk_id_wf(id);
    ensures \result == NORMFS_DISK_OK &&
            (\forall integer k; 0 <= k < \old(id->len) ==>
               \old(id->hex[k]) == 'f') ==>
              id->len == \old(id->len) + 1 &&
              id->hex[0] == '1' &&
              \forall integer k; 1 <= k < id->len ==> id->hex[k] == '0';
    ensures \result == NORMFS_DISK_OK &&
            !(\forall integer k; 0 <= k < \old(id->len) ==>
                \old(id->hex[k]) == 'f') ==>
              id->len == \old(id->len) &&
              \exists integer j; 0 <= j < id->len &&
                \old(id->hex[j]) != 'f' &&
                (\forall integer k; j < k < id->len ==> \old(id->hex[k]) == 'f') &&
                id->hex[j] == next_hex(\old(id->hex[j])) &&
                (\forall integer k; j < k < id->len ==> id->hex[k] == '0') &&
                (\forall integer k; 0 <= k < j ==> id->hex[k] == \old(id->hex[k]));
*/
static int
normfs_disk_id_increment(struct normfs_disk_id *id)
{
	size_t j;
	size_t k;

	/* Carry point first, so the overflow case writes nothing. */
	j = id->len;
	/*@ loop invariant 0 <= j <= id->len;
	    loop invariant \forall integer q; j <= q < id->len ==> id->hex[q] == 'f';
	    loop assigns j;
	    loop variant j;
	*/
	while (j > 0u && id->hex[j - 1u] == 'f')
		j--;

	if (j == 0u) {
		if (id->len == (size_t)NORMFS_DISK_ID_MAX)
			return NORMFS_DISK_ERR_ID_OVERFLOW;

		/*@ loop invariant 0 <= k <= id->len;
		    loop invariant \forall integer q; 0 <= q < k ==> id->hex[q] == '0';
		    loop assigns k, id->hex[0 .. \at(id->len, Pre) - 1];
		    loop variant id->len - k;
		*/
		for (k = 0u; k < id->len; k++)
			id->hex[k] = '0';
		id->hex[0] = '1';
		id->hex[id->len] = '0';
		id->len++;
		return NORMFS_DISK_OK;
	}

	/*@ loop invariant j <= k <= id->len;
	    loop invariant \forall integer q; j <= q < k ==> id->hex[q] == '0';
	    loop invariant \forall integer q; 0 <= q < j ==>
	                     id->hex[q] == \at(id->hex[q], Pre);
	    loop assigns k, id->hex[j .. \at(id->len, Pre) - 1];
	    loop variant id->len - k;
	*/
	for (k = j; k < id->len; k++)
		id->hex[k] = '0';

	if (id->hex[j - 1u] == '9')
		id->hex[j - 1u] = 'a';
	else
		id->hex[j - 1u] = (char)(id->hex[j - 1u] + 1);

	return NORMFS_DISK_OK;
}

/*@ requires 1 <= n <= NORMFS_DISK_MAX_DEPTH;
    requires \valid_read(chunks + (0 .. n - 1));
    requires \forall integer k; 0 <= k < n ==> chunks[k] < NORMFS_DISK_CHUNKS;
    requires \valid(id);
    requires \separated(id, chunks + (0 .. n - 1));
    assigns id->hex[0 .. NORMFS_DISK_ID_MAX - 1], id->len;
    ensures disk_id_wf(id);
    ensures \exists integer z; 0 <= z < 3 * n &&
              (\forall integer q; 0 <= q < z ==> disk_chunk_digit(chunks, q) == '0') &&
              (z == 3 * n - 1 || disk_chunk_digit(chunks, z) != '0') &&
              id->len == 3 * n - z &&
              (\forall integer k; 0 <= k < id->len ==>
                 id->hex[k] == disk_chunk_digit(chunks, z + k));
*/
static void
normfs_disk_id_from_chunks(const uint16_t *chunks, size_t n,
    struct normfs_disk_id *id)
{
	char d[NORMFS_DISK_ID_MAX];
	size_t i;
	size_t z;
	size_t k;
	unsigned c;

	/*@ loop invariant 0 <= i <= n;
	    loop invariant \forall integer p; 0 <= p < 3 * i ==>
	                     d[p] == disk_chunk_digit(chunks, p);
	    loop assigns i, c, d[0 .. 3 * n - 1];
	    loop variant n - i;
	*/
	for (i = 0u; i < n; i++) {
		c = chunks[i];
		d[3u * i] = normfs_disk_hex_digit(c / 256u);
		d[3u * i + 1u] = normfs_disk_hex_digit((c / 16u) % 16u);
		d[3u * i + 2u] = normfs_disk_hex_digit(c % 16u);
	}

	z = 0u;
	/*@ loop invariant 0 <= z <= 3 * n - 1;
	    loop invariant \forall integer q; 0 <= q < z ==> d[q] == '0';
	    loop assigns z;
	    loop variant 3 * n - 1 - z;
	*/
	while (z < 3u * n - 1u && d[z] == '0')
		z++;

	id->len = 3u * n - z;
	/*@ loop invariant 0 <= k <= id->len;
	    loop invariant id->len == 3 * n - z;
	    loop invariant \forall integer q; 0 <= q < k ==> id->hex[q] == d[z + q];
	    loop assigns k, id->hex[0 .. 3 * n - 1];
	    loop variant id->len - k;
	*/
	for (k = 0u; k < id->len; k++)
		id->hex[k] = d[z + k];
}

/*@ requires name_len == 0 || \valid_read(name + (0 .. name_len - 1));
    requires disk_kind_ok(kind);
    requires \valid(idx);
    requires name_len == 0 || \separated(idx, name + (0 .. name_len - 1));
    assigns *idx;
    ensures \result == 0 || \result == 1 || \result == 2;
    ensures \result == 1 <==>
              (name_len == 3 && is_hex(name[0]) && is_hex(name[1]) &&
               is_hex(name[2]));
    ensures \result == 2 <==>
              (name_len == 4 + disk_ext_len(kind) &&
               is_hex(name[0]) && is_hex(name[1]) && is_hex(name[2]) &&
               name[3] == '.' &&
               \forall integer k; 0 <= k < disk_ext_len(kind) ==>
                 name[4 + k] == disk_ext_byte(kind, k));
    ensures \result != 0 ==>
              *idx == hex_val(name[0]) * 256 + hex_val(name[1]) * 16 +
                      hex_val(name[2]);
    ensures \result != 0 ==> *idx < NORMFS_DISK_CHUNKS;
*/
static int
normfs_disk_parse_name(const char *name, size_t name_len, int kind,
    uint16_t *idx)
{
	size_t ext_len;
	size_t k;
	unsigned v;

	ext_len = (kind == NORMFS_DISK_STORE) ? (size_t)NORMFS_DISK_EXT_STORE_LEN
	                                      : (size_t)NORMFS_DISK_EXT_WAL_LEN;

	if (name_len != 3u && name_len != 4u + ext_len)
		return 0;
	if (!normfs_disk_is_hex(name[0]) || !normfs_disk_is_hex(name[1]) ||
	    !normfs_disk_is_hex(name[2]))
		return 0;

	v = normfs_disk_hex_val(name[0]) * 256u +
	    normfs_disk_hex_val(name[1]) * 16u + normfs_disk_hex_val(name[2]);

	if (name_len == 3u) {
		*idx = (uint16_t)v;
		return 1;
	}

	if (name[3] != '.')
		return 0;

	/*@ loop invariant 0 <= k <= ext_len;
	    loop invariant \forall integer q; 0 <= q < k ==>
	                     name[4 + q] == disk_ext_byte(kind, q);
	    loop assigns k;
	    loop variant ext_len - k;
	*/
	for (k = 0u; k < ext_len; k++) {
		char e = (kind == NORMFS_DISK_STORE) ? normfs_disk_ext_store[k]
		                                     : normfs_disk_ext_wal[k];

		if (name[4u + k] != e)
			return 0;
	}

	*idx = (uint16_t)v;
	return 2;
}

/*@ requires \valid(dirs + (0 .. NORMFS_DISK_CHUNKS / 64 - 1));
    requires idx < NORMFS_DISK_CHUNKS;
    assigns dirs[idx / 64];
*/
static void
normfs_disk_set_bit(uint64_t *dirs, size_t idx)
{
	dirs[idx / 64u] |= (uint64_t)1 << (idx % 64u);
}

/*@ requires \valid_read(dirs + (0 .. NORMFS_DISK_CHUNKS / 64 - 1));
    requires from <= NORMFS_DISK_CHUNKS;
    assigns \nothing;
    ensures from <= \result <= NORMFS_DISK_CHUNKS;
*/
static size_t
normfs_disk_next_bit(const uint64_t *dirs, size_t from)
{
	size_t i;

	/*@ loop invariant from <= i <= NORMFS_DISK_CHUNKS;
	    loop assigns i;
	    loop variant NORMFS_DISK_CHUNKS - i;
	*/
	for (i = from; i < (size_t)NORMFS_DISK_CHUNKS; i++) {
		if (((dirs[i / 64u] >> (i % 64u)) & (uint64_t)1) != 0u)
			return i;
	}

	return (size_t)NORMFS_DISK_CHUNKS;
}

/*@ requires \valid_read(id);
    requires disk_id_wf(id);
    requires \valid(out + (0 .. 4 * disk_groups(id->len) - 1));
    requires \separated(id, out + (0 .. 4 * disk_groups(id->len) - 1));
    assigns out[0 .. 4 * disk_groups(id->len) - 1];
    ensures disk_chunks_at{Pre, Post}(id, out, 0);
*/
static void
normfs_disk_write_chunks(const struct normfs_disk_id *id, char *out)
{
	size_t groups = (id->len + 2u) / 3u;
	size_t pad = 3u * groups - id->len;
	size_t g;
	size_t p;

	/*@ loop invariant 0 <= g <= groups;
	    loop invariant \forall integer gg; 0 <= gg < g ==>
	                     out[4 * gg] == disk_digit{Pre}(id, 3 * gg);
	    loop invariant \forall integer gg; 0 <= gg < g ==>
	                     out[4 * gg + 1] == disk_digit{Pre}(id, 3 * gg + 1);
	    loop invariant \forall integer gg; 0 <= gg < g ==>
	                     out[4 * gg + 2] == disk_digit{Pre}(id, 3 * gg + 2);
	    loop invariant \forall integer gg; 0 <= gg < g ==>
	                     out[4 * gg + 3] == (gg == groups - 1 ? '.' : '/');
	    loop assigns g, p, out[0 .. 4 * groups - 1];
	    loop variant groups - g;
	*/
	for (g = 0u; g < groups; g++) {
		p = 3u * g;
		out[4u * g] = (p < pad) ? '0' : id->hex[p - pad];
		out[4u * g + 1u] = (p + 1u < pad) ? '0' : id->hex[p + 1u - pad];
		out[4u * g + 2u] = id->hex[p + 2u - pad];
		out[4u * g + 3u] = (g + 1u == groups) ? '.' : '/';
	}
}

/*@ requires disk_kind_ok(kind);
    requires \valid(out + (0 .. disk_ext_len(kind) - 1));
    assigns out[0 .. disk_ext_len(kind) - 1];
    ensures \forall integer k; 0 <= k < disk_ext_len(kind) ==>
              out[k] == disk_ext_byte(kind, k);
*/
static void
normfs_disk_write_ext(int kind, char *out)
{
	size_t ext_len = (kind == NORMFS_DISK_STORE)
	    ? (size_t)NORMFS_DISK_EXT_STORE_LEN : (size_t)NORMFS_DISK_EXT_WAL_LEN;
	size_t k;

	/*@ loop invariant 0 <= k <= ext_len;
	    loop invariant \forall integer q; 0 <= q < k ==>
	                     out[q] == disk_ext_byte(kind, q);
	    loop assigns k, out[0 .. ext_len - 1];
	    loop variant ext_len - k;
	*/
	for (k = 0u; k < ext_len; k++)
		out[k] = (kind == NORMFS_DISK_STORE) ? normfs_disk_ext_store[k]
		                                     : normfs_disk_ext_wal[k];
}

/*@ requires \valid_read(dir + (0 .. dir_len));
    requires dir[dir_len] == 0;
    requires \valid_read(id);
    requires out_len == 0 || \valid(out + (0 .. out_len - 1));
    requires \valid(used);
    requires out_len == 0 ||
             \separated(out + (0 .. out_len - 1), dir + (0 .. dir_len));
    requires out_len == 0 || \separated(out + (0 .. out_len - 1), id);
    requires out_len == 0 || \separated(used, out + (0 .. out_len - 1));
    requires \separated(used, dir + (0 .. dir_len));
    requires \separated(used, id);
    assigns out[0 .. out_len - 1], *used;

    ensures \result.os_error == 0;
    ensures \result.status == NORMFS_DISK_OK ||
            \result.status == NORMFS_DISK_ERR_INVALID_ARG ||
            \result.status == NORMFS_DISK_ERR_PATH_TOO_LONG;
    ensures \result.status == NORMFS_DISK_ERR_INVALID_ARG <==>
              !(disk_kind_ok(kind) && disk_id_wf(id));
    ensures \result.status == NORMFS_DISK_ERR_PATH_TOO_LONG <==>
              (disk_kind_ok(kind) && disk_id_wf(id) &&
               (dir_len >= NORMFS_DISK_PATH_MAX ||
                out_len < disk_path_len(dir, dir_len, id->len, kind) + 1));
    // Completeness: without this the two clauses above are satisfied by a
    // function that never returns OK.
    ensures \result.status == NORMFS_DISK_OK <==>
              (disk_kind_ok(kind) && disk_id_wf(id) &&
               dir_len < NORMFS_DISK_PATH_MAX &&
               out_len >= disk_path_len(dir, dir_len, id->len, kind) + 1);

    ensures \result.status == NORMFS_DISK_OK ==>
              *used == disk_path_len(dir, dir_len, id->len, kind);
    ensures \result.status == NORMFS_DISK_OK ==> *used < out_len;
    ensures \result.status == NORMFS_DISK_OK ==> out[*used] == 0;
    ensures \result.status == NORMFS_DISK_OK ==>
              \forall integer k; 0 <= k < dir_len ==> out[k] == dir[k];
    ensures \result.status == NORMFS_DISK_OK && disk_sep(dir, dir_len) == 1 ==>
              out[dir_len] == '/';
    ensures \result.status == NORMFS_DISK_OK ==>
              disk_chunks_at{Pre, Post}(id, out,
                                        dir_len + disk_sep(dir, dir_len));
    ensures \result.status == NORMFS_DISK_OK ==>
              \forall integer k; 0 <= k < disk_ext_len(kind) ==>
                out[dir_len + disk_sep(dir, dir_len) +
                    4 * disk_groups(id->len) + k] == disk_ext_byte(kind, k);
    ensures \result.status != NORMFS_DISK_OK ==> *used == 0;
*/
struct normfs_disk_result
normfs_disk_path(const char *dir, size_t dir_len,
    const struct normfs_disk_id *id, int kind,
    char *out, size_t out_len, size_t *used)
{
	struct normfs_disk_result r;
	size_t sep;
	size_t base;
	size_t groups;
	size_t ext_len;
	size_t need;
	size_t k;

	r.os_error = 0;
	r.status = NORMFS_DISK_OK;

	if ((kind != NORMFS_DISK_STORE && kind != NORMFS_DISK_WAL) ||
	    !normfs_disk_id_ok(id)) {
		*used = 0u;
		r.status = NORMFS_DISK_ERR_INVALID_ARG;
		return r;
	}
	if (dir_len >= (size_t)NORMFS_DISK_PATH_MAX) {
		*used = 0u;
		r.status = NORMFS_DISK_ERR_PATH_TOO_LONG;
		return r;
	}

	/* Bounds the provers otherwise have to dig out of disk_id_wf. */
	//@ assert 1 <= id->len <= NORMFS_DISK_ID_MAX;

	sep = (dir_len > 0u && dir[dir_len - 1u] != '/') ? 1u : 0u;
	base = dir_len + sep;
	groups = (id->len + 2u) / 3u;
	//@ assert groups == disk_groups(id->len) && 1 <= groups <= NORMFS_DISK_MAX_DEPTH;
	ext_len = (kind == NORMFS_DISK_STORE) ? (size_t)NORMFS_DISK_EXT_STORE_LEN
	                                      : (size_t)NORMFS_DISK_EXT_WAL_LEN;

	need = base + 4u * groups + ext_len + 1u;
	if (out_len < need) {
		*used = 0u;
		r.status = NORMFS_DISK_ERR_PATH_TOO_LONG;
		return r;
	}

	/* Chunks before anything else is stored: the callee reads the id in
	 * the memory of the call, the postcondition reads it at Pre, and the
	 * copy below then only has to keep out[base ..] out of its frame. */
	normfs_disk_write_chunks(id, out + base);
	normfs_disk_write_ext(kind, out + base + 4u * groups);
	out[base + 4u * groups + ext_len] = '\0';

	/*@ loop invariant 0 <= k <= dir_len;
	    loop invariant \forall integer q; 0 <= q < k ==> out[q] == dir[q];
	    loop invariant disk_chunks_at{Pre, Here}(id, out, base);
	    loop invariant \forall integer q; 0 <= q < ext_len ==>
	                     out[base + 4 * groups + q] == disk_ext_byte(kind, q);
	    loop invariant out[base + 4 * groups + ext_len] == 0;
	    loop assigns k, out[0 .. dir_len - 1];
	    loop variant dir_len - k;
	*/
	for (k = 0u; k < dir_len; k++)
		out[k] = dir[k];

	if (sep == 1u)
		out[dir_len] = '/';

	*used = base + 4u * groups + ext_len;
	return r;
}

/* The path stays in `path` for an unlink to follow. */
/*@ requires \valid_read(dir + (0 .. dir_len));
    requires dir[dir_len] == 0;
    requires \valid_read(id);
    requires disk_id_wf(id);
    requires disk_kind_ok(kind);
    requires \valid(path + (0 .. NORMFS_DISK_PATH_MAX - 1));
    requires \valid(plen);
    requires \valid(size);
    requires \valid(os_error);
    requires \separated(path + (0 .. NORMFS_DISK_PATH_MAX - 1), plen, size,
                        os_error, id);
    requires \separated(path + (0 .. NORMFS_DISK_PATH_MAX - 1), plen, size,
                        os_error, dir + (0 .. dir_len));
    assigns path[0 .. NORMFS_DISK_PATH_MAX - 1], *plen, *size, *os_error;
    ensures \result == -2 || \result == -1 || \result == 0 || \result == 1;
    ensures \result >= -1 ==> *plen < NORMFS_DISK_PATH_MAX;
    ensures \result >= -1 ==> path[*plen] == 0;
    ensures \result == -1 ==> *os_error > 0;
    ensures \result != -1 ==> *os_error == 0;
*/
static int
normfs_disk_probe(const char *dir, size_t dir_len,
    const struct normfs_disk_id *id, int kind, char *path, size_t *plen,
    uint64_t *size, int *os_error)
{
	struct normfs_disk_result pr;

	*os_error = 0;
	*size = 0u;
	pr = normfs_disk_path(dir, dir_len, id, kind, path,
	    (size_t)NORMFS_DISK_PATH_MAX, plen);
	if (pr.status != NORMFS_DISK_OK)
		return -2;

	return normfs_disk_sys_file_size(path, *plen, size, os_error);
}

/*@ requires \valid_read(dir + (0 .. dir_len));
    requires dir[dir_len] == 0;
    requires \valid_read(id);
    requires \valid(size);
    requires \separated(size, dir + (0 .. dir_len));
    requires \separated(size, id);
    assigns *size;
    ensures \result.status == NORMFS_DISK_OK ||
            \result.status == NORMFS_DISK_ERR_INVALID_ARG ||
            \result.status == NORMFS_DISK_ERR_PATH_TOO_LONG ||
            \result.status == NORMFS_DISK_ERR_NOT_FOUND ||
            \result.status == NORMFS_DISK_ERR_IO;
    ensures \result.status == NORMFS_DISK_ERR_IO ==> \result.os_error > 0;
    ensures \result.status != NORMFS_DISK_ERR_IO ==> \result.os_error == 0;
*/
struct normfs_disk_result
normfs_disk_file_size(const char *dir, size_t dir_len,
    const struct normfs_disk_id *id, int kind, uint64_t *size)
{
	struct normfs_disk_result r;
	char path[NORMFS_DISK_PATH_MAX];
	size_t plen = 0u;
	uint64_t found = 0u;
	int e = 0;
	int rc;

	r.os_error = 0;
	r.status = NORMFS_DISK_OK;
	*size = 0u;

	if ((kind != NORMFS_DISK_STORE && kind != NORMFS_DISK_WAL) ||
	    !normfs_disk_id_ok(id)) {
		r.status = NORMFS_DISK_ERR_INVALID_ARG;
		return r;
	}

	rc = normfs_disk_probe(dir, dir_len, id, kind, path, &plen, &found, &e);
	if (rc == -2) {
		r.status = NORMFS_DISK_ERR_PATH_TOO_LONG;
	} else if (rc == -1) {
		r.os_error = e;
		r.status = NORMFS_DISK_ERR_IO;
	} else if (rc == 0) {
		r.status = NORMFS_DISK_ERR_NOT_FOUND;
	} else {
		*size = found;
	}

	return r;
}

/* chunks[0 .. depth) is the path down to this directory, chunks[depth] is
 * scratch for the file ids. */
/*@ requires path_len < NORMFS_DISK_PATH_MAX;
    requires \valid_read(path + (0 .. path_len));
    requires path[path_len] == 0;
    requires disk_kind_ok(kind);
    requires depth < NORMFS_DISK_MAX_DEPTH;
    requires \valid(chunks + (0 .. NORMFS_DISK_MAX_DEPTH - 1));
    requires \forall integer k; 0 <= k < depth ==> chunks[k] < NORMFS_DISK_CHUNKS;
    requires \valid(lv);
    requires \valid(acc);
    requires acc->has_min == 0 || acc->has_min == 1;
    requires acc->has_min == 1 ==> disk_id_wf(&acc->min);
    requires \valid(os_error);
    requires \separated(path + (0 .. path_len),
                        chunks + (0 .. NORMFS_DISK_MAX_DEPTH - 1),
                        lv, acc, os_error);
    assigns lv->dirs[0 .. NORMFS_DISK_CHUNKS / 64 - 1], lv->next,
            chunks[depth], acc->total, acc->min, acc->has_min, *os_error;
    ensures \result == NORMFS_DISK_OK ||
            \result == NORMFS_DISK_ERR_TOO_MANY ||
            \result == NORMFS_DISK_ERR_IO;
    ensures \result == NORMFS_DISK_ERR_IO ==> *os_error > 0;
    ensures \result != NORMFS_DISK_ERR_IO ==> *os_error == 0;
    ensures lv->next == 0;
    ensures acc->has_min == 0 || acc->has_min == 1;
    ensures acc->has_min == 1 ==> disk_id_wf(&acc->min);
*/
static int
normfs_disk_list_dir(const char *path, size_t path_len, int kind,
    uint16_t *chunks, size_t depth, struct normfs_disk_level *lv,
    struct normfs_disk_scan *acc, int *os_error)
{
	char name[NORMFS_DISK_SYS_NAME_MAX];
	struct normfs_disk_id cand;
	void *h;
	size_t name_len = 0u;
	size_t n;
	size_t i;
	uint64_t size = 0u;
	uint16_t idx = 0u;
	int ekind = 0;
	int parsed;
	int rc;
	int done = 0;

	*os_error = 0;
	lv->next = 0u;
	/*@ loop invariant 0 <= i <= NORMFS_DISK_CHUNKS / 64;
	    loop assigns i, lv->dirs[0 .. NORMFS_DISK_CHUNKS / 64 - 1];
	    loop variant NORMFS_DISK_CHUNKS / 64 - i;
	*/
	for (i = 0u; i < (size_t)(NORMFS_DISK_CHUNKS / 64); i++)
		lv->dirs[i] = 0u;

	h = normfs_disk_sys_dir_open(path, path_len, os_error);
	if (h == NULL) {
		/* Absent is empty: not written yet, or gone since the parent
		 * was listed. */
		if (*os_error == 0)
			return NORMFS_DISK_OK;
		return NORMFS_DISK_ERR_IO;
	}

	/*@ loop invariant 0 <= n <= NORMFS_DISK_DIR_ENTRIES_MAX;
	    loop invariant done == 0 || done == 1;
	    loop invariant *os_error == 0;
	    loop invariant acc->has_min == 0 || acc->has_min == 1;
	    loop invariant acc->has_min == 1 ==> disk_id_wf(&acc->min);
	    loop invariant \forall integer k; 0 <= k < depth ==>
	                     chunks[k] < NORMFS_DISK_CHUNKS;
	    loop assigns n, rc, parsed, name_len, ekind, size, idx, done,
	                 name[0 .. NORMFS_DISK_SYS_NAME_MAX - 1],
	                 cand, chunks[depth],
	                 lv->dirs[0 .. NORMFS_DISK_CHUNKS / 64 - 1],
	                 acc->total, acc->min, acc->has_min, *os_error;
	    loop variant NORMFS_DISK_DIR_ENTRIES_MAX - n;
	*/
	for (n = 0u; n < (size_t)NORMFS_DISK_DIR_ENTRIES_MAX && done == 0; n++) {
		rc = normfs_disk_sys_dir_next(h, name, sizeof(name), &name_len,
		    &ekind, &size, os_error);
		if (rc < 0) {
			normfs_disk_sys_dir_close(h);
			return NORMFS_DISK_ERR_IO;
		}
		if (rc == 0) {
			done = 1;
			continue;
		}

		parsed = normfs_disk_parse_name(name, name_len, kind, &idx);
		if (parsed == 1 && ekind == NORMFS_DISK_SYS_DIR) {
			normfs_disk_set_bit(lv->dirs, idx);
		} else if (parsed == 2 && ekind == NORMFS_DISK_SYS_FILE) {
			if (UINT64_MAX - acc->total < size)
				acc->total = UINT64_MAX;
			else
				acc->total += size;

			chunks[depth] = idx;
			normfs_disk_id_from_chunks(chunks, depth + 1u, &cand);
			if (acc->has_min == 0 ||
			    normfs_disk_id_cmp(&cand, &acc->min) < 0) {
				acc->min = cand;
				acc->has_min = 1;
			}
		}
	}

	normfs_disk_sys_dir_close(h);

	if (done == 0)
		return NORMFS_DISK_ERR_TOO_MANY;
	return NORMFS_DISK_OK;
}

/* Depth first with an explicit stack of bitmaps: one listing open at a
 * time, no allocation, and the order does not matter. */
/*@ requires \valid_read(dir + (0 .. dir_len));
    requires dir[dir_len] == 0;
    requires \valid(out);
    requires \separated(out, dir + (0 .. dir_len));
    assigns *out;
    ensures \result.status == NORMFS_DISK_OK ||
            \result.status == NORMFS_DISK_ERR_INVALID_ARG ||
            \result.status == NORMFS_DISK_ERR_PATH_TOO_LONG ||
            \result.status == NORMFS_DISK_ERR_TOO_DEEP ||
            \result.status == NORMFS_DISK_ERR_TOO_MANY ||
            \result.status == NORMFS_DISK_ERR_IO;
    ensures \result.status == NORMFS_DISK_ERR_INVALID_ARG <==>
              !disk_kind_ok(kind);
    ensures \result.status == NORMFS_DISK_ERR_IO ==> \result.os_error > 0;
    ensures \result.status != NORMFS_DISK_ERR_IO ==> \result.os_error == 0;
    ensures out->has_min == 0 || out->has_min == 1;
    ensures out->has_min == 1 ==> disk_id_wf(&out->min);
*/
struct normfs_disk_result
normfs_disk_scan(const char *dir, size_t dir_len, int kind,
    struct normfs_disk_scan *out)
{
	struct normfs_disk_result r;
	struct normfs_disk_level levels[NORMFS_DISK_MAX_DEPTH];
	uint16_t chunks[NORMFS_DISK_MAX_DEPTH];
	char path[NORMFS_DISK_PATH_MAX];
	size_t depth;
	size_t steps;
	size_t idx;
	size_t plen;
	size_t k;
	int e = 0;
	int st;

	r.os_error = 0;
	r.status = NORMFS_DISK_OK;
	out->total = 0u;
	out->has_min = 0;
	out->min.len = 1u;
	out->min.hex[0] = '0';

	if (kind != NORMFS_DISK_STORE && kind != NORMFS_DISK_WAL) {
		r.status = NORMFS_DISK_ERR_INVALID_ARG;
		return r;
	}
	if (dir_len >= (size_t)NORMFS_DISK_PATH_MAX) {
		r.status = NORMFS_DISK_ERR_PATH_TOO_LONG;
		return r;
	}

	/*@ loop invariant 0 <= k <= dir_len;
	    loop assigns k, path[0 .. dir_len - 1];
	    loop variant dir_len - k;
	*/
	for (k = 0u; k < dir_len; k++)
		path[k] = dir[k];
	path[dir_len] = '\0';

	levels[0].path_len = dir_len;
	st = normfs_disk_list_dir(path, dir_len, kind, chunks, 0u, &levels[0],
	    out, &e);
	if (st != NORMFS_DISK_OK) {
		r.os_error = e;
		r.status = st;
		return r;
	}

	depth = 0u;
	steps = 0u;
	/*@ loop invariant 0 <= depth < NORMFS_DISK_MAX_DEPTH;
	    loop invariant 0 <= steps <= NORMFS_DISK_WALK_STEPS_MAX;
	    loop invariant \forall integer d; 0 <= d <= depth ==>
	                     levels[d].path_len < NORMFS_DISK_PATH_MAX;
	    loop invariant \forall integer d; 0 <= d <= depth ==>
	                     levels[d].next <= NORMFS_DISK_CHUNKS;
	    loop invariant \forall integer d; 0 <= d < depth ==>
	                     chunks[d] < NORMFS_DISK_CHUNKS;
	    loop invariant out->has_min == 0 || out->has_min == 1;
	    loop invariant out->has_min == 1 ==> disk_id_wf(&out->min);
	    loop assigns depth, steps, idx, plen, st, e,
	                 levels[0 .. NORMFS_DISK_MAX_DEPTH - 1],
	                 chunks[0 .. NORMFS_DISK_MAX_DEPTH - 1],
	                 path[0 .. NORMFS_DISK_PATH_MAX - 1],
	                 *out;
	    loop variant NORMFS_DISK_WALK_STEPS_MAX - steps;
	*/
	for (;;) {
		if (steps == (size_t)NORMFS_DISK_WALK_STEPS_MAX) {
			r.status = NORMFS_DISK_ERR_TOO_MANY;
			return r;
		}
		steps++;

		idx = normfs_disk_next_bit(levels[depth].dirs,
		    levels[depth].next);
		if (idx == (size_t)NORMFS_DISK_CHUNKS) {
			if (depth == 0u)
				break;
			depth--;
			continue;
		}
		levels[depth].next = idx + 1u;

		if (depth + 1u >= (size_t)NORMFS_DISK_MAX_DEPTH) {
			r.status = NORMFS_DISK_ERR_TOO_DEEP;
			return r;
		}

		plen = levels[depth].path_len;
		if (plen + 5u > (size_t)NORMFS_DISK_PATH_MAX) {
			r.status = NORMFS_DISK_ERR_PATH_TOO_LONG;
			return r;
		}
		path[plen] = '/';
		path[plen + 1u] = normfs_disk_hex_digit((unsigned)(idx / 256u));
		path[plen + 2u] = normfs_disk_hex_digit((unsigned)((idx / 16u) % 16u));
		path[plen + 3u] = normfs_disk_hex_digit((unsigned)(idx % 16u));
		path[plen + 4u] = '\0';

		chunks[depth] = (uint16_t)idx;
		levels[depth + 1u].path_len = plen + 4u;
		st = normfs_disk_list_dir(path, plen + 4u, kind, chunks,
		    depth + 1u, &levels[depth + 1u], out, &e);
		if (st != NORMFS_DISK_OK) {
			r.os_error = e;
			r.status = st;
			return r;
		}
		depth++;
	}

	return r;
}

/*
 * A WAL file at an id that also has a store file is left alone: the pair
 * exists only while the store worker converts it, and the worker deletes the
 * WAL file itself. The path buffer lives here so the caller's event array is
 * never in the same frame.
 */
/*@ requires \valid(req);
    requires \valid_read(req->store_dir + (0 .. req->store_dir_len));
    requires req->store_dir[req->store_dir_len] == 0;
    requires \valid_read(req->wal_dir + (0 .. req->wal_dir_len));
    requires req->wal_dir[req->wal_dir_len] == 0;
    requires disk_id_wf(&req->next);
    requires req->has_bound == 0 || disk_id_wf(&req->bound);
    requires \valid(ev);
    requires \valid(stop);
    requires \separated(req, ev, stop,
                        req->store_dir + (0 .. req->store_dir_len),
                        req->wal_dir + (0 .. req->wal_dir_len));
    assigns *ev, req->to_free, *stop;
    ensures \result == NORMFS_DISK_OK ||
            \result == NORMFS_DISK_ERR_PATH_TOO_LONG;
    ensures \result == NORMFS_DISK_OK ==>
              (*stop == NORMFS_DISK_STOP_MORE ||
               *stop == NORMFS_DISK_STOP_GAP ||
               *stop == NORMFS_DISK_STOP_BOUND);
    ensures \result == NORMFS_DISK_OK && *stop == NORMFS_DISK_STOP_MORE ==>
              disk_event_wf(ev);
*/
static int
normfs_disk_evict_one(struct normfs_disk_evict_req *req,
    struct normfs_disk_event *ev, int *stop)
{
	char path[NORMFS_DISK_PATH_MAX];
	size_t plen = 0u;
	uint64_t size = 0u;
	int e = 0;
	int rc;
	int kind;

	*stop = NORMFS_DISK_STOP_MORE;

	rc = normfs_disk_probe(req->store_dir, req->store_dir_len, &req->next,
	    NORMFS_DISK_STORE, path, &plen, &size, &e);
	if (rc == -2)
		return NORMFS_DISK_ERR_PATH_TOO_LONG;
	kind = NORMFS_DISK_STORE;

	if (rc == 0) {
		rc = normfs_disk_probe(req->wal_dir, req->wal_dir_len,
		    &req->next, NORMFS_DISK_WAL, path, &plen, &size, &e);
		if (rc == -2)
			return NORMFS_DISK_ERR_PATH_TOO_LONG;
		kind = NORMFS_DISK_WAL;
		if (rc == 0) {
			*stop = NORMFS_DISK_STOP_GAP;
			return NORMFS_DISK_OK;
		}
	}

	if (req->has_bound != 0 &&
	    normfs_disk_id_cmp(&req->next, &req->bound) > 0) {
		*stop = NORMFS_DISK_STOP_BOUND;
		return NORMFS_DISK_OK;
	}

	normfs_disk_id_copy(&ev->id, &req->next);
	ev->kind = kind;
	ev->size = size;
	ev->deleted = 0;
	ev->os_error = 0;

	if (rc < 0) {
		ev->os_error = e;
	} else if (normfs_disk_sys_unlink(path, plen, &e) != 0) {
		ev->os_error = e;
	} else {
		ev->deleted = 1;
		req->to_free = (size >= req->to_free) ? 0u : req->to_free - size;
	}

	return NORMFS_DISK_OK;
}

/* Ids are consecutive, so the first id with neither file is the end. */
/*@ requires \valid(req);
    requires \valid_read(req->store_dir + (0 .. req->store_dir_len));
    requires req->store_dir[req->store_dir_len] == 0;
    requires \valid_read(req->wal_dir + (0 .. req->wal_dir_len));
    requires req->wal_dir[req->wal_dir_len] == 0;
    requires cap == 0 || \valid(events + (0 .. cap - 1));
    requires \valid(count);
    requires \valid(stop);
    requires \separated(req, count, stop, events + (0 .. cap - 1),
                        req->store_dir + (0 .. req->store_dir_len),
                        req->wal_dir + (0 .. req->wal_dir_len));
    assigns req->next, req->to_free, events[0 .. cap - 1], *count, *stop;
    ensures \result.os_error == 0;
    ensures \result.status == NORMFS_DISK_OK ||
            \result.status == NORMFS_DISK_ERR_INVALID_ARG ||
            \result.status == NORMFS_DISK_ERR_PATH_TOO_LONG ||
            \result.status == NORMFS_DISK_ERR_ID_OVERFLOW;
    ensures \result.status == NORMFS_DISK_ERR_INVALID_ARG <==>
              !(disk_id_wf(&req->next) &&
                (req->has_bound == 0 || disk_id_wf(&req->bound)));
    ensures *count <= cap;
    ensures \result.status == NORMFS_DISK_OK ==>
              (*stop == NORMFS_DISK_STOP_MORE ||
               *stop == NORMFS_DISK_STOP_FREED ||
               *stop == NORMFS_DISK_STOP_GAP ||
               *stop == NORMFS_DISK_STOP_BOUND);
    ensures \result.status == NORMFS_DISK_OK && *stop == NORMFS_DISK_STOP_MORE
              ==> *count == cap;
    ensures \result.status == NORMFS_DISK_OK && *stop == NORMFS_DISK_STOP_FREED
              ==> req->to_free == 0;
    ensures \result.status != NORMFS_DISK_ERR_INVALID_ARG ==>
              disk_id_wf(&req->next);
    ensures \forall integer k; 0 <= k < *count ==> disk_event_wf(&events[k]);
*/
struct normfs_disk_result
normfs_disk_evict(struct normfs_disk_evict_req *req,
    struct normfs_disk_event *events, size_t cap,
    size_t *count, int *stop)
{
	struct normfs_disk_result r;
	int st;
	int one = NORMFS_DISK_STOP_MORE;

	r.os_error = 0;
	r.status = NORMFS_DISK_OK;
	*count = 0u;
	*stop = NORMFS_DISK_STOP_MORE;

	if (!normfs_disk_id_ok(&req->next) ||
	    (req->has_bound != 0 && !normfs_disk_id_ok(&req->bound))) {
		r.status = NORMFS_DISK_ERR_INVALID_ARG;
		return r;
	}

	/*@ loop invariant 0 <= *count <= cap;
	    loop invariant disk_id_wf(&req->next);
	    loop invariant \forall integer k; 0 <= k < *count ==>
	                     disk_event_wf(&events[k]);
	    loop assigns *count, st, one, req->next, req->to_free,
	                 events[0 .. cap - 1];
	    loop variant cap - *count;
	*/
	while (*count < cap) {
		if (req->to_free == 0u) {
			*stop = NORMFS_DISK_STOP_FREED;
			return r;
		}

		st = normfs_disk_evict_one(req, &events[*count], &one);
		if (st != NORMFS_DISK_OK) {
			r.status = st;
			return r;
		}
		if (one != NORMFS_DISK_STOP_MORE) {
			*stop = one;
			return r;
		}
		*count += 1u;

		if (normfs_disk_id_increment(&req->next) != NORMFS_DISK_OK) {
			r.status = NORMFS_DISK_ERR_ID_OVERFLOW;
			return r;
		}
	}

	return r;
}
