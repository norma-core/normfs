#include "normfs/queue_name.h"

/* Inside a queue's directory, and at the top of the data directory. */
static const uint8_t normfs_queue_name_wal[3] = {'w', 'a', 'l'};
static const uint8_t normfs_queue_name_store[5] = {'s', 't', 'o', 'r', 'e'};
static const uint8_t normfs_queue_name_closed[6] = {'c', 'l', 'o', 's', 'e', 'd'};
static const uint8_t normfs_queue_name_tmp[3] = {'t', 'm', 'p'};

/*@ logic integer normfs_queue_name_kept{L}(uint8_t *s, integer n, integer k) =
      k <= 0 ? 0 :
      normfs_queue_name_kept{L}(s, n, k - 1) +
        ((\at(s[k - 1], L) != '/' || (k < n && \at(s[k], L) != '/')) ? 1 : 0);

    predicate normfs_queue_name_keeps(uint8_t *s, integer n, integer i) =
      s[i] != '/' || (i + 1 < n && s[i + 1] != '/');

    predicate normfs_queue_name_canonical_form(uint8_t *s, integer n) =
      (\forall integer i; 0 <= i < n - 1 ==> !(s[i] == '/' && s[i + 1] == '/')) &&
      (n > 0 ==> s[n - 1] != '/');
*/

/*@ requires len == 0 || \valid_read(name + (0 .. len - 1));
    requires len == 0 || \valid(out + (0 .. len - 1));
    requires \separated(name + (0 .. len - 1), out + (0 .. len - 1));
    assigns out[0 .. len - 1];
    ensures \result <= len;
    ensures \result == normfs_queue_name_kept{Pre}(name, len, len);
    ensures \forall integer i; 0 <= i < len && normfs_queue_name_keeps(name, len, i) ==>
              out[normfs_queue_name_kept{Pre}(name, len, i)] == name[i];
    ensures normfs_queue_name_canonical_form(out, \result);
    ensures normfs_queue_name_canonical_form(name, len) ==>
              \result == len && \forall integer i; 0 <= i < len ==> out[i] == name[i];
*/
size_t
normfs_queue_name_canonical(const uint8_t *name, size_t len, uint8_t *out)
{
	size_t i;
	size_t o = 0u;

	/*@ loop invariant 0 <= i <= len;
	    loop invariant o == normfs_queue_name_kept{Pre}(name, len, i);
	    loop invariant o <= i;
	    loop invariant \forall integer j; 0 <= j <= i ==> normfs_queue_name_kept{Pre}(name, len, j) <= o;
	    loop invariant \forall integer j; 0 <= j < i && normfs_queue_name_keeps(name, len, j) ==>
	                     normfs_queue_name_kept{Pre}(name, len, j) < o;
	    loop invariant \forall integer j; 0 <= j < i && normfs_queue_name_keeps(name, len, j) ==>
	                     out[normfs_queue_name_kept{Pre}(name, len, j)] == name[j];
	    loop invariant \forall integer j; 0 <= j < o - 1 ==> !(out[j] == '/' && out[j + 1] == '/');
	    loop invariant o > 0 && out[o - 1] == '/' ==> i < len && name[i] != '/';
	    loop invariant normfs_queue_name_canonical_form(name, len) ==>
	                     o == i && \forall integer j; 0 <= j < i ==> out[j] == name[j];
	    loop assigns i, o, out[0 .. len - 1];
	    loop variant len - i;
	*/
	for (i = 0u; i < len; i++) {
		/*@ assert normfs_queue_name_kept{Pre}(name, len, i + 1) ==
		           normfs_queue_name_kept{Pre}(name, len, i) +
		             ((name[i] != '/' || (i + 1 < len && name[i + 1] != '/')) ? 1 : 0);
		*/
		if (name[i] != '/' || (i + 1u < len && name[i + 1u] != '/')) {
			out[o] = name[i];
			o++;
		}
	}
	return o;
}

/*@ predicate normfs_queue_name_bad_byte(uint8_t c, integer windows) =
      c == 0 || (windows != 0 && (c == '\\' || c == ':'));

    predicate normfs_queue_name_seg_start(uint8_t *s, integer i) =
      i == 0 || s[i - 1] == '/';

    predicate normfs_queue_name_seg_end(uint8_t *s, integer n, integer i) =
      i == n || s[i] == '/';

    predicate normfs_queue_name_dots(uint8_t *s, integer n, integer i) =
      s[i] == '.' && normfs_queue_name_seg_start(s, i) &&
      (normfs_queue_name_seg_end(s, n, i + 1) ||
       (s[i + 1] == '.' && normfs_queue_name_seg_end(s, n, i + 2)));

    // Win32 drops a trailing dot or space, so "... " would open "..".
    predicate normfs_queue_name_win_tail(uint8_t *s, integer n, integer i, integer windows) =
      windows != 0 && (s[i] == '.' || s[i] == ' ') && normfs_queue_name_seg_end(s, n, i + 1);

    // macOS and Windows fold case by default, so WAL is wal there.
    predicate normfs_queue_name_same(uint8_t c, uint8_t w) =
      c == w || ('a' <= w <= 'z' && c == w - 32);

    predicate normfs_queue_name_word(uint8_t *s, integer n, integer i,
                                     uint8_t *w, integer wn) =
      i + wn <= n && normfs_queue_name_seg_end(s, n, i + wn) &&
      \forall integer k; 0 <= k < wn ==> normfs_queue_name_same(s[i + k], w[k]);

    predicate normfs_queue_name_reserved(uint8_t *s, integer n, integer i) =
      normfs_queue_name_seg_start(s, i) &&
      (normfs_queue_name_word(s, n, i, (uint8_t *)normfs_queue_name_wal, 3) ||
       normfs_queue_name_word(s, n, i, (uint8_t *)normfs_queue_name_store, 5) ||
       normfs_queue_name_word(s, n, i, (uint8_t *)normfs_queue_name_closed, 6) ||
       ((i == 0 || (i == 1 && s[0] == '/')) &&
        (s[i] == '.' ||
         normfs_queue_name_word(s, n, i, (uint8_t *)normfs_queue_name_tmp, 3))));

    predicate normfs_queue_name_bad_at(uint8_t *s, integer n, integer i, integer windows) =
      normfs_queue_name_bad_byte(s[i], windows) ||
      normfs_queue_name_dots(s, n, i) ||
      normfs_queue_name_win_tail(s, n, i, windows) ||
      normfs_queue_name_reserved(s, n, i);
*/

/*@ requires i < n && \valid_read(s + (0 .. n - 1));
    requires wn > 0 && \valid_read(w + (0 .. wn - 1));
    assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==> normfs_queue_name_word(s, n, i, w, wn);
*/
static int
normfs_queue_name_word_at(const uint8_t *s, size_t n, size_t i, const uint8_t *w, size_t wn)
{
	size_t k;

	if (wn > n - i)
		return 0;
	if (i + wn != n && s[i + wn] != '/')
		return 0;
	/*@ loop invariant 0 <= k <= wn;
	    loop invariant \forall integer j; 0 <= j < k ==>
	                     normfs_queue_name_same(s[i + j], w[j]);
	    loop assigns k;
	    loop variant wn - k;
	*/
	for (k = 0u; k < wn; k++) {
		uint8_t c = s[i + k];

		if (c != w[k] &&
		    !(w[k] >= 'a' && w[k] <= 'z' && c == (uint8_t)(w[k] - 32u)))
			return 0;
	}
	return 1;
}

/*@ requires len == 0 || \valid_read(name + (0 .. len - 1));
    assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==>
              len > 0 &&
              \forall integer i; 0 <= i < len ==>
                !normfs_queue_name_bad_at(name, len, i, windows);
*/
int
normfs_queue_name_ok(const uint8_t *name, size_t len, int windows)
{
	size_t i;

	if (len == 0u)
		return 0;
	/*@ loop invariant 0 <= i <= len;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     !normfs_queue_name_bad_at(name, len, j, windows);
	    loop assigns i;
	    loop variant len - i;
	*/
	for (i = 0u; i < len; i++) {
		uint8_t c = name[i];
		int end = i + 1u == len || name[i + 1u] == '/';

		if (c == 0u || (windows != 0 && (c == '\\' || c == ':'))) {
			/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
			return 0;
		}
		if (windows != 0 && (c == '.' || c == ' ') && end) {
			/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
			return 0;
		}
		if (c == '.' && (i == 0u || name[i - 1u] == '/')) {
			if (end) {
				/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
				return 0;
			}
			if (name[i + 1u] == '.' && (i + 2u == len || name[i + 2u] == '/')) {
				/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
				return 0;
			}
		}
		if (i == 0u || name[i - 1u] == '/') {
			if (normfs_queue_name_word_at(name, len, i, normfs_queue_name_wal, 3u) ||
			    normfs_queue_name_word_at(name, len, i, normfs_queue_name_store, 5u) ||
			    normfs_queue_name_word_at(name, len, i, normfs_queue_name_closed, 6u)) {
				/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
				return 0;
			}
			if ((i == 0u || (i == 1u && name[0] == '/')) &&
			    (c == '.' ||
			     normfs_queue_name_word_at(name, len, i, normfs_queue_name_tmp, 3u))) {
				/*@ assert normfs_queue_name_bad_at(name, len, i, windows); */
				return 0;
			}
		}
		/*@ assert !normfs_queue_name_bad_at(name, len, i, windows); */
	}
	return 1;
}
