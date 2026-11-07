#include "normfs/queue_name.h"

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

    predicate normfs_queue_name_bad_at(uint8_t *s, integer n, integer i, integer windows) =
      normfs_queue_name_bad_byte(s[i], windows) ||
      normfs_queue_name_dots(s, n, i) ||
      normfs_queue_name_win_tail(s, n, i, windows);
*/

/*@ requires len == 0 || \valid_read(name + (0 .. len - 1));
    assigns \nothing;
    ensures \result == 0 || \result == 1;
    ensures \result == 1 <==>
              \forall integer i; 0 <= i < len ==>
                !normfs_queue_name_bad_at(name, len, i, windows);
*/
int
normfs_queue_name_ok(const uint8_t *name, size_t len, int windows)
{
	size_t i;

	/*@ loop invariant 0 <= i <= len;
	    loop invariant \forall integer j; 0 <= j < i ==>
	                     !normfs_queue_name_bad_at(name, len, j, windows);
	    loop assigns i;
	    loop variant len - i;
	*/
	for (i = 0u; i < len; i++) {
		uint8_t c = name[i];
		int end = i + 1u == len || name[i + 1u] == '/';

		if (c == 0u || (windows != 0 && (c == '\\' || c == ':')))
			return 0;
		if (windows != 0 && (c == '.' || c == ' ') && end)
			return 0;
		if (c == '.' && (i == 0u || name[i - 1u] == '/')) {
			if (end)
				return 0;
			if (name[i + 1u] == '.' && (i + 2u == len || name[i + 2u] == '/'))
				return 0;
		}
	}
	return 1;
}
