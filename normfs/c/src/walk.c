#include "normfs/walk.h"

/*@ assigns \nothing;
    ensures \result == NORMFS_WALK_READ <==> begins_at <= next_id;
    ensures \result == NORMFS_WALK_BACK <==>
              (begins_at > next_id && file_id != 0 &&
               !(has_went_back != 0 && went_back == next_id));
    ensures \result == NORMFS_WALK_GAP <==>
              (begins_at > next_id &&
               (file_id == 0 || (has_went_back != 0 && went_back == next_id)));
*/
int
normfs_walk_step(uint64_t file_id, uint64_t begins_at, uint64_t next_id,
		 int has_went_back, uint64_t went_back)
{
	if (begins_at <= next_id) return NORMFS_WALK_READ;
	if (file_id == 0) return NORMFS_WALK_GAP;
	if (has_went_back != 0 && went_back == next_id) return NORMFS_WALK_GAP;
	return NORMFS_WALK_BACK;
}
