#include "normfs/walk.h"

/*@ predicate normfs_walk_back(integer file_id, integer begins_at, integer next_id,
                               integer has_cut_short, integer cut_short,
                               integer has_went_back, integer went_back) =
      begins_at > next_id && file_id != 0 &&
      has_cut_short != 0 && cut_short == file_id - 1 &&
      !(has_went_back != 0 && went_back == next_id);
*/

/*@ assigns \nothing;
    ensures \result == NORMFS_WALK_READ <==> begins_at <= next_id;
    ensures \result == NORMFS_WALK_BACK <==>
              normfs_walk_back(file_id, begins_at, next_id, has_cut_short, cut_short,
                               has_went_back, went_back);
    ensures \result == NORMFS_WALK_GAP <==>
              (begins_at > next_id &&
               !normfs_walk_back(file_id, begins_at, next_id, has_cut_short, cut_short,
                                 has_went_back, went_back));
*/
int
normfs_walk_step(uint64_t file_id, uint64_t begins_at, uint64_t next_id,
		 int has_cut_short, uint64_t cut_short,
		 int has_went_back, uint64_t went_back)
{
	if (begins_at <= next_id) return NORMFS_WALK_READ;
	if (file_id == 0 || has_cut_short == 0 || cut_short != file_id - 1)
		return NORMFS_WALK_GAP;
	if (has_went_back != 0 && went_back == next_id) return NORMFS_WALK_GAP;
	return NORMFS_WALK_BACK;
}
