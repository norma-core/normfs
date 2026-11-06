#ifndef NORMFS_WALK_H
#define NORMFS_WALK_H

#include <stdint.h>

/* What a read does with a file that begins at begins_at while it owes next_id. */
enum normfs_walk_step {
	NORMFS_WALK_READ = 0,
	/* The file before was read while the writer was still filling it. */
	NORMFS_WALK_BACK = 1,
	/* The ids in between reach no file: the read warns and goes on. */
	NORMFS_WALK_GAP = 2
};

/* cut_short is the file the read just left with a partial read, when has_cut_short;
 * went_back is the owed id it already went back for, when has_went_back. */
int normfs_walk_step(uint64_t file_id, uint64_t begins_at, uint64_t next_id,
		     int has_cut_short, uint64_t cut_short,
		     int has_went_back, uint64_t went_back);

#endif /* NORMFS_WALK_H */
