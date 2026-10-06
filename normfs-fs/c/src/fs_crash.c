#include "normfs/fs_crash.h"

void
normfs_fs_append_crash_prefix(const struct normfs_fs_plan *plan, uint64_t index)
{
	(void)index;
	normfs_fs_world_crash_file(plan->ino, 0);
	/*@ assert fs_recovered_len(plan->ino) > index; */
	/*@ assert fs_recovered_byte(plan->ino, index) == fs_certified_byte(plan->ino, index); */
}

void
normfs_fs_publish_crash_safe(const struct normfs_fs_plan *plan, uint64_t index)
{
	uint64_t candidate = 0u;
	(void)index;
	if (plan->op == NORMFS_FS_OP_RENAME || plan->op == NORMFS_FS_OP_FSYNC_DIR ||
		plan->op == NORMFS_FS_OP_DONE)
		candidate = plan->ino;
	normfs_fs_world_crash_publish(plan->dst, plan->dst_len, candidate, plan->total);
}

int
normfs_fs_publish_done_durable(const struct normfs_fs_plan *plan)
{
	(void)plan;
	/*@ assert fs_dur_ino(plan->dst, plan->dst_len) == plan->ino; */
	/*@ assert fs_dur_synced(plan->ino) == plan->total; */
	/*@ assert fs_dur_len(plan->ino) == plan->total; */
	return 1;
}

int
normfs_fs_append_boundary_holds(const struct normfs_fs_plan *plan)
{
	(void)plan;
	/*@ assert fs_dur_synced(plan->ino) == plan->at ||
	           fs_dur_synced(plan->ino) == plan->at + plan->total; */
	/*@ assert plan->op == NORMFS_FS_OP_DONE <==>
	           fs_dur_synced(plan->ino) == plan->at + plan->total; */
	return 1;
}

void
normfs_fs_rename_equal_paths(const char *a, const char *b, size_t len)
{
	normfs_fs_world_rename_ok(a, len, b, len);
}
