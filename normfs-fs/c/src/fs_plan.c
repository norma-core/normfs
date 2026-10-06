#include "normfs/fs_plan.h"

void
normfs_fs_publish_init(struct normfs_fs_plan *plan, const char *tmp,
	size_t tmp_len, const char *dst, size_t dst_len, int tmp_mode,
	uint64_t total)
{
	plan->kind = NORMFS_FS_PUBLISH;
	plan->op = NORMFS_FS_OP_OPEN;
	plan->tmp_mode = tmp_mode;
	plan->restored = 0;
	plan->old_present = 0;
	plan->os_error = 0;
	plan->tmp = tmp;
	plan->tmp_len = tmp_len;
	plan->dst = dst;
	plan->dst_len = dst_len;
	plan->at = 0u;
	plan->total = total;
	plan->written = 0u;
	plan->ino = 0u;
	plan->old_len = 0u;
}

void
normfs_fs_append_init(struct normfs_fs_plan *plan, const char *dst,
	size_t dst_len, uint64_t ino, uint64_t at, uint64_t total)
{
	plan->kind = NORMFS_FS_APPEND;
	plan->op = NORMFS_FS_OP_WRITE;
	plan->tmp_mode = NORMFS_FS_TMP_EXCL;
	plan->restored = 0;
	plan->old_present = 0;
	plan->os_error = 0;
	plan->tmp = dst;
	plan->tmp_len = dst_len;
	plan->dst = dst;
	plan->dst_len = dst_len;
	plan->at = at;
	plan->total = total;
	plan->written = 0u;
	plan->ino = ino;
	plan->old_len = 0u;
}

void
normfs_fs_create_init(struct normfs_fs_plan *plan, const char *dst,
	size_t dst_len, int tmp_mode, uint64_t total)
{
	plan->kind = NORMFS_FS_CREATE;
	plan->op = NORMFS_FS_OP_OPEN;
	plan->tmp_mode = tmp_mode;
	plan->restored = 0;
	plan->old_present = 0;
	plan->os_error = 0;
	plan->tmp = dst;
	plan->tmp_len = dst_len;
	plan->dst = dst;
	plan->dst_len = dst_len;
	plan->at = 0u;
	plan->total = total;
	plan->written = 0u;
	plan->ino = 0u;
	plan->old_len = 0u;
}

void
normfs_fs_remove_init(struct normfs_fs_plan *plan, const char *dst,
	size_t dst_len)
{
	plan->kind = NORMFS_FS_REMOVE;
	plan->op = NORMFS_FS_OP_UNLINK;
	plan->tmp_mode = NORMFS_FS_TMP_EXCL;
	plan->restored = 0;
	plan->old_present = 0;
	plan->os_error = 0;
	plan->tmp = dst;
	plan->tmp_len = dst_len;
	plan->dst = dst;
	plan->dst_len = dst_len;
	plan->at = 0u;
	plan->total = 0u;
	plan->written = 0u;
	plan->ino = 0u;
	plan->old_len = 0u;
}

void
normfs_fs_restore_init(struct normfs_fs_plan *plan, const char *dst,
	size_t dst_len, uint64_t ino, uint64_t at)
{
	plan->kind = NORMFS_FS_RESTORE;
	plan->op = NORMFS_FS_OP_TRUNCATE_BACK;
	plan->tmp_mode = NORMFS_FS_TMP_EXCL;
	plan->restored = 0;
	plan->old_present = 0;
	plan->os_error = 0;
	plan->tmp = dst;
	plan->tmp_len = dst_len;
	plan->dst = dst;
	plan->dst_len = dst_len;
	plan->at = at;
	plan->total = 0u;
	plan->written = 0u;
	plan->ino = ino;
	plan->old_len = 0u;
}

int
normfs_fs_plan_next(const struct normfs_fs_plan *plan)
{
	return plan->op;
}

int
normfs_fs_publish_ok(struct normfs_fs_plan *plan, uint64_t n)
{
	switch (plan->op) {
	case NORMFS_FS_OP_OPEN:
		if (n == 0u)
			return NORMFS_FS_ERR_STATE;
		normfs_fs_world_open_ok(plan->tmp, plan->tmp_len, n);
		plan->ino = n;
		plan->op = NORMFS_FS_OP_WRITE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_WRITE:
		if (n == 0u || n > plan->total - plan->written)
			return NORMFS_FS_ERR_STATE;
		normfs_fs_world_write_ok(plan->ino, n);
		plan->written += n;
		plan->op = (plan->written < plan->total) ?
		    NORMFS_FS_OP_WRITE : NORMFS_FS_OP_FSYNC_FILE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_ok(plan->ino);
		plan->op = NORMFS_FS_OP_CLOSE_FILE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_CLOSE_FILE:
		plan->op = NORMFS_FS_OP_STAT_DST;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_STAT_DST:
		plan->old_len = n;
		plan->old_present = 1;
		plan->op = NORMFS_FS_OP_RENAME;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_RENAME:
		normfs_fs_world_rename_ok(plan->tmp, plan->tmp_len, plan->dst,
		    plan->dst_len);
		plan->op = NORMFS_FS_OP_FSYNC_DIR;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_ok(plan->dst, plan->dst_len);
		plan->op = NORMFS_FS_OP_DONE;
		return NORMFS_FS_OK;
	default:
		return NORMFS_FS_ERR_STATE;
	}
}

int
normfs_fs_publish_absent(struct normfs_fs_plan *plan)
{
	if (plan->op != NORMFS_FS_OP_STAT_DST)
		return NORMFS_FS_ERR_STATE;
	plan->old_len = 0u;
	plan->old_present = 0;
	plan->op = NORMFS_FS_OP_RENAME;
	return NORMFS_FS_OK;
}

int
normfs_fs_publish_err(struct normfs_fs_plan *plan, int os_error)
{
	switch (plan->op) {
	case NORMFS_FS_OP_WRITE:
		normfs_fs_world_write_err(plan->ino);
		break;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_err(plan->ino);
		break;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_err(plan->dst, plan->dst_len);
		break;
	case NORMFS_FS_OP_OPEN:
	case NORMFS_FS_OP_CLOSE_FILE:
	case NORMFS_FS_OP_STAT_DST:
	case NORMFS_FS_OP_RENAME:
		break;
	default:
		return NORMFS_FS_ERR_STATE;
	}
	plan->os_error = os_error;
	plan->op = NORMFS_FS_OP_FAILED;
	return NORMFS_FS_OK;
}

int
normfs_fs_append_ok(struct normfs_fs_plan *plan, uint64_t n)
{
	switch (plan->op) {
	case NORMFS_FS_OP_WRITE:
		if (n == 0u || n > plan->total - plan->written)
			return NORMFS_FS_ERR_STATE;
		normfs_fs_world_write_ok(plan->ino, n);
		plan->written += n;
		plan->op = (plan->written < plan->total) ?
		    NORMFS_FS_OP_WRITE : NORMFS_FS_OP_FSYNC_FILE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_ok(plan->ino);
		plan->op = NORMFS_FS_OP_DONE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_TRUNCATE_BACK:
		normfs_fs_world_truncate_ok(plan->ino, plan->at);
		plan->restored = 1;
		plan->op = NORMFS_FS_OP_FAILED;
		return NORMFS_FS_OK;
	default:
		return NORMFS_FS_ERR_STATE;
	}
}

int
normfs_fs_append_err(struct normfs_fs_plan *plan, int os_error)
{
	switch (plan->op) {
	case NORMFS_FS_OP_WRITE:
		normfs_fs_world_write_err(plan->ino);
		plan->os_error = os_error;
		plan->op = NORMFS_FS_OP_TRUNCATE_BACK;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_err(plan->ino);
		plan->os_error = os_error;
		plan->op = NORMFS_FS_OP_TRUNCATE_BACK;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_TRUNCATE_BACK:
		/* The write's failure is the one to report; the truncate's only
		 * when there was none. */
		if (plan->os_error <= 0)
			plan->os_error = os_error;
		plan->restored = 0;
		plan->op = NORMFS_FS_OP_FAILED;
		return NORMFS_FS_OK;
	default:
		return NORMFS_FS_ERR_STATE;
	}
}

int
normfs_fs_create_ok(struct normfs_fs_plan *plan, uint64_t n)
{
	switch (plan->op) {
	case NORMFS_FS_OP_OPEN:
		if (n == 0u)
			return NORMFS_FS_ERR_STATE;
		normfs_fs_world_open_ok(plan->dst, plan->dst_len, n);
		plan->ino = n;
		plan->op = (plan->total > 0u) ?
		    NORMFS_FS_OP_WRITE : NORMFS_FS_OP_FSYNC_FILE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_WRITE:
		if (n == 0u || n > plan->total - plan->written)
			return NORMFS_FS_ERR_STATE;
		normfs_fs_world_write_ok(plan->ino, n);
		plan->written += n;
		plan->op = (plan->written < plan->total) ?
		    NORMFS_FS_OP_WRITE : NORMFS_FS_OP_FSYNC_FILE;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_ok(plan->ino);
		plan->op = NORMFS_FS_OP_FSYNC_DIR;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_ok(plan->dst, plan->dst_len);
		plan->op = NORMFS_FS_OP_DONE;
		return NORMFS_FS_OK;
	default:
		return NORMFS_FS_ERR_STATE;
	}
}

int
normfs_fs_create_err(struct normfs_fs_plan *plan, int os_error)
{
	switch (plan->op) {
	case NORMFS_FS_OP_WRITE:
		normfs_fs_world_write_err(plan->ino);
		break;
	case NORMFS_FS_OP_FSYNC_FILE:
		normfs_fs_world_fsync_err(plan->ino);
		break;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_err(plan->dst, plan->dst_len);
		break;
	case NORMFS_FS_OP_OPEN:
		break;
	default:
		return NORMFS_FS_ERR_STATE;
	}
	plan->os_error = os_error;
	plan->op = NORMFS_FS_OP_FAILED;
	return NORMFS_FS_OK;
}

int
normfs_fs_remove_ok(struct normfs_fs_plan *plan)
{
	switch (plan->op) {
	case NORMFS_FS_OP_UNLINK:
		normfs_fs_world_unlink_ok(plan->dst, plan->dst_len);
		plan->op = NORMFS_FS_OP_FSYNC_DIR;
		return NORMFS_FS_OK;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_ok(plan->dst, plan->dst_len);
		plan->op = NORMFS_FS_OP_DONE;
		return NORMFS_FS_OK;
	default:
		return NORMFS_FS_ERR_STATE;
	}
}

int
normfs_fs_remove_err(struct normfs_fs_plan *plan, int os_error)
{
	switch (plan->op) {
	case NORMFS_FS_OP_UNLINK:
		break;
	case NORMFS_FS_OP_FSYNC_DIR:
		normfs_fs_world_fsync_dir_err(plan->dst, plan->dst_len);
		break;
	default:
		return NORMFS_FS_ERR_STATE;
	}
	plan->os_error = os_error;
	plan->op = NORMFS_FS_OP_FAILED;
	return NORMFS_FS_OK;
}

int
normfs_fs_restore_report(struct normfs_fs_plan *plan, int os_error)
{
	if (plan->op != NORMFS_FS_OP_TRUNCATE_BACK)
		return NORMFS_FS_ERR_STATE;
	if (os_error == 0) {
		normfs_fs_world_truncate_ok(plan->ino, plan->at);
		plan->op = NORMFS_FS_OP_DONE;
	} else {
		plan->os_error = os_error;
		plan->op = NORMFS_FS_OP_FAILED;
	}
	return NORMFS_FS_OK;
}
