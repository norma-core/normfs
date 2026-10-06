#ifndef NORMFS_FS_PLAN_H
#define NORMFS_FS_PLAN_H

#include <stddef.h>
#include <stdint.h>

#include "normfs/fs_sys.h"

/* The executor must report each operation only after it completes. WP proves
 * the resulting transitions against the kernel model in fs_sys.h; see
 * verify/fs.md for crash guarantees and assumptions. Skipping a planned
 * fsync invalidates the durability guarantees. */

#define NORMFS_FS_OK 0
/* The report does not fit the state: a terminal op, or a count the step
 * cannot take. The plan is unchanged. */
#define NORMFS_FS_ERR_STATE 1

enum normfs_fs_kind {
	NORMFS_FS_PUBLISH = 1,
	NORMFS_FS_APPEND = 2,
	NORMFS_FS_CREATE = 3,
	NORMFS_FS_REMOVE = 4,
	NORMFS_FS_RESTORE = 5
};

enum normfs_fs_op {
	/* Open tmp (PUBLISH) or dst (CREATE) with O_CREAT; report the inode. */
	NORMFS_FS_OP_OPEN = 1,
	/* pwritev the unwritten tail of the runs at at + written; report bytes. */
	NORMFS_FS_OP_WRITE = 2,
	NORMFS_FS_OP_FSYNC_FILE = 3,
	NORMFS_FS_OP_CLOSE_FILE = 4,
	/* lstat dst; report its length, or absent. */
	NORMFS_FS_OP_STAT_DST = 5,
	NORMFS_FS_OP_RENAME = 6,
	NORMFS_FS_OP_FSYNC_DIR = 7,
	/* ftruncate the file to at. */
	NORMFS_FS_OP_TRUNCATE_BACK = 8,
	NORMFS_FS_OP_UNLINK = 9,
	NORMFS_FS_OP_DONE = 10,
	NORMFS_FS_OP_FAILED = 11
};

struct normfs_fs_plan {
	int kind;
	int op;
	int tmp_mode;
	/* APPEND: the truncate-back after a failure succeeded. */
	int restored;
	/* PUBLISH: dst existed when stat ran. */
	int old_present;
	/* The first failure, 0 while there was none. */
	int os_error;
	/* PUBLISH: the temporary name. Absent for the other kinds. */
	const char *tmp;
	size_t tmp_len;
	/* The name being published, created or removed; for APPEND and
	 * RESTORE the file's path, for fault injection only. */
	const char *dst;
	size_t dst_len;
	/* APPEND, RESTORE: the offset the file is known good to. */
	uint64_t at;
	uint64_t total;
	uint64_t written;
	uint64_t ino;
	uint64_t old_len;
};

/* Field-level well-formedness, as a macro so that every store to one field
 * leaves the others' facts in the frame. */
#define NORMFS_FS_PLAN_WF(p) \
    (((p)->kind == NORMFS_FS_PUBLISH || (p)->kind == NORMFS_FS_APPEND || \
      (p)->kind == NORMFS_FS_CREATE || (p)->kind == NORMFS_FS_REMOVE || \
      (p)->kind == NORMFS_FS_RESTORE) && \
     NORMFS_FS_OP_OPEN <= (p)->op <= NORMFS_FS_OP_FAILED && \
     ((p)->tmp_mode == NORMFS_FS_TMP_EXCL || \
      (p)->tmp_mode == NORMFS_FS_TMP_TRUNC) && \
     (p)->tmp_len < NORMFS_FS_PATH_MAX && \
     (p)->dst_len < NORMFS_FS_PATH_MAX && \
     \valid_read((p)->tmp + (0 .. (p)->tmp_len)) && \
     \valid_read((p)->dst + (0 .. (p)->dst_len)) && \
     \separated((p), (p)->tmp + (0 .. (p)->tmp_len)) && \
     \separated((p), (p)->dst + (0 .. (p)->dst_len)) && \
     (p)->written <= (p)->total && \
     (p)->at + (p)->total <= 0xFFFFFFFFFFFFFFFF)

/* What holds of the kernel at each op of a PUBLISH plan. Two facts carry
 * the theorem: the temporary name resolves to the inode until the rename
 * moves it, and from the file fsync on the whole file is on the medium. */
#define NORMFS_FS_PUBLISH_STATE(p) \
    ((p)->total > 0 && \
     (p)->op != NORMFS_FS_OP_TRUNCATE_BACK && (p)->op != NORMFS_FS_OP_UNLINK && \
     ((p)->op == NORMFS_FS_OP_OPEN ==> (p)->written == 0) && \
     ((p)->op == NORMFS_FS_OP_WRITE ==> \
        (p)->ino > 0 && fs_vol_ino((p)->tmp, (p)->tmp_len) == (p)->ino && \
        fs_vol_len((p)->ino) == (p)->written && \
        (p)->written < (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_FILE ==> \
        (p)->ino > 0 && fs_vol_ino((p)->tmp, (p)->tmp_len) == (p)->ino && \
        fs_vol_len((p)->ino) == (p)->total && (p)->written == (p)->total) && \
     (((p)->op == NORMFS_FS_OP_CLOSE_FILE || \
       (p)->op == NORMFS_FS_OP_STAT_DST || \
       (p)->op == NORMFS_FS_OP_RENAME) ==> \
        (p)->ino > 0 && fs_vol_ino((p)->tmp, (p)->tmp_len) == (p)->ino && \
        fs_dur_synced((p)->ino) == (p)->total && \
        fs_dur_len((p)->ino) == (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_DIR ==> \
        (p)->ino > 0 && fs_vol_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_dur_synced((p)->ino) == (p)->total && \
        fs_dur_len((p)->ino) == (p)->total) && \
     ((p)->op == NORMFS_FS_OP_DONE ==> \
        (p)->ino > 0 && fs_dur_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_dur_synced((p)->ino) == (p)->total && \
        fs_dur_len((p)->ino) == (p)->total))

/* One step of the crash theorem: the durable entry for dst is unchanged by
 * the step, or is the complete new file. Chained over every step from
 * init, that is "old file, no file, or the whole new file". */
#define NORMFS_FS_PUBLISH_STEP(p) \
    (fs_dur_ino((p)->dst, (p)->dst_len) == \
       \old(fs_dur_ino((p)->dst, (p)->dst_len)) || \
     (fs_dur_ino((p)->dst, (p)->dst_len) == (p)->ino && \
      fs_dur_synced((p)->ino) == (p)->total && \
      fs_dur_len((p)->ino) == (p)->total))

#define NORMFS_FS_APPEND_STATE(p) \
    ((p)->total > 0 && \
     ((p)->op == NORMFS_FS_OP_WRITE || (p)->op == NORMFS_FS_OP_FSYNC_FILE || \
      (p)->op == NORMFS_FS_OP_TRUNCATE_BACK || (p)->op == NORMFS_FS_OP_DONE || \
      (p)->op == NORMFS_FS_OP_FAILED) && \
     ((p)->op == NORMFS_FS_OP_WRITE ==> \
        fs_dur_synced((p)->ino) == (p)->at && \
        fs_vol_len((p)->ino) == (p)->at + (p)->written && \
        (p)->written < (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_FILE ==> \
        fs_dur_synced((p)->ino) == (p)->at && \
        fs_vol_len((p)->ino) == (p)->at + (p)->total) && \
     ((p)->op == NORMFS_FS_OP_TRUNCATE_BACK ==> \
        fs_dur_synced((p)->ino) == (p)->at) && \
     ((p)->op == NORMFS_FS_OP_DONE ==> \
        fs_dur_synced((p)->ino) == (p)->at + (p)->total && \
        fs_vol_len((p)->ino) == (p)->at + (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FAILED ==> \
        fs_dur_synced((p)->ino) == (p)->at && \
        ((p)->restored != 0 ==> fs_vol_len((p)->ino) == (p)->at)))

#define NORMFS_FS_CREATE_STATE(p) \
    (((p)->op == NORMFS_FS_OP_OPEN || (p)->op == NORMFS_FS_OP_WRITE || \
      (p)->op == NORMFS_FS_OP_FSYNC_FILE || (p)->op == NORMFS_FS_OP_FSYNC_DIR || \
      (p)->op == NORMFS_FS_OP_DONE || (p)->op == NORMFS_FS_OP_FAILED) && \
     ((p)->op == NORMFS_FS_OP_OPEN ==> (p)->written == 0) && \
     ((p)->op == NORMFS_FS_OP_WRITE ==> \
        (p)->ino > 0 && fs_vol_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_vol_len((p)->ino) == (p)->written && \
        (p)->written < (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_FILE ==> \
        (p)->ino > 0 && fs_vol_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_vol_len((p)->ino) == (p)->total) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_DIR ==> \
        (p)->ino > 0 && fs_vol_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_dur_synced((p)->ino) == (p)->total && \
        fs_dur_len((p)->ino) == (p)->total) && \
     ((p)->op == NORMFS_FS_OP_DONE ==> \
        (p)->ino > 0 && fs_dur_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_vol_ino((p)->dst, (p)->dst_len) == (p)->ino && \
        fs_dur_synced((p)->ino) == (p)->total && \
        fs_dur_len((p)->ino) == (p)->total))

#define NORMFS_FS_REMOVE_STATE(p) \
    (((p)->op == NORMFS_FS_OP_UNLINK || (p)->op == NORMFS_FS_OP_FSYNC_DIR || \
      (p)->op == NORMFS_FS_OP_DONE || (p)->op == NORMFS_FS_OP_FAILED) && \
     ((p)->op == NORMFS_FS_OP_FSYNC_DIR ==> \
        fs_vol_ino((p)->dst, (p)->dst_len) == 0) && \
     ((p)->op == NORMFS_FS_OP_DONE ==> \
        fs_vol_ino((p)->dst, (p)->dst_len) == 0 && \
        fs_dur_ino((p)->dst, (p)->dst_len) == 0))
/*@ requires \valid(plan);
    requires tmp_len < NORMFS_FS_PATH_MAX && dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(tmp + (0 .. tmp_len));
    requires \valid_read(dst + (0 .. dst_len));
    requires \separated(plan, tmp + (0 .. tmp_len));
    requires \separated(plan, dst + (0 .. dst_len));
    requires tmp_mode == NORMFS_FS_TMP_EXCL || tmp_mode == NORMFS_FS_TMP_TRUNC;
    requires total > 0;
    assigns *plan;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures plan->kind == NORMFS_FS_PUBLISH && plan->op == NORMFS_FS_OP_OPEN;
    ensures plan->tmp == tmp && plan->tmp_len == tmp_len;
    ensures plan->dst == dst && plan->dst_len == dst_len;
    ensures plan->total == total && plan->written == 0 && plan->at == 0;
    ensures plan->os_error == 0;
    ensures NORMFS_FS_PUBLISH_STATE(plan);
*/
void normfs_fs_publish_init(struct normfs_fs_plan *plan, const char *tmp,
    size_t tmp_len, const char *dst, size_t dst_len, int tmp_mode,
    uint64_t total);

/* The caller's obligation is the precondition on the kernel: the file is
 * synced and unwritten past `at`. A finished APPEND, a restored one or a
 * RESTORE plan each leave the file so; see verify/fs.md. */
/*@ requires \valid(plan);
    requires dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(dst + (0 .. dst_len));
    requires \separated(plan, dst + (0 .. dst_len));
    requires total > 0;
    requires at + total <= 0xFFFFFFFFFFFFFFFF;
    requires fs_dur_synced(ino) == at;
    requires fs_vol_len(ino) == at;
    assigns *plan;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures plan->kind == NORMFS_FS_APPEND && plan->op == NORMFS_FS_OP_WRITE;
    ensures plan->dst == dst && plan->dst_len == dst_len;
    ensures plan->at == at && plan->total == total && plan->written == 0;
    ensures plan->ino == ino && plan->restored == 0 && plan->os_error == 0;
    ensures NORMFS_FS_APPEND_STATE(plan);
*/
void normfs_fs_append_init(struct normfs_fs_plan *plan, const char *dst,
    size_t dst_len, uint64_t ino, uint64_t at, uint64_t total);

/*@ requires \valid(plan);
    requires dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(dst + (0 .. dst_len));
    requires \separated(plan, dst + (0 .. dst_len));
    requires tmp_mode == NORMFS_FS_TMP_EXCL || tmp_mode == NORMFS_FS_TMP_TRUNC;
    assigns *plan;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures plan->kind == NORMFS_FS_CREATE && plan->op == NORMFS_FS_OP_OPEN;
    ensures plan->dst == dst && plan->dst_len == dst_len;
    ensures plan->total == total && plan->written == 0 && plan->at == 0;
    ensures plan->os_error == 0;
    ensures NORMFS_FS_CREATE_STATE(plan);
*/
void normfs_fs_create_init(struct normfs_fs_plan *plan, const char *dst,
    size_t dst_len, int tmp_mode, uint64_t total);

/*@ requires \valid(plan);
    requires dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(dst + (0 .. dst_len));
    requires \separated(plan, dst + (0 .. dst_len));
    assigns *plan;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures plan->kind == NORMFS_FS_REMOVE && plan->op == NORMFS_FS_OP_UNLINK;
    ensures plan->dst == dst && plan->dst_len == dst_len;
    ensures plan->os_error == 0;
    ensures NORMFS_FS_REMOVE_STATE(plan);
*/
void normfs_fs_remove_init(struct normfs_fs_plan *plan, const char *dst,
    size_t dst_len);

/*@ requires \valid(plan);
    requires dst_len < NORMFS_FS_PATH_MAX;
    requires \valid_read(dst + (0 .. dst_len));
    requires \separated(plan, dst + (0 .. dst_len));
    assigns *plan;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures plan->kind == NORMFS_FS_RESTORE &&
            plan->op == NORMFS_FS_OP_TRUNCATE_BACK;
    ensures plan->dst == dst && plan->dst_len == dst_len;
    ensures plan->ino == ino && plan->at == at && plan->os_error == 0;
*/
void normfs_fs_restore_init(struct normfs_fs_plan *plan, const char *dst,
    size_t dst_len, uint64_t ino, uint64_t at);

/*@ requires \valid_read(plan);
    assigns \nothing;
    ensures \result == plan->op;
*/
int normfs_fs_plan_next(const struct normfs_fs_plan *plan);

/* ------------------------------------------------------------------------
 * Reports. `n` is the inode for OPEN, the byte count for WRITE, the length
 * for STAT_DST, and ignored elsewhere. Each function is one kind, so that a
 * goal is one transition; the frame is the fields a step can touch. */

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_PUBLISH;
    requires NORMFS_FS_PUBLISH_STATE(plan);
    requires plan->op == NORMFS_FS_OP_FSYNC_DIR ==>
        fs_parent_durable(plan->dst, plan->dst_len);
    assigns plan->op, plan->written, plan->ino, plan->old_len,
            plan->old_present,
            normfs_fs_vol_names, normfs_fs_vol_data,
            normfs_fs_dur_names, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_PUBLISH_STATE(plan);
    ensures NORMFS_FS_PUBLISH_STEP(plan);
    // The transition table. Total on the non-terminal ops, so the spec is
    // not met by a plan that never leaves OPEN.
    ensures \result == NORMFS_FS_OK <==>
              (\old(plan->op) == NORMFS_FS_OP_OPEN && n > 0) ||
              (\old(plan->op) == NORMFS_FS_OP_WRITE &&
               0 < n <= \old(plan->total) - \old(plan->written)) ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ||
              \old(plan->op) == NORMFS_FS_OP_CLOSE_FILE ||
              \old(plan->op) == NORMFS_FS_OP_STAT_DST ||
              \old(plan->op) == NORMFS_FS_OP_RENAME ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR;
    ensures \result == NORMFS_FS_ERR_STATE ==>
              plan->op == \old(plan->op) && plan->written == \old(plan->written);
    ensures \result == NORMFS_FS_OK && \old(plan->op) == NORMFS_FS_OP_OPEN ==>
              plan->op == NORMFS_FS_OP_WRITE && plan->ino == n;
    ensures \result == NORMFS_FS_OK && \old(plan->op) == NORMFS_FS_OP_WRITE ==>
              plan->written == \old(plan->written) + n &&
              (plan->written < plan->total ==> plan->op == NORMFS_FS_OP_WRITE) &&
              (plan->written == plan->total ==>
                 plan->op == NORMFS_FS_OP_FSYNC_FILE);
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ==>
              plan->op == NORMFS_FS_OP_CLOSE_FILE;
    ensures \old(plan->op) == NORMFS_FS_OP_CLOSE_FILE ==>
              plan->op == NORMFS_FS_OP_STAT_DST;
    ensures \old(plan->op) == NORMFS_FS_OP_STAT_DST ==>
              plan->op == NORMFS_FS_OP_RENAME && plan->old_len == n &&
              plan->old_present == 1;
    ensures \old(plan->op) == NORMFS_FS_OP_RENAME ==>
              plan->op == NORMFS_FS_OP_FSYNC_DIR;
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR ==>
              plan->op == NORMFS_FS_OP_DONE;
    ensures \result == NORMFS_FS_OK ==>
              (plan->op == NORMFS_FS_OP_DONE <==>
               \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR);
    ensures plan->total == \old(plan->total) && plan->tmp == \old(plan->tmp) &&
            plan->dst == \old(plan->dst);
    ensures \old(plan->op) != NORMFS_FS_OP_OPEN ==> plan->ino == \old(plan->ino);
*/
int normfs_fs_publish_ok(struct normfs_fs_plan *plan, uint64_t n);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_PUBLISH;
    requires NORMFS_FS_PUBLISH_STATE(plan);
    assigns plan->op, plan->old_len, plan->old_present;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_PUBLISH_STATE(plan);
    ensures \result == NORMFS_FS_OK <==> \old(plan->op) == NORMFS_FS_OP_STAT_DST;
    ensures \result == NORMFS_FS_OK ==>
              plan->op == NORMFS_FS_OP_RENAME && plan->old_present == 0 &&
              plan->old_len == 0;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
*/
int normfs_fs_publish_absent(struct normfs_fs_plan *plan);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_PUBLISH;
    requires NORMFS_FS_PUBLISH_STATE(plan);
    requires os_error > 0;
    assigns plan->op, plan->os_error,
            normfs_fs_vol_data, normfs_fs_dur_names, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_PUBLISH_STATE(plan);
    ensures NORMFS_FS_PUBLISH_STEP(plan);
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) != NORMFS_FS_OP_DONE &&
              \old(plan->op) != NORMFS_FS_OP_FAILED;
    ensures \result == NORMFS_FS_OK ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->os_error == os_error;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
*/
int normfs_fs_publish_err(struct normfs_fs_plan *plan, int os_error);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_APPEND;
    requires NORMFS_FS_APPEND_STATE(plan);
    assigns plan->op, plan->written, plan->restored,
            normfs_fs_vol_data, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_APPEND_STATE(plan);
    ensures fs_dur_synced(plan->ino) == plan->at ||
            fs_dur_synced(plan->ino) == plan->at + plan->total;
    ensures \result == NORMFS_FS_OK <==>
              (\old(plan->op) == NORMFS_FS_OP_WRITE &&
               0 < n <= \old(plan->total) - \old(plan->written)) ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ||
              \old(plan->op) == NORMFS_FS_OP_TRUNCATE_BACK;
    ensures \result == NORMFS_FS_ERR_STATE ==>
              plan->op == \old(plan->op) && plan->written == \old(plan->written);
    ensures \result == NORMFS_FS_OK && \old(plan->op) == NORMFS_FS_OP_WRITE ==>
              plan->written == \old(plan->written) + n &&
              (plan->written < plan->total ==> plan->op == NORMFS_FS_OP_WRITE) &&
              (plan->written == plan->total ==>
                 plan->op == NORMFS_FS_OP_FSYNC_FILE);
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ==>
              plan->op == NORMFS_FS_OP_DONE;
    ensures \old(plan->op) == NORMFS_FS_OP_TRUNCATE_BACK ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->restored == 1;
    // DONE exactly when the batch is on the medium: the watermark's licence.
    ensures plan->op == NORMFS_FS_OP_DONE <==>
              fs_dur_synced(plan->ino) == plan->at + plan->total;
    ensures plan->at == \old(plan->at) && plan->total == \old(plan->total) &&
            plan->ino == \old(plan->ino);
*/
int normfs_fs_append_ok(struct normfs_fs_plan *plan, uint64_t n);

/* WRITE or FSYNC_FILE failed: cut the file back before anything else. A
 * failed TRUNCATE_BACK ends the plan unrestored; RESTORE is the way back. */
/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_APPEND;
    requires NORMFS_FS_APPEND_STATE(plan);
    requires os_error > 0;
    assigns plan->op, plan->os_error, plan->restored,
            normfs_fs_vol_data, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_APPEND_STATE(plan);
    ensures fs_dur_synced(plan->ino) == plan->at ||
            fs_dur_synced(plan->ino) == plan->at + plan->total;
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) == NORMFS_FS_OP_WRITE ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ||
              \old(plan->op) == NORMFS_FS_OP_TRUNCATE_BACK;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
    ensures \result == NORMFS_FS_OK &&
            (\old(plan->op) == NORMFS_FS_OP_WRITE ||
             \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE) ==>
              plan->op == NORMFS_FS_OP_TRUNCATE_BACK;
    ensures \old(plan->op) == NORMFS_FS_OP_TRUNCATE_BACK ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->restored == 0;
    ensures \result == NORMFS_FS_OK ==> plan->os_error > 0;
    ensures \result == NORMFS_FS_OK ==> plan->op != NORMFS_FS_OP_DONE;
    ensures plan->at == \old(plan->at) && plan->total == \old(plan->total) &&
            plan->ino == \old(plan->ino);
*/
int normfs_fs_append_err(struct normfs_fs_plan *plan, int os_error);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_CREATE;
    requires NORMFS_FS_CREATE_STATE(plan);
    requires plan->op == NORMFS_FS_OP_FSYNC_DIR ==>
        fs_parent_durable(plan->dst, plan->dst_len);
    assigns plan->op, plan->written, plan->ino,
            normfs_fs_vol_names, normfs_fs_vol_data,
            normfs_fs_dur_names, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_CREATE_STATE(plan);
    ensures \result == NORMFS_FS_OK <==>
              (\old(plan->op) == NORMFS_FS_OP_OPEN && n > 0) ||
              (\old(plan->op) == NORMFS_FS_OP_WRITE &&
               0 < n <= \old(plan->total) - \old(plan->written)) ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR;
    ensures \result == NORMFS_FS_ERR_STATE ==>
              plan->op == \old(plan->op) && plan->written == \old(plan->written);
    ensures \result == NORMFS_FS_OK && \old(plan->op) == NORMFS_FS_OP_OPEN ==>
              plan->ino == n &&
              (plan->total > 0 ==> plan->op == NORMFS_FS_OP_WRITE) &&
              (plan->total == 0 ==> plan->op == NORMFS_FS_OP_FSYNC_FILE);
    ensures \result == NORMFS_FS_OK && \old(plan->op) == NORMFS_FS_OP_WRITE ==>
              plan->written == \old(plan->written) + n &&
              (plan->written < plan->total ==> plan->op == NORMFS_FS_OP_WRITE) &&
              (plan->written == plan->total ==>
                 plan->op == NORMFS_FS_OP_FSYNC_FILE);
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_FILE ==>
              plan->op == NORMFS_FS_OP_FSYNC_DIR;
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR ==>
              plan->op == NORMFS_FS_OP_DONE;
    ensures \result == NORMFS_FS_OK ==>
              (plan->op == NORMFS_FS_OP_DONE <==>
               \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR);
    ensures plan->total == \old(plan->total) && plan->dst == \old(plan->dst);
*/
int normfs_fs_create_ok(struct normfs_fs_plan *plan, uint64_t n);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_CREATE;
    requires NORMFS_FS_CREATE_STATE(plan);
    requires os_error > 0;
    assigns plan->op, plan->os_error,
            normfs_fs_vol_data, normfs_fs_dur_names, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_CREATE_STATE(plan);
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) != NORMFS_FS_OP_DONE &&
              \old(plan->op) != NORMFS_FS_OP_FAILED;
    ensures \result == NORMFS_FS_OK ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->os_error == os_error;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
*/
int normfs_fs_create_err(struct normfs_fs_plan *plan, int os_error);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_REMOVE;
    requires NORMFS_FS_REMOVE_STATE(plan);
    requires plan->op == NORMFS_FS_OP_FSYNC_DIR ==>
        fs_parent_durable(plan->dst, plan->dst_len);
    assigns plan->op, normfs_fs_vol_names, normfs_fs_dur_names;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_REMOVE_STATE(plan);
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) == NORMFS_FS_OP_UNLINK ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
    ensures \old(plan->op) == NORMFS_FS_OP_UNLINK ==>
              plan->op == NORMFS_FS_OP_FSYNC_DIR;
    ensures \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR ==>
              plan->op == NORMFS_FS_OP_DONE;
    ensures \result == NORMFS_FS_OK ==>
              (plan->op == NORMFS_FS_OP_DONE <==>
               \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR);
*/
int normfs_fs_remove_ok(struct normfs_fs_plan *plan);

/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_REMOVE;
    requires NORMFS_FS_REMOVE_STATE(plan);
    requires os_error > 0;
    assigns plan->op, plan->os_error, normfs_fs_dur_names;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures NORMFS_FS_REMOVE_STATE(plan);
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) == NORMFS_FS_OP_UNLINK ||
              \old(plan->op) == NORMFS_FS_OP_FSYNC_DIR;
    ensures \result == NORMFS_FS_OK ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->os_error == os_error;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
*/
int normfs_fs_remove_err(struct normfs_fs_plan *plan, int os_error);

/* The one step of RESTORE. On success the file is what the next APPEND at
 * the same offset requires. */
/*@ requires \valid(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_RESTORE;
    requires os_error >= 0;
    assigns plan->op, plan->os_error, normfs_fs_vol_data, normfs_fs_dur_data;
    ensures \result == NORMFS_FS_OK || \result == NORMFS_FS_ERR_STATE;
    ensures NORMFS_FS_PLAN_WF(plan);
    ensures \result == NORMFS_FS_OK <==>
              \old(plan->op) == NORMFS_FS_OP_TRUNCATE_BACK;
    ensures \result == NORMFS_FS_OK && os_error == 0 ==>
              plan->op == NORMFS_FS_OP_DONE &&
              fs_vol_len(plan->ino) == plan->at &&
              fs_dur_synced(plan->ino) <= plan->at;
    ensures \result == NORMFS_FS_OK && os_error > 0 ==>
              plan->op == NORMFS_FS_OP_FAILED && plan->os_error == os_error;
    ensures \result == NORMFS_FS_ERR_STATE ==> plan->op == \old(plan->op);
*/
int normfs_fs_restore_report(struct normfs_fs_plan *plan, int os_error);

/* ------------------------------------------------------------------------
 * Theorems: functions whose result nobody uses. WP discharging the asserts
 * in their bodies is the proof; \result == 1 is not a runtime check. */

/*@ requires \valid_read(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_PUBLISH;
    requires NORMFS_FS_PUBLISH_STATE(plan);
    requires plan->op == NORMFS_FS_OP_DONE;
    assigns \nothing;
    ensures \result == 1;
*/
int normfs_fs_publish_done_durable(const struct normfs_fs_plan *plan);

/*@ requires \valid_read(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_APPEND;
    requires NORMFS_FS_APPEND_STATE(plan);
    requires plan->op == NORMFS_FS_OP_DONE || plan->op == NORMFS_FS_OP_FAILED;
    assigns \nothing;
    ensures \result == 1;
*/
int normfs_fs_append_boundary_holds(const struct normfs_fs_plan *plan);

/*@ requires len < NORMFS_FS_PATH_MAX;
    requires \valid_read(a + (0 .. len));
    requires \valid_read(b + (0 .. len));
    requires fs_path_equal(a, len, b, len);
    requires fs_vol_ino(a, len) > 0;
    assigns normfs_fs_vol_names;
    ensures fs_vol_ino(a, len) == \old(fs_vol_ino(a, len));
    ensures fs_vol_ino(b, len) == \old(fs_vol_ino(b, len));
*/
void normfs_fs_rename_equal_paths(const char *a, const char *b, size_t len);

#endif /* NORMFS_FS_PLAN_H */
