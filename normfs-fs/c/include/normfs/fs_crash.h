#ifndef NORMFS_FS_CRASH_H
#define NORMFS_FS_CRASH_H

#include "normfs/fs_plan.h"

/*@ ghost extern int normfs_fs_recovery; */
/*@ axiomatic NormfsFsRecovery {
      logic integer fs_recovered_len{L}(integer ino) reads normfs_fs_recovery;
      logic integer fs_recovered_byte{L}(integer ino, integer off)
        reads normfs_fs_recovery;
      logic integer fs_recovered_ino{L}(char *path, integer len)
        reads normfs_fs_recovery, path[0 .. len];
    }
*/

/* Covers a crash before completion, including during write/fsync: only the
 * already certified prefix is required to survive. The rest is arbitrary.
 * A stable file has had no writes since its successful fsync. */
/*@ requires stable == 0 || stable == 1;
    requires stable != 0 ==> fs_vol_len(ino) == fs_dur_synced(ino) &&
        fs_dur_len(ino) == fs_dur_synced(ino);
    assigns normfs_fs_recovery;
    ensures fs_recovered_len(ino) >= fs_dur_synced(ino);
    ensures stable != 0 ==> fs_recovered_len(ino) == fs_dur_synced(ino);
    ensures \forall integer i; 0 <= i < fs_dur_synced(ino) ==>
        fs_recovered_byte(ino, i) == fs_certified_byte(ino, i);
*/
void normfs_fs_world_crash_file(uint64_t ino, int stable);

/* Atomic replacement is an explicit filesystem assumption, including an
 * interrupted rename or directory fsync. candidate is nonzero only once
 * the temporary inode is fully synced and rename may have begun. No other
 * writer, alias, or directory replacement may touch either path. */
/*@ requires \valid_read(path + (0 .. len));
    requires candidate > 0 ==> fs_dur_synced(candidate) == total &&
        fs_dur_len(candidate) == total;
    assigns normfs_fs_recovery;
    ensures fs_recovered_ino(path, len) == fs_dur_ino(path, len) ||
        fs_recovered_ino(path, len) == candidate;
    ensures fs_recovered_ino(path, len) == candidate && candidate > 0 ==>
        fs_recovered_len(candidate) == total;
    ensures \forall integer i; 0 <= i < total && candidate > 0 &&
        fs_recovered_ino(path, len) == candidate ==>
        fs_recovered_byte(candidate, i) == fs_certified_byte(candidate, i);
*/
void normfs_fs_world_crash_publish(const char *path, size_t len,
    uint64_t candidate, uint64_t total);

/*@ requires \valid_read(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_APPEND;
    requires NORMFS_FS_APPEND_STATE(plan);
    requires index < plan->at ||
        (plan->op == NORMFS_FS_OP_DONE && index < plan->at + plan->total);
    assigns normfs_fs_recovery;
    ensures fs_recovered_len(plan->ino) > index;
    ensures fs_recovered_byte(plan->ino, index) == fs_certified_byte(plan->ino, index);
*/
void normfs_fs_append_crash_prefix(const struct normfs_fs_plan *plan, uint64_t index);

/* A failed plan may have reached rename: it keeps no failure phase, so the
 * crash theorem is checked at each nonterminal phase and at DONE. */
/*@ requires \valid_read(plan);
    requires NORMFS_FS_PLAN_WF(plan);
    requires plan->kind == NORMFS_FS_PUBLISH;
    requires NORMFS_FS_PUBLISH_STATE(plan);
    requires plan->op != NORMFS_FS_OP_FAILED;
    requires index < plan->total;
    assigns normfs_fs_recovery;
    ensures fs_recovered_ino(plan->dst, plan->dst_len) ==
        fs_dur_ino(plan->dst, plan->dst_len) ||
        fs_recovered_ino(plan->dst, plan->dst_len) == 0 ||
        (fs_recovered_ino(plan->dst, plan->dst_len) == plan->ino &&
         fs_recovered_len(plan->ino) == plan->total &&
         fs_recovered_byte(plan->ino, index) == fs_certified_byte(plan->ino, index));
    ensures plan->op == NORMFS_FS_OP_DONE ==>
        fs_recovered_ino(plan->dst, plan->dst_len) == plan->ino &&
        fs_recovered_len(plan->ino) == plan->total &&
        fs_recovered_byte(plan->ino, index) == fs_certified_byte(plan->ino, index);
*/
void normfs_fs_publish_crash_safe(const struct normfs_fs_plan *plan, uint64_t index);

#endif
