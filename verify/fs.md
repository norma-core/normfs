# Filesystem protocol proof

`verify-fs-plan` verifies every function in `fs_plan.c`, `fs_dir.c`, and
`fs_crash.c`. The JSON report is judged by `check-proved.sh`, including smoke
tests. The Rust executor and the syscall bodies remain outside WP; C shim
tests and Rust integration tests cover that boundary.

## Kernel assumptions

The model separates cached names/data, durability certificates, directory
certificates, and a recovered state. A certificate records what a successful
sync guarantees, not everything the kernel might already have persisted.
In particular, unsynced writes and renames may reach storage in the
background. `fs_crash.h` supplies the permitted recovered states, including a
crash inside a syscall that has not yet returned a completion.

The assumptions are:

- A successful file fsync certifies the cached length and bytes. The storage
  stack must honor completed flushes. A later append or failed sync preserves
  the previously certified bytes; corruption of that prefix by the device or
  filesystem is outside the model. No whole-batch atomicity is assumed.
- Failed writes may leave a partial tail. Failed fsync may leave unusable
  dirty cache contents. The executor truncates and rewrites before retrying;
  failed restoration blocks subsequent appends, including the next flush.
- The explicit axioms `fs_vol_path_value` and `fs_dur_path_value` assert that
  names are interpreted by value, not by the address of their path buffers.
  Renaming a name onto itself, or onto the same inode, leaves the inode
  present. Publishing rejects equal paths before opening the temp.
- Namespace replacement is crash-atomic: after rename may have started,
  recovery can expose the previously certified target or the replacement.
  This is a filesystem assumption beyond live `rename` atomicity, not a
  consequence of POSIX alone. The replacement is synced before that window.
- A directory sync certifies its entries, conditional on its parent path
  already being durable and unchanged (`fs_parent_durable`). Existing directories
  are provisioned durable: existence alone is not a durability certificate.
  `mkdir_all` creates missing directories top-down and syncs each new directory
  and its immediate parent before creating children. Existing ancestors are
  traversed with search permission; they are not opened for directory sync.
  A process restart without a machine crash must reestablish the provisioning
  precondition if an earlier process died during directory creation.
- Nobody outside the serialized operation replaces its names, directories,
  or open inode. Callers must serialize operations sharing a target. A
  publish's temporary inode must not alias its target. Hard-link and symlink
  races, mount changes, and arbitrary writes through other descriptors are
  excluded. The alias checks do not make external races safe.

The syscall shims assume ordinary local regular files and directories. A
backend can claim these guarantees only if its filesystem and storage stack
satisfy those assumptions. Skipping fsync explicitly opts out of durability.

## Theorems and their connection to execution

PUBLISH preserves `NORMFS_FS_PUBLISH_STATE` at every report. Once file sync
succeeds the new inode has its full length certified, and only then can the
planner request rename. DONE additionally certifies the destination name.
`normfs_fs_publish_crash_safe` considers a crash at each non-failed phase,
including inside the next syscall. A replacement may appear only from the
rename phase onward, and its entire certified contents survive. DONE recovers
the replacement with exactly its complete length. A failed plan retains no
failure phase; the syscall's crash window is therefore checked at its input
phase, and completed failures retain the ordinary per-step invariant.

APPEND preserves a certificate ending at `at` until successful fsync, then at
`at + total`. This is an acknowledgment boundary, **not** a claim that recovery
contains either zero or all appended bytes. `normfs_fs_append_crash_prefix`
proves that every previously acknowledged byte is present and unchanged after
a crash at any phase. After DONE that includes the whole new batch. The
recovered tail before DONE may have arbitrary length and bytes; recovery must
validate its frames and discard a torn suffix. CRC decoding and the server's
recovery policy are separate from this filesystem proof.

CREATE proves durability only at DONE. A crash during creation can leave an
empty or partial file; callers must not infer a complete header from mere
existence. REMOVE proves durable absence at DONE. RESTORE proves the cached
length needed for the next append; it does not promise durable removal of a
failed tail, since ftruncate alone is not a durability barrier.

`normfs_fs_sync_created_dir` proves monotone retry stages: success requires
completion of both the new-directory sync and the parent sync. Failure retains
which barrier remains. The Rust directory coordinator serializes creation of the
same directory across Fs instances and retains pending stages until success. Its registry lock excludes
mkdir and fsync; unrelated directories have separate completion locks. Existing
directories avoid canonicalization when the pending registry is empty. Its path
traversal and retention are tested obligations outside WP. File operations sync only their
immediate parent. The planner assumes `fs_parent_durable`; it does not prove
provisioning of a preexisting directory tree.

`normfs_fs_rename_equal_paths` exercises two equal path values, including the
same pointer, so the rename model cannot derive absence of the source while
also retaining its inode at that same name. Its smoke tests check that this
case remains reachable.

## Executor obligations

The pool runs a plan's operations in order, and reports success only after the
corresponding syscall succeeds. Admission is bounded to 32 jobs per worker,
capped at 1024 jobs, including running jobs. Waiting for admission is async;
cancelling that wait submits nothing. A submitted job owns its permit until
completion, even if its receiver disappears. Caller-owned buffers waiting
for admission are not included in that bound.

User closures and publish accounting have separate panic boundaries. A normal
job panic is an error. Accounting runs after DONE: its panic is logged without
changing the successful publication result or retiring a worker. A publish cleans a temporary
name only after successfully opening it. Reads use positioned I/O on the same
pool, so cancelling a read cannot advance a shared kernel cursor. A read fills
a window ahead of the caller, from 64 KiB doubling to 1 MiB while the caller
drains it, and a seek inside the window is free; each fill is still one
positioned read on the pool.

FSYNC_DIR is shared between plans on the same parent directory. A plan that
arrives while no sync is running performs one; a plan that arrives during a
sync waits for the next one, which starts after its own rename completed. Every
plan is therefore reported done only after a directory sync that began after
its entry existed, which is what the directory certificate requires. A failed
shared sync fails every plan waiting on it with the same `errno`. The
provisioned-directory cache in `Fs` remembers paths a `mkdir_all` has made
durable and skips the pool for them; `remove_dir_all` forgets the subtree
before removing it.

Memory-pointer snapshots retain their serialization lock in a task that owns
the flush to completion. A dropped caller cannot release the fixed temporary
name to another writer or certify an unfinished snapshot. Errors restore the
dirty flag before releasing the lock. Runtime destruction is process shutdown,
not an orderly flush; clients must await close for its durability guarantee.

All production WAL/store/monitor I/O goes through `Fs`; startup crypto and
some filesystem walks use its blocking-closure entry point. These closures,
scans, reads, accounting, Rust synchronization, and the refinement from real
syscalls to ghost completions are tested rather than claimed proved by WP.

Reads and fsync currently share the bounded pool. Splitting their admission or
workers is a hardware-validation question: measure WAL commit tail latency
under concurrent scans on the rover's eMMC before choosing separate budgets.
