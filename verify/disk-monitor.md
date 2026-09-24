# Disk monitor: proof scope

## Assumed kernel behavior

WP verifies `disk_monitor.c` against the contracts in `disk_monitor_sys.h`.
The kernel and the syscall shim bodies are outside the proof.

`NormfsDiskKernel` assumes that a successful regular-file probe returns the
exact length, that successful unlink removes an existing name, and that a
failed unlink leaves the byte total alone. For a regular file, unlink
subtracts that length from `disk_fs_bytes`, which stays within `[0, 2^64)`.
Syscalls may fail; their contracts do not require eventual success.

The model excludes external namespace and file-size changes between probing
and unlinking, including replacement of the file or its parent directories.
The existing pathname-based TOCTOU remains. Bytes denote regular-file lengths
summed over directory entries, not allocated disk blocks. Hard links and open
descriptors can keep data alive after unlink; the native shim test checks this
boundary as well as name disappearance and repeated-unlink failure.

## Proved properties

For `evict_one`, the successful-unlink branch establishes pathname absence.
A successful deletion decreases modeled filesystem bytes by the measured file
size and sets `to_free` to `max(0, old_to_free - file_size)`. Anything else
leaves both unchanged. A failed stat or unlink stops the walk with
`STOP_ERROR`.

For `evict`, per call:

- Batch accounting: each event's `freed` is the previous event's plus its own
  size if deleted, the first event's is its own, and `disk_fs_bytes` decreases
  by the last event's `freed`. The recurrence has one solution, so this is
  the sum of every reported deletion.
- `to_free` ends at `max(0, old_to_free - freed)`, so a call that stops with
  `STOP_FREED` has lowered `disk_fs_bytes` by at least the deficit it was
  given.
- Only the last event can be a failure, and only under `STOP_ERROR`; `next`
  is then that event's id, so no newer file is deleted in place of one that
  stays, and the retry starts at it.
- `to_free` never increases.

## Limits

Accounting is per C call. Rust sums the calls a batch takes when the event
buffer fills; that composition is not proved.

Convergence of the running server to the target is not proved. What the
proof gives is one step: a call that returns `STOP_FREED` brings the modeled
filesystem down by at least the deficit, and any other stop names why it did
not (`GAP`, `BOUND`, `ERROR`, or a full buffer, which Rust resumes). Getting
from there to the running server needs accurate accounting, enough eligible
files, removals outpacing arrivals, offload completing, and the monitor
continuing to run. Protected files, a permanent unlink failure or continuous
writes can prevent progress; zero-size deletions do not reduce the deficit.

Scan correctness is not proved: there is no theorem that `scan` enumerates
every file of the layout, that `total` is their summed size, or that `min` is
the smallest id present. `dir_next` has no model of a directory's contents to
state it against. Cleanup no longer depends on it per tick: Rust starts each
eviction at the previous one's `next` and walks the store only at the
periodic rescan or when that id has no file. A `min` that is too high would
still leave older files behind until a rescan finds them. WP does not verify
Rust synchronization, scheduler fairness, or concurrent WAL writes.
