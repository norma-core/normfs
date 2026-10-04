# Toward a fully verified NormFS

What WP covers today, what it does not, and the order in which the rest moves
into C. The rule stays the one the existing modules follow: the C decides, the
Rust executes, and axioms appear only at syscalls, CPU instructions and
certified libraries.

## Covered

| Target | What is proved |
|---|---|
| `verify-varint` | varint32/64 encode, decode, size; decode of an encoding is the value |
| `verify-wal-header` | V1 WAL header codec and its round trip |
| `verify-wal-entry` | V1 entry framing, CRC32C over the frame, entry ids from `num_entries_before + index` |
| `verify-wal-page`, `-pool`, `-ring` | page append, lookup and cut; page ownership (a page has at most one holder); ring append and page retention |
| `verify-store-header` | V1 store header codec and its round trip |
| `verify-store-pack` | pack slot layout: a slot sized for `input_cap` holds any input up to it plus its sealed store file, without overlap and within the slot |
| `verify-seed` | seed file path and its load or create protocol over assumed syscalls |
| `verify-fs-plan` | publish and append durability protocols, crash states, directory barriers |
| `verify-disk-monitor` | eviction: removed files are gone, `to_free` and the byte total move by the file size |

## Not covered

Everything a restart depends on still has Rust in the decision path:

1. **WAL recovery scan.** `count_entries` and `get_wal_content` walk a file
   entry by entry and stop at the first frame that is cut short or corrupt.
   Each step calls the proved `iter_next`, but the walk itself, which decides
   how many entries survive a crash and which id comes next, is Rust.
2. **Store file read path.** `parser.rs` and `store_file.rs` split a store
   file into auth, header, nonce, body and tag. This is the inverse of the
   pack layout and is not tied to it by any proof.
3. **Pipelines.** The page writer, the WAL to store worker, the offloader and
   the system queue are async Rust state machines: retries, backoff, the order
   in which a file becomes durable, uploaded and acknowledged.
4. **Accounting.** `DiskUsage` and the range caches. Both P2 issues found on
   PR #49 were in this kind of code.
5. **Crypto and compression.** AES-GCM, Ed25519 and zstd are library calls.
   They become verification boundaries once the library choice is made.

## Next step

**Move the WAL recovery scan into C** (`normfs_wal_file_scan`) and prove:

- it reads only within the buffer and returns the length of the longest
  prefix of whole, CRC-valid frames after a valid header;
- the entry count and the next entry id follow from that prefix and
  `num_entries_before`;
- for any sequence of records written by `wal_page_append`, then cut at an
  arbitrary byte, the scan returns exactly the records whose frames were
  complete. This is the crash recovery theorem for the WAL.

Why this first: it runs on every restart and reads bytes nobody vouches for;
it decides which acknowledged entries survive; all the parts it composes
(header, entry, CRC32C) are already proved, so the new work is the loop
invariant and the cut theorem, not a new codec. It is also what the PR #49
review listed as outside WP ("end-to-end recovery").

## After that

1. **Store file parse** as the inverse of `verify-store-pack`: parsing a
   sealed file recovers the header and the body range the layout produced.
   This closes the store side the way the scan closes the WAL side.
2. **Pipeline planners.** The `fs_plan` pattern: each pipeline gets a pure C
   planner that, given the current state and the last completion, says what
   to do next; Rust only runs the I/O. First the offloader (a file is deleted
   locally only after its upload is acknowledged and recorded), then the page
   writer.
3. **Accounting in C**, with the invariant that the tracked total equals the
   sum of the sizes of the files that exist, across publish, failure and
   eviction.
4. **Crypto boundary**, once the library is chosen: contracts for the calls
   used, checked against known-answer vectors byte for byte.

When these land, the Rust left is the async runtime, the network client and
the syscall shims, each behind a contract the C relies on.
