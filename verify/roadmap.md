# Toward a fully verified NormFS

What WP covers in 0.4.12, what it does not, and the order in which the rest moves
into C. For migrated operations, C implements the decision and Rust provides
the async integration. Proofs rely on contracts at syscall, CPU-instruction,
and library boundaries; tests check those assumptions where WP cannot reach.

## Covered

| Target | What is proved |
|---|---|
| `verify-varint` | varint32/64 encode, decode, size; decode of an encoding is the value |
| `verify-queue-name` | queue-name validation and normalization |
| `verify-wal-header` | V1 WAL header codec and its round trip |
| `verify-wal-entry` | V1 entry framing, CRC32C over the frame, entry ids from `num_entries_before + index` |
| `verify-wal-scan` | bounds, count, and end of the contiguous V1 entry prefix; scanning stops at the first invalid/incomplete entry or id overflow |
| `verify-wal-page`, `-pool`, `-ring` | page append, lookup and cut; page ownership (a page has at most one holder); ring append and page retention |
| `verify-store-header` | V1 store header codec and its round trip |
| `verify-store-pack` | pack slot layout: a slot sized for `input_cap` holds any input up to it plus its sealed store file, without overlap and within the slot |
| `verify-seed` | seed file path and its load or create protocol over assumed syscalls |
| `verify-fs-plan` | publish and append durability protocols, crash states, directory barriers |
| `verify-disk-monitor` | eviction: removed files are gone, `to_free` and the byte total move by the file size |
| `verify-walk` | a read at a file boundary goes on, goes back to the file before (read while still being written), or walks past a hole, each exactly when its condition holds |
| `verify-frame`, `verify-net` | TCP frame headers and socket read/write operations over assumed syscall contracts |
| `verify-conn` | per-connection receive and transmit state, buffer and event bounds, and reply/read queue handling |
| `verify-request`, `verify-response`, `verify-gremlin-lemmas` | request decoding and response encoding contracts, with local lemmas for the vendored gremlin interface |

These are module contracts, not a proof that every acknowledged record survives
every system failure. The V1 scan now runs in C through `scan_whole_entries`;
header dispatch, legacy V0 scanning, and recovery across files remain Rust.
The network proofs depend on the syscall and gremlin contracts they call;
Tokio scheduling and the complete client/server execution are outside them.

Unix filesystem path walking rejects links below the configured data directory
under the shim assumptions in [fs.md](fs.md). Windows uses separate shims and
still resolves paths as given. Linux and Windows runtime tests exercise their
respective implementations; a proof under the pinned machine model does not
replace platform testing.

## Not covered

The remaining decision paths and proof boundaries include:

1. **End-to-end recovery.** Selecting files and layers, retrying failed reads,
   handling gaps and overlaps, and connecting a recovered prefix to write
   acknowledgements still involve Rust. The proved V1 scan covers the bytes
   given to it, not whether those bytes reached durable storage.
2. **Store file read path.** `parser.rs` and `store_file.rs` split a store
   file into auth, header, nonce, body and tag. This is the inverse of the
   pack layout and is not tied to it by any proof.
3. **Pipelines.** The page writer, the WAL to store worker, the offloader and
   the system queue are async Rust state machines: retries, backoff, the order
   in which a file becomes durable, uploaded and acknowledged.
4. **Accounting and concurrency.** `DiskUsage`, file/range caches, queue start
   coordination, record-id assignment, and data-directory lock ownership are
   not covered by an end-to-end proof.
5. **Crypto and compression.** AES-GCM, Ed25519 and zstd are library calls.
   Their implementations and security properties are not proved by the local
   WP targets. Store packing proves layout, not cryptographic isolation.

## Next step

The V1 WAL scan moved to C in #85. The next codec boundary is **store file
parsing**, as the inverse of `verify-store-pack`: parsing a sealed file should
recover the header and body range the layout produced, while rejecting
truncated or inconsistent layouts. Authentication and decompression remain
explicit library assumptions rather than consequences of a layout proof.

## After that

1. **Pipeline planners.** The `fs_plan` pattern: each pipeline gets a pure C
   planner that, given the current state and the last completion, says what
   to do next; Rust only runs the I/O. First the offloader (a file is deleted
   locally only after its upload is acknowledged and recorded), then the page
   writer.
2. **Accounting in C**, with the invariant that the tracked total equals the
   sum of the sizes of the files that exist, across publish, failure and
   eviction.
3. **Crypto boundary:** contracts for the calls used, checked against
   known-answer vectors byte for byte, with a separate model for key
   derivation, nonce use, and any proposed restricted key sharing.
4. **Compose recovery guarantees:** connect append, publish, durability
   acknowledgement, and recovery contracts across crashes and cancellation.

The goal is to reduce unproved decisions without duplicating the implementation
or treating a tested syscall/library assumption as a proved theorem.
