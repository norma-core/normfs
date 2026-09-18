# AGENTS.md

Conventions for AI agents and new contributors working in this repository.
These are the things that are not derivable from the code in a single reading,
and that reviewers have had to ask for more than once.

## Comments

**Do not write comments that restate the code.**

A comment earns its place only if a competent reader of this codebase learns
something from it that the code does not already say. In practice that means:
the *why*, not the *what*.

Delete on sight:

```c
/* Unspecified contents on failure. */          // the block above already said it
normfs_seed_zero(seed, NORMFS_SEED_SIZE);
```

```rust
// Seed file should exist
assert!(Seed::exists(temp_dir.path()));

// Create first seed
let seed1 = Seed::open(temp_dir.path()).unwrap();
```

Keep:

```c
/* Not retried on EINTR: Linux releases the descriptor regardless, so a
 * retry could close one already handed to another thread. */
if (close(fd) != 0)
```

```c
/* c99 has no _Static_assert, so a negative array size is the check. */
```

Rules of thumb:

- A test name that already says what the test does does not need a comment
  repeating it (`test_load_missing` needs no `/* Tests loading a missing file */`).
- Never state the same reason twice. If a function's block comment explains the
  wipe-on-failure, the wipe call itself does not get a second comment.
- Prefer one tight sentence to three discursive ones. `wal_entry.c` and
  `varint.c` carry zero prose comments and are not worse for it.
- Rust `// SAFETY:` comments on `unsafe` blocks are required and are not
  subject to this rule — but state the invariant, not the obvious.
- ACSL annotations are specification, not commentary. A `//` comment *inside*
  a contract is held to the same bar: use it only to justify a clause whose
  absence a reader would not notice (e.g. a completeness clause that stops a
  trivially-satisfiable spec).

## Repository shape

A Rust workspace (`Cargo.toml`), a Go client tree under `normfs_go/` with its
own `go.work` (explicitly excluded from the Cargo workspace), a shared wire
schema in `proto/normfs.proto`, and a verified C layer.

Crates: `normfs` (server/binary), `normfs-types`, `normfs-wal`, `normfs-store`,
`normfs-cloud`, `normfs-crypto`, `uintn-rs`.

Five of them carry C — `uintn-rs`, `normfs-wal`, `normfs-store`,
`normfs-crypto`, and `normfs` (the disk monitor) — and all five follow the
same layout:

```
<crate>/
  build.rs                 # cc::Build, compiles the C into the Rust staticlib
  src/                     # Rust, thin FFI wrapper over the C
  c/
    CMakeLists.txt         # library + tests + the verify-<module> WP target
    include/normfs/*.h     # public header, carries the ACSL contracts
    src/*.c                # the implementation
    tests/*.c              # only where WP cannot reach
```

Where a behaviour exists in both languages, **C is the implementation and Rust
is the wrapper.** Do not reimplement logic on the Rust side that the C already
proves.

## The C layer

The C is proved with Frama-C WP, not merely tested. Conventions the proof
depends on:

- **C99, `-Wall -Wextra -Werror -pedantic`.** `-Werror` is part of the claim.
  There is no `_Static_assert`; use the negative-array-size idiom.
- **A proved `.c` file must not include a system header.** Frama-C's own libc
  contracts are unusable here: none of them assigns `errno`, so every
  `os_error` postcondition would become *impossible* rather than merely
  unproven. Syscalls go behind hand-written contracts in a `*_sys.h`, with the
  bodies in a `*_sys.c` that WP never sees, discharged by C tests instead.
- **Every function with a body must be listed in the module's `WP_FCTS` set**,
  statics included. A function left off the list is silently never scheduled,
  and `check-proved.sh` cannot tell that apart from one that was proved.
- **`-machdep gcc_x86_64` is pinned.** This project is 64-bit only: no 32-bit
  support, no 32-bit tests, no second machine model. Do not add one. (Deployment
  targets are amd64 and arm64; both are LP64, so one model covers them.)
- **`frama-c` exits 0 whether or not it proved anything.** The verdict comes
  from `verify/check-proved.sh` ruling on a `-wp-report-json` report. Never
  read success off the console summary.
- Smoke tests (`-wp-smoke-tests`) count: a smoke test that *succeeds* is a
  reachable contradiction and is treated as a failure.
- C tests exist only for what WP cannot reach — assumed syscall shims, compiler
  intrinsics (the CRC32C hardware path is checked against the proven portable
  one). Do not add executable tests for something already proved.
- Proof parallelism is capped per module on purpose. `normfs-wal` holds it at 2
  because the entry codec's quantified byte clauses make Z3 time out under
  load, and a proof that depends on machine load is not a proof.

Neither `verify/Makefile` nor `.github/workflows/ci.yml` names modules or
targets — both discover them from the tree. Adding a module needs no edit to
either; adding a `verify-*` target to a module's `CMakeLists.txt` is enough.
Do not introduce a hand-maintained list that mirrors the tree.

## Rust ↔ C FFI

- The C returns a `#[repr(C)]` status struct, never a bare error code. Keep the
  field order and widths identical on both sides so neither has padding the
  other does not.
- Errors carry `errno` across the boundary explicitly. Without it,
  `ErrorKind::NotFound` and `AlreadyExists` are lost, and callers classify on
  exactly those. Rebuild with `io::Error::from_raw_os_error`.
- Every error enum gets an `UnknownStatus(c_int)` variant for a status the
  build does not recognise. Do not `panic!` or silently map to a neighbour.
- Sizes are cross-checked, not trusted: the C rejects a length that disagrees
  with its own constant rather than assuming the Rust constant is right.
- Pass paths as `(pointer, length)` with an explicit NUL, not as a C string
  alone — the length is what lets the ACSL talk about the bytes without
  Frama-C's string axiomatics.

## Rust conventions

- Edition 2024, `max_width = 100` (`rustfmt.toml` at the root). Run
  `cargo fmt` before pushing.
- **Errors are hand-written enums.** No `thiserror`, no `anyhow` anywhere in
  the workspace. Each error type implements `Display` and `std::error::Error`
  (with `source()` forwarding for wrapped errors) by hand. A crate-level
  aggregate (e.g. `WalError`) wraps the per-module ones through `From`.
- **Unit tests live in a sibling file**, not an inline `mod tests`: `foo.rs`
  pairs with `foo_test.rs`, wired in `lib.rs` as
  `#[cfg(test)] mod foo_test;`. This is the pattern in `normfs-wal`,
  `normfs-store`, and `uintn-rs`; follow it for new modules. (`normfs-crypto`
  still uses inline `mod tests` — do not use it as the model.)
- **Versioned wire formats get their own module**: `wal_header_v1.rs`,
  `store_header_v1.rs`, `wal_entry_v1.rs`, with the version constants and size
  bounds re-exported from `lib.rs`, and a `peek_version` for dispatch. New
  files are always written at the current version; older versions stay
  readable.
- `Cargo.lock` is **untracked** (it is in `.gitignore`). CI therefore runs
  `cargo test --workspace` without `--locked` — do not add the flag, and do not
  commit the lockfile.

## Secrets

`normfs-crypto` handles the root secret. Two rules that have already been
broken once:

- A type holding key material gets a hand-written `Drop` that wipes. A
  `#[derive(ZeroizeOnDrop)]` whose only field is `#[zeroize(skip)]` compiles to
  an empty `Drop` and wipes nothing.
- Never derive `Debug` on a type holding key material — it puts the secret in
  any log line that formats it. This is why some tests must `match` rather than
  `expect_err`.

## Commands

```sh
cargo test --workspace                       # Rust
cargo fmt                                    # before pushing
make -f verify/Makefile test                 # build + run every C module's tests
make -f verify/Makefile verify               # all proofs
make -f verify/Makefile verify-seed          # one module's proof
make -f verify/Makefile docker-verify        # inside the pinned image
```

CI needs `protoc` (`protobuf-compiler`) for the prost build.

`verify-*` targets work on macOS. `docker-verify` is currently broken under
Rosetta — use CI or a Linux host for the pinned-image run.

## Commits and PRs

- Branches are `feat/…` or `fix/…`; PRs land on `main`.
- Commit messages here explain *why* the change is shaped the way it is,
  including what was deliberately left alone (a port that preserves an existing
  TOCTOU says so). Match that register.
- A change to the C layer is not done until its proof target passes. State the
  goal count and smoke-test count in the PR description.
