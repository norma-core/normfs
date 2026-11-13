# NormFS 🚀

[![Crates.io](https://img.shields.io/crates/v/normfs.svg)](https://crates.io/crates/normfs)

**High-performance persistent queue storage for robotics and embedded systems**

Storage engine with automatic data lifecycle management across memory, disk,
and cloud. Built for high-frequency sensor data ingestion. Available as a Rust
or C/C++ embedded library and a standalone TCP server, with a Go client.

Runs on **Linux (x86_64 and ARM64)**, **macOS (Intel and Apple Silicon)**,
**FreeBSD (x86_64)**, and **Windows (x86_64, MSVC or MinGW)**.

## 📊 Latency

![TCP fanout latency](images/fanout-latency.png)

Time until *all* N subscribers have a 1 KB message over TCP — the number that
matters for multi-sensor coordination, where one late subscriber is a late
system. Straight on log-log axes means a power law: past roughly 64 subscribers
the fan-out itself is the cost, not the store.

📈 **[Full TCP benchmarks →](normfs_go/bench/README.md)**

## 📈 Throughput and Readers

![Device throughput and reader cost](images/device-and-readers.png)

Two machines. On a rover-alpha board writing to a class-10 SD card with zstd and
AES-GCM, records of 8 KiB and up put 17–22 MB/s on the card — the card itself
does 18–20 MB/s under `dd`, so the engine runs the medium at its limit. Replay
three camera streams faster than real time and the board's CPU gives out long
before the card does.

On an M4 Pro, an individual tail read stays flat from 1 to 4000 concurrent
readers, because a read is a page lookup and the page is borrowed rather than
copied. Propagation is flat to 1000 readers and then the fan-out starts to cost.

## ✨ Features

- 🗄️ **Tiered Storage**: Memory → WAL → Compressed Store → Cloud archival
- ⚡ **Efficient Replay**: Bounded decoded-file slots, cached queue file bounds, and batched file opening
- 🤖 **Multi-System Isolation**: Unique instance IDs for safe fleet-wide cloud syncing
- 🔒 **Stored Data Protection**: AES-256-GCM encryption + Ed25519 signatures
- 📡 **Wire Compression**: Negotiated zstd for read responses, with raw fallback when compression cannot keep up
- 📖 **Flexible Reads**: Absolute/relative positioning, tail reads, subscriptions, step queries
- ⏱️ **Time Sync**: Nanosecond-precision timestamps for distributed coordination
- 🔄 **Crash Recovery**: Write-ahead logging with automatic replay
- 🔍 **Diagnostics**: Read and write wait distributions, compression elapsed time, and storage pool load
- ✅ **Verified C Layer**: Frama-C WP contracts for codecs, V1 WAL scanning, page ownership, filesystem protocols, and TCP framing and connection state

## 🏗️ Architecture

```
Memory → WAL (Disk) → Store (Disk) → Cloud (S3-compatible)
  ↓         ↓             ↓              ↓
Fast    Durable    Compressed      Archival
```

Every stage after memory is optional, per queue (`QueueConfig.persist`):
with WAL and store (the default) the WAL writer batches records according to
`write_interval`; with store alone a sealed memory page becomes one store file
directly, and an explicit flush or close also seals the open page. Records
that have not reached durable storage can be lost on a crash. With all three
persistence stages disabled, the queue is memory-only and periodically
persists its last id rather than its records. `cloud` sends that queue's store
files to the bucket: offloaded from the local store when there is one, landed
there directly from the sealed page when there is not. Enabling WAL requires
the store stage to archive its files.

The WAL, the local store and the bucket are one interface, `Backend`
(`normfs-wal/src/backend.rs`), so these stages are a queue's file layers in
order: a sealed file lands in the first, one offloader moves it to the next,
and reads ask each layer for a file's range and bytes the same way. Appending
is part of it too; the bucket answers `Unsupported`, since an object cannot
be appended to.

NormFS records its own work in the queue `normfs/system`, one
`normfs.system.Event` (`normfs/proto/normfs_system.proto`) per record: each
store file once it is on the local disk (`ET_FILE_ON_DISK`) and once it is in
the bucket (`ET_FILE_IN_CLOUD`), with its id range, sizes and content
signature; failed uploads; files the disk monitor deleted or could not delete;
appenders waiting for memory; queues opened and closed; entries a read sent
raw because compression could not keep up. The proto's header
says which of these a queue gets for each `Persist`. Clients can read it
and cannot write it. It keeps its files locally, offloaded like any store
queue when cloud is configured, and in memory when queues keep nothing on disk
by default.
`NormFsSettings::system_queue` turns it off.

Store compression is configured per queue in the embedded API: `Zstd` is the
default, and `None` avoids recompressing payloads such as JPEG. Read responses
use zstd only when the client accepts it and the queue enables it; a client
must still accept raw entries. Pongs and write acknowledgements take priority
over read data at TCP frame boundaries.

## 🎯 Use Cases

🤖 **Robotics**: High-frequency sensor logging (IMU, lidar, GPS), multi-sensor sync, black box recording, simulation replay, fleet data aggregation

💾 **Embedded Systems**: Time-series data, event sourcing, audit logs, edge computing with cloud sync

🌐 **IoT & Edge**: Local-first storage with automatic cloud archival, multi-device coordination

## 🌍 Language Support

### 🦀 Rust (Embedded Library and Async Runtime)

**Embedded Library**: Runs in-process with your Rust application, using Tokio
for asynchronous work and the verified C layer for selected core operations.
No separate database or server process is required.

**Standalone Server**: TCP server for multi-language client access. The Rust
library also provides a WebSocket server behind the `websocket` Cargo feature.

📖 **Rust Documentation** - Coming soon

### 🐹 Go (Client Library)

Native Go client for connecting to NormFS servers.

📖 **[Go Documentation →](normfs_go/README.md)**

### C · C++ (Embedded Library)

`normfs-capi` builds a static library, `libnormfs_capi.a` (`normfs_capi.lib` on MSVC), with [`normfs.h`](normfs-capi/include/normfs.h): open a data directory, append, read, follow and flush queues from C or C++, in-process.

```bash
cargo build --release --locked -p normfs-capi
```

Calls block until complete, and a handle can be shared between threads. An
instance holds an operating-system lock on its data directory to prevent a
second embedded instance or server from opening the same directory. See the
header for callback lifetimes, shutdown ordering, and durability guarantees.

### 🐍 Python · 🟨 TypeScript

Coming soon. Protocol specification available for implementing additional clients.

## 🚀 Quick Start

Build prerequisites: Rust 1.99 or later, a C compiler, and Protocol Buffers'
`protoc` on `PATH` (`protobuf-compiler` on Debian/Ubuntu). On Windows, use
Visual Studio C++ Build Tools for MSVC, or a MinGW-w64 toolchain for GNU builds.

```bash
# Clone the repository
git clone --branch dev https://github.com/norma-core/normfs.git
cd normfs
```

### Run as Server

```bash
# Build server
cargo build --release --locked -p normfs --features server-bin --bin normfs-server

# Run server
./target/release/normfs-server --data-dir /tmp/normfs-data --addr 0.0.0.0:8888
```

On Windows, run the resulting executable from PowerShell:

```powershell
.\target\release\normfs-server.exe --data-dir C:\normfs-data --addr 0.0.0.0:8888
```

Server settings use CLI flags; there is no config file. Sizes accept plain bytes
or case-insensitive units: `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, `EiB` use powers of
1024; `KB`, `MB`, `GB`, `TB`, `PB`, `EB` use powers of 1000. Fractions such as
`1.5GiB` are accepted when they resolve to whole bytes.

| Flag | Default | Meaning |
|------|---------|---------|
| `--addr`, `-a` | `0.0.0.0:8888` | TCP listen address |
| `--data-dir`, `-d` | `./normfs_data` | Instance data directory |
| `--max-memory-usage` | `256MiB` | Shared active page budget |
| `--mem-page-size` | `256KiB` | Active page size; each record plus framing must fit |
| `--max-passive-memory-usage` | `8MiB` | Separate passive page budget |
| `--mem-passive-page-size` | `32KiB` | Passive page size |
| `--max-queue-disk-size` | `32GiB` | Per-queue WAL + store retention threshold |
| `--unlimited-disk` | off | Disable disk retention limits and size-triggered deletion |
| `--persist` | `wal-store` | `wal-store` or `store` (seal memory pages directly into store files) |
| `--cold-read-files` | Available CPU count, clamped to 2–8 | Shared limit on decoded store/cloud files across readers |
| `--store-workers` | Available CPU count / 4, rounded down, minimum 4 | Concurrent store sealing slots for compression and encryption; use a positive integer |
| `--s3-bucket` | unset | Enable S3 offloading to this bucket |
| `--s3-region` | `AWS_REGION` | Required when S3 is enabled |
| `--s3-endpoint` | unset | S3 endpoint URL; currently required when S3 is enabled |
| `--s3-access-key` | `AWS_ACCESS_KEY_ID` | Required when S3 is enabled |
| `--s3-secret-key` | `AWS_SECRET_ACCESS_KEY` | Required when S3 is enabled |
| `--s3-session-token` | `AWS_SESSION_TOKEN` | Accepted by the CLI but currently unused by the S3 client |
| `--help`, `-h` | — | Print help |
| `--version`, `-V` | — | Print the server version |

The server puts queues in the active pool. Each pool budget must hold at least
two of its pages. These budgets do not cap process RSS: buffers and metadata
add overhead, and queues can receive extra pages when the shared pool is full.
Disk cleanup is periodic, so usage can exceed the threshold between checks;
without S3 it deletes the oldest eligible files, and with S3 it waits for upload.
`--unlimited-disk` conflicts with an explicit `--max-queue-disk-size`; `0` is
not an unlimited value. Unlimited retention can fill the filesystem.

For a 512 MiB active budget, 1 MiB pages, and unlimited disk retention:

```bash
./target/release/normfs-server --data-dir /var/lib/normfs \
  --max-memory-usage 512MiB --mem-page-size 1MiB --unlimited-disk
```

Use `--persist store --mem-page-size 4MiB` to bypass the WAL and write sealed
pages directly to the store. Increasing `--store-workers` allows more sealing
work in parallel, with additional CPU and buffer use; `--cold-read-files`
controls decoded-file concurrency for readers. Neither setting is a total
process memory limit. The CPU-based defaults apply only when these flags are
omitted; explicit values are not clamped to those default ranges.
`--cold-read-files 0` is treated as one slot; `--store-workers 0` currently
causes a startup panic, so use at least one worker.

For S3 offloading, set `--s3-bucket` and `--s3-endpoint`. Supply the region and
credentials through `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and
`AWS_SECRET_ACCESS_KEY`, or the matching
`--s3-region`, `--s3-access-key`, and `--s3-secret-key` flags. In 0.4.12, the help
text calls the endpoint optional, but startup requires a valid URL and does
not derive an AWS endpoint from the region. Session tokens are not forwarded
to request signing, so temporary credentials requiring a session token are
not supported by this server yet.

Ctrl+C initiates a close and flush; Unix service managers can also use SIGTERM.

### Read, Write, and Pool Diagnostics

The server logs to stderr and uses `RUST_LOG` (default: `info`). For wait and
pool reports alongside warnings:

```bash
RUST_LOG=warn,normfs-read-wait=info ./target/release/normfs-server --data-dir /var/lib/normfs
```

In a Supervisor `[program:...]` section, use
`environment=RUST_LOG="warn,normfs-read-wait=info"` and capture stderr.
For the C/C++ library, set `NORMFS_LOG=warn,normfs-read-wait=info` before opening
the instance.

Reports run every 10 seconds over a sliding 60-second window while there is
activity to report. They include waits for the next read entry, queue write
waits, pool occupancy, and compression elapsed time. Read waits measure
whether the reader can supply its next entry, rather than network delivery
latency. Compression `elapsed=… s/s` sums wall time across calls, including
scheduling delays; it can exceed 1 and does not measure CPU utilization.

### Client Libraries

See language-specific documentation:

- 🐹 **Go**: [normfs_go/README.md](normfs_go/README.md)
- 🦀 **Rust**: Available (documentation coming soon)
- **C/C++**: [normfs-capi/include/normfs.h](normfs-capi/include/normfs.h)

### Cross-Compilation

For Linux x86_64 and ARM64, install Zig on `PATH` and
[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild), then add the
Rust targets:

```bash
cargo install cargo-zigbuild --locked
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu

cargo zigbuild --release --locked -p normfs --features server-bin --bin normfs-server --target x86_64-unknown-linux-gnu
cargo zigbuild --release --locked -p normfs --features server-bin --bin normfs-server --target aarch64-unknown-linux-gnu

# Binaries will be at: target/<target-triple>/release/normfs-server
```

For Windows, build natively with either toolchain:

```powershell
rustup target add x86_64-pc-windows-msvc x86_64-pc-windows-gnu
cargo build --release --locked -p normfs --features server-bin --bin normfs-server --target x86_64-pc-windows-msvc
cargo build --release --locked -p normfs --features server-bin --bin normfs-server --target x86_64-pc-windows-gnu
```

These require the corresponding C compiler and linker. Windows binaries end
in `.exe`. On macOS and FreeBSD, use the native server build command above.

## 💻 Platform Support

| Platform | Architecture / toolchain | Support and CI coverage |
|----------|--------------------------|-------------------------|
| Linux | x86_64 / AMD64, GNU | Supported; workspace tests, C tests, and cross-built WAL tests in CI |
| Linux | ARM64 / AArch64, GNU | Supported; WAL cross-build in CI |
| Windows | x86_64 MSVC | Supported; workspace tests in CI |
| Windows | x86_64 GNU / MinGW-w64 | Supported; workspace tests in CI |
| macOS | Intel x86_64, Apple Silicon ARM64 | Supported; no current CI job |
| FreeBSD | x86_64 | Supported; no current CI job |

The supported CPU families are x86_64 (also called AMD64 or Intel 64) and
ARM64 (also called AArch64), in the OS combinations listed above. There is no
32-bit support, and Windows ARM64 is not currently supported.
Windows support includes filesystem I/O, WAL recovery, crypto seed storage,
timestamps, TCP serving, and disk retention monitoring.

The WAL checksums with the CPU's CRC32 instruction, so x86_64 needs SSE4.2
(Nehalem, Bulldozer and later) and aarch64 needs the CRC extension. A CPU
without it is not supported and faults rather than falling back.

Queue names use `/` on every platform: `a//b` and `a/b/` normalize to `a/b`.
Names that escape the data directory, use backslashes, or collide with
reserved storage paths are rejected. On Unix, queue filesystem operations and
disk cleanup walk below the data directory without following symlinks.
Windows currently resolves paths as given; the Unix no-symlink guarantee does
not extend to Windows reparse points. See the
[filesystem assumptions](verify/fs.md) for the precise boundary.

## 📦 Components

- **normfs**: Core Rust library and server
- **normfs-types**: Shared types and queue-name validation
- **normfs-time**: Monotonic and wall-clock timestamps across platforms
- **normfs-fs**: Filesystem worker pool, publish/append protocols, and guarded Unix path operations
- **normfs-wal**: Write-ahead log, page pools, and verified V1 codecs and recovery scanning
- **normfs-store**: Compressed/encrypted persistent storage
- **normfs-cloud**: S3-compatible cloud integration
- **normfs-crypto**: Encryption and signing
- **normfs-capi**: Static library and C header for embedding NormFS in C and C++
- **normfs_go**: Go client library
- **uintn**: Variable-width integers and verified varint codecs; individual formats and APIs impose their own bounds

## ✅ Verification

Selected core operations are implemented in C with ACSL contracts checked by
Frama-C WP. Coverage includes WAL and store headers, V1 WAL entries and scans,
page ownership and retention, store pack layouts, queue-name validation, seed
file handling, filesystem durability protocols, disk eviction, file-boundary
read decisions, and TCP framing, request/response codecs, and connection state.

The Rust async pipelines, filesystem and socket shims, crypto and compression
libraries, and platform behavior remain outside those proofs or are assumed
at their boundaries. CI runs runtime tests as well as the applicable proof
targets. The [verification roadmap](verify/roadmap.md) describes current
coverage and the work toward end-to-end guarantees.

```bash
cargo test --workspace --locked
make -f verify/Makefile test
make -f verify/Makefile verify
```

The last command requires the verification toolchain; the pinned Linux image
can be used with `make -f verify/Makefile docker-verify`.

## 📊 Status

**v0.4.12** — Active development, API may change before 1.0. The `dev` branch
contains the current development version.

Recent 0.4.x changes include:

- Windows x86_64 support with MSVC and MinGW, plus the C/C++ embedded API and exclusive data-directory ownership.
- Verified C TCP framing, request decoding, response encoding, and connection state; incremental receive-buffer growth and recovery from temporary accept resource exhaustion.
- Negotiated read compression, priority for pongs and write acknowledgements, bounded cold reads, and ARMv8 AES acceleration through the updated crypto library.
- Fixes for backlog-to-follow transitions, concurrent WAL publication, and assigning record ids only after write capacity is available.
- Queue path validation, guarded Unix filesystem access, and disk-monitor path confinement.
- Direct-to-store CLI persistence, CPU-scaled store workers, and read/write/compression/pool diagnostics.

In 0.4.12, compression work stays in each read's final report
([#114](https://github.com/norma-core/normfs/pull/114)), queue file bounds are
cached ([#115](https://github.com/norma-core/normfs/pull/115)), cross-built tests
are discovered from Cargo's output
([#116](https://github.com/norma-core/normfs/pull/116)), and opening queue files
takes fewer filesystem pool trips
([#117](https://github.com/norma-core/normfs/pull/117)). These changes introduce
no new on-disk format: writers use V1, and
legacy V0 readers remain available.

## 📄 License

[MIT](LICENSE)
