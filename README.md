# NormFS 🚀

[![Crates.io](https://img.shields.io/crates/v/normfs.svg)](https://crates.io/crates/normfs)

**High-performance persistent queue storage for robotics and embedded systems**

Storage engine with automatic data lifecycle management across memory, disk, and cloud. Built for high-frequency sensor data ingestion. Available as embeddable library or standalone server.

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
- ⚡ **High Performance**: <100μs latency (p99)
- 🤖 **Multi-System Isolation**: Unique instance IDs for safe fleet-wide cloud syncing
- 🔒 **Security**: AES-256-GCM encryption + Ed25519 signatures
- 📖 **Flexible Reads**: Absolute/relative positioning, tail reads, subscriptions, step queries
- ⏱️ **Time Sync**: Nanosecond-precision timestamps for distributed coordination
- 🔄 **Crash Recovery**: Write-ahead logging with automatic replay
- ✅ **Proven Codecs**: The WAL entry and header codecs are C, verified with Frama-C WP and run in CI — a corrupt entry is detected, not decoded

## 🏗️ Architecture

```
Memory → WAL (Disk) → Store (Disk) → Cloud (S3-compatible)
  ↓         ↓             ↓              ↓
Fast    Durable    Compressed      Archival
```

Every stage after memory is optional, per queue (`QueueConfig.persist`):
with WAL and store (the default) records reach disk within `write_interval`;
with store alone a sealed memory page becomes one store file directly, so a
crash loses at most the open page; with neither the queue is memory-only and
only its last id survives a restart. `cloud` sends that queue's store files to
the bucket: offloaded from the local store when there is one, landed there
directly from the sealed page when there is not.

## 🎯 Use Cases

🤖 **Robotics**: High-frequency sensor logging (IMU, lidar, GPS), multi-sensor sync, black box recording, simulation replay, fleet data aggregation

💾 **Embedded Systems**: Time-series data, event sourcing, audit logs, edge computing with cloud sync

🌐 **IoT & Edge**: Local-first storage with automatic cloud archival, multi-device coordination

## 🌍 Language Support

### 🦀 Rust (Core Implementation + Embedded Library)

**Embedded Library**: Zero external dependencies, runs in-process with your Rust application. No separate database or server process required.

**Standalone Server**: TCP/WebSocket server for multi-language client access.

📖 **Rust Documentation** - Coming soon

### 🐹 Go (Client Library)

Native Go client for connecting to NormFS servers.

📖 **[Go Documentation →](normfs_go/README.md)**

### 🐍 Python · 🟨 TypeScript

Coming soon. Protocol specification available for implementing additional clients.

## 🚀 Quick Start

```bash
# Clone the repository
git clone https://github.com/norma-core/normfs.git
cd normfs
```

### Run as Server

```bash
# Build server
cargo build --release --features server-bin --bin normfs-server

# Run server
./target/release/normfs-server --data-dir /tmp/normfs-data --addr 0.0.0.0:8888
```

### Client Libraries

See language-specific documentation:
- 🐹 **Go**: [normfs_go/README.md](normfs_go/README.md)
- 🦀 **Rust**: Available (documentation coming soon)

### Cross-Compilation

Build the server for multiple platforms using [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```bash
# Install cargo-zigbuild
cargo install cargo-zigbuild

# Build server for different platforms
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-unknown-linux-gnu
cargo zigbuild --release --features server-bin --bin normfs-server --target aarch64-unknown-linux-gnu
cargo zigbuild --release --features server-bin --bin normfs-server --target aarch64-apple-darwin
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-apple-darwin
cargo zigbuild --release --features server-bin --bin normfs-server --target x86_64-unknown-freebsd

# Binaries will be at: target/<target-triple>/release/normfs-server
```

## 💻 Platform Support

| Platform | Arch | Status |
|----------|------|--------|
| Linux | x86_64, aarch64 | ✅ |
| macOS | x86_64, aarch64 | ✅ |
| FreeBSD | x86_64 | ✅ |

The WAL checksums with the CPU's CRC32 instruction, so x86_64 needs SSE4.2
(Nehalem, Bulldozer and later) and aarch64 needs the CRC extension. A CPU
without it is not supported and faults rather than falling back.

## 📦 Components

- **normfs**: Core Rust library and server
- **normfs-wal**: Write-ahead log, with a formally verified C entry codec
- **normfs-store**: Compressed/encrypted persistent storage
- **normfs-cloud**: S3-compatible cloud integration
- **normfs-crypto**: Encryption and signing
- **normfs_go**: Go client library
- **uintn**: Variable-width integers with infinite scaling

## 📊 Status

**v0.3.0** - Active development, API may change before 1.0

WAL files written by 0.1 are read by 0.2 unchanged. 0.2 writes a smaller entry
format that 0.1 cannot read, so a downgrade needs the queue drained first. 0.3
writes the same format as 0.2 and needs no migration in either direction.

## 📄 License

[MIT](LICENSE)
