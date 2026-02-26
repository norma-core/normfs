# NormFS 🚀

[![Crates.io](https://img.shields.io/crates/v/normfs.svg)](https://crates.io/crates/normfs)

**High-performance persistent queue storage for robotics and embedded systems**

Storage engine with automatic data lifecycle management across memory, disk, and cloud. Built for high-frequency sensor data ingestion. Available as embeddable library or standalone server.

## ✨ Features

- 🗄️ **Tiered Storage**: Memory → WAL → Compressed Store → Cloud archival
- ⚡ **High Performance**: <100μs latency (p99)
- 🤖 **Multi-System Isolation**: Unique instance IDs for safe fleet-wide cloud syncing
- 🔒 **Security**: AES-256-GCM encryption + Ed25519 signatures
- 📖 **Flexible Reads**: Absolute/relative positioning, tail reads, subscriptions, step queries
- ⏱️ **Time Sync**: Nanosecond-precision timestamps for distributed coordination
- 🔄 **Crash Recovery**: Write-ahead logging with automatic replay

## 🏗️ Architecture

```
Memory → WAL (Disk) → Store (Disk) → Cloud (S3-compatible)
  ↓         ↓             ↓              ↓
Fast    Durable    Compressed      Archival
```

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

Build for multiple platforms using [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```bash
# Install cargo-zigbuild
cargo install cargo-zigbuild

# Cross-compile for different targets
cargo zigbuild --release --target x86_64-unknown-linux-gnu
cargo zigbuild --release --target aarch64-unknown-linux-gnu
cargo zigbuild --release --target x86_64-unknown-freebsd
```

## 💻 Platform Support

| Platform | Arch | Status |
|----------|------|--------|
| Linux | x86_64, aarch64 | ✅ |
| macOS | x86_64, aarch64 | ✅ |
| FreeBSD | x86_64 | ✅ |

## 📦 Components

- **normfs**: Core Rust library and server
- **normfs-wal**: Write-ahead log
- **normfs-store**: Compressed/encrypted persistent storage
- **normfs-cloud**: S3-compatible cloud integration
- **normfs-crypto**: Encryption and signing
- **normfs_go**: Go client library
- **uintn**: Variable-width integers with infinite scaling

## 📊 Status

**v0.1.0-beta.0** - Active development, API may change before 1.0

## 📄 License

[MIT](LICENSE)
