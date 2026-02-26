# NormFS Go Client Benchmarks

Performance benchmarks for the NormFS Go client library over TCP.

## Quick Start

### Fast Benchmark (1 second per test, recommended)
```bash
make run-gobench
```

### Comprehensive Benchmark (configurable)
```bash
make run-bin                            # Default: 1 client, 200k iterations, 1kb message
make run-bin N=10000                    # Custom iteration count
make run-bin CLIENTS=10                 # Custom client count
make run-bin SIZE=4096                  # Custom message size (bytes)
make run-bin CLIENTS=10 N=50000         # Custom clients and iterations
```

## Requirements

- Go 1.21+
- Running NormFS TCP server (default: `localhost:8888`)
- **High concurrency (1000+ clients)**: Benchmark auto-sets ulimits to 256k. May need to expand local port range to 10000-65535.

## Benchmark Types

### 1. Fanout Latency (Binary)
Measures the time until **all N clients** receive a message after it's written.

**What it tests:**
- Message propagation latency across multiple concurrent clients
- Subscribe/Follow performance
- Server fanout capabilities

**Metrics reported:**
- Average latency
- P50, P95, P99 percentiles
- Total duration

**Run with:**
```bash
./benchmark --clients=10 --iterations=100 --addr=localhost:8888
```

**Parameters:**
- `--clients`: Number of concurrent subscriber clients (default: 1)
- `--iterations`: Number of test iterations (default: 100)
- `--size`: Payload size in bytes (default: 1024)
- `--queue`: Queue ID to use (default: "bench/latency")
- `--addr`: NormFS server address (default: "localhost:8888")

### 2. Go Test Benchmarks
Standard Go benchmarks for write and read operations.

**What it tests:**
- Single-client write latency
- Single-client read latency
- Memory allocations per operation

**Run with:**
```bash
go test -bench=. -benchmem
```

## Makefile Targets

- `make run-bin` - Run fanout latency benchmark (configurable clients/iterations/size)
- `make run-gobench` - Run fast Go test benchmarks
- `make clean` - Remove built binaries

## Example Output

```
🚀 === NormFS Latency Benchmark === 🚀

📡 Server:           localhost:8888
📂 Queue:            bench/latency
👥 Clients:          10
🔁 Iterations:       100
📦 Payload size:     1024 bytes

📢 Running fanout benchmark (time until all 10 clients see message)...

📊 Results:
  Operation:        Fanout (all clients)
  Iterations:       100
  Total duration:   15.2s
  Avg latency:      152ms
  P50 latency:      145ms
  P95 latency:      210ms
  P99 latency:      285ms

✅ Benchmark complete!
```

## Understanding Results

**Fanout Latency**: Time from write completion until the last of N clients receives the message via Follow/subscribe. Lower is better.

**P50/P95/P99**: Percentile latencies - P99 means 99% of operations completed faster than this time.

## Notes

- Each client uses connection pooling (4 read connections + 1 write connection per client)
- Each iteration waits for all clients to see the message before proceeding
- Subscriptions are established once and reused across all iterations
