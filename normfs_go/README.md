# NormFS Go Client 🐹

Go client library for connecting to NormFS servers.

## Installation

```bash
go get github.com/norma-core/normfs/normfs_go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    "log/slog"
    "os"

    normfs "github.com/norma-core/normfs/normfs_go"
)

func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

    // Connect to NormFS server
    client, err := normfs.NewClient("localhost:8888", logger)
    if err != nil {
        log.Fatal(err)
    }

    // Write data
    queuePath := "sensors/imu"
    data := []byte("sensor reading")

    id, err := client.Enqueue(queuePath, data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Written entry with ID: %s\n", id.String())

    // Read data
    qr := client.ReadFromOffset(queuePath, id, 1, 1, 10)
    for entry := range qr.Data {
        fmt.Printf("Entry %s: %s\n", entry.ID.ID.String(), string(entry.Data))
    }

    if qr.Err != nil {
        log.Fatal(qr.Err)
    }
}
```

## Features

- 🔌 **Simple API**: Easy-to-use client for all NormFS operations
- 📖 **Flexible Reads**: Absolute, tail-based, and subscription modes
- ⚡ **High Performance**: Zero-copy operations where possible
- 🤖 **Multi-Client**: Support for parallel connections with connection pooling

## API Overview

### Creating a Client

```go
// Basic connection with connection pooling (4 read connections, 1 write connection)
logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
client, err := normfs.NewClient("localhost:8888", logger)
```

### Writing Data

```go
// Single write
id, err := client.Enqueue("queue/path", []byte("data"))

// Batch write
ids, err := client.EnqueuePack("queue/path", [][]byte{
    []byte("data1"),
    []byte("data2"),
    []byte("data3"),
})
```

## Examples

### Sensor Data Logger

```go
package main

import (
    "encoding/json"
    "log"
    "log/slog"
    "os"
    "time"

    normfs "github.com/norma-core/normfs/normfs_go"
)

type SensorReading struct {
    Timestamp time.Time
    Value     float64
    SensorID  string
}

func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
    client, _ := normfs.NewClient("localhost:8888", logger)

    // Write sensor data
    reading := SensorReading{
        Timestamp: time.Now(),
        Value:     42.5,
        SensorID:  "imu-001",
    }

    data, _ := json.Marshal(reading)
    id, _ := client.Enqueue("sensors/imu", data)

    println("Logged sensor reading:", id.String())
}
```

### Real-Time Subscriber

```go
package main

import (
    "fmt"
    "log/slog"
    "os"

    normfs "github.com/norma-core/normfs/normfs_go"
)

func main() {
    logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
    client, _ := normfs.NewClient("localhost:8888", logger)

    // Subscribe to real-time updates
    entryChan := make(chan normfs.StreamEntry, 100)
    errChan := client.Follow("events/system", entryChan)

    fmt.Println("Listening for events...")

    go func() {
        for entry := range entryChan {
            fmt.Printf("[%s] %s\n", entry.ID.ID.String(), string(entry.Data))
        }
    }()

    if err := <-errChan; err != nil {
        panic(err)
    }
}
```

## Protocol

The Go client uses NormFS's Protocol Buffers-based wire protocol over TCP. The protocol specification is defined in the `proto/` directory of the main repository.

## Building

```bash
# Run tests
go test ./...

# Generate protobuf code (if needed)
go run gen.go
```

## Requirements

- Go 1.21+
- NormFS server running (see [main README](../README.md))

## License

[MIT](../LICENSE)
