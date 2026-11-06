package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/norma-core/normfs/normfs_go/bench"
)

func main() {
	addr := flag.String("addr", "localhost:8888", "NormFS server address")
	numClients := flag.Int("clients", 1, "number of concurrent clients")
	iterations := flag.Int("iterations", 100, "number of iterations")
	dataSize := flag.Int("size", 1024, "payload size in bytes")
	queueID := flag.String("queue", "bench/latency", "queue ID to use")
	flag.Parse()

	if err := setupRLimits(); err != nil {
		fmt.Printf("Failed to set up for high load: %v\n", err)
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	fmt.Printf("\n🚀 === NormFS Latency Benchmark === 🚀\n\n")
	fmt.Printf("📡 Server:           %s\n", *addr)
	fmt.Printf("📂 Queue:            %s\n", *queueID)
	fmt.Printf("👥 Clients:          %d\n", *numClients)
	fmt.Printf("🔁 Iterations:       %d\n", *iterations)
	fmt.Printf("📦 Payload size:     %d bytes\n", *dataSize)
	fmt.Printf("\n")

	// Create test data
	testData := make([]byte, *dataSize)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Set up defer to print results
	var result bench.LatencyResult
	defer func() {
		if result.TotalOps > 0 {
			fmt.Printf("\n")
			bench.PrintLatencyResult(result)
			fmt.Printf("\n✅ Benchmark complete!\n\n")
		}
	}()

	// Run fanout benchmark
	fmt.Printf("📢 Running fanout benchmark (time until all %d clients see message)...\n", *numClients)
	result = bench.RunFanoutLatencyBenchmark(*addr, logger, *queueID, *numClients, *iterations, testData)
}
