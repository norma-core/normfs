package bench

import (
	"fmt"
	"log"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	normfs_go "github.com/norma-core/normfs/normfs_go"
)

type LatencyResult struct {
	Operation    string
	NumClients   int
	TotalOps     int
	Duration     time.Duration
	AvgLatencyNs int64
	P50LatencyNs int64
	P95LatencyNs int64
	P99LatencyNs int64
}

// RunFanoutLatencyBenchmark measures time until all N clients see a message
func RunFanoutLatencyBenchmark(addr string, logger *slog.Logger, queuePath string, numClients int, iterations int, data []byte) LatencyResult {
	latencies := make([]int64, iterations)

	// Pre-create all reader clients and set up Follow once
	type readerState struct {
		client    normfs_go.Client
		entryChan chan normfs_go.StreamEntry
		errChan   <-chan error
	}
	readers := make([]readerState, numClients)

	for i := 0; i < numClients; i++ {
		client, err := normfs_go.NewClient(addr, logger)
		if err != nil {
			log.Fatalf("Failed to create reader client %d: %v", i, err)
		}

		entryChan := make(chan normfs_go.StreamEntry, 100)
		errChan := client.Follow(queuePath, entryChan)

		readers[i] = readerState{
			client:    client,
			entryChan: entryChan,
			errChan:   errChan,
		}
	}

	// Give all readers time to connect and subscribe
	time.Sleep(200 * time.Millisecond)

	// Pre-create writer client
	writeClient, err := normfs_go.NewClient(addr, logger)
	if err != nil {
		log.Fatalf("Failed to create writer client: %v", err)
	}

	for iter := 0; iter < iterations; iter++ {
		var wg sync.WaitGroup
		var allSeenTime atomic.Int64
		var clientsSeen atomic.Int32

		// Start goroutines to wait for messages on existing subscriptions
		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func(clientID int) {
				defer wg.Done()

				reader := readers[clientID]

				select {
				case <-reader.entryChan:
					now := time.Now().UnixNano()
					if clientsSeen.Add(1) == int32(numClients) {
						allSeenTime.Store(now)
					}
				case err := <-reader.errChan:
					if err != nil {
						log.Printf("Follow error on client %d: %v", clientID, err)
					}
					return
				case <-time.After(5 * time.Second):
					log.Printf("Timeout waiting for message on client %d", clientID)
					return
				}
			}(i)
		}

		// Write message and measure time
		writeStart := time.Now()
		_, err = writeClient.Enqueue(queuePath, data)
		if err != nil {
			log.Fatalf("Write failed: %v", err)
		}

		// Wait for all clients to see the message
		wg.Wait()

		fanoutLatency := allSeenTime.Load() - writeStart.UnixNano()
		latencies[iter] = fanoutLatency
	}

	totalDuration := time.Duration(0)
	for _, l := range latencies {
		totalDuration += time.Duration(l)
	}

	return CalculateLatencyResult("Fanout (all clients)", numClients, iterations, totalDuration, latencies)
}

func CalculateLatencyResult(operation string, numClients int, totalOps int, duration time.Duration, latencies []int64) LatencyResult {
	if len(latencies) == 0 {
		return LatencyResult{
			Operation:  operation,
			NumClients: numClients,
			TotalOps:   0,
		}
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var sum int64
	for _, l := range latencies {
		sum += l
	}

	return LatencyResult{
		Operation:    operation,
		NumClients:   numClients,
		TotalOps:     totalOps,
		Duration:     duration,
		AvgLatencyNs: sum / int64(len(latencies)),
		P50LatencyNs: latencies[len(latencies)*50/100],
		P95LatencyNs: latencies[len(latencies)*95/100],
		P99LatencyNs: latencies[len(latencies)*99/100],
	}
}

func PrintLatencyResult(r LatencyResult) {
	fmt.Printf("\n📊 Results:\n")
	fmt.Printf("  Operation:        %s\n", r.Operation)
	fmt.Printf("  Iterations:       %s\n", FormatNumber(r.TotalOps))
	fmt.Printf("  Total duration:   %v\n", r.Duration)
	fmt.Printf("  Avg latency:      %v\n", time.Duration(r.AvgLatencyNs))
	fmt.Printf("  P50 latency:      %v\n", time.Duration(r.P50LatencyNs))
	fmt.Printf("  P95 latency:      %v\n", time.Duration(r.P95LatencyNs))
	fmt.Printf("  P99 latency:      %v\n", time.Duration(r.P99LatencyNs))
}

func FormatNumber(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	var result []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, '_')
		}
		result = append(result, c)
	}
	return string(result)
}
