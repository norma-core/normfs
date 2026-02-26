package bench

import (
	"log/slog"
	"os"
	"testing"
)

var (
	testAddr  = "localhost:8888"
	testQueue = "bench/latency"
	testData  = make([]byte, 1024)
	logger    = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
)

func init() {
	for i := range testData {
		testData[i] = byte(i % 256)
	}
}

func BenchmarkFanoutLatency1Client(b *testing.B) {
	result := RunFanoutLatencyBenchmark(testAddr, logger, testQueue, 1, b.N, testData)
	b.ReportMetric(float64(result.AvgLatencyNs), "ns/op")
	b.ReportMetric(float64(result.P50LatencyNs), "p50-ns")
	b.ReportMetric(float64(result.P95LatencyNs), "p95-ns")
	b.ReportMetric(float64(result.P99LatencyNs), "p99-ns")
}

func BenchmarkFanoutLatency5Clients(b *testing.B) {
	result := RunFanoutLatencyBenchmark(testAddr, logger, testQueue, 5, b.N, testData)
	b.ReportMetric(float64(result.AvgLatencyNs), "ns/op")
	b.ReportMetric(float64(result.P50LatencyNs), "p50-ns")
	b.ReportMetric(float64(result.P95LatencyNs), "p95-ns")
	b.ReportMetric(float64(result.P99LatencyNs), "p99-ns")
}

func BenchmarkFanoutLatency10Clients(b *testing.B) {
	result := RunFanoutLatencyBenchmark(testAddr, logger, testQueue, 10, b.N, testData)
	b.ReportMetric(float64(result.AvgLatencyNs), "ns/op")
	b.ReportMetric(float64(result.P50LatencyNs), "p50-ns")
	b.ReportMetric(float64(result.P95LatencyNs), "p95-ns")
	b.ReportMetric(float64(result.P99LatencyNs), "p99-ns")
}
