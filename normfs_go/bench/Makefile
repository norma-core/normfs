.PHONY: clean run-bin run-gobench

N ?= 200000
CLIENTS ?= 1
SIZE ?= 1024

run-bin:
	@echo "Building benchmark binary..."
	@go build -o cmd/benchmark/benchmark cmd/benchmark/main.go
	@echo "Running benchmark with $(CLIENTS) clients, $(N) iterations, $(SIZE) bytes"
	@cd cmd/benchmark && ./benchmark --clients=$(CLIENTS) --iterations=$(N) --size=$(SIZE)
	@echo "✅ Done"

run-gobench:
	@echo "Running Go test benchmarks..."
	@go test -bench=. -benchmem
	@echo "✅ Done"

clean:
	@rm -f cmd/benchmark/benchmark
	@echo "✅ Clean complete"
