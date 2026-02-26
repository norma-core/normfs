.PHONY: clean run-bin run-gobench run-all

N ?= 200000
CLIENTS ?= 1
SIZE ?= 1024

run-bin:
	@echo "Building benchmark binary..."
	@go build -o cmd/benchmark/benchmark cmd/benchmark/main.go
	@echo "Running benchmark with $(CLIENTS) clients, $(N) iterations, $(SIZE) bytes"
	@cd cmd/benchmark && ./benchmark --clients=$(CLIENTS) --iterations=$(N) --size=$(SIZE)
	@echo "✅ Done"

run-all:
	@echo "Building benchmark binary..."
	@go build -o cmd/benchmark/benchmark cmd/benchmark/main.go
	@echo ""
	@echo "Running comprehensive benchmarks: 200k messages, 1kb size"
	@echo "Client counts: 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024"
	@echo ""
	@for clients in 1 2 4 8 16 32 64 128 256 512 1024; do \
		echo "=== $$clients Client(s) ==="; \
		./cmd/benchmark/benchmark --clients=$$clients --iterations=200000 --size=1024; \
		echo ""; \
	done
	@echo "✅ All benchmarks complete!"

run-gobench:
	@echo "Running Go test benchmarks..."
	@go test -bench=. -benchmem
	@echo "✅ Done"

clean:
	@rm -f cmd/benchmark/benchmark
	@echo "✅ Clean complete"
