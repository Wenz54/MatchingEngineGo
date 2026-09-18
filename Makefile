.PHONY: run build test race vet fmt-check bench load replay replay-load crash-matrix ops-demo test-drive test-drive-quick test-drive-heavy fuzz-book fuzz-wal

CONFIG ?= ./configs/dev.json
LOAD_CONFIG ?= ./configs/benchmark.json

run:
	go run ./cmd/engine -config $(CONFIG)

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./... -count=1

vet:
	go vet ./...

fmt-check:
	test -z "$$(gofmt -l .)"

bench:
	go test -bench=. -benchmem ./...

load:
	go run ./cmd/loadgen -config $(LOAD_CONFIG) -require-empty

replay:
	go run ./cmd/replay -config $(CONFIG)

replay-load:
	go run ./cmd/replay -config $(LOAD_CONFIG)

crash-matrix:
	bash ./scripts/crash_matrix.sh

ops-demo:
	bash ./scripts/ops_demo.sh

test-drive:
	bash ./scripts/test_drive.sh standard ./configs/dev.json

test-drive-quick:
	bash ./scripts/test_drive.sh quick ./configs/dev.json

test-drive-heavy:
	bash ./scripts/test_drive.sh heavy ./configs/benchmark.json

fuzz-book:
	go test -fuzz=FuzzBookApplyInvariants -fuzztime=10s ./internal/book

fuzz-wal:
	go test -fuzz=FuzzWALDecode -fuzztime=10s ./internal/journal
