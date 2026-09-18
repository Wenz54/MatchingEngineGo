# Matching Engine

A deterministic, sharded matching engine implemented in Go. The project covers order matching, durable command acknowledgement, snapshots, and verified recovery.

## Features

- Limit and market orders
- GTC, IOC, and FOK time-in-force policies
- Price-time priority with FIFO queues at each price level
- New, cancel, and replace commands
- Partial fills and multi-level sweeps
- Stable symbol-to-shard routing
- Per-shard write-ahead logs with CRC32C and segment rotation
- Group commit with acknowledgement after `fsync`
- Atomic snapshots and verified WAL-tail replay through the in-process API
- Fail-stop behavior after WAL append or flush errors

## Requirements

- Go 1.22 or later
- Bash for repository scripts

## Run

```bash
make run
```

The default configuration is `configs/dev.json`.

```bash
make run CONFIG=./configs/benchmark.json
```

## Verification

```bash
make fmt-check
make vet
make test
make race
make crash-matrix
```

GitHub Actions runs formatting, vet, tests, the race detector, and the crash/recovery matrix on pushes and pull requests.

Full local pipelines:

```bash
make test-drive-quick
make test-drive
make test-drive-heavy
```

The test-drive scripts may remove WAL and snapshot data selected by the configuration. See the operations runbook before using them with persistent data.

## Benchmarks

```bash
make bench
make load
make replay-load
```

`make bench` runs in-memory book and WAL codec microbenchmarks without `fsync`. `make load` exercises the durable path and requires empty benchmark WAL and snapshot directories. Local measurements and the test method are recorded in [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md) — execution model and design rationale
- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — local benchmark results
- [`docs/OPERATIONS_RUNBOOK.md`](docs/OPERATIONS_RUNBOOK.md) — operation and verification commands
- [`docs/CRASH_MATRIX.md`](docs/CRASH_MATRIX.md) — covered failure scenarios

## Scope

The repository provides an in-process engine and command-line verification tools. It does not implement a network protocol, authentication, accounts, risk checks, clearing, replication, or high availability.
