# Benchmarks

## Environment

| Item | Value |
|---|---|
| CPU | Intel Core i9-13900H |
| Logical CPUs | 20 |
| OS | Linux 7.0.10-arch1-1, amd64 |
| Go | 1.26.3-X:nodwarf5 |
| WAL filesystem | ext4 on NVMe |

`/tmp` is mounted as `tmpfs` on this host and was not used for the durable results below.

## Method

The durable workload used four symbols, four shards, 64 synchronous producers, a target rate of 100,000 commands per second, a fixed seed of 42, and clean WAL and snapshot directories. Each run lasted six seconds. A command was counted as processed only after its WAL batch completed `fsync`. Offline replay was run after each retained workload.

Command generation uses a fixed mix of limit orders, market orders, cancels, and replacements. Business rejections are included in throughput. Infrastructure failures are reported separately.

```bash
go run ./cmd/loadgen \
  -config ./configs/benchmark.json \
  -duration 6s \
  -producers 64 \
  -rate 100000 \
  -seed 42 \
  -require-empty

go run ./cmd/replay -config ./configs/benchmark.json
```

Results are local reference values. Filesystem, kernel, power state, and background I/O affect them.

## Group commit tuning

The current benchmark profile uses 128 commands, 64 KiB, and 100 microseconds as the group-commit thresholds.

| Batch delay | Throughput | p50 | p95 | p99 | Average batch | `fsync` p50 | `fsync` p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 us | 44,027 cmd/s | 1.405 ms | 2.164 ms | 2.547 ms | 20.49 | 0.792 ms | 1.201 ms |
| 250 us | 43,608 cmd/s | 1.318 ms | 2.337 ms | 2.672 ms | 28.45 | 0.833 ms | 1.400 ms |
| 500 us | 34,057 cmd/s | 1.830 ms | 2.836 ms | 3.331 ms | 29.43 | 0.826 ms | 1.211 ms |
| 1,000 us | 25,322 cmd/s | 2.498 ms | 2.958 ms | 3.323 ms | 31.84 | 0.817 ms | 1.374 ms |

Increasing the delay beyond 250 microseconds produced larger batches but reduced throughput. The measured `fsync` latency was already close to one millisecond, so the additional wait extended the commit cycle without a proportional reduction in sync operations.

The selected 100-microsecond profile was repeated with lower memory limits:

```text
throughput       45,331 cmd/s
p50               1.375 ms
p95               2.015 ms
p99               2.283 ms
p99.9             4.385 ms
average batch    20.47 commands
average bytes  5,177 bytes
fsync p50         0.702 ms
fsync p95         0.965 ms
fsync p99         1.149 ms
failed                0
```

Compared with the previous 1,000-microsecond benchmark profile, this run increased throughput from 25,322 to 45,331 commands per second and reduced p99 from 3.323 to 2.283 milliseconds.

## Development profile

A separate test used two shards, four producers, and a target rate of 4,000 commands per second.

| Profile | Throughput | p50 | p95 | p99 | Average batch |
|---|---:|---:|---:|---:|---:|
| 512 commands, 1 MiB, 500 us | 2,172 cmd/s | 2.021 ms | 2.498 ms | 2.688 ms | 4.00 |
| 64 commands, 64 KiB, 100 us | 3,276 cmd/s | 1.132 ms | 1.923 ms | 2.194 ms | 3.94 |

## Microbenchmarks

Median of five runs on the same host:

| Benchmark | Time | Memory | Allocations |
|---|---:|---:|---:|
| Rest and cancel pair | 402.7 ns/op | 1,376 B/op | 4 allocs/op |
| Rest and match pair | 447.0 ns/op | 1,440 B/op | 5 allocs/op |
| Add non-crossing limit order | 418.2 ns/op | approximately 754 B/op | 2 allocs/op |
| Mixed command stream | 459.5 ns/op | approximately 714 B/op | 1 alloc/op |
| Sweep 10 price levels, including setup | 4.658 us/op | 11,264 B/op | 44 allocs/op |
| Sweep 100 price levels, including setup | 53.265 us/op | 105,730 B/op | 330 allocs/op |
| Encode WAL record | 219.3 ns/op | 240 B/op | 1 alloc/op |
| Encode WAL record into batch buffer | 68.81 ns/op | 0 B/op | 0 allocs/op |
| Decode WAL record | 177.7 ns/op | 168 B/op | 2 allocs/op |

The book benchmarks exclude WAL and `fsync`. The durable workload includes sequencing, routing, matching, encoding, group commit, `fsync`, and acknowledgement.
