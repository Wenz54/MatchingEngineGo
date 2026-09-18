# Operations Runbook

## Preconditions

- Go 1.22 or later
- Bash
- Repository root as the current directory
- Separate WAL and snapshot directories for disposable tests

## Start

```bash
make run
```

Use another configuration:

```bash
make run CONFIG=./configs/benchmark.json
```

The process handles `SIGINT` and `SIGTERM` and performs a bounded shutdown.

## Stop

For an interactive process, send `SIGINT` with `Ctrl-C`. Do not remove WAL or snapshot files while the process is running.

## Verify persisted state

```bash
make replay
```

For the benchmark configuration:

```bash
make replay-load
```

Replay validates metadata, checksums, snapshot state when present, WAL ordering, and deterministic command results. Any replay error must be treated as an unavailable state set; do not start a writer on the affected directories.

## Snapshots

Snapshot creation is exposed by the in-process `Engine.CreateSnapshot` API and exercised by integration tests. The engine CLI does not provide a signal, command, or schedule for creating snapshots. Without an API caller, recovery replays the WAL from the beginning.

## Validation

```bash
make fmt-check
make vet
make test
make race
make crash-matrix
```

Profiled validation:

```bash
make test-drive-quick
make test-drive
make test-drive-heavy
```

Profile settings are stored in `configs/test_profiles.env`.

## Load test

`make load` uses `configs/benchmark.json` and refuses to run when its WAL or snapshot directory is non-empty.

```bash
make load
make replay-load
```

Custom run:

```bash
go run ./cmd/loadgen \
  -config ./configs/benchmark.json \
  -duration 10s \
  -producers 64 \
  -rate 100000 \
  -seed 42 \
  -require-empty
```

Use a dedicated configuration and empty directories for comparable runs.

## Destructive scripts

`make ops-demo` runs `scripts/ops_demo.sh`. The script unconditionally removes:

```text
./data/wal
./data/snapshots
```

It does not derive those paths from a custom configuration argument. Use it only when those default directories are disposable.

The test-drive scripts remove the WAL and snapshot directories read from the selected configuration when `CLEAN_STATE=1`. Disable cleanup when state must be retained:

```bash
CLEAN_STATE=0 make test-drive
```

`CLEAN_STATE=0` permits reuse but does not make benchmark results comparable to a clean-state run.

## WAL failure

A WAL append or flush failure moves the affected shard to terminal fail-stop state. Pending commands, later commands, barriers, and snapshot drains return an error.

Required response:

1. Stop command submission.
2. Shut down the process.
3. Preserve the WAL and snapshot directories.
4. Run offline replay.
5. Inspect filesystem capacity, mount state, and kernel I/O errors.
6. Restart only after replay succeeds or the state set has been replaced through an external recovery procedure.

## Recovery failure

Startup or offline replay fails on incompatible metadata, unsupported versions, checksum errors, or deterministic-result mismatches.

Do not modify the original files in place. Copy the state set before investigation. The repository does not provide automatic repair, WAL truncation, or snapshot rollback commands.

## Metrics

When metrics are enabled, the engine's periodic line reports:

- accepted, rejected, and backpressured command counts;
- ingress and shard queue high-water marks;
- WAL batch, command, and byte totals;
- submit, WAL sync, and snapshot pause p99 latency.

The load generator's final line reports attempted and processed counts, business acceptance and rejection counts, infrastructure failures, processed throughput, sample count, submit p50/p95/p99/p99.9, average WAL batch size, and WAL sync p50/p95/p99.

Sustained queue growth indicates insufficient processing or storage capacity. Increased WAL sync latency should be investigated before increasing the group-commit delay.
