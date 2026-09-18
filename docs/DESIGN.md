# Design

## Processing model

A submitted command follows this path:

1. The sequencer resolves the symbol to a stable shard or rejects an unknown symbol.
2. The sequencer assigns a global monotonic sequence number.
3. The command is enqueued on the selected shard.
4. The shard goroutine applies the command to the symbol's order book.
5. The command and deterministic result are encoded into the shard WAL.
6. The WAL writer groups pending records and calls `fsync`.
7. The shard returns the result after the corresponding WAL batch is durable.

Unknown symbols are rejected before sequence allocation. Commands for one symbol always reach the same shard and are applied by one goroutine.

## Matching

Each side of a book contains:

- a map from integer price to price level;
- a sorted slice of active prices;
- an intrusive doubly linked FIFO queue at each price level;
- a map from order ID to active order.

The best ask is the lowest ask price. The best bid is the highest bid price. Trades execute at the resting order's price.

Reducing quantity at the same price retains queue priority. Increasing quantity or changing price removes the order from its current queue and inserts the replacement at the tail of the resulting level.

FOK availability is checked before mutation. IOC and unfilled market remainders are canceled instead of resting.

## Ordering and concurrency

The global sequencer defines command order. It resolves the shard before assigning a sequence number, so an unknown symbol does not consume sequence space. Each shard owns its books and mutates them from one goroutine. This removes locks from the matching core and preserves FIFO behavior without concurrent book mutation.

Shards execute independently. Global sequence numbers may therefore become durable on different shards at different wall-clock times. Recovery uses per-shard watermarks and resumes allocation after the highest recovered sequence.

## Durability

Each shard has an append-only WAL writer. A record contains the command, its computed result, shard metadata, schema version, and CRC32C checksum.

The shard can keep multiple commands in flight. The writer encodes records directly into a reusable batch buffer. A batch is committed when a command threshold, byte threshold, or delay threshold is reached. All acknowledgements in that batch are released after a successful file `fsync`.

A WAL append or flush error places the shard in a terminal fail-stop state. Pending and subsequent commands receive the same failure. Snapshot drains propagate the failure and do not publish state derived from an unconfirmed WAL sequence.

WAL segments use exclusive creation and monotonically increasing indices. Segment headers bind data to an engine ID, shard ID, shard count, and schema version.

## Snapshots and recovery

Snapshot publication uses the following sequence:

1. Acquire a global drain that flushes and pauses all shards.
2. Serialize book state and per-shard sequence watermarks.
3. Write a temporary file.
4. Synchronize the file.
5. Rename it to the committed snapshot name.
6. Synchronize the containing directory.

If snapshots are present, startup loads the most recent snapshot file, validates engine and shard metadata, reconstructs the books, and replays later WAL records. It does not fall back to an older snapshot if the most recent file is invalid. Replay recomputes each command result and compares it with the stored result. A checksum error, metadata mismatch, unsupported version, or deterministic-result mismatch aborts startup.

## Technology choices

### Go

Go provides goroutines, channels, standard filesystem primitives, race detection, fuzzing, and low deployment overhead. The shard ownership model maps directly to goroutines and typed channels. The implementation uses the standard library only.

### Integer prices and quantities

Prices and quantities use signed 64-bit integers. Integer representation avoids floating-point comparison and rounding behavior in matching decisions. Scale selection is external to the engine.

### Single-writer shards

A single writer per shard removes synchronization from book mutation and makes command application deterministic. Parallelism is obtained by assigning different symbols to different shards. A hot symbol remains limited to one shard.

### Sorted price slices

Sorted slices keep best-price lookup direct and iteration cache-friendly for the expected prototype workload. Inserting or deleting a price level is linear in the number of active levels. A tree or skip list would provide different behavior for books with many frequently changing levels.

### Intrusive order queues

Orders contain their queue links and level pointer. Cancellation and replacement therefore remove a known order in constant time after the order-ID lookup. No separate list node allocation is required.

### Binary WAL and CRC32C

The WAL uses a fixed, versioned little-endian format. Direct encoding avoids reflection and permits encoding into the group-commit buffer without an intermediate allocation. CRC32C detects incomplete or corrupted records but does not provide cryptographic integrity.

### Group commit

Calling `fsync` for every command provides simple durability but limits throughput. Group commit amortizes one sync across multiple records. The configured delay controls how long the writer waits to form a batch, subject to scheduler timing; it does not relax durability. Results are still acknowledged only after sync completion.

### Per-shard WAL

Separate WAL writers allow shards to persist concurrently and avoid a shared file lock. Recovery must combine per-shard watermarks to resume the global sequence.

### Snapshots

Snapshots bound replay work and preserve FIFO queue order. The current snapshot payload uses Go `gob`, which is suitable for this prototype but is not a stable cross-language storage contract. WAL schema compatibility is explicit; snapshot evolution requires compatibility tests or a replacement codec.

### Bounded metrics reservoirs

Latency metrics use fixed-size reservoirs. Memory use does not grow with process lifetime. Reported percentiles represent the retained sample window rather than a complete historical distribution.

## Limits

- No network protocol or external API contract
- No authentication, balances, risk checks, or clearing
- No replication or automatic failover
- No WAL retention or compaction
- Snapshot creation has no CLI or scheduled trigger and is exposed through the in-process API
- Snapshot creation pauses command processing through a global drain
- WAL recovery reads complete segments into memory
- One hot symbol cannot use more than one shard
- Sorted price-level insertion and deletion are linear in the number of levels
