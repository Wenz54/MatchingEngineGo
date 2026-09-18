package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Sink interface {
	ObserveSubmitLatency(d time.Duration)
	IncAccepted()
	IncRejected()
	IncBackpressure()
	ObserveShardQueueDepth(depth int)
	ObserveIngressQueueDepth(depth int)
	ObserveWALBatch(commands int, bytes int)
	ObserveWALSyncLatency(d time.Duration)
	ObserveSnapshotPause(d time.Duration)
}

type NoopSink struct{}

func (NoopSink) ObserveSubmitLatency(time.Duration)  {}
func (NoopSink) IncAccepted()                        {}
func (NoopSink) IncRejected()                        {}
func (NoopSink) IncBackpressure()                    {}
func (NoopSink) ObserveShardQueueDepth(int)          {}
func (NoopSink) ObserveIngressQueueDepth(int)        {}
func (NoopSink) ObserveWALBatch(int, int)            {}
func (NoopSink) ObserveWALSyncLatency(time.Duration) {}
func (NoopSink) ObserveSnapshotPause(time.Duration)  {}

type Stats struct {
	Accepted uint64
	Rejected uint64

	Backpressure uint64

	SubmitCount uint64
	SubmitP50Ms float64
	SubmitP95Ms float64
	SubmitP99Ms float64

	IngressQueueHWM uint64
	ShardQueueHWM   uint64

	WALBatches          uint64
	WALBatchCommandsSum uint64
	WALBatchBytesSum    uint64
	WALSyncCount        uint64
	WALSyncP50Ms        float64
	WALSyncP95Ms        float64
	WALSyncP99Ms        float64

	SnapshotCount uint64
	SnapshotP50Ms float64
	SnapshotP95Ms float64
	SnapshotP99Ms float64
}

type Collector struct {
	accepted     atomic.Uint64
	rejected     atomic.Uint64
	backpressure atomic.Uint64

	ingressHWM atomic.Uint64
	shardHWM   atomic.Uint64

	walBatches          atomic.Uint64
	walBatchCommandsSum atomic.Uint64
	walBatchBytesSum    atomic.Uint64

	submits   reservoir
	walSync   reservoir
	snapshots reservoir
}

func NewCollector() *Collector {
	return &Collector{}
}

func (c *Collector) ObserveSubmitLatency(d time.Duration) {
	c.submits.add(durationToMicros(d))
}

func (c *Collector) IncAccepted() {
	c.accepted.Add(1)
}

func (c *Collector) IncRejected() {
	c.rejected.Add(1)
}

func (c *Collector) IncBackpressure() {
	c.backpressure.Add(1)
}

func (c *Collector) ObserveShardQueueDepth(depth int) {
	if depth < 0 {
		depth = 0
	}
	updateHWM(&c.shardHWM, uint64(depth))
}

func (c *Collector) ObserveIngressQueueDepth(depth int) {
	if depth < 0 {
		depth = 0
	}
	updateHWM(&c.ingressHWM, uint64(depth))
}

func (c *Collector) ObserveWALBatch(commands int, bytes int) {
	if commands < 0 {
		commands = 0
	}
	if bytes < 0 {
		bytes = 0
	}
	c.walBatches.Add(1)
	c.walBatchCommandsSum.Add(uint64(commands))
	c.walBatchBytesSum.Add(uint64(bytes))
}

func (c *Collector) ObserveWALSyncLatency(d time.Duration) {
	c.walSync.add(durationToMicros(d))
}

func (c *Collector) ObserveSnapshotPause(d time.Duration) {
	c.snapshots.add(durationToMicros(d))
}

func (c *Collector) Snapshot() Stats {
	submitSnapshot := c.submits.snapshot()
	walSyncSnapshot := c.walSync.snapshot()
	snapshotSnapshot := c.snapshots.snapshot()

	return Stats{
		Accepted:     c.accepted.Load(),
		Rejected:     c.rejected.Load(),
		Backpressure: c.backpressure.Load(),

		SubmitCount: submitSnapshot.count,
		SubmitP50Ms: microsToMillis(percentileMicros(submitSnapshot.values, 0.50)),
		SubmitP95Ms: microsToMillis(percentileMicros(submitSnapshot.values, 0.95)),
		SubmitP99Ms: microsToMillis(percentileMicros(submitSnapshot.values, 0.99)),

		IngressQueueHWM: c.ingressHWM.Load(),
		ShardQueueHWM:   c.shardHWM.Load(),

		WALBatches:          c.walBatches.Load(),
		WALBatchCommandsSum: c.walBatchCommandsSum.Load(),
		WALBatchBytesSum:    c.walBatchBytesSum.Load(),
		WALSyncCount:        walSyncSnapshot.count,
		WALSyncP50Ms:        microsToMillis(percentileMicros(walSyncSnapshot.values, 0.50)),
		WALSyncP95Ms:        microsToMillis(percentileMicros(walSyncSnapshot.values, 0.95)),
		WALSyncP99Ms:        microsToMillis(percentileMicros(walSyncSnapshot.values, 0.99)),

		SnapshotCount: snapshotSnapshot.count,
		SnapshotP50Ms: microsToMillis(percentileMicros(snapshotSnapshot.values, 0.50)),
		SnapshotP95Ms: microsToMillis(percentileMicros(snapshotSnapshot.values, 0.95)),
		SnapshotP99Ms: microsToMillis(percentileMicros(snapshotSnapshot.values, 0.99)),
	}
}

type reservoirSnapshot struct {
	count  uint64
	values []int64
}

type reservoir struct {
	mu    sync.Mutex
	count uint64
	next  uint64
	slots [2048]int64
}

func (r *reservoir) add(v int64) {
	r.mu.Lock()
	r.slots[r.next%uint64(len(r.slots))] = v
	r.next++
	r.count++
	r.mu.Unlock()
}

func (r *reservoir) snapshot() reservoirSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	limit := len(r.slots)
	if r.count < uint64(limit) {
		limit = int(r.count)
	}
	values := make([]int64, 0, limit)
	start := r.next - uint64(limit)
	for i := 0; i < limit; i++ {
		idx := (start + uint64(i)) % uint64(len(r.slots))
		values = append(values, r.slots[idx])
	}
	return reservoirSnapshot{count: r.count, values: values}
}

func durationToMicros(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return d.Microseconds()
}

func microsToMillis(us int64) float64 {
	return float64(us) / 1000.0
}

func percentileMicros(values []int64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	idx := int(float64(len(values)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func updateHWM(dst *atomic.Uint64, value uint64) {
	for {
		current := dst.Load()
		if value <= current {
			return
		}
		if dst.CompareAndSwap(current, value) {
			return
		}
	}
}
