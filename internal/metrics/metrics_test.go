package metrics

import (
	"testing"
	"time"
)

func TestCollectorReportsFirstSample(t *testing.T) {
	collector := NewCollector()
	collector.ObserveSubmitLatency(5 * time.Millisecond)

	stats := collector.Snapshot()
	if stats.SubmitCount != 1 {
		t.Fatalf("expected one sample, got %d", stats.SubmitCount)
	}
	if stats.SubmitP50Ms != 5 || stats.SubmitP95Ms != 5 || stats.SubmitP99Ms != 5 {
		t.Fatalf("unexpected percentiles: p50=%f p95=%f p99=%f", stats.SubmitP50Ms, stats.SubmitP95Ms, stats.SubmitP99Ms)
	}
}

func TestReservoirKeepsLatestSamples(t *testing.T) {
	var r reservoir
	for i := 1; i <= len(r.slots)+10; i++ {
		r.add(int64(i))
	}

	snapshot := r.snapshot()
	if snapshot.count != uint64(len(r.slots)+10) {
		t.Fatalf("unexpected count: %d", snapshot.count)
	}
	if len(snapshot.values) != len(r.slots) {
		t.Fatalf("unexpected reservoir size: %d", len(snapshot.values))
	}
	if snapshot.values[0] != 11 || snapshot.values[len(snapshot.values)-1] != int64(len(r.slots)+10) {
		t.Fatalf("reservoir does not contain latest window: first=%d last=%d", snapshot.values[0], snapshot.values[len(snapshot.values)-1])
	}
}
