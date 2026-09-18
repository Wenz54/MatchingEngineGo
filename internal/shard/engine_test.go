package shard

import (
	"context"
	"errors"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"petProjectMatchingEngine/internal/config"
	"petProjectMatchingEngine/internal/ingress"
	"petProjectMatchingEngine/internal/journal"
	"petProjectMatchingEngine/internal/model"
)

type orderSeq struct {
	orderID uint64
	seq     uint64
}

func TestConcurrentProducersPreservePerSymbolFIFO(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD"}
	cfg.ShardCount = 1
	cfg.IngressQueueSize = 1024
	cfg.ShardQueueSize = 1024

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	}()

	const totalOrders = 200
	results := make(chan orderSeq, totalOrders)

	var wg sync.WaitGroup
	for i := 0; i < totalOrders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := model.Command{
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     uint64(i + 1),
				Side:        model.SideSell,
				OrderType:   model.OrderTypeLimit,
				TimeInForce: model.TIFGTC,
				Price:       100,
				Quantity:    1,
			}

			res, err := engine.Submit(context.Background(), cmd)
			if err != nil {
				t.Errorf("submit failed: %v", err)
				return
			}
			results <- orderSeq{orderID: cmd.OrderID, seq: res.CommandSeq}
		}(i)
	}

	wg.Wait()
	close(results)

	ordered := make([]orderSeq, 0, totalOrders)
	for item := range results {
		ordered = append(ordered, item)
	}
	if len(ordered) != totalOrders {
		t.Fatalf("expected %d orders, got %d", totalOrders, len(ordered))
	}

	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].seq < ordered[j].seq
	})
	expectedMakerOrder := make([]uint64, 0, totalOrders)
	for _, item := range ordered {
		expectedMakerOrder = append(expectedMakerOrder, item.orderID)
	}

	sweep, err := engine.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     100000,
		Side:        model.SideBuy,
		OrderType:   model.OrderTypeMarket,
		TimeInForce: model.TIFIOC,
		Quantity:    int64(totalOrders),
	})
	if err != nil {
		t.Fatalf("sweep submit failed: %v", err)
	}

	trades := filterEvents(sweep.Events, model.EventTrade)
	if len(trades) != totalOrders {
		t.Fatalf("expected %d trades, got %d", totalOrders, len(trades))
	}

	for i, trade := range trades {
		if trade.MakerOrderID != expectedMakerOrder[i] {
			t.Fatalf("maker order mismatch at idx %d: got %d want %d", i, trade.MakerOrderID, expectedMakerOrder[i])
		}
	}
}

func TestEnginePipelinesWALGroupCommit(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD"}
	cfg.ShardCount = 1
	cfg.EnableMetrics = true
	cfg.MaxBatchCommands = 64
	cfg.MaxBatchDelayMicros = 20_000

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	}()

	const commands = 32
	var wg sync.WaitGroup
	for i := 0; i < commands; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := engine.Submit(context.Background(), model.Command{
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     uint64(i + 1),
				Side:        model.SideBuy,
				OrderType:   model.OrderTypeLimit,
				TimeInForce: model.TIFGTC,
				Price:       100,
				Quantity:    1,
			})
			if err != nil {
				t.Errorf("submit: %v", err)
			}
		}(i)
	}
	wg.Wait()

	stats, ok := engine.MetricsSnapshot()
	if !ok {
		t.Fatalf("metrics disabled")
	}
	if stats.WALBatchCommandsSum != commands {
		t.Fatalf("expected %d persisted commands, got %d", commands, stats.WALBatchCommandsSum)
	}
	if stats.WALBatches >= commands/2 {
		t.Fatalf("group commit ineffective: %d batches for %d commands", stats.WALBatches, commands)
	}
}

type flushDrivenWAL struct {
	acks []chan error
}

func (w *flushDrivenWAL) AppendAsync(context.Context, journal.Record) (<-chan error, error) {
	ack := make(chan error, 1)
	w.acks = append(w.acks, ack)
	return ack, nil
}

func (w *flushDrivenWAL) Flush(context.Context) error {
	for _, ack := range w.acks {
		ack <- nil
	}
	w.acks = w.acks[:0]
	return nil
}

func (w *flushDrivenWAL) Shutdown(context.Context) error { return nil }

type flushFailingWAL struct {
	flushDrivenWAL
	err error
}

func (w *flushFailingWAL) Flush(context.Context) error { return w.err }

func TestBarrierPropagatesFlushFailureToPendingCommands(t *testing.T) {
	inbox := make(chan ingress.Routed, 2)
	walErr := errors.New("sync failed")
	wal := &flushFailingWAL{err: walErr}
	loop := newLoop(0, []string{"BTC-USD"}, inbox, wal, nil, nil)
	loop.Start()

	reply := make(chan model.Outcome, 1)
	inbox <- ingress.Routed{
		Kind: ingress.RoutedCommand,
		Command: model.Command{
			Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy,
			OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1,
		},
		Reply: reply,
	}
	barrierDone := make(chan error, 1)
	inbox <- ingress.Routed{Kind: ingress.RoutedBarrier, BarrierDone: barrierDone}

	select {
	case err := <-barrierDone:
		if !errors.Is(err, walErr) {
			t.Fatalf("expected flush error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("barrier blocked waiting for WAL acknowledgements after flush failure")
	}
	select {
	case outcome := <-reply:
		if !errors.Is(outcome.Err, walErr) {
			t.Fatalf("expected pending command to fail, got %v", outcome.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending command was not failed after flush failure")
	}
	close(inbox)
	<-loop.Done()
}

func TestBarrierFlushesPendingCommandsBeforeWaitingForAcks(t *testing.T) {
	inbox := make(chan ingress.Routed, 2)
	wal := &flushDrivenWAL{}
	loop := newLoop(0, []string{"BTC-USD"}, inbox, wal, nil, nil)
	loop.Start()

	reply := make(chan model.Outcome, 1)
	inbox <- ingress.Routed{
		Kind: ingress.RoutedCommand,
		Command: model.Command{
			Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy,
			OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1,
		},
		Reply: reply,
	}
	barrierDone := make(chan error, 1)
	inbox <- ingress.Routed{Kind: ingress.RoutedBarrier, BarrierDone: barrierDone}

	if err := <-barrierDone; err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if outcome := <-reply; outcome.Err != nil {
		t.Fatalf("command: %v", outcome.Err)
	}
	close(inbox)
	<-loop.Done()
}

type failingWAL struct {
	appendCalls int
	err         error
}

func (w *failingWAL) AppendAsync(context.Context, journal.Record) (<-chan error, error) {
	w.appendCalls++
	return nil, w.err
}

func (w *failingWAL) Flush(context.Context) error    { return w.err }
func (w *failingWAL) Shutdown(context.Context) error { return nil }

func TestShardStopsApplyingAfterWALFailure(t *testing.T) {
	inbox := make(chan ingress.Routed, 4)
	walErr := errors.New("disk unavailable")
	wal := &failingWAL{err: walErr}
	loop := newLoop(0, []string{"BTC-USD"}, inbox, wal, nil, nil)
	loop.Start()

	command := model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}
	firstReply := make(chan model.Outcome, 1)
	inbox <- ingress.Routed{Kind: ingress.RoutedCommand, Command: command, Reply: firstReply}
	if outcome := <-firstReply; !errors.Is(outcome.Err, walErr) {
		t.Fatalf("expected WAL error, got %v", outcome.Err)
	}

	command.Seq = 2
	command.OrderID = 2
	secondReply := make(chan model.Outcome, 1)
	inbox <- ingress.Routed{Kind: ingress.RoutedCommand, Command: command, Reply: secondReply}
	if outcome := <-secondReply; !errors.Is(outcome.Err, walErr) {
		t.Fatalf("expected terminal WAL error, got %v", outcome.Err)
	}
	if wal.appendCalls != 1 {
		t.Fatalf("expected no append after terminal failure, got %d calls", wal.appendCalls)
	}

	drainAck := make(chan error, 1)
	release := make(chan struct{})
	inbox <- ingress.Routed{Kind: ingress.RoutedDrain, DrainAck: drainAck, DrainWait: release}
	if err := <-drainAck; !errors.Is(err, walErr) {
		t.Fatalf("expected drain to propagate WAL error, got %v", err)
	}
	close(release)
	close(inbox)
	<-loop.Done()
}

func baseTestConfig(t *testing.T) config.Config {
	t.Helper()
	baseDir := t.TempDir()

	return config.Config{
		EngineID:            "test-engine",
		Symbols:             []string{"BTC-USD", "ETH-USD"},
		ShardCount:          2,
		IngressQueueSize:    128,
		ShardQueueSize:      128,
		WALDir:              filepath.Join(baseDir, "wal"),
		SnapshotDir:         filepath.Join(baseDir, "snapshot"),
		MaxBatchCommands:    16,
		MaxBatchBytes:       1024,
		MaxBatchDelayMicros: 100,
	}
}

func filterEvents(events []model.Event, eventType model.EventType) []model.Event {
	out := make([]model.Event, 0, len(events))
	for _, e := range events {
		if e.Type == eventType {
			out = append(out, e)
		}
	}
	return out
}
