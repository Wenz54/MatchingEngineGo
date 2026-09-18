package shard

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"petProjectMatchingEngine/internal/book"
	"petProjectMatchingEngine/internal/config"
	"petProjectMatchingEngine/internal/ingress"
	"petProjectMatchingEngine/internal/journal"
	"petProjectMatchingEngine/internal/metrics"
	"petProjectMatchingEngine/internal/model"
	"petProjectMatchingEngine/internal/recovery"
	"petProjectMatchingEngine/internal/snapshot"
)

type Engine struct {
	registry     *SymbolRegistry
	sequencer    *ingress.Sequencer
	shardInboxes []chan ingress.Routed
	shards       []*Loop
	engineID     string
	snapshotDir  string
	metricsSink  metrics.Sink
	collector    *metrics.Collector
}

func VerifyReplay(cfg config.Config) error {
	registry, err := NewSymbolRegistry(cfg.Symbols, cfg.ShardCount)
	if err != nil {
		return err
	}
	_, _, err = recoverBooks(cfg, registry)
	return err
}

func NewEngine(cfg config.Config) (*Engine, error) {
	registry, err := NewSymbolRegistry(cfg.Symbols, cfg.ShardCount)
	if err != nil {
		return nil, err
	}

	recoveredBooks, recoveredSeqByShard, err := recoverBooks(cfg, registry)
	if err != nil {
		return nil, err
	}
	initialSequence := nextSequence(recoveredSeqByShard)
	if initialSequence == 0 {
		return nil, ingress.ErrSequenceExhausted
	}

	engine := &Engine{
		registry:     registry,
		shardInboxes: make([]chan ingress.Routed, cfg.ShardCount),
		shards:       make([]*Loop, cfg.ShardCount),
		engineID:     cfg.EngineID,
		snapshotDir:  cfg.SnapshotDir,
	}

	sink := metrics.Sink(metrics.NoopSink{})
	if cfg.EnableMetrics {
		collector := metrics.NewCollector()
		engine.collector = collector
		sink = collector
	}
	engine.metricsSink = sink

	sequencerInboxes := make([]chan<- ingress.Routed, cfg.ShardCount)
	for shardID := 0; shardID < cfg.ShardCount; shardID++ {
		walWriter, err := journal.NewWriter(journal.WriterConfig{
			Dir:              cfg.WALDir,
			EngineID:         cfg.EngineID,
			ShardID:          shardID,
			ShardCount:       cfg.ShardCount,
			QueueSize:        cfg.ShardQueueSize,
			MaxBatchCommands: cfg.MaxBatchCommands,
			MaxBatchBytes:    cfg.MaxBatchBytes,
			MaxBatchDelay:    time.Duration(cfg.MaxBatchDelayMicros) * time.Microsecond,
			SegmentMaxBytes:  64 << 20,
			Metrics:          sink,
		})
		if err != nil {
			return nil, err
		}

		inbox := make(chan ingress.Routed, cfg.ShardQueueSize)
		loop := newLoop(shardID, registry.SymbolsForShard(shardID), inbox, walWriter, recoveredBooks, sink)
		loop.lastAppliedSeq = recoveredSeqByShard[shardID]
		loop.Start()

		engine.shardInboxes[shardID] = inbox
		engine.shards[shardID] = loop
		sequencerInboxes[shardID] = inbox
	}

	engine.sequencer = ingress.NewSequencer(cfg.IngressQueueSize, initialSequence, sequencerInboxes, registry, sink)
	return engine, nil
}

func (e *Engine) Submit(ctx context.Context, cmd model.Command) (model.Result, error) {
	start := time.Now()
	reply := make(chan model.Outcome, 1)
	if err := e.sequencer.SubmitBlocking(ctx, cmd, reply); err != nil {
		return model.Result{}, err
	}

	select {
	case outcome := <-reply:
		if outcome.Err != nil {
			return model.Result{}, outcome.Err
		}
		e.metricsSink.ObserveSubmitLatency(time.Since(start))
		return outcome.Result, nil
	case <-ctx.Done():
		return model.Result{}, ctx.Err()
	}
}

func (e *Engine) TrySubmit(cmd model.Command) (model.Result, error) {
	start := time.Now()
	reply := make(chan model.Outcome, 1)
	if err := e.sequencer.SubmitNonBlocking(cmd, reply); err != nil {
		return model.Result{}, err
	}

	outcome := <-reply
	if outcome.Err != nil {
		return model.Result{}, outcome.Err
	}
	e.metricsSink.ObserveSubmitLatency(time.Since(start))
	return outcome.Result, nil
}

func (e *Engine) Barrier(ctx context.Context) error {
	return e.sequencer.Barrier(ctx)
}

func (e *Engine) Shutdown(ctx context.Context) error {
	if err := e.sequencer.Shutdown(ctx); err != nil {
		return err
	}

	var firstErr error
	for _, loop := range e.shards {
		select {
		case <-loop.Done():
			if loop.failedErr != nil && firstErr == nil {
				firstErr = loop.failedErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return firstErr
}

func (e *Engine) ValidateSymbolRouting(symbol string) error {
	if _, ok := e.registry.ShardFor(symbol); !ok {
		return fmt.Errorf("symbol is not registered: %s", symbol)
	}
	return nil
}

func (e *Engine) CreateSnapshot(ctx context.Context) (string, error) {
	start := time.Now()
	release, err := e.sequencer.AcquireDrain(ctx)
	if err != nil {
		return "", err
	}
	defer release()

	books := make([]book.Snapshot, 0, 256)
	lastApplied := make([]uint64, len(e.shards))
	for shardID, loop := range e.shards {
		books = append(books, loop.snapshots()...)
		lastApplied[shardID] = loop.lastApplied()
	}

	sort.Slice(books, func(i, j int) bool {
		return books[i].Symbol < books[j].Symbol
	})

	path, err := snapshot.SaveAtomically(e.snapshotDir, snapshot.EngineSnapshot{
		EngineID:           e.engineID,
		ShardCount:         len(e.shards),
		LastAppliedByShard: lastApplied,
		Books:              books,
	})
	if err != nil {
		return "", err
	}
	e.metricsSink.ObserveSnapshotPause(time.Since(start))
	return path, nil
}

func (e *Engine) MetricsSnapshot() (metrics.Stats, bool) {
	if e.collector == nil {
		return metrics.Stats{}, false
	}
	return e.collector.Snapshot(), true
}

func recoverBooks(cfg config.Config, registry *SymbolRegistry) (map[string]book.Snapshot, []uint64, error) {
	restoredBySymbol := make(map[string]book.Snapshot, len(cfg.Symbols))
	lastAppliedByShard := make([]uint64, cfg.ShardCount)

	snap, _, err := snapshot.LoadLatest(cfg.SnapshotDir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, err
		}
	} else {
		if snap.EngineID != cfg.EngineID {
			return nil, nil, fmt.Errorf("snapshot engine id mismatch")
		}
		if snap.ShardCount != cfg.ShardCount {
			return nil, nil, fmt.Errorf("snapshot shard count mismatch")
		}
		if len(snap.LastAppliedByShard) != cfg.ShardCount {
			return nil, nil, fmt.Errorf("snapshot shard watermark length mismatch")
		}
		for i := range snap.LastAppliedByShard {
			lastAppliedByShard[i] = snap.LastAppliedByShard[i]
		}
		for _, bs := range snap.Books {
			if _, ok := registry.ShardFor(bs.Symbol); !ok {
				return nil, nil, fmt.Errorf("snapshot contains unknown symbol: %s", bs.Symbol)
			}
			restoredBySymbol[bs.Symbol] = bs
		}
	}

	books := make(map[string]*book.Book, len(cfg.Symbols))
	for _, sym := range cfg.Symbols {
		if bs, ok := restoredBySymbol[sym]; ok {
			books[sym] = book.NewFromSnapshot(bs)
		} else {
			books[sym] = book.New(sym)
		}
	}

	for shardID := 0; shardID < cfg.ShardCount; shardID++ {
		records, err := journal.ReadShardRecordsForEngine(cfg.WALDir, cfg.EngineID, shardID, cfg.ShardCount)
		if err != nil {
			return nil, nil, err
		}

		prevSeq := lastAppliedByShard[shardID]
		for _, rec := range records {
			if rec.CommandSeq <= lastAppliedByShard[shardID] {
				continue
			}
			if rec.CommandSeq <= prevSeq {
				return nil, nil, fmt.Errorf("non-monotonic sequence in shard %d: prev=%d cur=%d", shardID, prevSeq, rec.CommandSeq)
			}
			prevSeq = rec.CommandSeq

			bookForSymbol, ok := books[rec.Command.Symbol]
			if !ok {
				return nil, nil, fmt.Errorf("wal contains unknown symbol: %s", rec.Command.Symbol)
			}
			computed := bookForSymbol.Apply(rec.Command)
			if !recovery.EqualResults(computed, rec.Result) {
				return nil, nil, fmt.Errorf("replay mismatch on shard %d seq %d", shardID, rec.CommandSeq)
			}
			lastAppliedByShard[shardID] = rec.CommandSeq
		}
	}

	out := make(map[string]book.Snapshot, len(books))
	for sym, b := range books {
		out[sym] = b.Snapshot()
	}
	return out, lastAppliedByShard, nil
}

func nextSequence(lastAppliedByShard []uint64) uint64 {
	var maxSequence uint64
	for _, sequence := range lastAppliedByShard {
		if sequence > maxSequence {
			maxSequence = sequence
		}
	}
	return maxSequence + 1
}
