package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"petProjectMatchingEngine/internal/config"
	"petProjectMatchingEngine/internal/model"
	"petProjectMatchingEngine/internal/shard"
)

func main() {
	cfgPath := flag.String("config", "./configs/benchmark.json", "path to config")
	duration := flag.Duration("duration", 10*time.Second, "benchmark duration")
	producers := flag.Int("producers", 4, "number of producer goroutines")
	rate := flag.Int("rate", 4000, "offered commands/sec across all producers")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed")
	requireEmpty := flag.Bool("require-empty", false, "refuse to run when WAL or snapshot state already exists")
	flag.Parse()
	if *duration <= 0 || *producers <= 0 || *rate <= 0 {
		log.Fatalf("duration, producers, and rate must be positive")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	if *requireEmpty {
		if err := verifyEmptyState(cfg.WALDir, cfg.SnapshotDir); err != nil {
			log.Fatalf("verify empty state: %v", err)
		}
	}
	if err := os.MkdirAll(cfg.WALDir, 0o755); err != nil {
		log.Fatalf("create wal dir: %v", err)
	}
	if err := os.MkdirAll(cfg.SnapshotDir, 0o755); err != nil {
		log.Fatalf("create snapshot dir: %v", err)
	}

	engine, err := shard.NewEngine(cfg)
	if err != nil {
		log.Fatalf("init engine: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = engine.Shutdown(ctx)
	}()

	runCtx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	runStarted := time.Now()

	var attempted atomic.Uint64
	var processed atomic.Uint64
	var accepted atomic.Uint64
	var rejected atomic.Uint64
	var failed atomic.Uint64
	var nextOrderID atomic.Uint64
	latencyBatches := make(chan []int64, *producers)

	perProducerRate := *rate / maxInt(*producers, 1)
	if perProducerRate <= 0 {
		perProducerRate = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(*seed + int64(workerID)*7919))
			activeOrders := make([]orderRef, 0, 1024)
			latencies := make([]int64, 0, 4096)
			defer func() { latencyBatches <- latencies }()
			ticker := time.NewTicker(time.Second / time.Duration(perProducerRate))
			defer ticker.Stop()

			for {
				select {
				case <-runCtx.Done():
					return
				case <-ticker.C:
					cmd := nextCommand(rng, cfg.Symbols, &nextOrderID, &activeOrders)
					attempted.Add(1)
					start := time.Now()
					result, err := engine.Submit(context.Background(), cmd)
					if err != nil {
						failed.Add(1)
						continue
					}
					processed.Add(1)
					if resultRejected(result) {
						rejected.Add(1)
					} else {
						accepted.Add(1)
					}
					latencies = append(latencies, time.Since(start).Microseconds())
				}
			}
		}(i)
	}

	wg.Wait()
	close(latencyBatches)

	latencies := make([]int64, 0, int(processed.Load()))
	for batch := range latencyBatches {
		latencies = append(latencies, batch...)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	elapsed := durationToSeconds(time.Since(runStarted))

	stats, _ := engine.MetricsSnapshot()
	walAverageBatch := 0.0
	walAverageBytes := 0.0
	if stats.WALBatches > 0 {
		walAverageBatch = float64(stats.WALBatchCommandsSum) / float64(stats.WALBatches)
		walAverageBytes = float64(stats.WALBatchBytesSum) / float64(stats.WALBatches)
	}
	fmt.Printf("attempted=%d processed=%d accepted=%d rejected=%d failed=%d throughput=%.2f cmd/s samples=%d p50=%.3fms p95=%.3fms p99=%.3fms p99.9=%.3fms wal_batches=%d wal_avg_batch=%.2f wal_avg_bytes=%.0f wal_sync_p50=%.3fms wal_sync_p95=%.3fms wal_sync_p99=%.3fms\n",
		attempted.Load(),
		processed.Load(),
		accepted.Load(),
		rejected.Load(),
		failed.Load(),
		float64(processed.Load())/elapsed,
		len(latencies),
		percentileMillis(latencies, 0.50),
		percentileMillis(latencies, 0.95),
		percentileMillis(latencies, 0.99),
		percentileMillis(latencies, 0.999),
		stats.WALBatches,
		walAverageBatch,
		walAverageBytes,
		stats.WALSyncP50Ms,
		stats.WALSyncP95Ms,
		stats.WALSyncP99Ms,
	)
}

type orderRef struct {
	symbol  string
	orderID uint64
}

func nextCommand(rng *rand.Rand, symbols []string, nextOrderID *atomic.Uint64, activeOrders *[]orderRef) model.Command {
	roll := rng.Intn(100)
	price := int64(100 + rng.Intn(20))
	qty := int64(1 + rng.Intn(5))

	if roll < 65 || len(*activeOrders) == 0 {
		symbol := symbols[rng.Intn(len(symbols))]
		side := model.SideBuy
		if rng.Intn(2) == 0 {
			side = model.SideSell
		}
		orderID := nextOrderID.Add(1)
		*activeOrders = append(*activeOrders, orderRef{symbol: symbol, orderID: orderID})
		return model.Command{
			Symbol:      symbol,
			Type:        model.CommandNew,
			OrderID:     orderID,
			Side:        side,
			OrderType:   model.OrderTypeLimit,
			TimeInForce: model.TIFGTC,
			Price:       price,
			Quantity:    qty,
		}
	}

	if roll < 80 {
		side := model.SideBuy
		if rng.Intn(2) == 0 {
			side = model.SideSell
		}
		return model.Command{
			Symbol:      symbols[rng.Intn(len(symbols))],
			Type:        model.CommandNew,
			OrderID:     nextOrderID.Add(1),
			Side:        side,
			OrderType:   model.OrderTypeMarket,
			TimeInForce: model.TIFIOC,
			Quantity:    qty,
		}
	}

	idx := rng.Intn(len(*activeOrders))
	active := (*activeOrders)[idx]
	if roll < 90 {
		last := len(*activeOrders) - 1
		(*activeOrders)[idx] = (*activeOrders)[last]
		*activeOrders = (*activeOrders)[:last]
		return model.Command{Symbol: active.symbol, Type: model.CommandCancel, OrderID: active.orderID}
	}

	return model.Command{
		Symbol:       active.symbol,
		Type:         model.CommandReplace,
		OrderID:      active.orderID,
		NewPrice:     price,
		NewLeavesQty: qty,
	}
}

func resultRejected(result model.Result) bool {
	for _, event := range result.Events {
		if event.Type == model.EventRejected {
			return true
		}
	}
	return false
}

func percentileMillis(sorted []int64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx]) / 1000.0
}

func verifyEmptyState(dirs ...string) error {
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return fmt.Errorf("%s is not empty", dir)
		}
	}
	return nil
}

func durationToSeconds(d time.Duration) float64 {
	return float64(d) / float64(time.Second)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
