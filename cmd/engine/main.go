package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"petProjectMatchingEngine/internal/config"
	"petProjectMatchingEngine/internal/shard"
)

func main() {
	cfgPath := flag.String("config", "./configs/dev.json", "path to config")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
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

	fmt.Printf("engine=%s shards=%d symbols=%d\n", cfg.EngineID, cfg.ShardCount, len(cfg.Symbols))
	if cfg.EnableMetrics {
		reportEvery := time.Second
		if cfg.MetricsReportMillis > 0 {
			reportEvery = time.Duration(cfg.MetricsReportMillis) * time.Millisecond
		}
		go func() {
			ticker := time.NewTicker(reportEvery)
			defer ticker.Stop()
			for range ticker.C {
				stats, ok := engine.MetricsSnapshot()
				if !ok {
					continue
				}
				fmt.Printf("metrics accepted=%d rejected=%d backpressure=%d ingress_hwm=%d shard_hwm=%d wal_batches=%d wal_cmds=%d wal_bytes=%d submit_p99_ms=%.3f wal_sync_p99_ms=%.3f snapshot_p99_ms=%.3f\n",
					stats.Accepted,
					stats.Rejected,
					stats.Backpressure,
					stats.IngressQueueHWM,
					stats.ShardQueueHWM,
					stats.WALBatches,
					stats.WALBatchCommandsSum,
					stats.WALBatchBytesSum,
					stats.SubmitP99Ms,
					stats.WALSyncP99Ms,
					stats.SnapshotP99Ms,
				)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown engine: %v", err)
	}

	if stats, ok := engine.MetricsSnapshot(); ok {
		fmt.Printf("final metrics accepted=%d rejected=%d backpressure=%d submit_count=%d wal_sync_count=%d snapshots=%d\n",
			stats.Accepted,
			stats.Rejected,
			stats.Backpressure,
			stats.SubmitCount,
			stats.WALSyncCount,
			stats.SnapshotCount,
		)
	}

	fmt.Println("shutdown complete")
}
