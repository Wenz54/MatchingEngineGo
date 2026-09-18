package shard

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"petProjectMatchingEngine/internal/model"
)

func TestSnapshotRecoveryReplaysWALTail(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD"}
	cfg.ShardCount = 1

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	_, err = engine.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     1,
		Side:        model.SideSell,
		OrderType:   model.OrderTypeLimit,
		TimeInForce: model.TIFGTC,
		Price:       101,
		Quantity:    7,
	})
	if err != nil {
		t.Fatalf("submit 1: %v", err)
	}

	if _, err := engine.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	_, err = engine.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     2,
		Side:        model.SideSell,
		OrderType:   model.OrderTypeLimit,
		TimeInForce: model.TIFGTC,
		Price:       101,
		Quantity:    4,
	})
	if err != nil {
		t.Fatalf("submit 2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	restarted, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = restarted.Shutdown(ctx)
	}()

	res, err := restarted.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     1000,
		Side:        model.SideBuy,
		OrderType:   model.OrderTypeMarket,
		TimeInForce: model.TIFIOC,
		Quantity:    11,
	})
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}

	trades := filterEvents(res.Events, model.EventTrade)
	if len(trades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(trades))
	}
	if trades[0].MakerOrderID != 1 || trades[0].Quantity != 7 {
		t.Fatalf("first maker mismatch: %+v", trades[0])
	}
	if trades[1].MakerOrderID != 2 || trades[1].Quantity != 4 {
		t.Fatalf("second maker mismatch: %+v", trades[1])
	}
	if res.CommandSeq != 3 {
		t.Fatalf("expected recovered sequence 3, got %d", res.CommandSeq)
	}

	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	defer secondCancel()
	if err := restarted.Shutdown(secondCtx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}

	restartedAgain, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("second restart: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = restartedAgain.Shutdown(ctx)
	}()

	cancelResult, err := restartedAgain.Submit(context.Background(), model.Command{
		Symbol:  "BTC-USD",
		Type:    model.CommandCancel,
		OrderID: 1,
	})
	if err != nil {
		t.Fatalf("cancel after second restart: %v", err)
	}
	if cancelResult.CommandSeq != 4 {
		t.Fatalf("expected recovered sequence 4, got %d", cancelResult.CommandSeq)
	}
	rejections := filterEvents(cancelResult.Events, model.EventRejected)
	if len(rejections) != 1 || rejections[0].Reason != model.RejectUnknownOrderID {
		t.Fatalf("expected replayed sweep to leave order absent, got %+v", cancelResult.Events)
	}
}

func TestReplayMismatchDetected(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD"}
	cfg.ShardCount = 1

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	_, err = engine.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     1,
		Side:        model.SideSell,
		OrderType:   model.OrderTypeLimit,
		TimeInForce: model.TIFGTC,
		Price:       101,
		Quantity:    2,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if _, err := engine.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	_, err = engine.Submit(context.Background(), model.Command{
		Symbol:      "BTC-USD",
		Type:        model.CommandNew,
		OrderID:     2,
		Side:        model.SideSell,
		OrderType:   model.OrderTypeLimit,
		TimeInForce: model.TIFGTC,
		Price:       102,
		Quantity:    3,
	})
	if err != nil {
		t.Fatalf("submit tail: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(cfg.WALDir, "shard-00-*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("glob wal: %v", err)
	}
	sort.Strings(files)
	last := files[len(files)-1]

	data, err := os.ReadFile(last)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(data) < 96 {
		t.Fatalf("wal too small for tamper test")
	}
	data[len(data)-16] ^= 0x01
	if err := os.WriteFile(last, data, 0o644); err != nil {
		t.Fatalf("tamper wal: %v", err)
	}

	if _, err := NewEngine(cfg); err == nil {
		t.Fatalf("expected replay verification failure")
	}
}

func TestRecoveryRejectsSnapshotEngineIDMismatch(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD"}
	cfg.ShardCount = 1

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if _, err := engine.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	badCfg := cfg
	badCfg.EngineID = "another-engine"
	if _, err := NewEngine(badCfg); err == nil {
		t.Fatalf("expected engine id mismatch error")
	}
}

func TestRecoveryRejectsSnapshotShardCountMismatch(t *testing.T) {
	cfg := baseTestConfig(t)
	cfg.Symbols = []string{"BTC-USD", "ETH-USD"}
	cfg.ShardCount = 2

	engine, err := NewEngine(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	if _, err := engine.CreateSnapshot(context.Background()); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	badCfg := cfg
	badCfg.ShardCount = 1
	badCfg.Symbols = []string{"BTC-USD"}
	if _, err := NewEngine(badCfg); err == nil {
		t.Fatalf("expected shard count mismatch error")
	}
}
