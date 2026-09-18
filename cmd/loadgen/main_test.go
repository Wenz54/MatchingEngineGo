package main

import (
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func TestResultRejected(t *testing.T) {
	if resultRejected(model.Result{Events: []model.Event{{Type: model.EventAccepted}}}) {
		t.Fatalf("accepted result classified as rejected")
	}
	if !resultRejected(model.Result{Events: []model.Event{{Type: model.EventRejected}}}) {
		t.Fatalf("rejected result not classified as rejected")
	}
}

func TestVerifyEmptyState(t *testing.T) {
	empty := t.TempDir()
	if err := verifyEmptyState(empty, filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Fatalf("expected empty state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(empty, "state.wal"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if err := verifyEmptyState(empty); err == nil {
		t.Fatalf("expected non-empty state error")
	}
}

func TestGeneratedCancelAndReplaceReferenceIssuedOrders(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var nextOrderID atomic.Uint64
	active := make([]orderRef, 0)
	issued := make(map[uint64]string)
	var cancels, replaces int

	for i := 0; i < 1000; i++ {
		cmd := nextCommand(rng, []string{"BTC-USD", "ETH-USD"}, &nextOrderID, &active)
		switch cmd.Type {
		case model.CommandNew:
			issued[cmd.OrderID] = cmd.Symbol
		case model.CommandCancel:
			cancels++
			if issued[cmd.OrderID] != cmd.Symbol {
				t.Fatalf("cancel references unknown order %d for %s", cmd.OrderID, cmd.Symbol)
			}
		case model.CommandReplace:
			replaces++
			if issued[cmd.OrderID] != cmd.Symbol {
				t.Fatalf("replace references unknown order %d for %s", cmd.OrderID, cmd.Symbol)
			}
		}
	}
	if cancels == 0 || replaces == 0 {
		t.Fatalf("expected cancel and replace commands, got cancels=%d replaces=%d", cancels, replaces)
	}
}
