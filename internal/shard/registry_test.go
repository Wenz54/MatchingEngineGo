package shard

import "testing"

func TestSymbolRegistryDeterministicMapping(t *testing.T) {
	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"}

	r1, err := NewSymbolRegistry(symbols, 4)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	r2, err := NewSymbolRegistry(symbols, 4)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}

	for _, symbol := range symbols {
		a, okA := r1.ShardFor(symbol)
		b, okB := r2.ShardFor(symbol)
		if !okA || !okB {
			t.Fatalf("missing symbol: %s", symbol)
		}
		if a != b {
			t.Fatalf("non-deterministic mapping for %s: %d != %d", symbol, a, b)
		}
	}
}
