package shard

import (
	"fmt"
	"hash/fnv"
)

type SymbolRegistry struct {
	shardCount     int
	bySymbol       map[string]int
	symbolsByShard map[int][]string
}

func NewSymbolRegistry(symbols []string, shardCount int) (*SymbolRegistry, error) {
	if shardCount <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("symbols cannot be empty")
	}

	registry := &SymbolRegistry{
		shardCount:     shardCount,
		bySymbol:       make(map[string]int, len(symbols)),
		symbolsByShard: make(map[int][]string, shardCount),
	}

	for _, symbol := range symbols {
		if symbol == "" {
			return nil, fmt.Errorf("symbol cannot be empty")
		}
		if _, exists := registry.bySymbol[symbol]; exists {
			return nil, fmt.Errorf("duplicate symbol: %s", symbol)
		}
		shardID := stableShardID(symbol, shardCount)
		registry.bySymbol[symbol] = shardID
		registry.symbolsByShard[shardID] = append(registry.symbolsByShard[shardID], symbol)
	}

	return registry, nil
}

func (r *SymbolRegistry) ShardFor(symbol string) (int, bool) {
	shardID, ok := r.bySymbol[symbol]
	return shardID, ok
}

func (r *SymbolRegistry) ShardCount() int {
	return r.shardCount
}

func (r *SymbolRegistry) SymbolsForShard(shardID int) []string {
	symbols := r.symbolsByShard[shardID]
	out := make([]string, len(symbols))
	copy(out, symbols)
	return out
}

func stableShardID(symbol string, shardCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(symbol))
	return int(h.Sum32() % uint32(shardCount))
}
