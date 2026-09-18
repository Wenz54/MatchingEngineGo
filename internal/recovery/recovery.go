package recovery

import (
	"fmt"

	"petProjectMatchingEngine/internal/book"
	"petProjectMatchingEngine/internal/journal"
	"petProjectMatchingEngine/internal/model"
	"petProjectMatchingEngine/internal/snapshot"
)

type ReplayedBook interface {
	Apply(model.Command) model.Result
}

type BookFactory func(snapshot book.Snapshot) ReplayedBook

type RecoverResult struct {
	SnapshotPath   string
	LastSeqByShard []uint64
}

func RecoverFromSnapshotAndWAL(
	snapshotDir string,
	walDir string,
	engineID string,
	shardCount int,
	factory BookFactory,
) (RecoverResult, error) {
	snap, path, err := snapshot.LoadLatest(snapshotDir)
	if err != nil {
		return RecoverResult{}, err
	}
	if snap.EngineID != engineID {
		return RecoverResult{}, fmt.Errorf("engine id mismatch")
	}
	if snap.ShardCount != shardCount {
		return RecoverResult{}, fmt.Errorf("shard count mismatch")
	}

	books := make(map[string]ReplayedBook, len(snap.Books))
	for _, b := range snap.Books {
		books[b.Symbol] = factory(b)
	}

	for shardID := 0; shardID < shardCount; shardID++ {
		records, err := journal.ReadShardRecordsForEngine(walDir, engineID, shardID, shardCount)
		if err != nil {
			return RecoverResult{}, err
		}

		lastSeq := snap.LastAppliedByShard[shardID]
		for _, rec := range records {
			if rec.CommandSeq <= lastSeq {
				continue
			}
			bookForSymbol, ok := books[rec.Command.Symbol]
			if !ok {
				bookForSymbol = factory(book.Snapshot{Symbol: rec.Command.Symbol})
				books[rec.Command.Symbol] = bookForSymbol
			}
			computed := bookForSymbol.Apply(rec.Command)
			if !EqualResults(computed, rec.Result) {
				return RecoverResult{}, fmt.Errorf("replay mismatch on seq %d", rec.CommandSeq)
			}
			if rec.CommandSeq > lastSeq {
				lastSeq = rec.CommandSeq
			}
		}
		snap.LastAppliedByShard[shardID] = lastSeq
	}

	return RecoverResult{SnapshotPath: path, LastSeqByShard: snap.LastAppliedByShard}, nil
}

func EqualResults(a, b model.Result) bool {
	if a.CommandSeq != b.CommandSeq {
		return false
	}
	if len(a.Events) != len(b.Events) {
		return false
	}
	for i := range a.Events {
		ae, be := a.Events[i], b.Events[i]
		if ae.Type != be.Type || ae.Reason != be.Reason || ae.OrderID != be.OrderID || ae.MakerOrderID != be.MakerOrderID || ae.TakerOrderID != be.TakerOrderID || ae.TradeID != be.TradeID || ae.Price != be.Price || ae.Quantity != be.Quantity || ae.LeavesQty != be.LeavesQty || ae.CanceledQty != be.CanceledQty || ae.CancelReason != be.CancelReason || ae.PriorityRetained != be.PriorityRetained {
			return false
		}
	}
	return true
}
