package book

import (
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func TestLimitMatchUsesMakerPriceAndFIFO(t *testing.T) {
	b := New("BTC-USD")

	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})

	res := b.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 10, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 8})

	trades := collectEvents(res, model.EventTrade)
	if len(trades) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(trades))
	}
	if trades[0].MakerOrderID != 1 || trades[1].MakerOrderID != 2 {
		t.Fatalf("expected FIFO makers 1->2, got %d->%d", trades[0].MakerOrderID, trades[1].MakerOrderID)
	}
	if trades[0].Price != 101 || trades[1].Price != 101 {
		t.Fatalf("expected maker price 101")
	}

	if err := b.CheckInvariants(); err != nil {
		t.Fatalf("invariant failed: %v", err)
	}
}

func TestIOCUnfilledRemainderCanceled(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 2})

	res := b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 10, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFIOC, Price: 101, Quantity: 5})

	cancels := collectEvents(res, model.EventCanceled)
	if len(cancels) != 1 {
		t.Fatalf("expected IOC remainder cancel")
	}
	if cancels[0].CanceledQty != 3 {
		t.Fatalf("expected canceled qty 3, got %d", cancels[0].CanceledQty)
	}
}

func TestFOKRejectedWithoutSideEffects(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 2})

	res := b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 10, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFFOK, Price: 101, Quantity: 3})

	rej := collectEvents(res, model.EventRejected)
	if len(rej) != 1 || rej[0].Reason != model.RejectWouldNotFullyFill {
		t.Fatalf("expected FOK reject")
	}

	if err := b.CheckInvariants(); err != nil {
		t.Fatalf("invariant failed: %v", err)
	}
}

func TestReplaceReduceKeepsPriority(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})

	res := b.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: 1, NewLeavesQty: 3})
	repl := collectEvents(res, model.EventReplaced)
	if len(repl) != 1 || !repl[0].PriorityRetained {
		t.Fatalf("expected replace retaining priority")
	}

	res = b.Apply(model.Command{Seq: 4, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 10, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 3})
	trades := collectEvents(res, model.EventTrade)
	if len(trades) == 0 || trades[0].MakerOrderID != 1 {
		t.Fatalf("expected first maker to remain order 1")
	}
}

func TestReplaceIncreaseLosesPriority(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})

	res := b.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: 1, NewLeavesQty: 6})
	repl := collectEvents(res, model.EventReplaced)
	if len(repl) != 1 || repl[0].PriorityRetained {
		t.Fatalf("expected replace to lose priority")
	}

	res = b.Apply(model.Command{Seq: 4, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 10, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 5})
	trades := collectEvents(res, model.EventTrade)
	if len(trades) == 0 || trades[0].MakerOrderID != 2 {
		t.Fatalf("expected maker to be order 2 after order 1 moved to tail")
	}
}

func TestReplacePriceChangeMatchesImmediately(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 105, Quantity: 3})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 3})

	res := b.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: 2, NewPrice: 105, NewLeavesQty: 3})
	trades := collectEvents(res, model.EventTrade)
	if len(trades) != 1 || trades[0].MakerOrderID != 1 || trades[0].TakerOrderID != 2 || trades[0].Quantity != 3 {
		t.Fatalf("unexpected replace trade: %+v", trades)
	}
	if err := b.CheckInvariants(); err != nil {
		t.Fatalf("invariant failed: %v", err)
	}
}

func TestMarketFOKFillsAcrossLevels(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 2})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 102, Quantity: 3})

	res := b.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 3, Side: model.SideBuy, OrderType: model.OrderTypeMarket, TimeInForce: model.TIFFOK, Quantity: 5})
	trades := collectEvents(res, model.EventTrade)
	if len(trades) != 2 || trades[0].Price != 101 || trades[1].Price != 102 {
		t.Fatalf("unexpected FOK trades: %+v", trades)
	}
	if len(collectEvents(res, model.EventCanceled)) != 0 {
		t.Fatalf("fully filled FOK must not be canceled")
	}
}

func TestSnapshotRoundTripPreservesFIFO(t *testing.T) {
	b := New("BTC-USD")
	b.Apply(model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 1})
	b.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 101, Quantity: 1})

	restored := NewFromSnapshot(b.Snapshot())
	res := restored.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 3, Side: model.SideBuy, OrderType: model.OrderTypeMarket, TimeInForce: model.TIFIOC, Quantity: 2})
	trades := collectEvents(res, model.EventTrade)
	if len(trades) != 2 || trades[0].MakerOrderID != 1 || trades[1].MakerOrderID != 2 {
		t.Fatalf("snapshot changed FIFO order: %+v", trades)
	}
}

func TestValidationReportsSpecificRejectReason(t *testing.T) {
	b := New("BTC-USD")
	tests := []struct {
		name   string
		cmd    model.Command
		reason model.RejectReason
	}{
		{name: "order id", cmd: model.Command{Seq: 1, Symbol: "BTC-USD", Type: model.CommandNew, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}, reason: model.RejectInvalidOrderID},
		{name: "side", cmd: model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}, reason: model.RejectInvalidSide},
		{name: "order type", cmd: model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}, reason: model.RejectInvalidOrderType},
		{name: "tif", cmd: model.Command{Seq: 4, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy, OrderType: model.OrderTypeLimit, Price: 100, Quantity: 1}, reason: model.RejectInvalidTimeInForce},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := b.Apply(tt.cmd)
			rejections := collectEvents(result, model.EventRejected)
			if len(rejections) != 1 || rejections[0].Reason != tt.reason {
				t.Fatalf("expected reason %d, got %+v", tt.reason, rejections)
			}
		})
	}
}

func collectEvents(result model.Result, eventType model.EventType) []model.Event {
	out := make([]model.Event, 0)
	for _, e := range result.Events {
		if e.Type == eventType {
			out = append(out, e)
		}
	}
	return out
}
