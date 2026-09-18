package ingress

import (
	"context"
	"testing"

	"petProjectMatchingEngine/internal/model"
)

type stubRouter struct{}

func (stubRouter) ShardFor(symbol string) (int, bool) {
	if symbol == "BTC-USD" {
		return 0, true
	}
	return 0, false
}

func (stubRouter) ShardCount() int { return 1 }

func TestSubmitInvalidSymbolRejectedBySequencer(t *testing.T) {
	inbox := make(chan Routed, 1)
	seq := NewSequencer(1, 1, []chan<- Routed{inbox}, stubRouter{}, nil)
	defer func() {
		_ = seq.Shutdown(context.Background())
	}()

	reply1 := make(chan model.Outcome, 1)
	if err := seq.SubmitNonBlocking(model.Command{Symbol: "UNKNOWN", Type: model.CommandNew, OrderID: 1, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}, reply1); err != nil {
		t.Fatalf("unexpected enqueue error: %v", err)
	}

	outcome := <-reply1
	if outcome.Err != nil {
		t.Fatalf("unexpected outcome error: %v", outcome.Err)
	}
	if len(outcome.Result.Events) != 1 || outcome.Result.Events[0].Type != model.EventRejected {
		t.Fatalf("expected single rejected event")
	}
	if outcome.Result.Events[0].Reason != model.RejectInvalidSymbol {
		t.Fatalf("expected invalid symbol rejection")
	}
	if outcome.Result.CommandSeq != 0 {
		t.Fatalf("unroutable command must not consume a durable sequence")
	}

	reply2 := make(chan model.Outcome, 1)
	if err := seq.SubmitNonBlocking(model.Command{Symbol: "BTC-USD", Type: model.CommandNew, OrderID: 2, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1}, reply2); err != nil {
		t.Fatalf("unexpected valid enqueue error: %v", err)
	}
	routed := <-inbox
	if routed.Command.Seq != 1 {
		t.Fatalf("expected first routable sequence to be 1, got %d", routed.Command.Seq)
	}
}
