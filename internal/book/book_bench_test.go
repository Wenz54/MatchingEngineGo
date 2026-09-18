package book

import (
	"fmt"
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func BenchmarkBookRestAndCancelSteadyState(b *testing.B) {
	book := New("BTC-USD")
	b.ReportAllocs()
	b.ReportMetric(2, "commands/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		orderID := uint64(i + 1)
		book.Apply(model.Command{Seq: uint64(i*2 + 1), Symbol: "BTC-USD", Type: model.CommandNew, OrderID: orderID, Side: model.SideBuy, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1})
		book.Apply(model.Command{Seq: uint64(i*2 + 2), Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: orderID})
	}
}

func BenchmarkBookRestAndMatchSteadyState(b *testing.B) {
	book := New("BTC-USD")
	b.ReportAllocs()
	b.ReportMetric(2, "commands/op")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		makerID := uint64(i*2 + 1)
		takerID := makerID + 1
		book.Apply(model.Command{Seq: makerID, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: makerID, Side: model.SideSell, OrderType: model.OrderTypeLimit, TimeInForce: model.TIFGTC, Price: 100, Quantity: 1})
		book.Apply(model.Command{Seq: takerID, Symbol: "BTC-USD", Type: model.CommandNew, OrderID: takerID, Side: model.SideBuy, OrderType: model.OrderTypeMarket, TimeInForce: model.TIFIOC, Quantity: 1})
	}
}

func BenchmarkBookAddLimitNonCrossing(b *testing.B) {
	book := New("BTC-USD")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cmd := model.Command{
			Seq:         uint64(i + 1),
			Symbol:      "BTC-USD",
			Type:        model.CommandNew,
			OrderID:     uint64(i + 1),
			Side:        model.SideBuy,
			OrderType:   model.OrderTypeLimit,
			TimeInForce: model.TIFGTC,
			Price:       100,
			Quantity:    1,
		}
		book.Apply(cmd)
	}
}

func BenchmarkBookSweepTenLevels(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		book := New("BTC-USD")
		for lvl := 0; lvl < 10; lvl++ {
			cmd := model.Command{
				Seq:         uint64(lvl + 1),
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     uint64(lvl + 1),
				Side:        model.SideSell,
				OrderType:   model.OrderTypeLimit,
				TimeInForce: model.TIFGTC,
				Price:       int64(100 + lvl),
				Quantity:    1,
			}
			book.Apply(cmd)
		}

		sweep := model.Command{
			Seq:         1000,
			Symbol:      "BTC-USD",
			Type:        model.CommandNew,
			OrderID:     1000,
			Side:        model.SideBuy,
			OrderType:   model.OrderTypeMarket,
			TimeInForce: model.TIFIOC,
			Quantity:    10,
		}
		book.Apply(sweep)
	}
}

func BenchmarkBookCancelHeadMiddleTail(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		book := New("BTC-USD")
		for id := 1; id <= 10; id++ {
			book.Apply(model.Command{
				Seq:         uint64(id),
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     uint64(id),
				Side:        model.SideBuy,
				OrderType:   model.OrderTypeLimit,
				TimeInForce: model.TIFGTC,
				Price:       100,
				Quantity:    1,
			})
		}

		book.Apply(model.Command{Seq: 100, Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: 1})
		book.Apply(model.Command{Seq: 101, Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: 5})
		book.Apply(model.Command{Seq: 102, Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: 10})
	}
}

func BenchmarkBookReplaceRetainAndLosePriority(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		book := New("BTC-USD")
		book.Apply(model.Command{
			Seq:         1,
			Symbol:      "BTC-USD",
			Type:        model.CommandNew,
			OrderID:     1,
			Side:        model.SideBuy,
			OrderType:   model.OrderTypeLimit,
			TimeInForce: model.TIFGTC,
			Price:       100,
			Quantity:    10,
		})

		book.Apply(model.Command{Seq: 2, Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: 1, NewLeavesQty: 8})
		book.Apply(model.Command{Seq: 3, Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: 1, NewPrice: 101, NewLeavesQty: 12})
	}
}

func BenchmarkBookMixedStream(b *testing.B) {
	book := New("BTC-USD")
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := uint64(i + 1)
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5:
			book.Apply(model.Command{
				Seq:         uint64(i + 1),
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     id,
				Side:        model.SideBuy,
				OrderType:   model.OrderTypeLimit,
				TimeInForce: model.TIFGTC,
				Price:       100 + int64(i%5),
				Quantity:    1,
			})
		case 6:
			book.Apply(model.Command{
				Seq:         uint64(i + 1),
				Symbol:      "BTC-USD",
				Type:        model.CommandNew,
				OrderID:     id,
				Side:        model.SideSell,
				OrderType:   model.OrderTypeMarket,
				TimeInForce: model.TIFIOC,
				Quantity:    1,
			})
		case 7, 8:
			book.Apply(model.Command{Seq: uint64(i + 1), Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: id / 2})
		default:
			book.Apply(model.Command{Seq: uint64(i + 1), Symbol: "BTC-USD", Type: model.CommandReplace, OrderID: id / 2, NewLeavesQty: 1, NewPrice: 100})
		}
	}

	if err := book.CheckInvariants(); err != nil {
		b.Fatalf("invariants: %v", err)
	}
}

func BenchmarkBookScenarioSweepLevels(b *testing.B) {
	for _, levels := range []int{2, 10, 100} {
		b.Run(fmt.Sprintf("levels_%d", levels), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				book := New("BTC-USD")
				for lvl := 0; lvl < levels; lvl++ {
					book.Apply(model.Command{
						Seq:         uint64(lvl + 1),
						Symbol:      "BTC-USD",
						Type:        model.CommandNew,
						OrderID:     uint64(lvl + 1),
						Side:        model.SideSell,
						OrderType:   model.OrderTypeLimit,
						TimeInForce: model.TIFGTC,
						Price:       int64(100 + lvl),
						Quantity:    1,
					})
				}
				book.Apply(model.Command{
					Seq:         uint64(levels + 1),
					Symbol:      "BTC-USD",
					Type:        model.CommandNew,
					OrderID:     999999,
					Side:        model.SideBuy,
					OrderType:   model.OrderTypeMarket,
					TimeInForce: model.TIFIOC,
					Quantity:    int64(levels),
				})
			}
		})
	}
}
