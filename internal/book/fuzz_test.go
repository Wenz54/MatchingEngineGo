package book

import (
	"fmt"
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func FuzzBookApplyInvariants(f *testing.F) {
	f.Add([]byte{1, 10, 10, 0, 1, 1, 0})
	f.Add([]byte{1, 11, 11, 1, 2, 2, 0, 2, 11, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		book := New("BTC-USD")
		nextSeq := uint64(1)

		for i := 0; i+6 < len(data); i += 7 {
			op := data[i] % 3
			orderID := uint64(data[i+1]) + 1
			price := int64(90 + int(data[i+2])%30)
			qty := int64(1 + int(data[i+3])%10)
			side := model.SideBuy
			if data[i+4]%2 == 0 {
				side = model.SideSell
			}
			tif := model.TIFGTC
			switch data[i+5] % 3 {
			case 1:
				tif = model.TIFIOC
			case 2:
				tif = model.TIFFOK
			}
			orderType := model.OrderTypeLimit
			if data[i+6]%5 == 0 {
				orderType = model.OrderTypeMarket
			}

			var cmd model.Command
			switch op {
			case 0:
				cmd = model.Command{
					Seq:         nextSeq,
					Symbol:      "BTC-USD",
					Type:        model.CommandNew,
					OrderID:     orderID,
					Side:        side,
					OrderType:   orderType,
					TimeInForce: tif,
					Price:       price,
					Quantity:    qty,
				}
			case 1:
				cmd = model.Command{Seq: nextSeq, Symbol: "BTC-USD", Type: model.CommandCancel, OrderID: orderID}
			case 2:
				cmd = model.Command{
					Seq:          nextSeq,
					Symbol:       "BTC-USD",
					Type:         model.CommandReplace,
					OrderID:      orderID,
					NewPrice:     price,
					NewLeavesQty: qty,
				}
			}
			nextSeq++
			book.Apply(cmd)
		}

		if err := book.CheckInvariants(); err != nil {
			t.Fatalf("invariants failed: %v, input=%s", err, fmt.Sprintf("%v", data))
		}
	})
}
