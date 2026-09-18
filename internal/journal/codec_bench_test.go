package journal

import (
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func BenchmarkEncodeRecord(b *testing.B) {
	rec := sampleRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeRecord(rec); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func BenchmarkAppendEncodedRecord(b *testing.B) {
	rec := sampleRecord()
	buf := make([]byte, 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		var err error
		buf, err = appendEncodedRecord(buf, rec)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func BenchmarkDecodeRecord(b *testing.B) {
	rec := sampleRecord()
	data, err := EncodeRecord(rec)
	if err != nil {
		b.Fatalf("encode fixture: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeRecord(data); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func sampleRecord() Record {
	return Record{
		ShardID:    1,
		CommandSeq: 42,
		Command: model.Command{
			Seq:         42,
			Symbol:      "BTC-USD",
			Type:        model.CommandNew,
			OrderID:     1001,
			Side:        model.SideBuy,
			OrderType:   model.OrderTypeLimit,
			TimeInForce: model.TIFGTC,
			Price:       100,
			Quantity:    3,
		},
		Result: model.Result{
			CommandSeq: 42,
			Events: []model.Event{
				{Type: model.EventAccepted, OrderID: 1001},
				{Type: model.EventOrderRested, OrderID: 1001, Price: 100, LeavesQty: 3},
			},
		},
	}
}
