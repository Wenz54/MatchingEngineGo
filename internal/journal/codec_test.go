package journal

import (
	"reflect"
	"testing"

	"petProjectMatchingEngine/internal/model"
)

func TestAppendEncodedRecordPreservesPrefixAndEncoding(t *testing.T) {
	record := sampleRecord()
	encoded, err := EncodeRecord(record)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	prefix := []byte{1, 2, 3, 4}
	appended, err := appendEncodedRecord(append([]byte(nil), prefix...), record)
	if err != nil {
		t.Fatalf("append encode: %v", err)
	}
	if !reflect.DeepEqual(appended[:len(prefix)], prefix) {
		t.Fatalf("prefix changed: got %v want %v", appended[:len(prefix)], prefix)
	}
	if !reflect.DeepEqual(appended[len(prefix):], encoded) {
		t.Fatalf("appended encoding differs from standalone encoding")
	}
}

func TestRecordCodecRoundTrip(t *testing.T) {
	original := Record{
		ShardID:    1,
		CommandSeq: 42,
		Command: model.Command{
			Seq:          42,
			Symbol:       "BTC-USD",
			Type:         model.CommandNew,
			OrderID:      9001,
			Side:         model.SideBuy,
			OrderType:    model.OrderTypeLimit,
			TimeInForce:  model.TIFGTC,
			Price:        101,
			Quantity:     5,
			NewPrice:     0,
			NewLeavesQty: 0,
		},
		Result: model.Result{
			CommandSeq: 42,
			Events: []model.Event{
				{Type: model.EventAccepted, OrderID: 9001},
				{Type: model.EventOrderRested, OrderID: 9001, Price: 101, LeavesQty: 5},
			},
		},
	}

	encoded, err := EncodeRecord(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, consumed, err := DecodeRecord(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if consumed != len(encoded) {
		t.Fatalf("consumed bytes mismatch: got %d want %d", consumed, len(encoded))
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("decoded record mismatch:\ngot:  %+v\nwant: %+v", decoded, original)
	}
}
