package journal

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"petProjectMatchingEngine/internal/model"
)

func TestWriterReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       2,
		QueueSize:        64,
		MaxBatchCommands: 4,
		MaxBatchBytes:    4096,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	for i := 1; i <= 10; i++ {
		err := w.Append(context.Background(), mkRecord(uint64(i), uint64(i)))
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	records, err := ReadShardRecords(dir, 0)
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	if len(records) != 10 {
		t.Fatalf("expected 10 records, got %d", len(records))
	}
	if records[0].CommandSeq != 1 || records[9].CommandSeq != 10 {
		t.Fatalf("sequence mismatch")
	}
}

func TestReaderIgnoresTruncatedTailOnLastSegment(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        64,
		MaxBatchCommands: 1,
		MaxBatchBytes:    512,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if err := w.Append(context.Background(), mkRecord(uint64(i), uint64(i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "shard-00-*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("glob failed: %v", err)
	}
	sort.Strings(files)
	last := files[len(files)-1]

	data, err := os.ReadFile(last)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < 16 {
		t.Fatalf("segment unexpectedly small")
	}
	if err := os.WriteFile(last, data[:len(data)-8], 0o644); err != nil {
		t.Fatalf("truncate tail: %v", err)
	}

	records, err := ReadShardRecords(dir, 0)
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	if len(records) < 2 || len(records) > 3 {
		t.Fatalf("unexpected record count after tail trim: %d", len(records))
	}
}

func TestReaderFailsOnMidSegmentCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        64,
		MaxBatchCommands: 4,
		MaxBatchBytes:    4096,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	for i := 1; i <= 5; i++ {
		if err := w.Append(context.Background(), mkRecord(uint64(i), uint64(i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "shard-00-*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("glob failed: %v", err)
	}
	sort.Strings(files)
	path := files[0]

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if len(data) < 64 {
		t.Fatalf("segment too small for corruption test")
	}
	data[40] ^= 0x7F
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}

	if _, err := ReadShardRecords(dir, 0); err == nil {
		t.Fatalf("expected corruption error")
	}
}

func TestWriterRotatesSegments(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        64,
		MaxBatchCommands: 1,
		MaxBatchBytes:    512,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  256,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	for i := 1; i <= 12; i++ {
		if err := w.Append(context.Background(), mkRecord(uint64(i), uint64(i))); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "shard-00-*.wal"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("expected rotation to create multiple segments")
	}
}

func TestWriterRestartAppendsUsingNextSegmentIndex(t *testing.T) {
	dir := t.TempDir()

	first, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        64,
		MaxBatchCommands: 1,
		MaxBatchBytes:    512,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  256,
	})
	if err != nil {
		t.Fatalf("new writer first: %v", err)
	}
	for i := 1; i <= 4; i++ {
		if err := first.Append(context.Background(), mkRecord(uint64(i), uint64(i))); err != nil {
			t.Fatalf("first append %d: %v", i, err)
		}
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown first: %v", err)
	}

	second, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        64,
		MaxBatchCommands: 1,
		MaxBatchBytes:    512,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  256,
	})
	if err != nil {
		t.Fatalf("new writer second: %v", err)
	}
	for i := 5; i <= 8; i++ {
		if err := second.Append(context.Background(), mkRecord(uint64(i), uint64(i))); err != nil {
			t.Fatalf("second append %d: %v", i, err)
		}
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown second: %v", err)
	}

	records, err := ReadShardRecords(dir, 0)
	if err != nil {
		t.Fatalf("read records: %v", err)
	}
	if len(records) != 8 {
		t.Fatalf("expected 8 records after restart append, got %d", len(records))
	}
	for i, rec := range records {
		want := uint64(i + 1)
		if rec.CommandSeq != want {
			t.Fatalf("sequence mismatch at idx=%d got=%d want=%d", i, rec.CommandSeq, want)
		}
	}
}

func TestValidatedReaderRejectsSegmentMetadataMismatch(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(WriterConfig{
		Dir:              dir,
		EngineID:         "test",
		ShardID:          0,
		ShardCount:       1,
		QueueSize:        4,
		MaxBatchCommands: 1,
		MaxBatchBytes:    512,
		MaxBatchDelay:    time.Millisecond,
		SegmentMaxBytes:  1 << 20,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := w.Append(context.Background(), mkRecord(1, 1)); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if _, err := ReadShardRecordsForEngine(dir, "other", 0, 1); err == nil {
		t.Fatalf("expected engine id mismatch")
	}
	if _, err := ReadShardRecordsForEngine(dir, "test", 0, 2); err == nil {
		t.Fatalf("expected shard count mismatch")
	}
}

func TestReaderIgnoresTruncatedHeaderOnLastSegment(t *testing.T) {
	dir := t.TempDir()
	header, err := EncodeSegmentHeader(SegmentHeader{EngineID: "test", ShardID: 0, ShardCount: 1})
	if err != nil {
		t.Fatalf("encode header: %v", err)
	}
	path := filepath.Join(dir, "shard-00-000000.wal")
	if err := os.WriteFile(path, header[:5], 0o644); err != nil {
		t.Fatalf("write truncated header: %v", err)
	}

	records, err := ReadShardRecordsForEngine(dir, "test", 0, 1)
	if err != nil {
		t.Fatalf("read truncated final header: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no records, got %d", len(records))
	}
}

func mkRecord(seq, orderID uint64) Record {
	cmd := model.Command{
		Seq:          seq,
		Symbol:       "BTC-USD",
		Type:         model.CommandNew,
		OrderID:      orderID,
		Side:         model.SideBuy,
		OrderType:    model.OrderTypeLimit,
		TimeInForce:  model.TIFGTC,
		Price:        100,
		Quantity:     1,
		NewPrice:     0,
		NewLeavesQty: 0,
	}
	res := model.Result{
		CommandSeq: seq,
		Events: []model.Event{
			{Type: model.EventAccepted, OrderID: orderID},
			{Type: model.EventOrderRested, OrderID: orderID, Price: 100, LeavesQty: 1},
		},
	}
	return Record{ShardID: 0, CommandSeq: seq, Command: cmd, Result: res}
}
