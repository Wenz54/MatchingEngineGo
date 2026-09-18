package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"petProjectMatchingEngine/internal/book"
)

func TestSaveLoadLatestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	s1 := EngineSnapshot{EngineID: "e1", ShardCount: 2, LastAppliedByShard: []uint64{2, 3}, Books: []book.Snapshot{{Symbol: "BTC-USD", NextTradeSeq: 5}}}
	s2 := EngineSnapshot{EngineID: "e1", ShardCount: 2, LastAppliedByShard: []uint64{9, 8}, Books: []book.Snapshot{{Symbol: "ETH-USD", NextTradeSeq: 11}}}

	if _, err := SaveAtomically(dir, s1); err != nil {
		t.Fatalf("save 1: %v", err)
	}
	if _, err := SaveAtomically(dir, s2); err != nil {
		t.Fatalf("save 2: %v", err)
	}

	loaded, _, err := LoadLatest(dir)
	if err != nil {
		t.Fatalf("load latest: %v", err)
	}
	if loaded.EngineID != s2.EngineID || loaded.ShardCount != s2.ShardCount {
		t.Fatalf("header mismatch")
	}
	if len(loaded.LastAppliedByShard) != 2 || loaded.LastAppliedByShard[0] != 9 {
		t.Fatalf("watermark mismatch")
	}
	if len(loaded.Books) != 1 || loaded.Books[0].Symbol != "ETH-USD" {
		t.Fatalf("book payload mismatch")
	}
}

func TestLoadLatestChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	s := EngineSnapshot{EngineID: "e1", ShardCount: 1, LastAppliedByShard: []uint64{1}}
	path, err := SaveAtomically(dir, s)
	if err != nil {
		t.Fatalf("save: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	data[len(data)-1] ^= 0x5A
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	if _, _, err := LoadLatest(dir); err == nil {
		t.Fatalf("expected checksum error")
	}
}

func TestLoadLatestIgnoresTemporaryFiles(t *testing.T) {
	dir := t.TempDir()

	base := EngineSnapshot{EngineID: "e1", ShardCount: 1, LastAppliedByShard: []uint64{7}}
	if _, err := SaveAtomically(dir, base); err != nil {
		t.Fatalf("save baseline: %v", err)
	}

	tmpPayload := []byte("not-a-valid-snapshot")
	tmpPath := filepath.Join(dir, "snapshot-99999999999999999999.snap.tmp")
	if err := os.WriteFile(tmpPath, tmpPayload, 0o644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	loaded, _, err := LoadLatest(dir)
	if err != nil {
		t.Fatalf("load latest: %v", err)
	}
	if len(loaded.LastAppliedByShard) != 1 || loaded.LastAppliedByShard[0] != 7 {
		t.Fatalf("unexpected latest snapshot selected")
	}
}

func TestLoadLatestUnsupportedVersion(t *testing.T) {
	dir := t.TempDir()

	p := filePayload{
		Magic:   snapshotMagic,
		Version: snapshotVersion + 1,
		Data: EngineSnapshot{
			EngineID:           "e1",
			ShardCount:         1,
			LastAppliedByShard: []uint64{1},
		},
		CRC32C: 0,
	}
	bytes, err := encodeWithCRC(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	path := filepath.Join(dir, "snapshot-00000000000000000001.snap")
	if err := os.WriteFile(path, bytes, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, _, err := LoadLatest(dir); err == nil {
		t.Fatalf("expected unsupported version error")
	}
}
