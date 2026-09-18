package journal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

func ReadShardRecords(dir string, shardID int) ([]Record, error) {
	return readShardRecords(dir, "", shardID, 0, false)
}

func ReadShardRecordsForEngine(dir, engineID string, shardID, shardCount int) ([]Record, error) {
	return readShardRecords(dir, engineID, shardID, shardCount, true)
}

func readShardRecords(dir, engineID string, shardID, shardCount int, validateMetadata bool) ([]Record, error) {
	pattern := filepath.Join(dir, fmt.Sprintf("shard-%02d-*.wal", shardID))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(files)

	out := make([]Record, 0, 1024)
	for idx, path := range files {
		isLast := idx == len(files)-1
		records, err := readSegment(path, engineID, uint16(shardID), uint16(shardCount), validateMetadata, isLast)
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
	}
	return out, nil
}

func readSegment(path, expectedEngineID string, expectedShardID, expectedShardCount uint16, validateMetadata, allowTruncatedTail bool) ([]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	header, offset, err := DecodeSegmentHeader(data)
	if err != nil {
		if allowTruncatedTail && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
			return nil, nil
		}
		return nil, fmt.Errorf("decode segment header %s: %w", path, err)
	}
	if header.ShardID != expectedShardID {
		return nil, fmt.Errorf("segment shard mismatch: got %d want %d", header.ShardID, expectedShardID)
	}
	if validateMetadata && header.EngineID != expectedEngineID {
		return nil, fmt.Errorf("segment engine id mismatch: got %q want %q", header.EngineID, expectedEngineID)
	}
	if validateMetadata && header.ShardCount != expectedShardCount {
		return nil, fmt.Errorf("segment shard count mismatch: got %d want %d", header.ShardCount, expectedShardCount)
	}

	records := make([]Record, 0, 256)
	for offset < len(data) {
		rec, consumed, err := DecodeRecord(data[offset:])
		if err != nil {
			if allowTruncatedTail && (err == io.ErrUnexpectedEOF || err == io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode record %s offset %d: %w", path, offset, err)
		}
		if rec.ShardID != expectedShardID {
			return nil, fmt.Errorf("record shard mismatch at %s offset %d", path, offset)
		}
		records = append(records, rec)
		offset += consumed
	}

	return records, nil
}
