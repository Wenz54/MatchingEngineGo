package snapshot

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"petProjectMatchingEngine/internal/book"
)

const (
	snapshotMagic   uint32 = 0x53504E31
	snapshotVersion uint16 = 1
)

type EngineSnapshot struct {
	EngineID           string
	ShardCount         int
	LastAppliedByShard []uint64
	Books              []book.Snapshot
}

type filePayload struct {
	Magic   uint32
	Version uint16
	Data    EngineSnapshot
	CRC32C  uint32
}

func SaveAtomically(dir string, s EngineSnapshot) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	name := fmt.Sprintf("snapshot-%020d.snap", maxSeq(s.LastAppliedByShard))
	tmp := filepath.Join(dir, name+".tmp")
	finalPath := filepath.Join(dir, name)

	payload := filePayload{Magic: snapshotMagic, Version: snapshotVersion, Data: s}
	crcBytes, err := encodeWithoutCRC(payload)
	if err != nil {
		return "", err
	}
	payload.CRC32C = crc32.Checksum(crcBytes, crc32.MakeTable(crc32.Castagnoli))

	allBytes, err := encodeWithCRC(payload)
	if err != nil {
		return "", err
	}

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(allBytes); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}

	if err := os.Rename(tmp, finalPath); err != nil {
		return "", err
	}
	if err := syncDir(dir); err != nil {
		return "", err
	}

	return finalPath, nil
}

func LoadLatest(dir string) (EngineSnapshot, string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "snapshot-*.snap"))
	if err != nil {
		return EngineSnapshot{}, "", err
	}
	if len(files) == 0 {
		return EngineSnapshot{}, "", os.ErrNotExist
	}
	sort.Strings(files)
	latest := files[len(files)-1]

	data, err := os.ReadFile(latest)
	if err != nil {
		return EngineSnapshot{}, "", err
	}

	payload, err := decodeWithCRC(data)
	if err != nil {
		return EngineSnapshot{}, "", err
	}
	if payload.Magic != snapshotMagic {
		return EngineSnapshot{}, "", fmt.Errorf("invalid snapshot magic")
	}
	if payload.Version != snapshotVersion {
		return EngineSnapshot{}, "", fmt.Errorf("unsupported snapshot version: %d", payload.Version)
	}

	crcBytes, err := encodeWithoutCRC(payload)
	if err != nil {
		return EngineSnapshot{}, "", err
	}
	want := crc32.Checksum(crcBytes, crc32.MakeTable(crc32.Castagnoli))
	if want != payload.CRC32C {
		return EngineSnapshot{}, "", fmt.Errorf("snapshot checksum mismatch")
	}

	return payload.Data, latest, nil
}

func maxSeq(seqs []uint64) uint64 {
	var m uint64
	for _, s := range seqs {
		if s > m {
			m = s
		}
	}
	return m
}

func encodeWithoutCRC(payload filePayload) ([]byte, error) {
	tmp := payload
	tmp.CRC32C = 0
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(tmp); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeWithCRC(payload filePayload) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeWithCRC(data []byte) (filePayload, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))
	var payload filePayload
	if err := dec.Decode(&payload); err != nil {
		return filePayload{}, err
	}
	return payload, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
