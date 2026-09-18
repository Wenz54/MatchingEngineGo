#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

echo "[1/3] WAL crash cases"
go test ./internal/journal -run 'TestReaderIgnoresTruncatedTailOnLastSegment|TestReaderIgnoresTruncatedHeaderOnLastSegment|TestReaderFailsOnMidSegmentCorruption|TestWriterRestartAppendsUsingNextSegmentIndex|TestValidatedReaderRejectsSegmentMetadataMismatch' -count=1

echo "[2/3] Snapshot crash cases"
go test ./internal/snapshot -run 'TestLoadLatestChecksumMismatch|TestLoadLatestIgnoresTemporaryFiles|TestLoadLatestUnsupportedVersion' -count=1

echo "[3/3] Recovery crash cases"
go test ./internal/shard -run 'TestSnapshotRecoveryReplaysWALTail|TestReplayMismatchDetected|TestRecoveryRejectsSnapshotEngineIDMismatch|TestRecoveryRejectsSnapshotShardCountMismatch' -count=1

echo "Crash matrix passed."
