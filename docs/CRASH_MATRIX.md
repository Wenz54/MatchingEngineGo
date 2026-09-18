# Crash and Recovery Matrix

Run the targeted suite:

```bash
make crash-matrix
```

The runner executes WAL, snapshot, and recovery tests without running the complete repository test suite.

## WAL

| Test | Fault | Expected result |
|---|---|---|
| `TestReaderIgnoresTruncatedTailOnLastSegment` | Incomplete final record | Valid preceding records are returned; the incomplete tail is ignored |
| `TestReaderIgnoresTruncatedHeaderOnLastSegment` | Incomplete final segment header | The incomplete final segment is ignored |
| `TestReaderFailsOnMidSegmentCorruption` | Modified bytes in a committed segment | Checksum validation fails |
| `TestWriterRestartAppendsUsingNextSegmentIndex` | Writer restart after existing segments | A new segment index is used; existing files are not overwritten |
| `TestValidatedReaderRejectsSegmentMetadataMismatch` | Engine ID or shard count mismatch | Recovery fails before replay |

## Snapshot

| Test | Fault | Expected result |
|---|---|---|
| `TestLoadLatestChecksumMismatch` | Modified snapshot payload | Snapshot loading fails checksum validation |
| `TestLoadLatestIgnoresTemporaryFiles` | Stale temporary snapshot | Temporary files are ignored |
| `TestLoadLatestUnsupportedVersion` | Unsupported snapshot version | Snapshot loading fails with a version error |

## Recovery

| Test | Fault | Expected result |
|---|---|---|
| `TestSnapshotRecoveryReplaysWALTail` | Commands persisted after a snapshot | Snapshot state and WAL tail are restored; sequence allocation continues |
| `TestReplayMismatchDetected` | Stored WAL result or payload modified | Recomputed and stored results differ; startup fails |
| `TestRecoveryRejectsSnapshotEngineIDMismatch` | Snapshot belongs to another engine | Startup fails |
| `TestRecoveryRejectsSnapshotShardCountMismatch` | Snapshot topology differs from configuration | Startup fails |

## Additional failure tests

The full test suite also covers WAL append and flush failures, barrier propagation, terminal shard behavior, and segment rotation. `TestSnapshotRecoveryReplaysWALTail` includes a second restart and verifies sequence continuity.

```bash
make test
make race
```
