#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-./configs/dev.json}"
DURATION="${DURATION:-8s}"
PRODUCERS="${PRODUCERS:-4}"
RATE="${RATE:-4000}"

cd "$ROOT_DIR"

echo "Cleaning previous state"
rm -rf ./data/wal ./data/snapshots
mkdir -p ./data/wal ./data/snapshots

echo "Running load demo"
go run ./cmd/loadgen -config "$CONFIG_PATH" -duration "$DURATION" -producers "$PRODUCERS" -rate "$RATE"

echo "Creating and validating replay state"
go run ./cmd/replay -config "$CONFIG_PATH"

echo "Running crash matrix"
bash ./scripts/crash_matrix.sh

echo "Operational demo completed successfully."
