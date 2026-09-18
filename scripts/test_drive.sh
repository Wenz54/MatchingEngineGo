#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROFILE="${1:-standard}"
CONFIG_PATH="${2:-./configs/dev.json}"
PROFILE_FILE="${PROFILE_FILE:-./configs/test_profiles.env}"

cd "$ROOT_DIR"

if [[ ! -f "$PROFILE_FILE" ]]; then
  echo "profile file not found: $PROFILE_FILE"
  exit 1
fi

source "$PROFILE_FILE"

upper_profile="$(echo "$PROFILE" | tr '[:lower:]' '[:upper:]')"

pick() {
  local key="$1"
  local var_name="${upper_profile}_${key}"
  local value="${!var_name:-}"
  if [[ -z "$value" ]]; then
    echo "unknown profile or missing variable: $var_name"
    exit 1
  fi
  echo "$value"
}

PROFILE_DURATION="$(pick DURATION)"
PROFILE_PRODUCERS="$(pick PRODUCERS)"
PROFILE_RATE="$(pick RATE)"
PROFILE_RUN_RACE="$(pick RUN_RACE)"
PROFILE_RUN_CRASH_MATRIX="$(pick RUN_CRASH_MATRIX)"
PROFILE_CLEAN_STATE="$(pick CLEAN_STATE)"

DURATION="${DURATION:-$PROFILE_DURATION}"
PRODUCERS="${PRODUCERS:-$PROFILE_PRODUCERS}"
RATE="${RATE:-$PROFILE_RATE}"
RUN_RACE="${RUN_RACE:-$PROFILE_RUN_RACE}"
RUN_CRASH_MATRIX="${RUN_CRASH_MATRIX:-$PROFILE_RUN_CRASH_MATRIX}"
CLEAN_STATE="${CLEAN_STATE:-$PROFILE_CLEAN_STATE}"

extract_json_string() {
  local file_path="$1"
  local key="$2"
  local value
  value="$(sed -n "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" "$file_path" | head -n1)"
  if [[ -z "$value" ]]; then
    echo "failed to read '$key' from $file_path"
    exit 1
  fi
  echo "$value"
}

echo "Profile: $PROFILE"
echo "Config: $CONFIG_PATH"
echo "Load settings: DURATION=$DURATION PRODUCERS=$PRODUCERS RATE=$RATE"

if [[ "$CLEAN_STATE" == "1" ]]; then
  WAL_DIR="$(extract_json_string "$CONFIG_PATH" "wal_dir")"
  SNAPSHOT_DIR="$(extract_json_string "$CONFIG_PATH" "snapshot_dir")"
  echo "Cleaning state dirs: $WAL_DIR $SNAPSHOT_DIR"
  rm -rf "$WAL_DIR" "$SNAPSHOT_DIR"
  mkdir -p "$WAL_DIR" "$SNAPSHOT_DIR"
fi

echo "[1/4] Unit/integration tests"
go test ./... -count=1

if [[ "$RUN_RACE" == "1" ]]; then
  echo "[2/4] Race detector"
  go test -race ./... -count=1
else
  echo "[2/4] Race detector skipped (RUN_RACE=$RUN_RACE)"
fi

echo "[3/4] Load + replay"
go run ./cmd/loadgen -config "$CONFIG_PATH" -duration "$DURATION" -producers "$PRODUCERS" -rate "$RATE"
go run ./cmd/replay -config "$CONFIG_PATH"

if [[ "$RUN_CRASH_MATRIX" == "1" ]]; then
  echo "[4/4] Crash matrix"
  bash ./scripts/crash_matrix.sh
else
  echo "[4/4] Crash matrix skipped (RUN_CRASH_MATRIX=$RUN_CRASH_MATRIX)"
fi

echo "Test drive completed."
