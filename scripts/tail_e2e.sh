#!/usr/bin/env bash
# Run an E2E suite with a side-car LogsReaper tail that streams anomalies vs.
# the current baseline. Usage:
#   ./tail_e2e.sh <service> <log_file> [scenario]
#
# The tail process exits when the log file stops growing for 5 idle ticks
# (tick = 1s). Anomalies are appended to ./anomalies.<service>.ndjson and
# echoed to the terminal as they appear.
set -euo pipefail

SERVICE="${1:?service required (e.g. accounts)}"
LOG_FILE="${2:?log file path required}"
SCENARIO="${3:-}"
LR_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT="anomalies.${SERVICE}.ndjson"
: >"$OUT"

echo "[tail] service=$SERVICE log=$LOG_FILE scenario=${SCENARIO:-auto}"
echo "[tail] anomalies streaming to $OUT"

# Background: print new lines from the anomalies file as they appear.
( tail -n0 -F "$OUT" 2>/dev/null | jq -c . 2>/dev/null ) &
TAIL_PID=$!
trap 'kill "$TAIL_PID" 2>/dev/null || true' EXIT

cd "$LR_DIR"
python3 -m logs_reaper tail \
    --input "$LOG_FILE" \
    --service "$SERVICE" \
    ${SCENARIO:+--scenario "$SCENARIO"} \
    --out "$OUT" \
    --tick 1.0 \
    --stop-on-eof 5
