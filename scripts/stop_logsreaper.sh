#!/usr/bin/env bash
# stop_logsreaper.sh — tear-down del container logs-reaper (idempotente).
set -euo pipefail
NAME="${LOGS_REAPER_NAME:-logs-reaper}"
if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "[stop_logsreaper] stopping & removing $NAME..."
    docker rm -f "$NAME" >/dev/null
    echo "[stop_logsreaper] done"
else
    echo "[stop_logsreaper] $NAME is not running"
fi
