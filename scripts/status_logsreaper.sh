#!/usr/bin/env bash
# status_logsreaper.sh — diagnóstico rápido del container + endpoint stats.
set -euo pipefail
NAME="${LOGS_REAPER_NAME:-logs-reaper}"
PORT="${LOGS_REAPER_PORT:-9110}"

if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "$NAME: not running"
    exit 1
fi

echo "=== container ==="
docker ps --filter "name=$NAME" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

echo
echo "=== health endpoint ==="
if curl -sf -m 2 "http://localhost:${PORT}/api/health"; then echo; else echo " (no answer)"; fi

echo
echo "=== live stats (truncated) ==="
curl -s -m 2 "http://localhost:${PORT}/api/stats" | python3 -m json.tool 2>/dev/null | head -40 \
    || echo "  (stats endpoint not reachable)"

echo
echo "Dashboard: http://localhost:${PORT}/"
