#!/usr/bin/env bash
# stop-overnight.sh — kill what run-overnight.sh started.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDFILE="${ROOT}/data/run.pids"

if [[ -f "${PIDFILE}" ]]; then
  while IFS='=' read -r name pid; do
    if kill -0 "${pid}" 2>/dev/null; then
      echo "→ kill ${name} (${pid})"
      kill "${pid}" || true
    fi
  done < "${PIDFILE}"
  rm -f "${PIDFILE}"
else
  echo "no PID file at ${PIDFILE}; falling back to pkill"
  pkill -f 'binance-producer|polymarket-producer|spark-stream' || true
  pkill caffeinate || true
fi

echo "→ docker compose down"
docker compose -f "${ROOT}/infra/docker-compose.yml" down
echo "done."
