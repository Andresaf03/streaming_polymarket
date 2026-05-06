#!/usr/bin/env bash
# run-overnight.sh — start the full streaming stack and detach so it survives
# terminal close. Logs go to data/logs/. Pair with stop-overnight.sh.
#
# Prevents macOS sleep via `caffeinate -d` so the producers keep getting WS
# events through the night. Without it, the laptop sleeps and Kafka idles.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGDIR="${ROOT}/data/logs"
PIDFILE="${ROOT}/data/run.pids"
VENV="${ROOT}/.venv/bin"

mkdir -p "${LOGDIR}"
: > "${PIDFILE}"

echo "→ docker compose up"
docker compose -f "${ROOT}/infra/docker-compose.yml" up -d

echo "→ waiting for kafka healthy"
until docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "→ launching producers + spark + scorer"
nohup "${VENV}/binance-producer" \
  > "${LOGDIR}/binance.log" 2>&1 &
echo "binance=$!" >> "${PIDFILE}"

nohup "${VENV}/polymarket-producer" --asset btc --window 5 \
  > "${LOGDIR}/polymarket.log" 2>&1 &
echo "polymarket=$!" >> "${PIDFILE}"

nohup "${VENV}/spark-stream" --sigma 50 \
    --output-path "${ROOT}/data" \
    --checkpoint-path "${ROOT}/data/checkpoints/spark" \
  > "${LOGDIR}/spark.log" 2>&1 &
echo "spark=$!" >> "${PIDFILE}"

if [ -f "${ROOT}/data/model.sarimax.pkl" ]; then
  nohup "${VENV}/score-stream" \
    > "${LOGDIR}/scorer.log" 2>&1 &
  echo "scorer=$!" >> "${PIDFILE}"
  echo "  → score-stream started (model found)"
else
  echo "  → score-stream skipped (no model.sarimax.pkl — run train-model first)"
fi

echo "→ caffeinate (keep laptop awake)"
nohup caffeinate -d > /dev/null 2>&1 &
echo "caffeinate=$!" >> "${PIDFILE}"

echo
echo "Stack is running. Logs:"
echo "  tail -f ${LOGDIR}/{binance,polymarket,spark,scorer}.log"
echo "Stop with:"
echo "  ${ROOT}/scripts/stop-overnight.sh"
echo
echo "PIDs saved to ${PIDFILE}:"
cat "${PIDFILE}"
