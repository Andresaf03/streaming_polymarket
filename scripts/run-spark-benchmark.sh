#!/usr/bin/env bash
# run-spark-benchmark.sh — automated CPU vs GPU Spark streaming benchmark.
#
# Lifecycle:
#   1. docker compose up (Kafka + Grafana)
#   2. Start Binance + Polymarket producers
#   3. CPU run: spark-stream (no RAPIDS) for --warmup + --duration minutes
#      Writes per-batch progress to JSONL during measurement window only.
#   4. GPU run: spark-stream --rapids for the same duration
#   5. Compare results: CSV + markdown table saved to results/spark_bench_<ts>/
#   6. docker compose down (unless --no-cleanup)
#
# Usage:
#   bash scripts/run-spark-benchmark.sh
#   bash scripts/run-spark-benchmark.sh --duration 10 --warmup 5
#   bash scripts/run-spark-benchmark.sh --no-cleanup   # keep docker up after
#   bash scripts/run-spark-benchmark.sh --rapids-jar /path/to/rapids.jar
#
# Total runtime (defaults): ~28 min
#   Docker/Kafka startup      ~2 min
#   CPU run  (3 warm + 5 meas + ~3 Spark boot) ~11 min
#   GPU run  (3 warm + 5 meas + ~5 Spark boot) ~13 min
#   Cleanup + report          ~1 min
#
# Requires: WSL2 Ubuntu, docker, curl, nvidia-smi, project .venv installed.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV="${ROOT}/.venv-bench/bin"
LOGDIR="${ROOT}/data/logs"
TS=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="${ROOT}/results/spark_bench_${TS}"

DURATION_MIN=5
WARMUP_MIN=3
DO_CLEANUP=1
RAPIDS_JAR_ARG=""

for arg in "$@"; do
  case "$arg" in
    --duration=*)  DURATION_MIN="${arg#*=}" ;;
    --warmup=*)    WARMUP_MIN="${arg#*=}" ;;
    --no-cleanup)  DO_CLEANUP=0 ;;
    --rapids-jar=*) RAPIDS_JAR_ARG="--rapids-jar=${arg#*=}" ;;
    -h|--help)
      sed -n '2,28p' "$0" | sed 's/^# //; s/^#//'
      exit 0 ;;
  esac
done

DURATION_S=$((DURATION_MIN * 60))
WARMUP_S=$((WARMUP_MIN * 60))

mkdir -p "$RESULTS_DIR" "$LOGDIR"

PIDFILE="${ROOT}/data/bench.pids"
: > "$PIDFILE"

# ---- helpers ----------------------------------------------------------------

log() { echo "[$(date +%H:%M:%S)] $*"; }

cleanup() {
  log "→ cleaning up processes"
  if [[ -f "$PIDFILE" ]]; then
    while IFS='=' read -r name pid; do
      if kill -0 "$pid" 2>/dev/null; then
        log "  kill $name ($pid)"
        kill "$pid" 2>/dev/null || true
      fi
    done < "$PIDFILE"
    rm -f "$PIDFILE"
  fi
  if [[ "$DO_CLEANUP" -eq 1 ]]; then
    log "→ docker compose down"
    docker compose -f "${ROOT}/infra/docker-compose.yml" down || true
  fi
}
trap cleanup EXIT

wait_for_kafka() {
  log "→ waiting for Kafka..."
  local elapsed=0
  while ! docker exec kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
    sleep 5; elapsed=$((elapsed + 5))
    [[ $elapsed -ge 120 ]] && { log "ERROR: Kafka not ready after 120s"; exit 1; }
  done
  log "  Kafka ready"
}

wait_for_spark_ui() {
  log "→ waiting for Spark UI at localhost:4040 (can take up to 10 min on first run)..."
  local elapsed=0
  while ! curl -sf http://localhost:4040/api/v1/applications >/dev/null 2>&1; do
    sleep 10; elapsed=$((elapsed + 10))
    log "  still waiting... ${elapsed}s"
  done
  log "  Spark UI ready (took ${elapsed}s)"
}

clean_spark_state() {
  if [ -d "${ROOT}/data/ticks/_spark_metadata" ]; then
    local stamp; stamp=$(date +%Y%m%d_%H%M%S)
    mv "${ROOT}/data/ticks/_spark_metadata" \
       "${ROOT}/data/ticks/_spark_metadata.archive_${stamp}"
  fi
  rm -rf "${ROOT}/data/checkpoints/spark"
}

start_producers() {
  log "→ starting producers"
  nohup "${VENV}/binance-producer" \
    > "${LOGDIR}/bench_binance.log" 2>&1 &
  echo "binance=$!" >> "$PIDFILE"

  nohup "${VENV}/polymarket-producer" --assets btc eth sol xrp bnb doge --window 5 \
    > "${LOGDIR}/bench_polymarket.log" 2>&1 &
  echo "polymarket=$!" >> "$PIDFILE"
  sleep 3
  log "  producers started"
}

run_spark_phase() {
  local label="$1"        # cpu | gpu
  local metrics_file="$2" # path to output JSONL
  local extra_args="${3:-}"

  log "→ [$label] cleaning Spark state"
  clean_spark_state

  local spark_log="${LOGDIR}/bench_spark_${label}.log"
  local lstm_log="${LOGDIR}/bench_lstm_${label}.log"
  log "→ [$label] starting spark-stream + score-lstm ($extra_args)"

  # Start with a placeholder metrics file so the listener has somewhere to write
  : > "$metrics_file"

  nohup "${VENV}/spark-stream" \
    --output-path "${ROOT}/data" \
    --checkpoint-path "${ROOT}/data/checkpoints/spark" \
    --metrics-file "$metrics_file" \
    $extra_args \
    > "$spark_log" 2>&1 &
  local spark_pid=$!
  echo "spark_${label}=${spark_pid}" >> "$PIDFILE"

  # score-lstm: CPU run hides the GPU so the scorer runs on CPU too.
  # GPU run lets it auto-select CUDA (cuda > mps > cpu).
  if [ -f "${ROOT}/data/model.lstm.pt" ]; then
    if [[ "$label" == "cpu" ]]; then
      nohup env CUDA_VISIBLE_DEVICES="" "${VENV}/score-lstm" \
        > "$lstm_log" 2>&1 &
    else
      nohup "${VENV}/score-lstm" \
        > "$lstm_log" 2>&1 &
    fi
    local lstm_pid=$!
    echo "lstm_${label}=${lstm_pid}" >> "$PIDFILE"
    log "  score-lstm started (pid=$lstm_pid)"
  else
    log "  score-lstm skipped (no model.lstm.pt)"
  fi

  wait_for_spark_ui

  log "→ [$label] warming up for ${WARMUP_MIN} min (metrics not counted)..."
  sleep "$WARMUP_S"

  log "→ [$label] measurement window: ${DURATION_MIN} min"
  # Reset the metrics file — only capture steady-state batches
  : > "$metrics_file"
  sleep "$DURATION_S"

  log "→ [$label] measurement complete — stopping spark + scorer"
  kill "$spark_pid" 2>/dev/null || true
  wait "$spark_pid" 2>/dev/null || true
  sed -i "/spark_${label}=/d" "$PIDFILE"

  # Stop lstm scorer for this phase
  local lstm_entry; lstm_entry=$(grep "lstm_${label}=" "$PIDFILE" 2>/dev/null || true)
  if [[ -n "$lstm_entry" ]]; then
    local lstm_kill_pid="${lstm_entry#*=}"
    kill "$lstm_kill_pid" 2>/dev/null || true
    sed -i "/lstm_${label}=/d" "$PIDFILE"
  fi

  local n_lines
  n_lines=$(wc -l < "$metrics_file" 2>/dev/null || echo 0)
  log "  [$label] captured $n_lines batch progress records"
}

# ---- 1. docker + kafka -------------------------------------------------------

log "→ docker compose up"
docker compose -f "${ROOT}/infra/docker-compose.yml" up -d
wait_for_kafka

# ---- 2. producers ------------------------------------------------------------

start_producers

# ---- 3. cpu run --------------------------------------------------------------

CPU_JSONL="${RESULTS_DIR}/cpu_progress.jsonl"
run_spark_phase "cpu" "$CPU_JSONL" "--sigma 50"

# Wait a moment for Spark port 4040 to release
sleep 10

# ---- 4. gpu run --------------------------------------------------------------

GPU_JSONL="${RESULTS_DIR}/gpu_progress.jsonl"
run_spark_phase "gpu" "$GPU_JSONL" "--sigma 50 --rapids ${RAPIDS_JAR_ARG}"

# ---- 5. stop producers -------------------------------------------------------

log "→ stopping producers"
while IFS='=' read -r name pid; do
  if [[ "$name" =~ ^(binance|polymarket)$ ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
  fi
done < "$PIDFILE"

# ---- 6. compare --------------------------------------------------------------

log "→ generating comparison report"
COMPARE_OUT="${RESULTS_DIR}/comparison"

PYTHONPATH="${ROOT}" "${VENV}/python" "${ROOT}/scripts/compare_spark_runs.py" \
  "$CPU_JSONL" \
  "$GPU_JSONL" \
  --out "$COMPARE_OUT" \
  2>&1 | tee "${RESULTS_DIR}/compare.log"

echo
log "✓ done"
log "  results → $RESULTS_DIR"
log "  CPU progress → $CPU_JSONL"
log "  GPU progress → $GPU_JSONL"
log "  comparison   → ${COMPARE_OUT}.csv"
log "  comparison   → ${COMPARE_OUT}.md"
log "  spark logs   → ${LOGDIR}/bench_spark_{cpu,gpu}.log"
