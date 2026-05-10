#!/usr/bin/env bash
# run-inference-benchmark.sh — one-command CPU-vs-GPU LSTM inference benchmark.
#
# Extracts data/handoff/lstm_inference_bundle.tar.gz (model + meta + holdout
# parquet, ~1.7 MB) into data/, then runs scripts/benchmark_inference_lstm.py
# with --devices auto. On a CUDA box that means cpu + cuda in one pass; on a
# pure-CPU box it just runs CPU and notes that GPU is unavailable. Saves the
# table as CSV under results/ so it can be diff-ed against other runs.
#
# Designed so anyone with the repo + a working torch (CPU or CUDA) can run
# the benchmark with no manual setup. Andres on the RTX 2060 / WSL2 box is
# the primary target; same script works on Mac CPU for a local sanity pass.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

VENV_PY="${ROOT}/.venv/bin/python"
PY="${VENV_PY:-python3}"
[ -x "$VENV_PY" ] || PY="python3"

BUNDLE="${ROOT}/data/handoff/lstm_inference_bundle.tar.gz"
RESULTS_DIR="${ROOT}/results"
TS=$(date +%Y%m%d_%H%M%S)
HOSTNAME_TAG=$(hostname -s 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo "host")
CSV_OUT="${RESULTS_DIR}/lstm_inference_${HOSTNAME_TAG}_${TS}.csv"
LOG_OUT="${RESULTS_DIR}/lstm_inference_${HOSTNAME_TAG}_${TS}.log"

mkdir -p "$RESULTS_DIR"

# --- 1. torch / cuda sanity
echo "→ checking torch + device availability"
"$PY" - <<'PY'
import torch
print(f"  torch       = {torch.__version__}")
print(f"  cuda avail. = {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"  cuda device = {torch.cuda.get_device_name(0)}")
    print(f"  cuda count  = {torch.cuda.device_count()}")
else:
    print("  (no CUDA — benchmark will run CPU only)")
PY

# --- 2. extract bundle if model/meta/dataset aren't already there
NEED_EXTRACT=0
[ -f "${ROOT}/data/model.lstm.pt"   ] || NEED_EXTRACT=1
[ -f "${ROOT}/data/lstm_meta.json"  ] || NEED_EXTRACT=1
[ -d "${ROOT}/data/training_lstm"   ] || NEED_EXTRACT=1
# Even if files exist, prefer the bundled versions for reproducibility — keep
# whatever is on disk if it matches the bundle's mtime, but force-extract if
# the user passes --force.
if [ "${1:-}" = "--force" ]; then
  NEED_EXTRACT=1
fi

if [ "$NEED_EXTRACT" -eq 1 ]; then
  if [ ! -f "$BUNDLE" ]; then
    echo "ERROR: bundle missing at $BUNDLE"
    echo "       expected: model.lstm.pt + lstm_meta.json + training_lstm/"
    exit 1
  fi
  echo "→ extracting $BUNDLE"
  TMPDIR=$(mktemp -d)
  tar -xzf "$BUNDLE" -C "$TMPDIR"
  cp -f "$TMPDIR/lstm_inference_bundle/model.lstm.pt"  "${ROOT}/data/model.lstm.pt"
  cp -f "$TMPDIR/lstm_inference_bundle/lstm_meta.json" "${ROOT}/data/lstm_meta.json"
  mkdir -p "${ROOT}/data/training_lstm"
  cp -f "$TMPDIR/lstm_inference_bundle/training_lstm/"*.parquet "${ROOT}/data/training_lstm/"
  rm -rf "$TMPDIR"
  echo "  → extracted to data/{model.lstm.pt, lstm_meta.json, training_lstm/}"
else
  echo "→ model + meta + dataset already on disk (use --force to re-extract)"
fi

# --- 3. run the benchmark
echo "→ running benchmark on cpu + cuda (if available)"
echo
"$PY" "${ROOT}/scripts/benchmark_inference_lstm.py" \
  --csv-out "$CSV_OUT" \
  2>&1 | tee "$LOG_OUT"

echo
echo "✓ done"
echo "  CSV → $CSV_OUT"
echo "  log → $LOG_OUT"
echo
echo "  to compare with the Mac CPU baseline saved in this repo:"
echo "    diff <(awk -F, '{print \$1,\$2,\$3}' results/lstm_inference_*_cpu.csv) \\"
echo "         <(awk -F, '{print \$1,\$2,\$3}' \"$CSV_OUT\")"
