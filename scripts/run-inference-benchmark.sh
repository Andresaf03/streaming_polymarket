#!/usr/bin/env bash
# run-inference-benchmark.sh — one-command CPU-vs-GPU LSTM inference benchmark.
#
# Extracts data/handoff/lstm_inference_bundle.tar.gz (model + meta + holdout
# parquet, ~1.7 MB) into data/, then runs scripts/benchmark_inference_lstm.py
# with --devices auto. On a CUDA box that means cpu + cuda in one pass; on a
# pure-CPU box it just runs CPU and notes that GPU is unavailable. Saves the
# table as CSV under results/ so it can be diff-ed against other runs.
#
# Auto-bootstraps the runtime: if `nvidia-smi` is on the PATH but the project
# venv has CPU-only torch (or no torch at all), creates a sidecar venv at
# `.venv-bench/` and installs the cu121 PyTorch wheel into it. First run on
# a fresh box takes 2-3 minutes (~2.5 GB download); subsequent runs reuse
# the cached venv.
#
# Designed for two boxes:
#   - This Mac        : no nvidia-smi → uses existing .venv → runs CPU only.
#   - Andres / WSL2   : nvidia-smi present + RTX 2060 → auto-builds .venv-bench
#                       with cu121 wheel → runs cpu + cuda.
#
# Flags:
#   --force        : re-extract the bundle even if files exist on disk
#   --no-install   : do NOT auto-create .venv-bench/; use whatever PY we find
#                    (useful if Andres has manually configured a different env)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# ----- arg parsing
FORCE_EXTRACT=0
ALLOW_INSTALL=1
for arg in "$@"; do
  case "$arg" in
    --force)      FORCE_EXTRACT=1 ;;
    --no-install) ALLOW_INSTALL=0 ;;
    -h|--help)
      sed -n '2,22p' "$0" | sed 's/^# //; s/^#//'
      exit 0 ;;
  esac
done

BUNDLE="${ROOT}/data/handoff/lstm_inference_bundle.tar.gz"
RESULTS_DIR="${ROOT}/results"
TS=$(date +%Y%m%d_%H%M%S)
HOSTNAME_TAG=$(hostname -s 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo "host")
CSV_OUT="${RESULTS_DIR}/lstm_inference_${HOSTNAME_TAG}_${TS}.csv"
LOG_OUT="${RESULTS_DIR}/lstm_inference_${HOSTNAME_TAG}_${TS}.log"
BENCH_VENV="${ROOT}/.venv-bench"

mkdir -p "$RESULTS_DIR"

# ----- 1. pick a Python interpreter
# Prefer project venv if it has torch; fall back to system python3.
pick_python() {
  local cand="${ROOT}/.venv/bin/python"
  if [ -x "$cand" ] && "$cand" -c "import torch" >/dev/null 2>&1; then
    echo "$cand"
    return
  fi
  if command -v python3 >/dev/null 2>&1 && python3 -c "import torch" >/dev/null 2>&1; then
    command -v python3
    return
  fi
  echo ""
}

# torch_sees_gpu <python>
torch_sees_gpu() {
  "$1" -c "import torch; assert torch.cuda.is_available()" >/dev/null 2>&1
}

PY=$(pick_python)

# ----- 2. if nvidia-smi exists but torch+CUDA doesn't, build a sidecar venv
HAS_NVIDIA_SMI=0
if command -v nvidia-smi >/dev/null 2>&1; then
  HAS_NVIDIA_SMI=1
fi

NEED_BENCH_VENV=0
if [ "$HAS_NVIDIA_SMI" -eq 1 ]; then
  if [ -z "$PY" ] || ! torch_sees_gpu "$PY"; then
    NEED_BENCH_VENV=1
  fi
fi

if [ "$NEED_BENCH_VENV" -eq 1 ]; then
  if [ "$ALLOW_INSTALL" -eq 0 ]; then
    echo "ERROR: nvidia-smi present but torch+CUDA not available, and --no-install was passed."
    echo "       either run without --no-install (auto-builds .venv-bench/), or pip-install"
    echo "       a CUDA wheel of torch into your active environment manually:"
    echo "         pip install torch --index-url https://download.pytorch.org/whl/cu121"
    exit 1
  fi
  if [ ! -x "${BENCH_VENV}/bin/python" ]; then
    echo "→ building benchmark venv at ${BENCH_VENV}"
    echo "  (one-time; downloads cu121 PyTorch wheel ~2.5 GB)"
    python3 -m venv "$BENCH_VENV"
    "${BENCH_VENV}/bin/pip" install --quiet --upgrade pip
    "${BENCH_VENV}/bin/pip" install --quiet \
        torch --index-url https://download.pytorch.org/whl/cu121
    "${BENCH_VENV}/bin/pip" install --quiet \
        numpy pandas pyarrow scikit-learn
    echo "  → done"
  else
    echo "→ reusing existing ${BENCH_VENV}"
  fi
  PY="${BENCH_VENV}/bin/python"
fi

if [ -z "$PY" ]; then
  echo "ERROR: no usable Python found. Install python3 + torch and re-run."
  exit 1
fi

# ----- 3. torch / cuda sanity
echo "→ runtime:"
"$PY" - <<'PY'
import platform, sys
import torch
print(f"  python      = {sys.version.split()[0]}  ({platform.platform()})")
print(f"  torch       = {torch.__version__}")
print(f"  cuda avail. = {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"  cuda device = {torch.cuda.get_device_name(0)}")
    print(f"  cuda count  = {torch.cuda.device_count()}")
else:
    print("  (no CUDA — benchmark will run CPU only)")
PY

# ----- 4. extract bundle if model/meta/dataset aren't already there
NEED_EXTRACT=0
[ -f "${ROOT}/data/model.lstm.pt"   ] || NEED_EXTRACT=1
[ -f "${ROOT}/data/lstm_meta.json"  ] || NEED_EXTRACT=1
[ -d "${ROOT}/data/training_lstm"   ] || NEED_EXTRACT=1
[ "$FORCE_EXTRACT" -eq 1 ] && NEED_EXTRACT=1

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

# ----- 5. run the benchmark
echo "→ running benchmark on cpu + cuda (if available)"
echo
PYTHONPATH="${ROOT}" "$PY" "${ROOT}/scripts/benchmark_inference_lstm.py" \
  --csv-out "$CSV_OUT" \
  2>&1 | tee "$LOG_OUT"

echo
echo "✓ done"
echo "  CSV → $CSV_OUT"
echo "  log → $LOG_OUT"
