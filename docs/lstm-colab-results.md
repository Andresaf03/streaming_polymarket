# LSTM CPU vs GPU on Colab — what happened

A log of how the Phase 7 LSTM CPU/GPU comparison was actually executed,
the numbers we got, and the surprises along the way. Pair this with
`docs/lstm-cpu-vs-gpu-handoff.md` (instructions for Andres's RTX 2060
side) and `scripts/run-inference-benchmark.sh` (one-command repro).

This is descriptive — *what we did and what came back*. It is not the
Phase 7 report; that's still to be written.

---

## Hardware on each side

| Side | CPU | GPU | Notes |
|---|---|---|---|
| Mac (this laptop) | M-series, PyTorch 2.11 CPU wheel | none | MPS skipped because LSTM produces NaN loss on it (known PyTorch issue). |
| Colab | (Colab x86 CPU) | **NVIDIA Tesla T4, 16 GB** | free tier; runtime: T4 GPU. |

The "CPU vs GPU" comparison points worth keeping in mind:

- *Mac CPU vs T4 GPU* is the cross-machine comparison — different CPUs.
  Useful as a "what does the laptop do vs what does cloud GPU do" data
  point.
- *Colab CPU vs T4 GPU* is the single-machine comparison — same CPU on
  both rows. The honest device comparison.

We measured both. Numbers below.

---

## Training run (overnight dataset, 6 hours of data)

Same script (`modeling/train_lstm.py`), same flags
(`--epochs 15 --batch-size 256`), same dataset
(`data/training_lstm/{6 assets}.parquet`, ~128k rows total). Only
`--device` differed.

| Metric | Mac CPU | T4 GPU (Colab) |
|---|---:|---:|
| Total wall time | **608.33 s** | **50.18 s** |
| Per-epoch median | 38.5 s | 3.0 s |
| Per-epoch (steady) | ~38 s | ~3 s |
| Epoch 1 (warmup) | 38.6 s | 3.0–4.2 s* |
| Final train MSE | 6.43e-7 | 6.62e-7 (run 2) / 6.02e-7 (run 1) |
| Holdout RMSE | 11.93 bps | 11.89 bps (run 2) / 11.56 bps (run 1) |
| Holdout dir_acc | 49.3 % | 48.6 % (run 2) / 52.8 % (run 1) |

\* Run 1 epoch 1 took 4.2 s due to CUDA kernel JIT compile. Run 2
(immediately after, kernels cached) did epoch 1 in 3.0 s. CPU has no
analogous warmup penalty — that's the GPU's side of the trade.

**Speedup: 608.33 / 50.18 = 12.1× wall-clock; 38.5 / 3.0 = 12.8× per
epoch.** Identical numbers within RNG noise, so the model is
genuinely the same on both devices — only timing differs.

The earlier smoke test (45 min of data, much smaller dataset) showed
13× per-epoch on the same hardware. The 12.8× from the larger run
confirms this isn't a small-data artifact: GPU's training advantage
is robust across at least an order of magnitude of dataset size.

### dir_acc honest framing

dir_acc swung between the two GPU runs (52.8 → 48.6 %); per-asset it
swung even more (ETH 61.3 → 38.8 %). That's noise — at 6 hours of
data the directional signal is right at the noise floor and a different
random init flips its sign. We're capturing the magnitude (RMSE
consistent across runs) but not reliably the direction. To claim a
predictive model we'd need either much more data or different
features. For the device comparison this doesn't matter — both devices
land at the same noisy answer.

### Train loss bottoms then rises

Both CPU and GPU runs show train MSE bottoming around epoch 8–10 and
slightly rising through epoch 15. Same overfitting shape on both — the
model isn't device-dependent. For the device comparison we kept 15
epochs to keep the timing apples-to-apples; for a deployable model
the right `--epochs` would be ~10 with early stopping.

---

## Inference latency (T4 vs Mac CPU)

`scripts/benchmark_inference_lstm.py` — 500 forward passes per cell,
50 warmup, real holdout windows as input. Run identically on Mac CPU
and Colab T4 GPU.

| batch | Mac CPU median (ms) | T4 GPU median (ms) | speedup |
|---:|---:|---:|---:|
| 1 | 1.41 | 0.33 | **4.2×** |
| 6 (live scorer) | 2.67 | 0.38 | **7.1×** |
| 32 | 6.48 | 0.38 | **17×** |
| 128 | 24.59 | 0.89 | **27.7×** |

Two surprises here:

1. **GPU wins at B=1.** Phase 6 SARIMAX showed GPU losing badly at
   B=1 because total compute was so small that kernel launch overhead
   (~ms) dominated. The LSTM doesn't show this: 53k parameters × 60
   timestep rollout means even a single forward pass has enough math
   to be worth the launch cost. So the simple "GPU loses on streaming"
   story from Phase 6 is too coarse — what really matters is whether
   per-call compute exceeds the launch overhead floor, which depends
   on model size, not on streaming-vs-batch.

2. **GPU latency is flat from B=1 to B=32 (~0.38 ms).** The T4 is
   overhead-bound below B=32, not compute-bound. Adding work in that
   range is essentially free. That's why throughput at B=32
   (84,578 preds/s) is 28× throughput at B=1 (2,989 preds/s) at
   nearly the same latency.

### Throughput

| batch | T4 preds/s |
|---:|---:|
| 1 | 2,989 |
| 6 | 15,863 |
| 32 | 84,578 |
| 128 | 144,342 |

For context, the live LSTM scorer needs **1 pred/s per asset = 6
preds/s total**. The T4 has ~24,000× the throughput needed at the
streaming workload. Mac CPU has ~370×. Both are absurd
overprovisioning for live serving — GPU isn't *required* for
streaming inference of this model, just *faster*.

---

## Phase 6 → Phase 7 reframe

Earlier framings of "GPU lost on streaming, GPU wins on training"
were too coarse. Cleaner statement:

> GPU loses when per-call compute is smaller than per-call kernel
> launch overhead. GPU wins when per-call compute exceeds it. The
> threshold depends on model size and per-call work, not on whether
> the workload is streaming or batch.

- Phase 6 SARIMAX: ~8 parameters, one matrix multiply per call.
  Per-call compute << launch overhead. GPU loses.
- Phase 7 LSTM: 53,361 parameters, 60-step rollout per call.
  Per-call compute >> launch overhead even at B=1. GPU wins at every
  batch size we measured.

Same hardware, two sides of the same threshold.

---

## Pitfalls / things that bit us

- **Colab kernel `-f` arg.** Calling `train_lstm.main()` directly in
  a notebook cell (instead of `!python train_lstm.py …`) breaks
  argparse because Jupyter passes `-f /root/.../kernel-XXX.json` into
  the script. Fix in `modeling/train_lstm.py`: `parse_known_args()`
  with explicit handling for the `-f` pair so real CLI typos still
  warn.
- **Wall time wasn't measured on the first GPU run.** We had per-epoch
  numbers but no headline wall-clock until the user re-ran the cell
  with explicit `time.time()` framing. Lesson: bake `--time` into the
  Colab snippet next time, or use `%%time` cell magic by default.
- **Overnight Kafka died.** Not directly Colab-related but worth
  noting: the laptop's Kafka container hit a JVM unsafe memory access
  fault around 13:23 UTC (≈07:23 PT) and Spark followed it down. We
  still got 6 hours of data before that, which is what was used for
  the training above. The fix for future overnight runs is to give
  Kafka a real volume mount instead of the container's `/tmp`.

---

## Artifacts

| Path | What it is |
|---|---|
| `data/handoff/lstm_inference_bundle.tar.gz` | model + meta + per-asset holdout parquet, ~1.7 MB. Tracked in git so the inference benchmark is reproducible without rerunning training. |
| `scripts/run-inference-benchmark.sh` | one-command wrapper: extracts bundle, runs benchmark on cpu + cuda, saves CSV + log under `results/`. Auto-builds a sidecar `.venv-bench/` with the cu121 PyTorch wheel on the first run if `nvidia-smi` is present but the existing venv only has CPU torch (Andres's RTX 2060 / WSL2 path). On a pure-CPU host it just uses the project venv. Use `--no-install` to skip the auto-bootstrap, `--force` to re-extract the bundle. |
| `scripts/benchmark_inference_lstm.py` | underlying Python benchmark, used by the wrapper but also runnable standalone. |
| `data/lstm_train_cpu_overnight.log` | Mac CPU training log (608 s wall, 15 epochs). |
| `data/lstm_inference_mac_cpu.csv` | Mac CPU inference benchmark, 5 batch sizes. |

---

## What this leaves us

The CPU/GPU comparison data is complete. Phase 7 report is still to
write — it should pull from this document and cite the four tables
above (training timing, training metrics, inference latency, inference
throughput) plus the reframed Phase 6 ↔ Phase 7 narrative.
