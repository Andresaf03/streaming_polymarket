# LSTM inference: CPU vs GPU handoff for Andrés

Phase 7 needs CPU-vs-GPU inference timings on your RTX 2060 / WSL2 box
to close the loop with the Phase 6 SARIMAX comparison. This is **one
command** for you — everything else is bundled in the repo.

## What you're producing

A CSV table of LSTM forward-pass latencies at five batch sizes, on
both `cpu` and `cuda` of your machine. The comparison table in the
Phase 7 report has slots for your numbers next to the Mac and Colab
ones we already collected.

---

## The one command

```bash
git pull
./scripts/run-inference-benchmark.sh
```

That's it. The wrapper handles everything:

1. Detects `nvidia-smi` on your box → builds a sidecar venv at
   `.venv-bench/` (one-time, downloads the cu121 PyTorch wheel,
   ~2-3 minutes the first time, ~2.5 GB).
2. Extracts the model + holdout dataset from
   `data/handoff/lstm_inference_bundle.tar.gz` (1.7 MB, already in
   the repo).
3. Runs the benchmark on `cpu` then `cuda`: 5 batch sizes
   (1, 6, 32, 128, 1024) × 500 forward passes per cell + 50 warmup.
4. Saves CSV + log under `results/lstm_inference_<hostname>_<ts>.{csv,log}`.

Subsequent runs skip step 1 (venv cached) and finish in ~10 seconds.

## What to send back

Two things:

1. **The CSV file** at `results/lstm_inference_<your-hostname>_*.csv`
   (~10 rows: 5 batches × 2 devices).
2. **The console output** (or the matching `.log` file) — useful
   if something looks off.

That's enough to populate the Phase 7 comparison.

---

## Numbers we already have, for context

These are the CPU and Colab GPU rows of the comparison table you'll
be the third row in:

### Inference latency (median ms per forward pass)

| batch | Mac M-series CPU | Colab T4 GPU | RTX 2060 (you) |
|---:|---:|---:|---:|
| 1 | 1.41 | 0.33 | __ |
| 6 (live scorer) | 2.67 | 0.38 | __ |
| 32 | 6.48 | 0.38 | __ |
| 128 | 24.59 | 0.89 | __ |
| 1024 | 119.74 | (not run) | __ |

### Training (overnight, 6 h of data, 15 epochs)

| Metric | Mac CPU | Colab T4 |
|---|---:|---:|
| Total wall time | 608.33 s | 50.18 s |
| Per-epoch median | 38.5 s | 3.0 s |
| Final train MSE | 6.43e-7 | 6.62e-7 |
| Holdout RMSE | 11.93 bps | 11.89 bps |

The training comparison is already complete (Mac CPU vs Colab T4).
Your role is the **inference** comparison on local hardware. If you
also want to add a "RTX 2060 training" row, the dataset is the same
`data/training_lstm/` extracted by the wrapper — just run
`train-lstm --device cuda --epochs 15 --batch-size 256`. Optional;
not blocking the report.

---

## What to expect on first run

Console output shape (paths edited for brevity):

```
→ building benchmark venv at .venv-bench
  (one-time; downloads cu121 PyTorch wheel ~2.5 GB)
  → done
→ runtime:
  python      = 3.11.x  (Linux …)
  torch       = 2.x.x
  cuda avail. = True
  cuda device = NVIDIA GeForce RTX 2060
  cuda count  = 1
→ extracting data/handoff/lstm_inference_bundle.tar.gz
  → extracted to data/{model.lstm.pt, lstm_meta.json, training_lstm/}
→ running benchmark on cpu + cuda (if available)

benchmark configuration:
  devices    = ['cpu', 'cuda']
  batches    = [1, 6, 32, 128, 1024]
  iterations = 500 per cell  (+50 warmup)

device  batch   median_ms    p99_ms   min_ms     calls/s     preds/s
--------------------------------------------------------------------
cpu         1      X.XXXX    ...      ...           ...         ...
cpu         6      X.XXXX    ...      ...           ...         ...
…
cuda        1      X.XXXX    ...      ...           ...         ...
cuda        6      X.XXXX    ...      ...           ...         ...
…

✓ done
  CSV → results/lstm_inference_<hostname>_<ts>.csv
  log → results/lstm_inference_<hostname>_<ts>.log
```

Total runtime including the one-time venv build: ~3-4 minutes. After
that it's ~10 seconds per run.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Wrapper says "ERROR: nvidia-smi present but torch+CUDA not available" | You passed `--no-install` and torch isn't set up | Re-run without `--no-install`, or `pip install torch --index-url https://download.pytorch.org/whl/cu121` into your active env |
| `nvidia-smi` reports the GPU but `torch.cuda.is_available()` is False after install | cu121 wheel doesn't match your driver | Try cu118: `python3 -m venv .venv-bench && .venv-bench/bin/pip install torch --index-url https://download.pytorch.org/whl/cu118`, then re-run with `--no-install` |
| Wheel download fails partway through | Network hiccup | Delete `.venv-bench/`, re-run. The script doesn't resume; it rebuilds. |
| `cuda` rows have ~same latency as `cpu` rows | torch fell back to CPU silently | Check the runtime banner at the top — `cuda avail.` should be `True`. If False, see above |
| Results CSV missing | The script printed an error before saving | Check the `.log` file; rerun with `--force` to re-extract data if you suspect partial extraction |

---

## What the model is

53,361-parameter joint multi-asset LSTM (one model that handles all
6 crypto assets with a per-asset embedding):

```
asset_idx ──► nn.Embedding(6, 8) ─┐
                                   ├─► LSTM(input=12, hidden=64,
features ────────────────────────┘    layers=2, dropout=0.2)
                                       │
                                       ▼
                                   Linear → ŷ (5-min log return)
```

Trained on 6 hours of overnight data (102k training sequences, 25k
holdout). Architecture, weights, and standardization stats all saved
in the bundle — the wrapper restores them so the inference timings
are repeatable across machines.

---

## Why this comparison closes the loop with Phase 6

Phase 6 SARIMAX inference (you measured): GPU was **0.26× CPU
throughput** on the RTX 2060 — slower than CPU. The reason was
launch-overhead-dominated: the SARIMAX model had ~8 parameters and
one matmul per call, so kernel launch cost (~ms) > total compute.

Phase 7 LSTM inference (this round): same hardware, but the model has
53k parameters and a 60-step rollout per call. From the Colab T4 run,
GPU wins **4.2-27.7× across batches 1-128** because per-call compute
now exceeds launch overhead. We expect the RTX 2060 to show the same
qualitative pattern — not necessarily the same magnitude (T4 is
faster than a 2060 on tensor cores) but the crossover happens at
similar batch shape.

So this measurement isn't just "another data point." It tells us
whether the Phase 6 → Phase 7 reframe holds on the *same hardware*
where Phase 6 was measured — i.e., whether the difference was really
about the model's compute profile, not about a hardware difference
between Colab T4 and your 2060.

If your numbers show GPU winning on LSTM inference at the same RTX
2060 where it lost on SARIMAX inference, the report's argument lands
cleanly. If GPU loses to CPU on LSTM at the 2060 too, that would be
genuinely interesting and we'd dig into what's different.

---

## After you send back the CSV

I'll write `docs/phase-7-report.md` (the actual report) using:

- Mac CPU and your CPU as the two CPU columns (different chips,
  same pattern)
- Colab T4 and your RTX 2060 as the two GPU columns
- The training comparison from `docs/lstm-colab-results.md`

Estimated time-to-report after I have your CSV: ~30 minutes.
