#!/usr/bin/env python3
"""
benchmark_inference_lstm.py — CPU vs CUDA inference latency for the joint
multi-asset LSTM. Companion to benchmark_inference.py (Phase 6 SARIMAX).

Sweeps batch sizes [1, 6, 32, 128, 1024] on each device, runs `--iterations`
forward passes per cell with `--warmup` discarded, and reports per-call
median + p99 latency plus throughput in calls/s and predictions/s.

Real holdout windows are used as input so feature distributions match the
deployed scorer; standardization uses the means/stds saved in
data/lstm_meta.json (same as training).

Usage (local, Mac):
    python scripts/benchmark_inference_lstm.py

Usage (Colab, both devices in one run):
    python benchmark_inference_lstm.py --csv-out data/lstm_inference.csv
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch

# Be friendly to Colab: this script may live next to a copy of train_lstm.py
# (no package), or be invoked from the repo root (modeling.train_lstm). Try
# both import paths.
try:
    from modeling.train_lstm import JointAssetLSTM, FEATURE_COLS
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from train_lstm import JointAssetLSTM, FEATURE_COLS  # type: ignore


def load_model(
    model_path: Path, meta_path: Path, device: torch.device
) -> tuple[JointAssetLSTM, dict, np.ndarray, np.ndarray]:
    meta = json.loads(meta_path.read_text())
    model = JointAssetLSTM(
        num_assets=len(meta["assets"]),
        embed_dim=meta["embed_dim"],
        num_features=len(meta["feature_cols"]),
        hidden=meta["hidden"],
        num_layers=meta["num_layers"],
        dropout=meta["dropout"],
    ).to(device)
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.eval()
    means = np.array(meta["feature_means"], dtype=np.float32)
    stds = np.array(meta["feature_stds"], dtype=np.float32)
    return model, meta, means, stds


def build_real_input(
    training_dir: Path, assets: list[str], seq_len: int,
    means: np.ndarray, stds: np.ndarray, batch_size: int,
    holdout_frac: float = 0.2,
) -> tuple[torch.Tensor, torch.Tensor]:
    """(B, T, F) tensor + (B,) asset_idx tensor from real holdout windows.

    Cycles through assets, sliding the window forward when wrapping so each
    row in the batch sees a different point in the holdout. Standardization
    uses the saved means/stds (same as training)."""
    asset_to_idx = {a: i for i, a in enumerate(assets)}
    holdouts: list[tuple[np.ndarray, int]] = []
    for asset in assets:
        path = training_dir / f"{asset}.parquet"
        if not path.exists():
            continue
        df = pd.read_parquet(path).sort_index()
        cutoff = int(len(df) * (1 - holdout_frac))
        arr = df[FEATURE_COLS].iloc[cutoff:].to_numpy(dtype=np.float32)
        if len(arr) > seq_len:
            holdouts.append((arr, asset_to_idx[asset]))
    if not holdouts:
        raise SystemExit("no holdout data — run build-dataset-lstm + train-lstm first")

    windows: list[np.ndarray] = []
    asset_indices: list[int] = []
    for i in range(batch_size):
        arr, idx = holdouts[i % len(holdouts)]
        offset = (i // len(holdouts)) * seq_len
        if offset + seq_len > len(arr):
            offset = 0
        w = (arr[offset : offset + seq_len] - means) / stds
        windows.append(w)
        asset_indices.append(idx)

    x = torch.from_numpy(np.stack(windows)).float()
    a = torch.tensor(asset_indices, dtype=torch.long)
    return x, a


def time_one_call(model: JointAssetLSTM, x: torch.Tensor, a: torch.Tensor, is_cuda: bool) -> float:
    # CUDA ops are async — sync before timing, sync after, otherwise we'd
    # measure kernel-launch enqueue time rather than actual compute.
    if is_cuda:
        torch.cuda.synchronize()
    t0 = time.perf_counter()
    model(x, a)
    if is_cuda:
        torch.cuda.synchronize()
    return (time.perf_counter() - t0) * 1000.0


def benchmark_cell(
    model: JointAssetLSTM, x: torch.Tensor, a: torch.Tensor,
    device: torch.device, warmup: int, iters: int,
) -> dict:
    is_cuda = device.type == "cuda"
    with torch.no_grad():
        for _ in range(warmup):
            model(x, a)
        if is_cuda:
            torch.cuda.synchronize()
        latencies = [time_one_call(model, x, a, is_cuda) for _ in range(iters)]

    arr = np.asarray(latencies)
    return {
        "median_ms": float(np.median(arr)),
        "p99_ms": float(np.percentile(arr, 99)),
        "min_ms": float(np.min(arr)),
        "mean_ms": float(np.mean(arr)),
    }


def resolve_devices(requested: list[str]) -> list[str]:
    if requested == ["auto"]:
        out = ["cpu"]
        if torch.cuda.is_available():
            out.append("cuda")
        return out
    return requested


def main() -> None:
    parser = argparse.ArgumentParser(description="LSTM inference latency benchmark")
    parser.add_argument("--training-dir", default="data/training_lstm")
    parser.add_argument("--model", default="data/model.lstm.pt")
    parser.add_argument("--meta", default="data/lstm_meta.json")
    parser.add_argument("--devices", nargs="+", default=["auto"],
                        help="auto = cpu + cuda if available; or list explicitly")
    parser.add_argument("--batches", nargs="+", type=int,
                        default=[1, 6, 32, 128, 1024])
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--warmup", type=int, default=50)
    parser.add_argument("--csv-out", default=None,
                        help="optional path to save the table as CSV")
    # parse_known_args() — Jupyter / Colab inject `-f kernel-XXXX.json`
    args, unknown = parser.parse_known_args()
    leftover: list[str] = []
    skip_next = False
    for tok in unknown:
        if skip_next:
            skip_next = False
            continue
        if tok == "-f":
            skip_next = True
            continue
        leftover.append(tok)
    if leftover:
        print(f"warning: ignoring unrecognized args: {leftover}")

    devices = resolve_devices(args.devices)
    print("benchmark configuration:")
    print(f"  devices    = {devices}")
    print(f"  batches    = {args.batches}")
    print(f"  iterations = {args.iterations} per cell  (+{args.warmup} warmup)")
    print(f"  model      = {args.model}")
    print()

    rows: list[dict] = []
    header = (f"{'device':<7} {'batch':>5}  {'median_ms':>10} "
              f"{'p99_ms':>9} {'min_ms':>8}  {'calls/s':>10} {'preds/s':>11}")
    print(header)
    print("-" * len(header))

    for dev_name in devices:
        device = torch.device(dev_name)
        model, meta, means, stds = load_model(Path(args.model), Path(args.meta), device)
        for B in args.batches:
            x, a = build_real_input(
                Path(args.training_dir), meta["assets"], meta["seq_len"],
                means, stds, B,
            )
            x = x.to(device, non_blocking=True)
            a = a.to(device, non_blocking=True)
            stats = benchmark_cell(model, x, a, device, args.warmup, args.iterations)
            calls_per_sec = 1000.0 / stats["median_ms"]
            preds_per_sec = calls_per_sec * B
            print(f"{dev_name:<7} {B:>5}  "
                  f"{stats['median_ms']:>10.4f} "
                  f"{stats['p99_ms']:>9.4f} "
                  f"{stats['min_ms']:>8.4f}  "
                  f"{calls_per_sec:>10,.0f} "
                  f"{preds_per_sec:>11,.0f}")
            rows.append({
                "device": dev_name,
                "batch": B,
                "median_ms": round(stats["median_ms"], 4),
                "p99_ms": round(stats["p99_ms"], 4),
                "min_ms": round(stats["min_ms"], 4),
                "mean_ms": round(stats["mean_ms"], 4),
                "calls_per_sec": round(calls_per_sec, 2),
                "preds_per_sec": round(preds_per_sec, 2),
            })

    if args.csv_out:
        out = Path(args.csv_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(out, index=False)
        print(f"\nsaved {out}")


if __name__ == "__main__":
    main()
