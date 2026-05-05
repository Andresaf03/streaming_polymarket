#!/usr/bin/env python3
"""
benchmark_inference.py — CPU vs GPU inference latency for SARIMAX / cuML ARIMAX.

Runs N 1-step-ahead forecasts on both backends and reports p50/p95/p99 latency
per prediction. Designed for the Fase 6 CPU (Mac) vs GPU (Windows RTX 2060)
architecture comparison.

CPU backend : statsmodels SARIMAX      — standard, always available
GPU backend : cuml.tsa.ARIMA + exog    — requires a RAPIDS/cuML build with exog support
                                         Run on Windows with WSL2 + NVIDIA drivers

Usage:
    # CPU only (no RAPIDS needed)
    python scripts/benchmark_inference.py

    # CPU + GPU (run on Windows machine with RAPIDS)
    python scripts/benchmark_inference.py --gpu

    # Custom iterations / data
    python scripts/benchmark_inference.py --n 500 --training data/training.parquet
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

EXOG_COLS = ["poly_p_up", "poly_p_up_change_5m", "btc_volatility_5m"]
TARGET_COL = "log_return_5m"


def scalar(value) -> float:
    return float(np.asarray(value).reshape(-1)[0])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_data(path: Path) -> tuple[pd.Series, pd.DataFrame]:
    df = pd.read_parquet(path).sort_index().reset_index(drop=True)
    return df[TARGET_COL], df[EXOG_COLS]


def percentile(data: list[float], p: float) -> float:
    data_sorted = sorted(data)
    idx = (len(data_sorted) - 1) * p / 100
    lo, hi = int(idx), min(int(idx) + 1, len(data_sorted) - 1)
    return data_sorted[lo] + (data_sorted[hi] - data_sorted[lo]) * (idx - lo)


def print_latency_table(name: str, latencies_us: list[float]) -> None:
    print(f"\n{'─' * 55}")
    print(f"  {name}")
    print(f"{'─' * 55}")
    print(f"  n          : {len(latencies_us):,}")
    print(f"  mean       : {statistics.mean(latencies_us):>8.1f} µs")
    print(f"  p50        : {percentile(latencies_us, 50):>8.1f} µs")
    print(f"  p95        : {percentile(latencies_us, 95):>8.1f} µs")
    print(f"  p99        : {percentile(latencies_us, 99):>8.1f} µs")
    print(f"  max        : {max(latencies_us):>8.1f} µs")
    print(f"  throughput : {1e6 / statistics.mean(latencies_us):>8,.0f} forecasts/s")


# ---------------------------------------------------------------------------
# CPU benchmark — statsmodels SARIMAX
# ---------------------------------------------------------------------------

def benchmark_cpu(y: pd.Series, X: pd.DataFrame, n: int, order: tuple) -> list[float]:
    """Time N 1-step-ahead forecasts using statsmodels SARIMAX."""
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    print(f"\nCPU — fitting SARIMAX{order} on {len(y)} training rows…")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = SARIMAX(
            y,
            exog=X,
            order=order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        fit = model.fit(disp=False)
    print(f"  fit done. Running {n} forecast() calls…")

    # Use the last row of exog for every forecast (same input, isolates call overhead)
    exog_row = X.iloc[[-1]].values

    latencies: list[float] = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for _ in range(n):
            t0 = time.perf_counter()
            fit.forecast(steps=1, exog=exog_row)
            latencies.append((time.perf_counter() - t0) * 1e6)

    return latencies


# ---------------------------------------------------------------------------
# GPU benchmark — cuML ARIMAX (RAPIDS)
# ---------------------------------------------------------------------------

def benchmark_gpu(y: pd.Series, X: pd.DataFrame, n: int, order: tuple) -> list[float]:
    """Time N 1-step-ahead forecasts using cuML ARIMA with exogenous features."""
    try:
        from cuml.tsa.arima import ARIMA as CuMLARIMA
    except ImportError:
        print(
            "\n[GPU] cuml not found — install RAPIDS/cuML with the release selector "
            "and a build where cuml.tsa.ARIMA supports exog=."
        )
        return []

    p, d, q = order
    print(f"\nGPU — fitting cuML ARIMAX({p},{d},{q}) on {len(y)} training rows…")
    y_gpu = y.values.astype(np.float64)
    X_gpu = X.values.astype(np.float64)
    exog_row = X.iloc[[-1]].values.astype(np.float64)

    t_fit = time.perf_counter()
    try:
        gpu_model = CuMLARIMA(
            y_gpu,
            exog=X_gpu,
            order=(p, d, q),
            fit_intercept=True,
            output_type="numpy",
        )
    except TypeError as exc:
        raise SystemExit(
            "This cuML build does not accept exog= for ARIMA. Install a RAPIDS "
            "version with ARIMA exogenous-regressor support before using --gpu."
        ) from exc
    gpu_model.fit()
    print(f"  fit done in {(time.perf_counter() - t_fit) * 1e3:.1f} ms. Running {n} forecast() calls…")

    latencies: list[float] = []
    for _ in range(n):
        t0 = time.perf_counter()
        scalar(gpu_model.forecast(nsteps=1, exog=exog_row))
        latencies.append((time.perf_counter() - t0) * 1e6)

    return latencies


# ---------------------------------------------------------------------------
# Training time comparison
# ---------------------------------------------------------------------------

def compare_training_time(y: pd.Series, X: pd.DataFrame, order: tuple, use_gpu: bool) -> None:
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    print(f"\n{'═' * 55}")
    print("  Training time comparison")
    print(f"{'═' * 55}")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        t0 = time.perf_counter()
        model = SARIMAX(y, exog=X, order=order, enforce_stationarity=False, enforce_invertibility=False)
        model.fit(disp=False)
        cpu_ms = (time.perf_counter() - t0) * 1e3
    print(f"  CPU statsmodels SARIMAX{order}  : {cpu_ms:>8.1f} ms")

    if use_gpu:
        try:
            from cuml.tsa.arima import ARIMA as CuMLARIMA
            p, d, q = order
            y_gpu = y.values.astype(np.float64)
            X_gpu = X.values.astype(np.float64)
            t0 = time.perf_counter()
            try:
                gpu_m = CuMLARIMA(
                    y_gpu,
                    exog=X_gpu,
                    order=(p, d, q),
                    fit_intercept=True,
                    output_type="numpy",
                )
            except TypeError as exc:
                raise SystemExit(
                    "This cuML build does not accept exog= for ARIMA. Install a RAPIDS "
                    "version with ARIMA exogenous-regressor support before using --gpu."
                ) from exc
            gpu_m.fit()
            gpu_ms = (time.perf_counter() - t0) * 1e3
            print(f"  GPU cuML ARIMAX({p},{d},{q})         : {gpu_ms:>8.1f} ms")
            print(f"  speedup (train)              : {cpu_ms / gpu_ms:>8.2f}×")
        except ImportError:
            print("  GPU cuml not available")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CPU vs GPU inference latency benchmark")
    parser.add_argument("--training", default="data/training.parquet")
    parser.add_argument("--features", default="data/feature_list.json")
    parser.add_argument("--n", type=int, default=200, help="Number of forecast() calls to time")
    parser.add_argument("--gpu", action="store_true", help="Also run GPU benchmark (requires cuML)")
    args = parser.parse_args()

    training_path = Path(args.training)
    if not training_path.exists():
        print(f"Training data not found: {training_path}  →  run: build-dataset")
        raise SystemExit(1)

    meta = json.loads(Path(args.features).read_text())
    if meta.get("exog") != EXOG_COLS:
        raise SystemExit(f"Feature mismatch: metadata has {meta.get('exog')}, benchmark uses {EXOG_COLS}")
    order = tuple(meta["order"])

    print(f"Loading {training_path}…")
    y, X = load_data(training_path)
    print(f"  {len(y):,} rows  order={order}  exog={list(X.columns)}")

    compare_training_time(y, X, order, args.gpu)

    cpu_lat = benchmark_cpu(y, X, args.n, order)
    print_latency_table("CPU — statsmodels SARIMAX (1-step forecast)", cpu_lat)

    if args.gpu:
        gpu_lat = benchmark_gpu(y, X, args.n, order)
        if not gpu_lat:
            raise SystemExit("GPU benchmark requested but did not run.")

        print_latency_table("GPU — cuML ARIMAX (1-step forecast)", gpu_lat)
        cpu_mean = statistics.mean(cpu_lat)
        gpu_mean = statistics.mean(gpu_lat)
        print(f"\n  inference speedup (mean): {cpu_mean / gpu_mean:.2f}×  "
              f"({'GPU faster' if gpu_mean < cpu_mean else 'CPU faster — GPU overhead dominates at this scale'})")

    print(f"\n{'─' * 55}")
    print("  Methodology note:")
    print("    CPU: statsmodels SARIMAX — same y, same exog, same ARIMA order")
    print("    GPU: cuML ARIMA + exog   — same y, same exog, same ARIMA order")
    print("  Both latency rows fit once, then time repeated 1-step forecast() calls.")
    print("  Copy these results to docs/phase-6-report.md for the full breakdown.")
    print(f"{'─' * 55}\n")


if __name__ == "__main__":
    main()
