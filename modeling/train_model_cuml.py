#!/usr/bin/env python3
"""
train_model_cuml.py — GPU ARIMA training via RAPIDS cuML (Fase 6 comparison).

Trains an ARIMA(p, 0, q) model on the same training.parquet used by
train_model.py, but using cuML on an NVIDIA GPU instead of statsmodels
on CPU. Reports the same RMSE / MAE / dir_acc metrics for a direct
side-by-side comparison.

IMPORTANT — cuML ARIMA limitation:
  cuML ARIMA (RAPIDS 24.x) does not support exogenous regressors (ARIMAX/SARIMAX).
  This script benchmarks an endogenous-only ARIMA, which is methodologically
  different from the CPU model (SARIMAX + 3 exog). The gap in holdout metrics
  between the two backends reflects both the hardware difference AND the exog
  contribution. Both differences are documented in docs/phase-6-report.md.

Setup (Windows WSL2 with RTX 2060):
  conda create -n rapids -c rapidsai -c conda-forge -c nvidia \
      rapids=24.10 python=3.11 cuda-version=12.0
  conda activate rapids
  # Copy training.parquet to the WSL2 environment and run this script.

Usage:
    python modeling/train_model_cuml.py
    python modeling/train_model_cuml.py --max-p 3 --max-q 3 --holdout-frac 0.2
"""

from __future__ import annotations

import argparse
import json
import math
import time
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

TARGET_COL = "log_return_5m"


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def report(name: str, y_true: np.ndarray, y_pred: np.ndarray) -> None:
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    nonzero = y_true != 0
    dir_acc = float(np.mean(np.sign(y_pred[nonzero]) == np.sign(y_true[nonzero]))) if nonzero.any() else float("nan")
    print(
        f"  {name:<32}  "
        f"RMSE={rmse * 10_000:6.2f} bps  "
        f"MAE={mae * 10_000:6.2f} bps  "
        f"dir_acc={dir_acc * 100:5.1f}%"
    )


# ---------------------------------------------------------------------------
# AIC grid search (cuML)
# ---------------------------------------------------------------------------

def select_order_cuml(y_train: np.ndarray, max_p: int, max_q: int) -> tuple[int, int, int]:
    from cuml.tsa.arima import ARIMA as CuMLARIMA

    print(f"\nAIC grid over p, q ∈ {{0..{max_p}}} × {{0..{max_q}}} (GPU):")
    candidates: list[dict] = []
    for p in range(max_p + 1):
        for q in range(max_q + 1):
            if p == 0 and q == 0:
                continue
            try:
                m = CuMLARIMA(y_train, order=(p, 0, q), fit_intercept=True, output_type="numpy")
                m.fit()
                aic = float(m.aic)
                candidates.append({"order": (p, 0, q), "aic": aic})
                print(f"  ({p},0,{q})  AIC={aic:.2f}")
            except Exception as exc:
                print(f"  ({p},0,{q})  failed: {type(exc).__name__}")

    valid = [c for c in candidates if math.isfinite(c["aic"])]
    if not valid:
        raise SystemExit("no order converged — collect more data")
    best = min(valid, key=lambda c: c["aic"])
    print(f"\nbest order (GPU): {best['order']}  AIC={best['aic']:.2f}")
    return tuple(best["order"])


# ---------------------------------------------------------------------------
# Walk-forward (cuML)
# ---------------------------------------------------------------------------

def walk_forward_cuml(
    y_train: np.ndarray,
    y_ho: np.ndarray,
    order: tuple,
) -> np.ndarray:
    from cuml.tsa.arima import ARIMA as CuMLARIMA

    print(f"\nGPU walk-forward over {len(y_ho)} holdout rows…")
    preds: list[float] = []
    # cuML does not support stateful append() — refit on growing window
    for i in range(len(y_ho)):
        window = np.concatenate([y_train, y_ho[:i]])
        try:
            m = CuMLARIMA(window, order=order, fit_intercept=True, output_type="numpy")
            m.fit()
            fc = m.forecast(nsteps=1)
            preds.append(float(fc[0]))
        except Exception:
            preds.append(0.0)
        if (i + 1) % 20 == 0:
            print(f"  step {i + 1}/{len(y_ho)}")
    return np.array(preds)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="GPU cuML ARIMA training (Fase 6 comparison)")
    parser.add_argument("--training-path", default="data/training.parquet")
    parser.add_argument("--features-out", default="data/feature_list_cuml.json")
    parser.add_argument("--holdout-frac", type=float, default=0.2)
    parser.add_argument("--max-p", type=int, default=3)
    parser.add_argument("--max-q", type=int, default=3)
    args = parser.parse_args()

    try:
        from cuml.tsa.arima import ARIMA as CuMLARIMA  # noqa: F401
    except ImportError:
        print("cuML not found. Install RAPIDS:")
        print("  conda install -c rapidsai -c conda-forge -c nvidia rapids=24.10 python=3.11 cuda-version=12.0")
        raise SystemExit(1)

    training_path = Path(args.training_path)
    if not training_path.exists():
        print(f"Training data not found: {training_path}  →  run: build-dataset")
        raise SystemExit(1)

    df = pd.read_parquet(training_path).sort_index().reset_index(drop=True)
    print(f"loaded {training_path}: {len(df):,} rows")
    print("  Note: cuML ARIMA is endogenous-only — exog columns not used")

    y = df[TARGET_COL].values.astype(np.float64)
    cutoff = int(len(y) * (1 - args.holdout_frac))
    y_tr, y_ho = y[:cutoff], y[cutoff:]
    print(f"  train: {len(y_tr):,}  holdout: {len(y_ho):,}")

    t_select = time.perf_counter()
    best_order = select_order_cuml(y_tr, args.max_p, args.max_q)
    print(f"  order selection: {(time.perf_counter() - t_select) * 1e3:.0f} ms")

    t_wf = time.perf_counter()
    p_ho = walk_forward_cuml(y_tr, y_ho, best_order)
    print(f"  walk-forward: {(time.perf_counter() - t_wf) * 1e3:.0f} ms")

    sigma_log = float(y_tr.std())
    p_poly_ho = (2.0 * df["poly_p_up"].values[cutoff:] - 1.0) * sigma_log if "poly_p_up" in df.columns else np.zeros_like(p_ho)
    p_zero_ho = np.zeros_like(p_ho)

    print(f"\nσ_log (train) = {sigma_log:.6f}  ({sigma_log * 10_000:.2f} bps)")
    print("\nmetrics  (lower RMSE/MAE is better; dir_acc > 50% means signal):")
    report("cuML ARIMA · holdout (walk-fwd, GPU)", y_ho, p_ho)
    if "poly_p_up" in df.columns:
        report("polymarket · holdout", y_ho, p_poly_ho)
    report("zero-baseline · holdout", y_ho, p_zero_ho)

    out = Path(args.features_out)
    out.write_text(json.dumps(
        {
            "backend": "cuml",
            "order": list(best_order),
            "sigma_log": sigma_log,
            "note": "endogenous-only ARIMA — exog not supported by cuML",
        },
        indent=2,
    ))
    print(f"\nsaved {out}")
    print("\nCopy these results to docs/phase-6-report.md for the CPU vs GPU comparison.")


if __name__ == "__main__":
    main()
