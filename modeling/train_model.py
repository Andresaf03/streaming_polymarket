#!/usr/bin/env python3
"""
train_model.py — LightGBM regressor on the prepared training table.

Predicts BTC's 5-minute log return so the model lives on the same axis as
the dashboard's `implied_target` line (which is built from Polymarket's
implied drift). Apples-to-apples three-line comparison:
    actual BTC   ←  realized
    Polymarket   ←  (2·P_up − 1) × σ_log
    model        ←  this regressor

Holdout metrics report all three so we can answer "do we beat Polymarket
on RMSE?" — and a directional-accuracy line is included as a sanity check.

Usage:
    train-model
    train-model --holdout-frac 0.3 --num-rounds 300
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

FEATURES = [
    "btc_return_5m",
    "btc_volatility_5m",
    "poly_p_up",
    "poly_p_up_change_5m",
]
TARGET = "log_return_5m"


def report(name: str, y_true: pd.Series, y_pred: np.ndarray) -> None:
    """Print regression + directional-accuracy metrics for one prediction series."""
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    # Directional accuracy ignores rows where the realized return was exactly 0.
    nonzero = y_true != 0
    if nonzero.any():
        dir_acc = float(np.mean(np.sign(y_pred[nonzero]) == np.sign(y_true[nonzero])))
    else:
        dir_acc = float("nan")
    print(
        f"  {name:<26}  "
        f"RMSE={rmse * 10_000:6.2f} bps  "
        f"MAE={mae * 10_000:6.2f} bps  "
        f"dir_acc={dir_acc * 100:5.1f}%"
    )


def polymarket_implied_log_return(p_up: pd.Series, sigma_log: float) -> np.ndarray:
    """Convert Polymarket P(up) to an implied log return using σ_log of past returns."""
    return ((2.0 * p_up.to_numpy() - 1.0) * sigma_log)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train LightGBM regressor on training.parquet")
    parser.add_argument("--training-path", default="data/training.parquet")
    parser.add_argument("--model-out", default="data/model.lgb")
    parser.add_argument("--features-out", default="data/feature_list.json")
    parser.add_argument("--holdout-frac", type=float, default=0.2)
    parser.add_argument("--num-rounds", type=int, default=200)
    parser.add_argument("--learning-rate", type=float, default=0.05)
    parser.add_argument("--num-leaves", type=int, default=31)
    args = parser.parse_args()

    df = pd.read_parquet(args.training_path).sort_index()
    print(f"loaded {args.training_path}: {len(df):,} rows")

    cutoff = int(len(df) * (1 - args.holdout_frac))
    train, holdout = df.iloc[:cutoff], df.iloc[cutoff:]
    if len(train) == 0 or len(holdout) == 0:
        raise SystemExit("not enough data — collect more ticks before training")
    print(f"  train: {len(train):,}  holdout: {len(holdout):,}")
    print(
        f"  realized return (bps) train mean={train[TARGET].mean()*1e4:.3f}, "
        f"std={train[TARGET].std()*1e4:.3f}; "
        f"holdout mean={holdout[TARGET].mean()*1e4:.3f}, "
        f"std={holdout[TARGET].std()*1e4:.3f}"
    )

    X_tr, y_tr = train[FEATURES], train[TARGET]
    X_ho, y_ho = holdout[FEATURES], holdout[TARGET]

    params = {
        "objective": "regression",
        "metric": "rmse",
        "learning_rate": args.learning_rate,
        "num_leaves": args.num_leaves,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq": 5,
        "verbose": -1,
    }
    model = lgb.train(params, lgb.Dataset(X_tr, y_tr), num_boost_round=args.num_rounds)

    p_tr = model.predict(X_tr)
    p_ho = model.predict(X_ho)

    # σ_log baseline: stddev of training-set returns. Used to convert
    # Polymarket's P(up) into an implied log return. Identical math to the
    # dashboard's drift, just expressed in log space instead of dollars.
    sigma_log = float(y_tr.std())
    p_poly_ho = polymarket_implied_log_return(X_ho["poly_p_up"], sigma_log)
    p_zero_ho = np.zeros_like(p_ho)  # drift-naive baseline

    print(f"\nσ_log (train) = {sigma_log:.6f}  ({sigma_log * 10_000:.2f} bps)")

    print("\nmetrics  (lower RMSE / MAE is better; dir_acc > 50% means signal):")
    report("model · train", y_tr, p_tr)
    report("model · holdout", y_ho, p_ho)
    report("polymarket · holdout", y_ho, p_poly_ho)
    report("zero-baseline · holdout", y_ho, p_zero_ho)

    print("\nfeature importance (gain):")
    imp = (
        pd.Series(model.feature_importance(importance_type="gain"), index=FEATURES)
        .sort_values(ascending=False)
    )
    for f, v in imp.items():
        print(f"  {f:<28} {v:>12.0f}")

    out_model = Path(args.model_out)
    out_features = Path(args.features_out)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(str(out_model))
    out_features.write_text(
        json.dumps({"features": FEATURES, "target": TARGET, "sigma_log": sigma_log}, indent=2)
    )
    print(f"\nsaved {out_model}, {out_features}")


if __name__ == "__main__":
    main()
