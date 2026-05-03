#!/usr/bin/env python3
"""
train_model.py — train a LightGBM classifier on the prepared training table.

Reads `data/training.parquet`, splits time-wise (oldest → train, newest →
holdout), fits LightGBM, reports AUC + log loss + Brier on both halves, and
also reports the same metrics for the Polymarket implied probability used
on its own as a baseline (so we can answer "do we beat the market?").

Usage:
    train-model                                  # data/training.parquet → data/model.lgb
    train-model --holdout-frac 0.3 --num-rounds 300
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

FEATURES = [
    "btc_return_5m",
    "btc_volatility_5m",
    "poly_p_up",
    "poly_p_up_change_5m",
]


def report(name: str, y_true: pd.Series, y_pred: pd.Series) -> None:
    """Print AUC / log loss / Brier in a consistent block.

    Handles tiny degenerate splits (holdout has only one class) without
    crashing — AUC is undefined in that case, logloss/brier still are.
    """
    p = y_pred.clip(1e-3, 1 - 1e-3)
    auc_str = (
        f"{roc_auc_score(y_true, y_pred):.4f}"
        if y_true.nunique() == 2
        else "  n/a "
    )
    print(
        f"  {name:<22}  "
        f"AUC={auc_str}  "
        f"logloss={log_loss(y_true, p, labels=[0, 1]):.4f}  "
        f"brier={brier_score_loss(y_true, y_pred):.4f}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train LightGBM on training.parquet")
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
    print(f"  train labels: {train['label'].value_counts().to_dict()}")
    print(f"  holdout labels: {holdout['label'].value_counts().to_dict()}")

    X_tr, y_tr = train[FEATURES], train["label"]
    X_ho, y_ho = holdout[FEATURES], holdout["label"]

    params = {
        "objective": "binary",
        "metric": "binary_logloss",
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
    p_poly_ho = X_ho["poly_p_up"]

    print("\nmetrics:")
    report("model · train", y_tr, p_tr)
    report("model · holdout", y_ho, p_ho)
    report("polymarket · holdout", y_ho, p_poly_ho)

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
    out_features.write_text(json.dumps(FEATURES, indent=2))
    print(f"\nsaved {out_model}, {out_features}")


if __name__ == "__main__":
    main()
