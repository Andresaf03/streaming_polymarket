#!/usr/bin/env python3
"""
train_model.py — SARIMAX regressor on the prepared training table.

Why SARIMAX (not LightGBM):
  At ~hundreds-of-rows scale and 5-minute-return horizon, a sequence-aware
  linear model with exogenous regressors is the textbook fit. ARIMA's AR
  and MA terms model autocorrelation in the return series itself; the
  exogenous block adds Polymarket P(up) and BTC trailing volatility as
  features that move the conditional mean.

Endogenous : log_return_5m
Exogenous  : poly_p_up, poly_p_up_change_5m, btc_volatility_5m
Order      : SARIMAX(p, 0, q), no seasonal, no differencing (5-min log
             returns are stationary by construction).
Selection  : AIC grid search over p, q ∈ {0..max_p} × {0..max_q}
             (excluding pure noise (0,0,0)).
Validation : walk-forward — re-fit at each holdout step, warm-started from
             the previous fit's parameters for speed, predict 1 step ahead.
Output     : pickled SARIMAXResults at data/model.sarimax.pkl, plus a JSON
             with chosen order, σ_log, exog list, and the AIC grid.

Usage:
    train-model
    train-model --max-p 2 --max-q 2 --holdout-frac 0.3
"""

from __future__ import annotations

import argparse
import json
import pickle
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from statsmodels.tools.sm_exceptions import ConvergenceWarning, ValueWarning
from statsmodels.tsa.statespace.sarimax import SARIMAX

# Silence statsmodels noise that floods the console with no actionable info:
#   - ConvergenceWarning fires on orders the AIC grid will reject anyway
#     (we exclude NaN-AIC candidates downstream).
#   - ValueWarning / FutureWarning are about the missing index frequency on
#     the loaded Parquet; doesn't affect estimation or forecast accuracy.
warnings.filterwarnings("ignore", category=ConvergenceWarning)
warnings.filterwarnings("ignore", category=ValueWarning)
warnings.filterwarnings("ignore", category=FutureWarning, module="statsmodels.*")

EXOG = ["poly_p_up", "poly_p_up_change_5m", "btc_volatility_5m"]
TARGET = "log_return_5m"


def report(name: str, y_true: pd.Series, y_pred: np.ndarray) -> None:
    """Print regression + directional-accuracy metrics for one prediction series."""
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    nonzero = y_true != 0
    if nonzero.any():
        dir_acc = float(np.mean(np.sign(y_pred[nonzero]) == np.sign(y_true[nonzero])))
    else:
        dir_acc = float("nan")
    print(
        f"  {name:<28}  "
        f"RMSE={rmse * 10_000:6.2f} bps  "
        f"MAE={mae * 10_000:6.2f} bps  "
        f"dir_acc={dir_acc * 100:5.1f}%"
    )


def fit_sarimax(y: pd.Series, exog: pd.DataFrame, order: tuple[int, int, int],
                start_params: np.ndarray | None = None):
    """Fit SARIMAX(p, 0, q), no seasonal. enforce_* off so the optimizer
    doesn't reject otherwise-fine parameter sets on a non-stationary draw."""
    model = SARIMAX(
        y,
        exog=exog,
        order=order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    return model.fit(disp=False, start_params=start_params)


def select_order(y_train: pd.Series, exog_train: pd.DataFrame,
                 max_p: int, max_q: int) -> tuple[tuple[int, int, int], list[dict]]:
    """AIC grid search. Returns the best (p, 0, q) and the full grid for diagnostics."""
    candidates: list[dict] = []
    print(f"\nAIC grid search over p, q ∈ {{0..{max_p}}} × {{0..{max_q}}} "
          f"(excluding pure white noise):")
    for p in range(max_p + 1):
        for q in range(max_q + 1):
            if p == 0 and q == 0:
                continue
            try:
                fit = fit_sarimax(y_train, exog_train, (p, 0, q))
                aic = float(fit.aic)
                candidates.append({"order": (p, 0, q), "aic": aic})
                print(f"  ({p},0,{q})  AIC={aic:.2f}")
            except Exception as exc:
                candidates.append({"order": (p, 0, q), "aic": float("nan"),
                                   "error": f"{type(exc).__name__}: {exc}"})
                print(f"  ({p},0,{q})  failed: {type(exc).__name__}")

    valid = [c for c in candidates if np.isfinite(c["aic"])]
    if not valid:
        raise SystemExit("no SARIMAX order converged — collect more data")
    best = min(valid, key=lambda c: c["aic"])
    print(f"\nbest order: {tuple(best['order'])} (AIC={best['aic']:.2f})")
    return tuple(best["order"]), candidates


def walk_forward(y_train: pd.Series, exog_train: pd.DataFrame,
                 y_ho: pd.Series, exog_ho: pd.DataFrame,
                 order: tuple[int, int, int]) -> np.ndarray:
    """Walk-forward 1-step-ahead forecast using append(refit=False).

    Standard production pattern: fit SARIMAX once on the training set, then
    for each holdout step:
      1. Forecast 1 step ahead given that step's exogenous values.
      2. Append the realized (y, exog) to update the Kalman filter state.
      3. Keep the MLE parameters frozen — no re-optimization per step.

    Trades a bit of estimation honesty (vs. full refit) for ~50× speed:
    fits ≈ 1 (cold) + N×O(state_update) instead of N×O(MLE).
    """
    print(f"\nwalk-forward over {len(y_ho)} holdout rows "
          f"(fit-once + append, no per-step refit)...")
    fit = fit_sarimax(y_train, exog_train, order)
    preds: list[float] = []
    forecast_failures = 0
    state_failures = 0

    for i in range(len(y_ho)):
        # 1. Forecast next step. Always append exactly one entry to preds.
        try:
            forecast = fit.forecast(steps=1, exog=exog_ho.iloc[i:i + 1])
            pred = float(forecast.iloc[0])
        except Exception as exc:
            if forecast_failures < 3:
                print(f"  step {i} forecast: {type(exc).__name__}: {exc}")
            pred = 0.0
            forecast_failures += 1
        preds.append(pred)

        # 2. Update the Kalman state with the realized observation.
        # Failure here is non-fatal — predictions get worse from this point
        # on (stale state), but the loop still produces one pred per holdout.
        try:
            fit = fit.append(
                y_ho.iloc[i:i + 1],
                exog=exog_ho.iloc[i:i + 1],
                refit=False,
            )
        except Exception as exc:
            if state_failures < 3:
                print(f"  step {i} state update: {type(exc).__name__}: {exc}")
            state_failures += 1

        if (i + 1) % 100 == 0:
            print(f"  step {i + 1}/{len(y_ho)}")

    if forecast_failures:
        print(f"  forecast failed on {forecast_failures}/{len(y_ho)} steps "
              f"(used 0.0 fallback)")
    if state_failures:
        print(f"  state update failed on {state_failures}/{len(y_ho)} steps "
              f"(continued with stale state)")

    return np.array(preds)


def polymarket_implied_log_return(p_up: pd.Series, sigma_log: float) -> np.ndarray:
    """(2·P − 1)·σ — same form as the dashboard's drift, in log-return space."""
    return (2.0 * p_up.to_numpy() - 1.0) * sigma_log


def main() -> None:
    parser = argparse.ArgumentParser(description="Train SARIMAX on training.parquet")
    parser.add_argument("--training-path", default="data/training.parquet")
    parser.add_argument("--model-out", default="data/model.sarimax.pkl")
    parser.add_argument("--features-out", default="data/feature_list.json")
    parser.add_argument("--holdout-frac", type=float, default=0.2)
    parser.add_argument("--max-p", type=int, default=3)
    parser.add_argument("--max-q", type=int, default=3)
    args = parser.parse_args()

    df = pd.read_parquet(args.training_path).sort_index()
    # SARIMAX's append() requires time-contiguous indices. Our snapshot grid
    # has gaps (dropna removes minutes where features or future label are
    # missing), so we drop the DatetimeIndex in favor of a sequential integer
    # one. The actual timestamps are still implicit in row order — predictions
    # align to holdout rows positionally.
    df = df.reset_index(drop=True)
    print(f"loaded {args.training_path}: {len(df):,} rows")

    cutoff = int(len(df) * (1 - args.holdout_frac))
    train, holdout = df.iloc[:cutoff], df.iloc[cutoff:]
    if len(train) < 10 or len(holdout) < 5:
        raise SystemExit(
            f"not enough data — train={len(train)}, holdout={len(holdout)}; "
            "need at least ~hundreds of training rows for a meaningful SARIMAX fit"
        )
    print(f"  train: {len(train):,}  holdout: {len(holdout):,}")
    print(
        f"  realized return (bps) train mean={train[TARGET].mean()*1e4:.3f}, "
        f"std={train[TARGET].std()*1e4:.3f}; "
        f"holdout mean={holdout[TARGET].mean()*1e4:.3f}, "
        f"std={holdout[TARGET].std()*1e4:.3f}"
    )

    y_tr, X_tr = train[TARGET], train[EXOG]
    y_ho, X_ho = holdout[TARGET], holdout[EXOG]

    best_order, aic_grid = select_order(y_tr, X_tr, args.max_p, args.max_q)
    p_ho = walk_forward(y_tr, X_tr, y_ho, X_ho, best_order)

    # In-sample fit on training set, used for the "model · train" report row
    # and as the persisted artifact (consumed by score_stream.py later).
    final_fit = fit_sarimax(y_tr, X_tr, best_order)
    p_tr = final_fit.fittedvalues.to_numpy()

    sigma_log = float(y_tr.std())
    p_poly_ho = polymarket_implied_log_return(X_ho["poly_p_up"], sigma_log)
    p_zero_ho = np.zeros_like(p_ho)

    print(f"\nσ_log (train) = {sigma_log:.6f}  ({sigma_log * 10_000:.2f} bps)")

    print("\nmetrics  (lower RMSE/MAE is better; dir_acc > 50% means signal):")
    report("model · train (in-sample)", y_tr, p_tr)
    report("model · holdout (walk-fwd)", y_ho, p_ho)
    report("polymarket · holdout", y_ho, p_poly_ho)
    report("zero-baseline · holdout", y_ho, p_zero_ho)

    print("\ncoefficient summary (SARIMAX on training data):")
    try:
        print(final_fit.summary().tables[1])
    except Exception:
        # statsmodels can fail to render the table for very small fits;
        # don't let that mask a successful save.
        print("  (summary table unavailable for this fit)")

    out_model = Path(args.model_out)
    out_features = Path(args.features_out)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    with open(out_model, "wb") as fh:
        pickle.dump(final_fit, fh)
    out_features.write_text(json.dumps(
        {
            "exog": EXOG,
            "target": TARGET,
            "order": list(best_order),
            "sigma_log": sigma_log,
            "aic_grid": aic_grid,
        },
        indent=2,
    ))
    print(f"\nsaved {out_model}, {out_features}")


if __name__ == "__main__":
    main()
