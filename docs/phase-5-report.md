# Phase 5 — Supervised model on the Parquet archive

**Branch:** `feat/phase-5-model-training`
**Status:** model trained, holdout metrics in hand, artifacts shipped.

---

## Headline

**SARIMAX(2, 0, 2)** with three exogenous regressors beats the Polymarket-implied baseline on every holdout metric. Walk-forward 1-step-ahead RMSE on out-of-sample data:

| Series | RMSE (bps) | MAE (bps) | dir_acc |
| --- | --- | --- | --- |
| **Model · holdout** | **2.45** | **1.92** | **85.1%** |
| Polymarket · holdout | 5.36 | 4.21 | 58.2% |
| Zero-baseline · holdout | 4.53 | 3.64 | — |
| Model · train (in-sample) | 3.20 | 2.28 | 81.5% |

The model RMSE is **54% lower** than the Polymarket-implied target on holdout. Directional accuracy 85.1% vs 58.2%. σ_log (train) = 5.37 bps.

---

## Data

12 hours of overnight collection produced:

| Source | Events |
| --- | --- |
| polymarket.events | 21,303,885 |
| binance.book (bookTicker + depth20) | 4,386,836 |
| binance.trades (aggTrade) | 335,991 |
| Spark `btc.forecast` (1/sec aggregate) | 43,732 |

Raw archive: 7.0 GB across 269,270 Parquet files under `data/ticks/source=…/date=…/hour=…/`. **Not committed** (size). Reproducible by running `./scripts/run-overnight.sh` — see `docs/phase-3-report.md` for the pipeline that produces these.

After resampling BTC mid + Polymarket BTC-5m P(up) to a 1-second grid and sampling on a 1-minute snapshot grid + dropping snapshots with any feature/label nulls: **~707 minute-bars** in `data/training.parquet` (committed).

Two reliability events were caught and recovered by the new retry-with-backoff loop:

- `binance WS error after 571s: AttributeError: 'NoneType' …` (early run)
- `binance WS error after 31570s: ConnectionClosedError: no close frame received` (~8h45m in)

Both followed by `[yellow]reconnecting in 1s…[/yellow]` and the producer kept emitting. Without the fix the overnight run would have lost 11+ hours of Binance ticks.

---

## Why SARIMAX, not LightGBM

The original Phase 5 commit used LightGBM. Pivoted because:

1. **Data is sequential**: 1-min returns are autocorrelated; treating rows as IID throws away the AR(p) and MA(q) structure that an ARIMA-family model captures directly.
2. **Scale**: a few hundred training rows is comfortable for SARIMAX (5-10 parameters) and risky for tree boosting (overfit territory).
3. **Output axis**: the dashboard already projects Polymarket into a *price* (`implied_target = BTC + (2P − 1)·σ`). The model output should live on the same axis, so a regression on log return — converted to a price target — is the natural fit. Classification was the wrong target.

LightGBM stays in `pyproject.toml` extras for future comparison work but is no longer the production model.

---

## Pipeline

```
Parquet ticks  ──►  build_dataset.py  ──►  data/training.parquet
                                                  │
                                                  ▼
                                          train_model.py
                                                  │
                          ┌───────────────────────┼───────────────────────┐
                          ▼                       ▼                       ▼
                    AIC grid select       walk-forward holdout      final fit on
                    over (p, 0, q)        (fit-once + append,        train data
                    p,q ∈ {0..3}           refit=False)              (persisted)
                          │                       │                       │
                          └────────► AIC grid     │                       │
                                  feature_list    │                       │
                                                  ▼                       ▼
                                            metrics table          model.sarimax.pkl
```

`train-model` writes:

- `data/model.sarimax.pkl` — pickled `SARIMAXResults`
- `data/feature_list.json` — chosen order, exog list, σ_log, full AIC grid

`score_stream.py` (Phase 6, see TODO) is intended to consume these directly.

---

## Order selection — AIC grid

Grid over `p, q ∈ {0..3}` (excluding pure white noise (0,0,0)), `d=0` because 5-min log returns are stationary by construction.

| (p, d, q) | AIC |
| --- | --- |
| (0, 0, 1) | −7063.67 |
| (0, 0, 2) | −7220.55 |
| (0, 0, 3) | −7281.23 |
| (1, 0, 0) | −7483.22 |
| (1, 0, 1) | −7473.09 |
| (1, 0, 2) | −7467.20 |
| (1, 0, 3) | −7455.26 |
| (2, 0, 0) | −7476.43 |
| (2, 0, 1) | −6516.91 |
| **(2, 0, 2)** | **−7494.56** ← chosen |
| (2, 0, 3) | −7473.27 |
| (3, 0, 0) | −7490.06 |
| (3, 0, 1) | −7483.04 |
| (3, 0, 2) | −7490.85 |
| (3, 0, 3) | −7481.94 |

The full grid is also persisted in `data/feature_list.json` under the `aic_grid` key.

---

## Coefficient summary (final fit on training data)

| Term | Coefficient | Std err | z | P > \|z\| |
| --- | --- | --- | --- | --- |
| `poly_p_up` | −0.0001 | 3.21e-05 | −3.749 | 0.000 |
| `poly_p_up_change_5m` | 7.821e-05 | 2.96e-05 | 2.643 | 0.008 |
| `btc_volatility_5m` | 0.3436 | 0.066 | 5.171 | 0.000 |
| `ar.L1` | 1.6852 | 0.035 | 47.730 | 0.000 |
| `ar.L2` | −0.8226 | 0.028 | −29.371 | 0.000 |
| `ma.L1` | −0.9038 | 0.046 | −19.493 | 0.000 |
| `ma.L2` | 0.2037 | 0.045 | 4.486 | 0.000 |
| `sigma²` | 9.097e-08 | 3.61e-09 | 25.189 | 0.000 |

Every coefficient is significant at p < 0.01.

**Reading the exogenous block:**

- `poly_p_up` (level) carries a small *negative* coefficient — when Polymarket's implied probability of up is high, returns are slightly *lower* on average. Mild contrarian effect, well within margins of measurement noise but statistically significant.
- `poly_p_up_change_5m` (delta) carries a small *positive* coefficient — recent rises in P(up) predict positive forward returns. The change carries signal that the level alone doesn't.
- `btc_volatility_5m` is the strongest exogenous predictor — high recent realized volatility expands the magnitude of expected forward moves.

The AR(2) + MA(2) structure (`ar.L1=+1.69`, `ar.L2=−0.82`, `ma.L1=−0.90`, `ma.L2=+0.20`) is a typical mean-reverting-with-momentum signature of crypto returns at the 1-minute scale.

---

## Files in this PR

| File | Purpose | Size | Tracked |
| --- | --- | --- | --- |
| `modeling/build_dataset.py` | Parquet → 1-min snapshots → training table | source | ✅ |
| `modeling/train_model.py` | AIC grid + walk-forward holdout + final fit | source | ✅ |
| `data/training.parquet` | 707-row training table (features + log_return_5m) | 36 KB | ✅ |
| `data/model.sarimax.pkl` | Pickled `SARIMAXResults` (final fit on training data) | 1.3 MB | ✅ |
| `data/feature_list.json` | Order, exog list, σ_log, AIC grid | 1.7 KB | ✅ |
| `data/ticks/` | Raw tick archive (12h overnight) | 7.0 GB | ❌ — too large |
| `data/checkpoints/` | Spark Streaming state | 18 MB | ❌ — derived |

The `.gitignore` carves three explicit exceptions so the `data/training.parquet`, `data/model.sarimax.pkl`, and `data/feature_list.json` artifacts ship; everything else under `data/` stays ignored.

---

## How to retrain on more data

Same flow as the overnight run:

```bash
./scripts/run-overnight.sh    # accumulate ticks
./scripts/stop-overnight.sh   # tear down
build-dataset                 # regenerate data/training.parquet
train-model                   # SARIMAX on the new training table
```

`train-model` overwrites `model.sarimax.pkl` and `feature_list.json` in place. Both files are committed at the latest training-set size; rerunning regenerates them with whatever data is present.

CLI knobs worth knowing:

```
train-model --max-p 4 --max-q 4         # widen the AIC search
train-model --holdout-frac 0.3          # bigger holdout
train-model --num-rounds 500            # not used by SARIMAX (residual from LGB era; safe to ignore)
```

---

## TODO

Items below are not in this PR. They are the natural next steps once the model is in.

- [ ] **Calibration analysis (Phase 5b).** Jupyter or Streamlit notebook reading `data/model.sarimax.pkl` + `data/training.parquet`. Two calibration curves on one chart — model's implied direction-probability and Polymarket's `poly_p_up`, plotted against the diagonal reference (empirical "up" frequency by predicted-probability bucket). Saves `docs/figures/calibration.png` for the informe. The existing AR(2) + MA(2) coefficients suggest the model should produce well-calibrated short-horizon probabilities; this confirms or refutes that.

- [ ] **Live scoring (Phase 6).** New module (e.g. `modeling/score_stream.py` or under `consumers/`) that loads `data/model.sarimax.pkl` + `data/feature_list.json`, consumes the existing Kafka input topics, computes the same features that `build_dataset.py` produces (1-minute snapshots of BTC mid, Polymarket P(up), trailing volatility, change), and publishes 1-step-ahead predictions to a new `btc.predictions` topic. Grafana picks up the new topic via the existing `consumers/spark_stream.py` integration pattern and plots a third line on the main BTC chart (actual / Polymarket-implied / model-predicted, all on the `$` axis). σ_log persisted in `feature_list.json` so the conversion from log return to dollar target stays consistent with training.

---

## Limitations to flag in the informe

- **Single 80/20 holdout split.** Walk-forward CV across multiple folds would be more robust against a regime-specific holdout window. Acknowledged tradeoff.
- **`append(refit=False)` keeps the MLE parameters frozen on the training-set estimate.** Standard production pattern, but a "true" walk-forward would re-estimate parameters periodically (e.g. every N steps) — see option B in the train_model.py docstring.
- **σ_log = sample stddev of training-set returns.** A rolling estimate would be more honest; not material at this horizon.
- **Target is BTC mid (not transactable price).** No transaction costs modeled. The 2.45 bps holdout RMSE doesn't translate directly to P&L.
- **Two retry-recovered events overnight.** The `AttributeError: 'NoneType'` at t=571s likely indicates a malformed Binance message that breaks our parser — worth a separate investigation post-Phase 5. The retry loop hides it but the underlying parse bug remains.
