#!/usr/bin/env python3
"""
build_dataset_lstm.py — per-event 1-second grid LSTM training table.

Reads `data/ticks/` (partitioned by source/date/hour) and writes one Parquet
per asset under `data/training_lstm/{asset}.parquet`. Each output row is
one second of one asset, with features designed for a joint multi-asset LSTM.

Output schema (per row):
    ts                       timestamptz
    mid                      float64    Binance bookTicker mid (forward-filled briefly)
    log_return_1s            float64    log(mid_t / mid_{t-1})
    poly_p_up                float64    Polymarket {asset}-updown-5m-* Up outcome
    poly_p_change_1s         float64    poly_p_up_t − poly_p_up_{t-1}
    vol_60s                  float64    rolling stdev of log_return_1s over 60s
    log_return_5m_target     float64    log(mid_{t+300} / mid_t)   ← target

Sequence construction (X = 60 timesteps of the four feature cols, y =
log_return_5m_target) happens in memory during `train_lstm.py`. Saving
per-asset 1-second tables keeps the artifact small (~few MB per asset
for a 12h overnight run) and makes the training script's data loader
straightforward.

Usage:
    build-dataset-lstm                                # all 6 assets
    build-dataset-lstm --assets btc eth               # subset
    build-dataset-lstm --label-horizon-sec 300        # change forward horizon
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.dataset as ds

ASSETS = ["btc", "eth", "sol", "xrp", "bnb", "doge"]
SPOT_SYMBOLS = {a: f"{a}usdt" for a in ASSETS}


def load_ticks(ticks_path: Path, since_hours: float | None) -> pd.DataFrame:
    """Load every Parquet partition under `ticks_path` into one DataFrame.

    Skips 0-byte files (occasional crashed-batch leftovers). When `since_hours`
    is set, the partition pruner drops files outside the (now − since_hours, now]
    window using the `date` and `hour` partition columns, so a fresh overnight
    run isn't bloated by older single-asset archives sitting in `data/ticks/`.
    """
    if not ticks_path.exists():
        raise FileNotFoundError(f"{ticks_path} does not exist — run the pipeline first")
    files = [str(p) for p in ticks_path.rglob("*.parquet") if p.stat().st_size > 0]
    if not files:
        raise FileNotFoundError(f"no non-empty parquet files under {ticks_path}")

    if since_hours is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        # Partition fields are strings (date='YYYY-MM-DD', hour='H' int-ish);
        # we filter on a synthetic 'date >= cutoff_date' which prunes whole
        # date partitions early. Within the boundary date the per-row ts
        # filter below trims the rest.
        dataset = ds.dataset(files, format="parquet", partitioning="hive")
        cutoff_date = cutoff.date().isoformat()
        try:
            dataset_filt = dataset.filter(pc.field("date") >= cutoff_date)
            table = dataset_filt.to_table()
        except Exception:
            # Older pyarrow versions reject filter() on a Dataset; fall back.
            table = dataset.to_table(filter=pc.field("date") >= cutoff_date)
        df = table.to_pandas()
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df = df[df["ts"] >= cutoff].reset_index(drop=True)
    else:
        dataset = ds.dataset(files, format="parquet", partitioning="hive")
        df = dataset.to_table().to_pandas()
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def binance_mid_per_second(df: pd.DataFrame, symbol: str) -> pd.Series:
    """Latest Binance bookTicker mid per second, forward-filled briefly."""
    mask = (
        (df["source"] == "binance")
        & (df["ident"] == symbol)
        & (df["type"] == "bookTicker")
    )
    sub = df.loc[mask, ["ts", "price"]].sort_values("ts").set_index("ts")
    if sub.empty:
        return pd.Series(dtype="float64", name="mid")
    return sub["price"].resample("1s").last().ffill(limit=10).rename("mid")


def polymarket_p_up_per_second(df: pd.DataFrame, asset: str) -> pd.Series:
    """Latest Polymarket {asset}-updown-5m-* Up outcome P(up) per second.

    Polymarket events come in bursts; we forward-fill up to 120 s so brief
    gaps in a single market don't drop rows. The 5-min market itself rolls
    every 5 minutes — by filtering on the slug *prefix* we capture each
    successive market in turn without needing to know the exact slug.
    """
    slug_prefix = f"{asset}-updown-5m-"
    mask = (
        (df["source"] == "polymarket")
        & df["market_slug"].fillna("").str.startswith(slug_prefix)
        & df["market_outcome"].isin(["Up", "Yes"])
    )
    sub = df.loc[mask, ["ts", "price"]].sort_values("ts").set_index("ts")
    if sub.empty:
        return pd.Series(dtype="float64", name="poly_p_up")
    return sub["price"].resample("1s").last().ffill(limit=120).rename("poly_p_up")


def build_asset_table(
    raw: pd.DataFrame, asset: str, label_horizon_sec: int
) -> pd.DataFrame:
    """Per-second feature + target table for one asset."""
    mid = binance_mid_per_second(raw, SPOT_SYMBOLS[asset])
    poly = polymarket_p_up_per_second(raw, asset)

    if mid.empty:
        return pd.DataFrame()
    if poly.empty:
        # Some assets may have no Polymarket coverage in the captured window.
        # Build the table with poly columns left as NaN; dropna will remove
        # the rows. The asset still keeps its slot in the model so downstream
        # code stays uniform.
        poly = pd.Series(np.nan, index=mid.index, name="poly_p_up")

    df = pd.concat([mid, poly], axis=1)
    df["log_return_1s"] = np.log(df["mid"] / df["mid"].shift(1))
    df["poly_p_change_1s"] = df["poly_p_up"] - df["poly_p_up"].shift(1)
    df["vol_60s"] = df["log_return_1s"].rolling(window=60, min_periods=20).std()

    future = df["mid"].shift(-label_horizon_sec)
    df["log_return_5m_target"] = np.log(future / df["mid"])

    keep = [
        "mid",
        "log_return_1s",
        "poly_p_up",
        "poly_p_change_1s",
        "vol_60s",
        "log_return_5m_target",
    ]
    df = df[keep].dropna()
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build per-asset 1s-grid LSTM dataset")
    parser.add_argument("--ticks-path", default="data/ticks")
    parser.add_argument("--output-dir", default="data/training_lstm")
    parser.add_argument("--assets", nargs="+", default=ASSETS, choices=ASSETS)
    parser.add_argument(
        "--label-horizon-sec",
        type=int,
        default=300,
        help="Forward horizon for the log-return target. 300 = 5 minutes.",
    )
    parser.add_argument(
        "--since-hours",
        type=float,
        default=None,
        help="Only load events from the last N hours. Useful when data/ticks/ "
             "contains older single-asset archives that would bloat the load.",
    )
    args = parser.parse_args()

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"loading {args.ticks_path}", end="")
    if args.since_hours is not None:
        print(f"  (last {args.since_hours} hours)")
    else:
        print()
    raw = load_ticks(Path(args.ticks_path), args.since_hours)
    span = (raw["ts"].min(), raw["ts"].max())
    print(f"  {len(raw):,} raw events from {span[0]} → {span[1]}")

    summary = []
    for asset in args.assets:
        df = build_asset_table(raw, asset, args.label_horizon_sec)
        out = out_dir / f"{asset}.parquet"
        df.to_parquet(out)
        summary.append((asset, len(df), out))
        print(f"  {asset:<5}  rows={len(df):>7,}  → {out}")

    total = sum(n for _, n, _ in summary)
    print(f"\ntotal rows across {len(summary)} assets: {total:,}")
    if total == 0:
        print(
            "\nWARNING: 0 rows produced. Common causes:\n"
            "  - data/ticks/ contains data from a single-asset capture\n"
            "  - producers were not running with --assets btc eth sol xrp bnb doge\n"
            "  - capture window is shorter than the 60s + 5min minimum"
        )


if __name__ == "__main__":
    main()
