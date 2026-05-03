#!/usr/bin/env python3
"""
build_dataset.py — turn the Parquet tick archive into a (features, target) table.

Reads `data/ticks/` (partitioned by source/date/hour), extracts BTC mid +
Polymarket BTC-5m P(up), aligns them on a 1-minute snapshot grid, and writes
a compact Parquet ready for `train-model`.

Target: `log_return_5m = log(BTC_mid_T+5 / BTC_mid_T)`. Continuous (regression).
Picked over a binary "did it go up?" label because the dashboard already shows
a market-implied *price* (drift derived from Polymarket P(up) × σ), and we want
the model's output to live on the same axis — three lines, one chart, one
unit. Sign-of-prediction can still drive a directional / calibration view if
needed.

Usage:
    build-dataset                                # data/ticks → data/training.parquet
    build-dataset --ticks-path data/ticks --output data/training.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds


def load_ticks(ticks_path: Path) -> pd.DataFrame:
    """Load every Parquet partition under `ticks_path` into one DataFrame.

    Skips 0-byte files (occasional leftovers from crashed Spark micro-batches),
    and uses pyarrow.dataset with `partitioning='hive'` so the path-encoded
    partition columns (`source=…`, `date=…`, `hour=…`) are restored.
    """
    if not ticks_path.exists():
        raise FileNotFoundError(f"{ticks_path} does not exist — run the pipeline first")

    files = [str(p) for p in ticks_path.rglob("*.parquet") if p.stat().st_size > 0]
    if not files:
        raise FileNotFoundError(f"no non-empty parquet files under {ticks_path}")

    dataset = ds.dataset(files, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def btc_mid_per_second(df: pd.DataFrame) -> pd.Series:
    """Latest Binance BTCUSDT bookTicker mid per second, forward-filled briefly."""
    mask = (
        (df["source"] == "binance")
        & (df["ident"] == "btcusdt")
        & (df["type"] == "bookTicker")
    )
    s = (
        df.loc[mask, ["ts", "price"]]
        .sort_values("ts")
        .set_index("ts")["price"]
        .resample("1s")
        .last()
    )
    return s.ffill(limit=10)


def poly_p_up_per_second(df: pd.DataFrame) -> pd.Series:
    """Latest Polymarket BTC-5m P(up) per second.

    Filters to the rolling btc-updown-5m-* market series and the 'Up' (or 'Yes')
    outcome token, then takes the latest price each second. Polymarket events
    come in bursts; we forward-fill up to 120 s so brief gaps don't drop rows.
    """
    mask = (
        (df["source"] == "polymarket")
        & df["market_slug"].fillna("").str.startswith("btc-updown-5m-")
        & df["market_outcome"].isin(["Up", "Yes"])
    )
    s = (
        df.loc[mask, ["ts", "price"]]
        .sort_values("ts")
        .set_index("ts")["price"]
        .resample("1s")
        .last()
    )
    return s.ffill(limit=120)


def build_snapshots(
    btc_1s: pd.Series, poly_1s: pd.Series, label_horizon_min: int = 5
) -> pd.DataFrame:
    """Sample features on a 1-minute grid; label = sign of the next-5-min BTC return."""
    if btc_1s.empty:
        raise ValueError("no BTC mid data — check the Parquet contents")

    start = btc_1s.index.min().ceil("1min")
    end = btc_1s.index.max().floor("1min")
    grid = pd.date_range(start, end, freq="1min", tz="UTC")

    df = pd.DataFrame(
        {
            "btc_mid": btc_1s.reindex(grid),
            "poly_p_up": poly_1s.reindex(grid),
        }
    )

    # Features
    df["btc_return_5m"] = np.log(df["btc_mid"] / df["btc_mid"].shift(5))
    df["btc_volatility_5m"] = df["btc_return_5m"].rolling(window=5).std()
    df["poly_p_up_change_5m"] = df["poly_p_up"] - df["poly_p_up"].shift(5)

    # Regression target = forward 5-minute log return
    future = df["btc_mid"].shift(-label_horizon_min)
    df["log_return_5m"] = np.log(future / df["btc_mid"])

    # Drop rows missing any feature or target
    needed = [
        "btc_mid",
        "btc_return_5m",
        "btc_volatility_5m",
        "poly_p_up",
        "poly_p_up_change_5m",
        "log_return_5m",
    ]
    df = df.dropna(subset=needed)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Build training table from Parquet ticks")
    parser.add_argument("--ticks-path", default="data/ticks")
    parser.add_argument("--output", default="data/training.parquet")
    parser.add_argument("--label-horizon-min", type=int, default=5)
    args = parser.parse_args()

    ticks = Path(args.ticks_path)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"loading {ticks}")
    raw = load_ticks(ticks)
    print(f"  {len(raw):,} raw events from {raw['ts'].min()} → {raw['ts'].max()}")

    btc_1s = btc_mid_per_second(raw)
    poly_1s = poly_p_up_per_second(raw)
    print(f"  BTC mids (1s): {btc_1s.notna().sum():,}")
    print(f"  Poly P(up) (1s): {poly_1s.notna().sum():,}")

    df = build_snapshots(btc_1s, poly_1s, args.label_horizon_min)
    print(f"  training rows after dropna: {len(df):,}")

    df.to_parquet(out)
    print(f"\nwrote {out}")
    print(f"\nlog_return_5m distribution (bps):")
    print((df["log_return_5m"] * 10_000).describe().round(2).to_string())
    print("\nfeature summary:")
    cols = [c for c in df.columns if c != "log_return_5m"]
    print(df[cols].describe().round(4).to_string())


if __name__ == "__main__":
    main()
