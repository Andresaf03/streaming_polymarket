#!/usr/bin/env python3
"""
score_lstm_stream.py — live multi-asset LSTM scorer.

Loads `data/model.lstm.pt` + `data/lstm_meta.json`, subscribes to
`binance.book` and `polymarket.events`, maintains per-asset rolling event
buffers, and every 1 second:

  1. Builds (B, seq_len, F) feature tensor batched across all assets that
     have enough history.
  2. Runs ONE forward pass through the joint LSTM. This is the GPU win:
     amortizing launch overhead across all 6 assets per tick.
  3. Emits one message per asset to `lstm.forecast.clean` (partition 0)
     with schema:
        {ts, asset, mid, pred_log_return, pred_price, poly_p_up}

Cold-start: each asset needs ~seq_len + 60s of history before the first
emission.

Device: cuda > mps > cpu, picked automatically.

Usage:
    score-lstm
    score-lstm --kafka-bootstrap localhost:9092 --debug
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from aiokafka import AIOKafkaConsumer
from rich.console import Console

from common import KafkaSink

console = Console(legacy_windows=False)

INPUT_TOPICS = ["binance.book", "polymarket.events"]
OUTPUT_TOPIC = "lstm.forecast.clean"
ASSETS_DEFAULT = ["btc", "eth", "sol", "xrp", "bnb", "doge"]
SPOT_SYMBOLS = {a: f"{a}usdt" for a in ASSETS_DEFAULT}


# ------------------------------------------------------------ model loading

# Imported lazily so the module is importable without torch when only running
# `--help`.
def _load_model_class():
    from modeling.train_lstm import JointAssetLSTM  # noqa: WPS433
    return JointAssetLSTM


def load_model_and_meta(model_path: Path, meta_path: Path, device: torch.device):
    if not model_path.exists():
        raise FileNotFoundError(f"{model_path} missing — run train-lstm first")
    if not meta_path.exists():
        raise FileNotFoundError(f"{meta_path} missing — run train-lstm first")
    meta = json.loads(meta_path.read_text())
    JointAssetLSTM = _load_model_class()
    model = JointAssetLSTM(
        num_assets=len(meta["assets"]),
        embed_dim=meta["embed_dim"],
        num_features=len(meta["feature_cols"]),
        hidden=meta["hidden"],
        num_layers=meta["num_layers"],
        dropout=meta["dropout"],
    ).to(device)
    model.load_state_dict(torch.load(str(model_path), map_location=device))
    model.eval()
    return model, meta


# ------------------------------------------------------------ feature buffer

@dataclass
class AssetBuffer:
    asset: str
    capacity_seconds: int

    def __post_init__(self) -> None:
        self.mids: deque = deque(maxlen=self.capacity_seconds * 5)
        self.polys: deque = deque(maxlen=self.capacity_seconds * 5)

    def add_mid(self, ts: float, mid: float) -> None:
        self.mids.append((ts, mid))

    def add_poly(self, ts: float, p_up: float) -> None:
        self.polys.append((ts, p_up))

    def latest_mid(self) -> float | None:
        return self.mids[-1][1] if self.mids else None

    def latest_poly(self) -> float | None:
        return self.polys[-1][1] if self.polys else None

    def feature_sequence(
        self, now: float, seq_len: int, means: np.ndarray, stds: np.ndarray
    ) -> np.ndarray | None:
        """Build a (seq_len, F) standardized feature window ending at `now`.

        Returns None if buffers don't yet have enough history. Mirrors
        `build_dataset_lstm` exactly: 1-second resample with brief ffill.
        """
        if not self.mids or not self.polys:
            return None

        window = seq_len + 65  # extra for vol_60s rolling at the start
        cutoff = now - window
        mids = [(t, m) for (t, m) in self.mids if t >= cutoff]
        polys = [(t, p) for (t, p) in self.polys if t >= cutoff]
        if len(mids) < seq_len or len(polys) < 1:
            return None

        end_ts = pd.Timestamp(now, unit="s", tz="UTC").floor("1s")
        start_ts = end_ts - pd.Timedelta(seconds=window - 1)
        grid = pd.date_range(start_ts, end_ts, freq="1s", tz="UTC")

        mid_s = (
            pd.Series(
                {pd.Timestamp(t, unit="s", tz="UTC"): m for (t, m) in mids}
            )
            .sort_index()
            .resample("1s")
            .last()
            .reindex(grid)
            .ffill(limit=10)
            .rename("mid")
        )
        poly_s = (
            pd.Series(
                {pd.Timestamp(t, unit="s", tz="UTC"): p for (t, p) in polys}
            )
            .sort_index()
            .resample("1s")
            .last()
            .reindex(grid)
            .ffill(limit=120)
            .rename("poly_p_up")
        )

        df = pd.concat([mid_s, poly_s], axis=1)
        df["log_return_1s"] = np.log(df["mid"] / df["mid"].shift(1))
        df["poly_p_change_1s"] = df["poly_p_up"] - df["poly_p_up"].shift(1)
        df["vol_60s"] = df["log_return_1s"].rolling(60, min_periods=20).std()

        feat = df[["log_return_1s", "poly_p_up", "poly_p_change_1s", "vol_60s"]]
        if len(feat) < seq_len:
            return None
        feat_arr = feat.iloc[-seq_len:].to_numpy(dtype=np.float32)
        if not np.isfinite(feat_arr).all():
            return None
        return ((feat_arr - means) / stds).astype(np.float32)


# ------------------------------------------------------------ async loops

def slug_prefix(asset: str) -> str:
    return f"{asset}-updown-5m-"


def detect_asset_from_slug(slug: str) -> str | None:
    for asset in ASSETS_DEFAULT:
        if slug.startswith(slug_prefix(asset)):
            return asset
    return None


async def ingest_loop(consumer: AIOKafkaConsumer, buffers: dict[str, AssetBuffer]) -> None:
    """Route incoming Kafka messages into per-asset buffers."""
    async for msg in consumer:
        env = msg.value
        if not isinstance(env, dict):
            continue
        ts = float(env.get("recv_ts", time.time()))
        source = env.get("source")

        if source == "binance" and env.get("type") == "bookTicker":
            symbol = env.get("symbol", "")
            asset = next(
                (a for a, s in SPOT_SYMBOLS.items() if s == symbol), None
            )
            if asset is None or asset not in buffers:
                continue
            payload = env.get("payload", {})
            try:
                bid = float(payload.get("b"))
                ask = float(payload.get("a"))
                buffers[asset].add_mid(ts, (bid + ask) / 2.0)
            except (TypeError, ValueError):
                continue

        elif source == "polymarket":
            market = env.get("market") or {}
            slug = market.get("slug", "") or ""
            outcome = market.get("outcome", "")
            if outcome not in ("Up", "Yes"):
                continue
            asset = detect_asset_from_slug(slug)
            if asset is None or asset not in buffers:
                continue
            payload = env.get("payload", {})
            etype = env.get("type")
            price = None
            if etype in ("price_change", "last_trade_price"):
                price = payload.get("price")
            elif etype == "book":
                price = payload.get("last_trade_price")
            if price is None:
                continue
            try:
                buffers[asset].add_poly(ts, float(price))
            except (TypeError, ValueError):
                continue


async def tick_loop(
    sink: KafkaSink,
    model,
    meta: dict,
    buffers: dict[str, AssetBuffer],
    device: torch.device,
    debug: bool,
) -> None:
    """Every 1 second, batch all assets with enough history and emit forecasts."""
    seq_len = meta["seq_len"]
    means = np.array(meta["feature_means"], dtype=np.float32)
    stds = np.array(meta["feature_stds"], dtype=np.float32)
    asset_to_idx = meta["asset_to_idx"]

    emitted = 0
    while True:
        await asyncio.sleep(1.0)
        now = time.time()

        seqs: list[np.ndarray] = []
        labels: list[str] = []
        for asset, buf in buffers.items():
            seq = buf.feature_sequence(now, seq_len, means, stds)
            if seq is None:
                continue
            seqs.append(seq)
            labels.append(asset)

        if not seqs:
            if debug and emitted == 0:
                console.print("[dim]warming up — no asset has enough history yet[/dim]")
            continue

        x = torch.from_numpy(np.stack(seqs)).to(device, non_blocking=True)
        a = torch.tensor(
            [asset_to_idx[a] for a in labels], dtype=torch.long, device=device
        )
        with torch.no_grad():
            preds, _, _ = model(x, a)
        pred_arr = preds.detach().cpu().numpy()

        for asset, pred_log_return in zip(labels, pred_arr):
            mid = buffers[asset].latest_mid()
            poly = buffers[asset].latest_poly()
            if mid is None:
                continue
            pred_price = float(mid * np.exp(float(pred_log_return)))
            msg = {
                "ts": now,
                "asset": asset.upper(),
                "mid": float(mid),
                "pred_log_return": float(pred_log_return),
                "pred_price": pred_price,
                "poly_p_up": float(poly) if poly is not None else None,
            }
            await sink.send(OUTPUT_TOPIC, msg, key=asset.upper(), partition=0)
            emitted += 1

        if debug and emitted % 60 == 0:
            console.print(
                f"[dim]emitted={emitted}  active_assets={len(labels)}  "
                f"seqs={[a.upper() for a in labels]}[/dim]"
            )


async def run(
    bootstrap: str, model_path: Path, meta_path: Path, debug: bool
) -> None:
    device = (
        torch.device("cuda") if torch.cuda.is_available()
        else torch.device("mps") if torch.backends.mps.is_available()
        else torch.device("cpu")
    )
    console.print(f"[cyan]device:[/cyan] {device}")
    model, meta = load_model_and_meta(model_path, meta_path, device)
    console.print(
        f"[green]loaded {model_path.name}[/green]  "
        f"seq_len={meta['seq_len']}  hidden={meta['hidden']}  "
        f"assets={meta['assets']}"
    )

    capacity_seconds = max(meta["seq_len"], 60) + 60
    buffers = {asset: AssetBuffer(asset, capacity_seconds) for asset in meta["assets"]}

    consumer = AIOKafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode()) if b else None,
        auto_offset_reset="latest",
        group_id=None,
    )
    await consumer.start()
    console.print(f"[green]consumer connected → {bootstrap}[/green]")

    try:
        async with KafkaSink(bootstrap) as sink:
            console.print(f"[green]producer → {sink.bootstrap}[/green]  topic={OUTPUT_TOPIC}")
            await asyncio.gather(
                ingest_loop(consumer, buffers),
                tick_loop(sink, model, meta, buffers, device, debug),
            )
    finally:
        await consumer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live multi-asset LSTM scorer")
    parser.add_argument("--model", default="data/model.lstm.pt")
    parser.add_argument("--meta", default="data/lstm_meta.json")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(run(args.kafka_bootstrap, Path(args.model), Path(args.meta), args.debug))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
