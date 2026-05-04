#!/usr/bin/env python3
"""
score_stream.py — live SARIMAX scoring → btc.sarimax-forecast.

Subscribes to binance.book + polymarket.events via Kafka, maintains a
rolling 1-minute snapshot buffer, computes the same feature set as
build_dataset.py, and emits 1-step-ahead price forecasts from the trained
SARIMAX model every minute.

Output topic btc.sarimax-forecast mirrors the schema of btc.forecast
(emitted by spark_stream.py) so Grafana can plot a third line alongside
actual BTC and Polymarket-implied.

Requires: pip install -e ".[all]"  (needs statsmodels + aiokafka)

Usage:
    score-stream
    score-stream --kafka-bootstrap localhost:9092 --debug
    score-stream --model data/model.sarimax.pkl --features data/feature_list.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import time
import warnings
from collections import deque
from pathlib import Path
from typing import NamedTuple

import statsmodels.api as sm
from aiokafka import AIOKafkaConsumer
from rich.console import Console

from common import KafkaSink

INPUT_TOPICS = ["binance.book", "polymarket.events"]
OUTPUT_TOPIC = "btc.sarimax-forecast"
SLUG_PREFIX = "btc-updown-5m-"

console = Console(legacy_windows=False)


# ---------------------------------------------------------------------------
# Feature state — mirrors build_dataset.py
# ---------------------------------------------------------------------------

class MinuteBar(NamedTuple):
    ts: float        # unix ts of minute boundary (multiple of 60)
    btc_mid: float
    poly_p_up: float


class FeatureState:
    """Rolling 1-minute bar buffer with the same feature logic as build_dataset.py.

    Features at bar T:
      btc_return_5m[T]     = log(btc_mid[T] / btc_mid[T-5])
      btc_volatility_5m[T] = sample std of btc_return_5m[T-4 : T+1]
      poly_p_up_change_5m  = poly_p_up[T] - poly_p_up[T-5]

    Needs 10 bars of history before the first exog vector can be emitted.
    """

    def __init__(self) -> None:
        self._btc: dict[int, float] = {}   # second_bucket → latest btc_mid
        self._poly: dict[int, float] = {}  # second_bucket → latest poly_p_up
        self.bars: deque[MinuteBar] = deque(maxlen=12)
        self._last_min: int = -1

    def record_btc(self, ts: float, mid: float) -> None:
        bucket = int(ts)
        self._btc[bucket] = mid
        cutoff = bucket - 120
        for k in [k for k in list(self._btc) if k < cutoff]:
            del self._btc[k]

    def record_poly(self, ts: float, p_up: float) -> None:
        bucket = int(ts)
        self._poly[bucket] = p_up
        cutoff = bucket - 360
        for k in [k for k in list(self._poly) if k < cutoff]:
            del self._poly[k]

    def _latest(self, store: dict[int, float], before_ts: int, max_stale: int) -> float | None:
        for k in range(before_ts, before_ts - max_stale - 1, -1):
            if k in store:
                return store[k]
        return None

    def maybe_close_bar(self, ts: float) -> bool:
        """Close a 1-minute bar when ts crosses a minute boundary. Returns True if closed."""
        cur_min = int(ts) // 60
        if cur_min <= self._last_min:
            return False
        self._last_min = cur_min
        t = cur_min * 60
        btc = self._latest(self._btc, t, max_stale=30)
        poly = self._latest(self._poly, t, max_stale=180)
        if btc is None or poly is None:
            return False
        self.bars.append(MinuteBar(ts=float(t), btc_mid=btc, poly_p_up=poly))
        return True

    def compute_exog(self) -> list[float] | None:
        """Return [poly_p_up, poly_p_up_change_5m, btc_volatility_5m] or None."""
        if len(self.bars) < 10:
            return None
        bars = list(self.bars)
        btc = [b.btc_mid for b in bars]
        poly = [b.poly_p_up for b in bars]

        # 5-min log returns at each bar i (bar[i] vs bar[i-5])
        ret5m: list[float] = []
        for i in range(5, len(btc)):
            prev, curr = btc[i - 5], btc[i]
            ret5m.append(math.log(curr / prev) if prev > 0 and curr > 0 else 0.0)

        if len(ret5m) < 5:
            return None

        last5 = ret5m[-5:]
        mean_r = sum(last5) / 5
        var_r = sum((r - mean_r) ** 2 for r in last5) / 4  # sample variance
        vol = math.sqrt(var_r) if var_r > 0 else 0.0

        poly_now = poly[-1]
        poly_5ago = poly[-6] if len(poly) >= 6 else poly[0]

        return [poly_now, poly_now - poly_5ago, vol]


# ---------------------------------------------------------------------------
# Message parsing — mirrors spark_stream.py extract_price_events
# ---------------------------------------------------------------------------

def extract_btc_mid(msg: dict) -> float | None:
    if msg.get("source") != "binance":
        return None
    if msg.get("type") != "bookTicker":
        return None
    if (msg.get("symbol") or "").lower() != "btcusdt":
        return None
    payload = msg.get("payload") or {}
    try:
        return (float(payload["b"]) + float(payload["a"])) / 2
    except (KeyError, TypeError, ValueError):
        return None


def extract_poly_p_up(msg: dict) -> float | None:
    if msg.get("source") != "polymarket":
        return None
    market = msg.get("market") or {}
    if not (market.get("slug") or "").startswith(SLUG_PREFIX):
        return None
    if (market.get("outcome") or "") not in ("Up", "Yes"):
        return None
    payload = msg.get("payload") or {}
    try:
        return float(payload["price"])
    except (KeyError, TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Scoring loop
# ---------------------------------------------------------------------------

async def score_loop(
    bootstrap: str,
    model,
    sigma_log: float,
    state: FeatureState,
    sink: KafkaSink,
    debug: bool,
) -> None:
    consumer = AIOKafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode()) if b else None,
        auto_offset_reset="latest",
        group_id=None,
    )
    await consumer.start()
    console.print(f"[green]Consumer ready → {', '.join(INPUT_TOPICS)}[/green]")

    msg_count = 0
    forecast_count = 0
    try:
        async for record in consumer:
            msg = record.value
            if not isinstance(msg, dict):
                continue

            ts = msg.get("recv_ts") or time.time()
            msg_count += 1

            mid = extract_btc_mid(msg)
            if mid is not None:
                state.record_btc(ts, mid)

            p_up = extract_poly_p_up(msg)
            if p_up is not None:
                state.record_poly(ts, p_up)

            if not state.maybe_close_bar(ts):
                continue

            exog = state.compute_exog()
            if exog is None:
                if debug:
                    console.print(f"[dim]bar closed, {len(state.bars)}/10 history[/dim]")
                continue

            btc_mid = state.bars[-1].btc_mid
            poly_prob = state.bars[-1].poly_p_up
            bar_ts = state.bars[-1].ts

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    log_ret = float(model.forecast(steps=1, exog=[exog]).iloc[0])
            except Exception as exc:
                if debug:
                    console.print(f"[red]forecast error: {type(exc).__name__}: {exc}[/red]")
                continue

            # btc_mid * (1 + log_return) ≈ btc_mid * exp(log_return) for small returns
            sarimax_forecast = btc_mid * (1.0 + log_ret)

            await sink.send(
                OUTPUT_TOPIC,
                {
                    "ts": bar_ts,
                    "btc_mid": btc_mid,
                    "poly_prob": poly_prob,
                    "log_return_pred": log_ret,
                    "sarimax_forecast": sarimax_forecast,
                    "sigma_log": sigma_log,
                },
                key="btc",
            )
            forecast_count += 1

            console.print(
                f"[bold cyan]▸ SARIMAX[/bold cyan]  "
                f"btc={btc_mid:>10,.2f}  "
                f"forecast={sarimax_forecast:>10,.2f}  "
                f"Δ={log_ret * 10_000:>+7.1f} bps  "
                f"P(up)={poly_prob:.3f}  "
                f"[dim]msgs={msg_count:,} forecasts={forecast_count}[/dim]"
            )

    finally:
        await consumer.stop()


async def _run(bootstrap: str, model, sigma_log: float, debug: bool) -> None:
    state = FeatureState()
    backoff = 1.0
    async with KafkaSink(bootstrap) as sink:
        console.print(f"[green]Kafka → {bootstrap}[/green]")
        while True:
            t0 = time.monotonic()
            try:
                await score_loop(bootstrap, model, sigma_log, state, sink, debug)
                uptime = time.monotonic() - t0
                console.print(f"[yellow]consumer closed after {uptime:.0f}s[/yellow]")
            except Exception as exc:
                uptime = time.monotonic() - t0
                console.print(
                    f"[red]error after {uptime:.0f}s: {type(exc).__name__}: {exc}[/red]"
                )
            if uptime > 30:
                backoff = 1.0
            console.print(f"[yellow]reconnecting in {backoff:.0f}s…[/yellow]")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Live SARIMAX scorer → btc.sarimax-forecast")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--model", default="data/model.sarimax.pkl")
    parser.add_argument("--features", default="data/feature_list.json")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    model_path = Path(args.model)
    if not model_path.exists():
        console.print(f"[red]Model not found: {model_path}  →  run: train-model[/red]")
        raise SystemExit(1)

    console.print(f"[cyan]Loading model from {model_path}…[/cyan]")
    model = sm.load(str(model_path))
    meta = json.loads(Path(args.features).read_text())
    sigma_log = meta["sigma_log"]
    order = tuple(meta["order"])
    console.print(f"[green]SARIMAX{order}  σ_log={sigma_log * 10_000:.2f} bps[/green]")

    try:
        asyncio.run(_run(args.kafka_bootstrap, model, sigma_log, args.debug))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
