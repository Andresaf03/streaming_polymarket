#!/usr/bin/env python3
"""
score_stream.py — live SARIMAX scoring -> btc.sarimax-forecast.clean.

Subscribes to Spark's dashboard-ready btc.forecast.clean Kafka topic, maintains
a rolling 1-minute snapshot buffer, computes the same feature set as
build_dataset.py, and emits 1-step-ahead price forecasts from the trained
SARIMAX model on the same per-second timestamps Grafana already plots.

Output topic btc.sarimax-forecast.clean only carries SARIMAX-specific prediction
fields. The BTC midline and Polymarket-implied forecast stay on
btc.forecast.clean, which keeps Grafana from drawing duplicate BTC values from
both topics.

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
from datetime import UTC, datetime
from pathlib import Path
from typing import NamedTuple

from common import configure_utf8_console
from rich.console import Console

configure_utf8_console()

DEFAULT_INPUT_TOPIC = "btc.forecast.clean"
DEFAULT_OUTPUT_TOPIC = "btc.sarimax-forecast.clean"
EXOG_COLS = ["poly_p_up", "poly_p_up_change_5m", "btc_volatility_5m"]
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "data/model.sarimax.pkl"
DEFAULT_FEATURES = "data/feature_list.json"

console = Console(legacy_windows=False)


# ---------------------------------------------------------------------------
# Feature state — mirrors build_dataset.py
# ---------------------------------------------------------------------------

class MinuteBar(NamedTuple):
    ts: float        # unix timestamp for a second snapshot or closed minute bar
    btc_mid: float
    poly_p_up: float


class FeatureState:
    """Rolling 1-minute bar buffer with the same feature logic as build_dataset.py.

    Features at bar T:
      btc_return_5m[T]     = log(btc_mid[T] / btc_mid[T-5])
      btc_volatility_5m[T] = sample std of btc_return_5m[T-4 : T+1]
      poly_p_up_change_5m  = poly_p_up[T] - poly_p_up[T-5]

    Exact training-matched features need 10 consecutive 1-minute bars. For the
    live dashboard we can also emit a warm-start exog vector using priors until
    enough history exists.
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
        """Return EXOG_COLS values or None."""
        if len(self.bars) < 10:
            return None
        bars = list(self.bars)[-10:]
        for prev, curr in zip(bars, bars[1:]):
            if abs((curr.ts - prev.ts) - 60.0) > 1e-6:
                return None

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

    def compute_live_exog(
        self, current: MinuteBar, sigma_log: float, warm_start: bool
    ) -> tuple[list[float], str] | None:
        """Return live exog plus mode: `exact` once enough history exists, else `warm`.

        Warm mode keeps the dashboard live before the 10-minute exact feature
        window is available. It uses the trained sigma_log as a volatility prior
        and the earliest available Polymarket probability as the change anchor.
        """
        exact = self.compute_exog()
        if exact is not None:
            return exact, "exact"
        if not warm_start:
            return None

        bars = [b for b in self.bars if b.ts <= current.ts]
        if not bars or bars[-1].ts < current.ts:
            bars.append(current)

        poly_now = current.poly_p_up
        anchor_ts = current.ts - 300.0
        poly_anchor = None
        for bar in reversed(bars):
            if bar.ts <= anchor_ts:
                poly_anchor = bar.poly_p_up
                break
        if poly_anchor is None:
            poly_anchor = bars[0].poly_p_up

        vol = sigma_log
        returns: list[float] = []
        for prev, curr in zip(bars, bars[1:]):
            if prev.btc_mid > 0 and curr.btc_mid > 0:
                returns.append(math.log(curr.btc_mid / prev.btc_mid))
        if len(returns) >= 2:
            # Scale short-horizon 1-minute-ish returns toward a 5-minute prior.
            recent = returns[-5:]
            mean_r = sum(recent) / len(recent)
            denom = max(len(recent) - 1, 1)
            var_r = sum((r - mean_r) ** 2 for r in recent) / denom
            vol = max(math.sqrt(var_r) * math.sqrt(5), sigma_log * 0.25)

        return [poly_now, poly_now - poly_anchor, vol], "warm"


class LiveSarimax:
    """Mutable SARIMAX wrapper for live walk-forward state updates.

    The trained target is a 5-minute forward return. A bar at T only becomes a
    realized training observation when bar T+5 arrives, so we keep the exog
    vector for each forecast and append it once the target is known.
    """

    def __init__(self, model, horizon_bars: int = 5) -> None:
        self.model = model
        self.horizon_bars = horizon_bars
        self.pending_exog: dict[float, list[float]] = {}
        self.state_updates = 0
        self.state_failures = 0

    def remember_exog(self, bar_ts: float, exog: list[float]) -> None:
        self.pending_exog[bar_ts] = exog
        cutoff = bar_ts - 3600
        for ts in [ts for ts in self.pending_exog if ts < cutoff]:
            del self.pending_exog[ts]

    def append_realized(self, bars: deque[MinuteBar], debug: bool) -> float | None:
        if len(bars) <= self.horizon_bars:
            return None

        current = bars[-1]
        origin = bars[-(self.horizon_bars + 1)]
        exog = self.pending_exog.pop(origin.ts, None)
        if exog is None:
            return None
        if abs((current.ts - origin.ts) - self.horizon_bars * 60.0) > 1e-6:
            return None

        if origin.btc_mid <= 0 or current.btc_mid <= 0:
            realized = 0.0
        else:
            realized = math.log(current.btc_mid / origin.btc_mid)

        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                self.model = self.model.append([realized], exog=[exog], refit=False)
            self.state_updates += 1
            return realized
        except Exception as exc:
            self.state_failures += 1
            if self.state_failures <= 3 or debug:
                console.print(f"[red]state update error: {type(exc).__name__}: {exc}[/red]")
            return None

    def forecast(self, exog: list[float]) -> float:
        return float(self.model.forecast(steps=1, exog=[exog]).iloc[0])


# ---------------------------------------------------------------------------
# Message parsing — consumes spark_stream.py forecast_stats output
# ---------------------------------------------------------------------------

def parse_forecast_ts(raw: object) -> float | None:
    if isinstance(raw, (int, float)):
        return float(raw)
    if not isinstance(raw, str) or not raw.strip():
        return None

    value = raw.strip()
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        try:
            return float(raw)
        except ValueError:
            return None


def extract_forecast_point(msg: dict) -> MinuteBar | None:
    ts_raw = msg.get("ts_unix")
    if ts_raw is None:
        ts_raw = msg.get("ts")
    ts = parse_forecast_ts(ts_raw)
    if ts is None:
        return None

    try:
        btc_mid = float(msg["btc_mid"])
        poly_p_up = float(msg["poly_prob"])
    except (KeyError, TypeError, ValueError):
        return None
    if not (math.isfinite(btc_mid) and math.isfinite(poly_p_up)):
        return None
    return MinuteBar(ts=float(int(ts)), btc_mid=btc_mid, poly_p_up=poly_p_up)


# ---------------------------------------------------------------------------
# Scoring loop
# ---------------------------------------------------------------------------

async def score_loop(
    bootstrap: str,
    input_topic: str,
    input_offset_reset: str,
    output_topic: str,
    live_model: LiveSarimax,
    sigma_log: float,
    state: FeatureState,
    sink: KafkaSink,
    debug: bool,
    emit_interval: float,
    warm_start: bool,
) -> None:
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        input_topic,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode()) if b else None,
        auto_offset_reset=input_offset_reset,
        group_id=None,
    )
    await consumer.start()
    console.print(f"[green]Consumer ready -> {input_topic}[/green]")

    msg_count = 0
    forecast_count = 0
    forecast_failures = 0
    last_emit_ts = 0.0
    try:
        async for record in consumer:
            msg = record.value
            if not isinstance(msg, dict):
                continue

            snapshot = extract_forecast_point(msg)
            if snapshot is None:
                continue

            msg_count += 1
            ts = snapshot.ts

            state.record_btc(ts, snapshot.btc_mid)
            state.record_poly(ts, snapshot.poly_p_up)

            realized = None
            if state.maybe_close_bar(ts):
                realized = live_model.append_realized(state.bars, debug)
                closed_bar = state.bars[-1]
                closed_exog = state.compute_live_exog(closed_bar, sigma_log, warm_start)
                if closed_exog is not None:
                    live_model.remember_exog(closed_bar.ts, closed_exog[0])
                if debug:
                    mode = closed_exog[1] if closed_exog is not None else "waiting"
                    console.print(f"[dim]bar closed, history={len(state.bars)} mode={mode}[/dim]")

            if snapshot.ts - last_emit_ts < emit_interval:
                continue
            live_exog = state.compute_live_exog(snapshot, sigma_log, warm_start)
            if live_exog is None:
                continue
            exog, feature_mode = live_exog

            btc_mid = snapshot.btc_mid
            poly_prob = snapshot.poly_p_up
            forecast_ts = snapshot.ts

            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    log_ret = live_model.forecast(exog)
            except Exception as exc:
                forecast_failures += 1
                if forecast_failures <= 3 or debug:
                    console.print(f"[red]forecast error: {type(exc).__name__}: {exc}[/red]")
                continue

            sarimax_forecast = btc_mid * math.exp(log_ret)
            sarimax_drift = sarimax_forecast - btc_mid
            z = log_ret / max(sigma_log, 1e-12)
            sarimax_prob_up = min(
                1.0,
                max(0.0, 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))),
            )

            await sink.send_and_wait(
                output_topic,
                {
                    "ts": datetime.fromtimestamp(forecast_ts, UTC).isoformat().replace("+00:00", "Z"),
                    "ts_unix": forecast_ts,
                    "target_ts": datetime.fromtimestamp(
                        forecast_ts + 300.0, UTC
                    ).isoformat().replace("+00:00", "Z"),
                    "target_horizon_s": 300,
                    "model": "sarimax",
                    "feature_mode": feature_mode,
                    "sarimax_input_btc_mid": btc_mid,
                    "sarimax_poly_prob": poly_prob,
                    "log_return_pred": log_ret,
                    "sarimax_forecast": sarimax_forecast,
                    "sarimax_drift": sarimax_drift,
                    "sarimax_prob_up": sarimax_prob_up,
                    "sigma_log": sigma_log,
                },
                key="btc",
            )
            last_emit_ts = forecast_ts
            forecast_count += 1

            console.print(
                f"[bold cyan]▸ SARIMAX[/bold cyan]  "
                f"btc={btc_mid:>10,.2f}  "
                f"forecast={sarimax_forecast:>10,.2f}  "
                f"Δ={log_ret * 10_000:>+7.1f} bps  "
                f"P_sarimax={sarimax_prob_up:.3f}  P_poly={poly_prob:.3f}  "
                f"[dim]mode={feature_mode} msgs={msg_count:,} forecasts={forecast_count} "
                f"updates={live_model.state_updates}"
                f"{f' update_failures={live_model.state_failures}' if live_model.state_failures else ''}"
                f"{f' realized={realized * 10_000:+.1f}bps' if realized is not None else ''}[/dim]"
            )

    finally:
        await consumer.stop()


async def _run(
    bootstrap: str,
    input_topic: str,
    input_offset_reset: str,
    output_topic: str,
    model,
    sigma_log: float,
    debug: bool,
    emit_interval: float,
    warm_start: bool,
) -> None:
    from common import KafkaSink

    state = FeatureState()
    live_model = LiveSarimax(model)
    backoff = 1.0
    async with KafkaSink(bootstrap) as sink:
        console.print(f"[green]Kafka → {bootstrap}[/green]")
        while True:
            t0 = time.monotonic()
            try:
                await score_loop(
                    bootstrap,
                    input_topic,
                    input_offset_reset,
                    output_topic,
                    live_model,
                    sigma_log,
                    state,
                    sink,
                    debug,
                    emit_interval,
                    warm_start,
                )
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


def resolve_input_path(raw: str, default_relative: str) -> Path:
    """Resolve default data paths from the repo root, not the caller's cwd."""
    path = Path(raw)
    if path.is_absolute():
        return path
    if raw == default_relative:
        return REPO_ROOT / default_relative
    return path.resolve()


def require_existing_file(path: Path, label: str, hint: str) -> None:
    if path.exists():
        return
    console.print(
        f"[red]{label} not found: {path}[/red]\n"
        f"[dim]cwd:  {Path.cwd()}[/dim]\n"
        f"[dim]repo: {REPO_ROOT}[/dim]\n"
        f"[yellow]{hint}[/yellow]"
    )
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Live SARIMAX scorer -> btc.sarimax-forecast.clean")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument(
        "--input-topic",
        default=os.environ.get(
            "SARIMAX_INPUT_TOPIC",
            os.environ.get("FORECAST_TOPIC", DEFAULT_INPUT_TOPIC),
        ),
        help="Spark forecast topic to score from; timestamps are reused for Grafana alignment",
    )
    parser.add_argument(
        "--output-topic",
        default=os.environ.get("SARIMAX_FORECAST_TOPIC", DEFAULT_OUTPUT_TOPIC),
        help="Kafka topic for dashboard-ready SARIMAX forecasts",
    )
    parser.add_argument(
        "--input-offset-reset",
        choices=("earliest", "latest"),
        default=os.environ.get("SARIMAX_INPUT_OFFSET_RESET", "earliest"),
        help="Offset policy for the clean forecast topic when no group offset exists",
    )
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--features", default=DEFAULT_FEATURES)
    parser.add_argument(
        "--emit-interval",
        type=float,
        default=1.0,
        help="Seconds between dashboard forecasts once BTC and Polymarket are available",
    )
    parser.add_argument(
        "--strict-history",
        action="store_true",
        help="Wait for exact 10-minute feature history before emitting forecasts",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    model_path = resolve_input_path(args.model, DEFAULT_MODEL)
    features_path = resolve_input_path(args.features, DEFAULT_FEATURES)
    require_existing_file(model_path, "Model", "Run: train-model")
    require_existing_file(features_path, "Feature metadata", "Run: build-dataset and train-model")

    import statsmodels.api as sm

    console.print(f"[cyan]Loading model from {model_path}…[/cyan]")
    model = sm.load(str(model_path))
    meta = json.loads(features_path.read_text(encoding="utf-8"))
    if meta.get("exog") != EXOG_COLS:
        console.print(
            f"[red]Feature mismatch: model expects {meta.get('exog')}, "
            f"scorer computes {EXOG_COLS}[/red]"
        )
        raise SystemExit(1)
    sigma_log = meta["sigma_log"]
    order = tuple(meta["order"])
    console.print(f"[green]SARIMAX{order}  σ_log={sigma_log * 10_000:.2f} bps[/green]")
    if not args.strict_history:
        console.print(
            f"[cyan]Live warm start enabled: emitting every {args.emit_interval:g}s; "
            "feature_mode becomes exact after enough history.[/cyan]"
        )

    try:
        asyncio.run(
            _run(
                args.kafka_bootstrap,
                args.input_topic,
                args.input_offset_reset,
                args.output_topic,
                model,
                sigma_log,
                args.debug,
                max(args.emit_interval, 1.0),
                not args.strict_history,
            )
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
