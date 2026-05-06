#!/usr/bin/env python3
"""
binance_producer.py — Binance WebSocket → Kafka producer.

Publishes aggTrade to `binance.trades`; bookTicker and depth streams to
`binance.book`. Uses the shared envelope/KafkaSink/RateTracker helpers.

Usage:
    python producers/binance_producer.py
    python producers/binance_producer.py --symbols btcusdt ethusdt
    python producers/binance_producer.py --streams aggTrade bookTicker
    python producers/binance_producer.py --kafka-bootstrap localhost:9092 --debug
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from typing import Iterable

import websockets
from rich.console import Console

from common import KafkaSink, NullSink, RateTracker, envelope, log_progress, ssl_context

BINANCE_WS = "wss://stream.binance.com:9443/stream"
DEFAULT_SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
DEFAULT_STREAMS = ["aggTrade", "bookTicker", "depth20@100ms"]
TRADE_TOPIC = "binance.trades"
BOOK_TOPIC = "binance.book"

console = Console(legacy_windows=False)


def build_stream_url(symbols: Iterable[str], streams: Iterable[str]) -> str:
    parts = [f"{s.lower()}@{st}" for s in symbols for st in streams]
    return f"{BINANCE_WS}?streams={'/'.join(parts)}"


def route_topic(stream_type: str) -> str:
    return TRADE_TOPIC if stream_type == "aggTrade" else BOOK_TOPIC


def parse_stream(stream_name: str) -> tuple[str, str]:
    """'btcusdt@depth20@100ms' → ('btcusdt', 'depth20@100ms')."""
    symbol, _, rest = stream_name.partition("@")
    return symbol, rest


async def publish(raw: bytes | str, sink: KafkaSink, tracker: RateTracker) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return
    stream_name = msg.get("stream", "")
    data = msg.get("data")
    if not stream_name or data is None:
        return

    symbol, stream_type = parse_stream(stream_name)
    env = envelope("binance", stream_type, data, symbol=symbol)
    await sink.send(route_topic(stream_type), env, key=symbol)
    tracker.record(label=stream_type)


async def stream(ws, sink: KafkaSink, tracker: RateTracker, log_every: int) -> None:
    async for raw in ws:
        await publish(raw, sink, tracker)
        if tracker.total % log_every == 0:
            log_progress(console, tracker)


async def _stream_with_reconnect(
    url: str, sink: KafkaSink, tracker: RateTracker, log_every: int
) -> None:
    """Run `stream()` forever, reconnecting on any WS error with backoff.

    Backoff schedule: 1s → 2s → 4s → 8s → 16s → 32s → 60s cap. Resets to 1s
    whenever a connection stayed up for more than 30s, so transient flaps
    don't keep stretching the wait. open_timeout is bumped to 30s because
    Binance's TLS handshake occasionally exceeds the 10s default.
    """
    backoff = 1.0
    while True:
        connect_start = time.monotonic()
        try:
            async with websockets.connect(
                url,
                ssl=ssl_context(),
                open_timeout=30,
                ping_interval=180,
                ping_timeout=600,
                max_size=10 * 1024 * 1024,
            ) as ws:
                console.print("[bold green]Streaming…[/bold green]")
                await stream(ws, sink, tracker, log_every)
            uptime = time.monotonic() - connect_start
            console.print(
                f"[yellow]binance WS closed normally after {uptime:.0f}s[/yellow]"
            )
        except Exception as exc:
            uptime = time.monotonic() - connect_start
            console.print(
                f"[red]binance WS error after {uptime:.0f}s: "
                f"{type(exc).__name__}: {exc}[/red]"
            )

        if uptime > 30:
            backoff = 1.0
        console.print(f"[yellow]reconnecting in {backoff:.0f}s…[/yellow]")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


async def run(
    bootstrap: str, symbols: list[str], streams: list[str], debug: bool, dry_run: bool
) -> None:
    url = build_stream_url(symbols, streams)
    log_every = 50 if debug else 1000

    console.print(f"[cyan]Binance:[/cyan] {len(symbols)} symbols × {len(streams)} streams")
    console.print(f"[dim]{url[:160]}{'…' if len(url) > 160 else ''}[/dim]")

    tracker = RateTracker()
    if dry_run:
        console.print("[yellow]DRY RUN: reading WebSocket and parsing, not writing Kafka[/yellow]")
        await _stream_with_reconnect(url, NullSink(), tracker, log_every)
        return

    async with KafkaSink(bootstrap) as sink:
        console.print(f"[green]Kafka → {sink.bootstrap}[/green]")
        await _stream_with_reconnect(url, sink, tracker, log_every)


def main() -> None:
    parser = argparse.ArgumentParser(description="Binance WS → Kafka producer")
    parser.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    parser.add_argument("--streams", nargs="+", default=DEFAULT_STREAMS)
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and parse the WebSocket stream without writing to Kafka",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    try:
        asyncio.run(
            run(
                bootstrap=args.kafka_bootstrap,
                symbols=[s.lower() for s in args.symbols],
                streams=args.streams,
                debug=args.debug,
                dry_run=args.dry_run,
            )
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
