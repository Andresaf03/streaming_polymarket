#!/usr/bin/env python3
"""
polymarket_producer.py — Polymarket CLOB WebSocket → Kafka producer.

Evolved from ws_live.py + bitcoin_5m.py. Gamma discovery lives in
polymarket_discovery. Shared helpers come from common/.

Discovery modes (mutually exclusive):
    --query KEYWORD        Top-N markets by volume filtered by keyword
    --all                  Top-100 markets by volume, no filter
    --asset btc            Rotating {asset}-updown-{window}m-* market
      [--window 5|15]

Usage:
    python producers/polymarket_producer.py --query bitcoin --top 5
    python producers/polymarket_producer.py --asset btc --window 5 --debug
    python producers/polymarket_producer.py --all --kafka-bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from typing import Iterator

import aiohttp
import websockets
from rich.console import Console
from rich.rule import Rule

from common import KafkaSink, RateTracker, envelope, log_progress, ssl_context
from producers.polymarket_discovery import (
    build_asset_map,
    fetch_top_markets,
    fetch_updown_market,
)

CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TOPIC = "polymarket.events"
WS_CONNECT_KWARGS: dict = {
    "ping_interval": 10,
    "ping_timeout": 15,
    "max_size": 10 * 1024 * 1024,
}

console = Console(legacy_windows=False)


def _slug_window_end(slug: str, window_min: int) -> float:
    """Unix seconds when the rolling-market window closes.

    Slugs look like `btc-updown-5m-1776670200`; the trailing integer is the
    window-start unix timestamp. Falls back to now + window_min minutes if
    the slug can't be parsed.
    """
    try:
        start_ts = int(slug.rsplit("-", 1)[-1])
        return float(start_ts + window_min * 60)
    except (ValueError, IndexError):
        return time.time() + window_min * 60


async def select_markets(args: argparse.Namespace) -> dict[str, dict]:
    """Run the appropriate discovery path and print a short summary."""
    connector = aiohttp.TCPConnector(ssl=ssl_context())
    async with aiohttp.ClientSession(connector=connector) as session:
        if args.asset:
            market, is_live = await fetch_updown_market(session, args.asset, args.window)
            if not market:
                console.print(f"[red]No {args.asset}-updown-{args.window}m market found[/red]")
                return {}
            if not is_live:
                console.print("[yellow]No live window right now; using next.[/yellow]")
            console.print(f"[green]Found:[/green] {market.get('question', '')}")
            console.print(f"  slug: [dim]{market.get('slug', '')}[/dim]")
            console.print(f"  resolves: [yellow]{market.get('endDate', '?')}[/yellow]")
            return build_asset_map([market])

        top_n = 100 if args.all else args.top
        query = None if args.all else args.query
        markets = await fetch_top_markets(session, query=query, top_n=top_n)
        if not markets:
            console.print("[red]No markets match.[/red]")
            return {}

        console.print(f"[green]Found {len(markets)} markets[/green]")
        for m in markets[:8]:
            vol = m.get("volume", 0)
            try:
                vol_s = f"${float(vol):,.0f}"
            except (TypeError, ValueError):
                vol_s = str(vol)
            console.print(f"  [dim]•[/dim] {m.get('question', '')[:70]}  [dim]{vol_s}[/dim]")
        if len(markets) > 8:
            console.print(f"  [dim]… and {len(markets) - 8} more[/dim]")
        return build_asset_map(markets)


def iter_envelope_items(
    raw: bytes | str, asset_map: dict[str, dict], recv_ts: float
) -> Iterator[tuple[str, dict]]:
    """Yield (asset_id, envelope) for each atomic event in one WS message.

    CLOB sends either a single dict or a list. `price_change` carries a list
    of per-asset price_changes items; we split those so partitioning by
    asset_id stays clean.
    """
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return
    events = data if isinstance(data, list) else [data]

    for event in events:
        etype = event.get("event_type", event.get("type", "?"))

        if etype == "price_change":
            for ch in event.get("price_changes", []):
                aid = str(ch.get("asset_id", ""))
                if aid:
                    yield aid, envelope(
                        "polymarket", "price_change", ch,
                        asset_id=aid, market=asset_map.get(aid, {}),
                    )
            continue

        aid = str(event.get("asset_id", ""))
        if aid:
            yield aid, envelope(
                "polymarket", etype, event,
                asset_id=aid, market=asset_map.get(aid, {}),
            )


async def publish(
    raw: bytes | str,
    asset_map: dict[str, dict],
    sink: KafkaSink,
    tracker: RateTracker,
) -> None:
    recv_ts = time.time()
    for aid, env in iter_envelope_items(raw, asset_map, recv_ts):
        await sink.send(TOPIC, env, key=aid)
        tracker.record(label=env["type"])


async def stream(
    ws,
    asset_map: dict[str, dict],
    sink: KafkaSink,
    tracker: RateTracker,
    log_every: int,
) -> None:
    async for raw in ws:
        await publish(raw, asset_map, sink, tracker)
        if tracker.total and tracker.total % log_every == 0:
            log_progress(console, tracker)


async def _stream_subscription(
    asset_map: dict[str, dict],
    sink: KafkaSink,
    tracker: RateTracker,
    log_every: int,
    deadline: float | None = None,
) -> None:
    """Open one WebSocket, subscribe to asset_map's keys, stream until deadline.

    If `deadline` is None, streams until the WS closes (caller handles errors).
    If set, wraps stream() in asyncio.wait_for so the coroutine exits cleanly
    at the given unix timestamp — used by rolling-market rediscovery.
    """
    subscribe_msg = {"assets_ids": list(asset_map.keys()), "type": "market"}
    async with websockets.connect(CLOB_WS, ssl=ssl_context(), **WS_CONNECT_KWARGS) as ws:
        await ws.send(json.dumps(subscribe_msg))
        console.print("[bold green]Streaming…[/bold green]")
        if deadline is None:
            await stream(ws, asset_map, sink, tracker, log_every)
        else:
            timeout = max(1.0, deadline - time.time())
            try:
                await asyncio.wait_for(
                    stream(ws, asset_map, sink, tracker, log_every),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                pass


async def _run_static(
    args: argparse.Namespace,
    sink: KafkaSink,
    tracker: RateTracker,
    log_every: int,
) -> None:
    """Single discovery + reconnecting long-lived WS. For --query / --all / default.

    Same backoff scheme as the rolling path: 1s → 60s cap, reset to 1s after
    the stream stayed up for >30s. The discovered asset_map is reused on every
    reconnect (these markets don't roll over like the up-down ones).
    """
    asset_map = await select_markets(args)
    if not asset_map:
        return
    console.print(f"\n[cyan]Subscribing to {len(asset_map)} tokens…[/cyan]")

    backoff = 1.0
    while True:
        connect_start = time.monotonic()
        try:
            await _stream_subscription(asset_map, sink, tracker, log_every)
            uptime = time.monotonic() - connect_start
            console.print(
                f"[yellow]polymarket WS closed normally after {uptime:.0f}s[/yellow]"
            )
        except Exception as exc:
            uptime = time.monotonic() - connect_start
            console.print(
                f"[red]polymarket WS error after {uptime:.0f}s: "
                f"{type(exc).__name__}: {exc}[/red]"
            )

        if uptime > 30:
            backoff = 1.0
        console.print(f"[yellow]reconnecting in {backoff:.0f}s…[/yellow]")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


async def _run_rolling(
    args: argparse.Namespace,
    sink: KafkaSink,
    tracker: RateTracker,
    log_every: int,
) -> None:
    """Rolling-market mode: rediscover the active window each cycle.

    For --asset btc --window 5 etc., the market slug rotates every N minutes.
    We open a fresh WebSocket per window, stream until a few seconds past the
    slug's window_end, then loop to rediscover the next window's asset_ids.
    """
    grace_seconds = 5.0
    while True:
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context())
        ) as session:
            market, is_live = await fetch_updown_market(session, args.asset, args.window)

        if not market:
            console.print(
                f"[yellow]No {args.asset}-updown-{args.window}m market found; "
                f"retrying in 10s[/yellow]"
            )
            await asyncio.sleep(10)
            continue

        asset_map = build_asset_map([market])
        slug = market.get("slug", "")
        deadline = _slug_window_end(slug, args.window) + grace_seconds
        status = "LIVE" if is_live else "waiting for open"
        console.print(
            f"\n[green]{market.get('question', '')}[/green]  "
            f"[dim]{slug}  ({status}, ~{deadline - time.time():.0f}s until next discovery)[/dim]"
        )

        try:
            await _stream_subscription(
                asset_map, sink, tracker, log_every, deadline=deadline
            )
        except Exception as exc:
            console.print(
                f"[red]Stream error ({type(exc).__name__}): {exc}; retry in 5s[/red]"
            )
            await asyncio.sleep(5)
        else:
            console.print("[yellow]Window ended, rediscovering…[/yellow]")


async def run(args: argparse.Namespace) -> None:
    console.print(Rule("[bold cyan]Polymarket CLOB → Kafka[/bold cyan]"))
    log_every = 50 if args.debug else 500
    tracker = RateTracker()

    async with KafkaSink(args.kafka_bootstrap) as sink:
        console.print(f"[green]Kafka → {sink.bootstrap}[/green]")
        if args.asset:
            await _run_rolling(args, sink, tracker, log_every)
        else:
            await _run_static(args, sink, tracker, log_every)


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket CLOB WS → Kafka producer")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--query", default=None, help="Keyword filter for top markets")
    mode.add_argument("--all", action="store_true", help="Top-100 markets, no filter")
    mode.add_argument(
        "--asset",
        choices=["btc", "eth", "sol", "xrp", "bnb", "doge", "hype"],
        help="Asset for rotating up-or-down market",
    )
    parser.add_argument("--top", type=int, default=20, help="N when using --query or default")
    parser.add_argument("--window", type=int, default=5, choices=[5, 15], help="Window for --asset")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    if not args.asset and not args.query and not args.all:
        args.asset = "btc"

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
