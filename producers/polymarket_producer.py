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

from common import KafkaSink, RateTracker, envelope, ssl_context
from producers.polymarket_discovery import (
    build_asset_map,
    fetch_top_markets,
    fetch_updown_market,
)

CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
TOPIC = "polymarket.events"

console = Console()


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


def log_progress(tracker: RateTracker) -> None:
    console.print(
        f"[dim]rate={tracker.rate()}/s  avg={tracker.avg_rate:.1f}/s  "
        f"total={tracker.total}  counts={tracker.counts}[/dim]"
    )


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
            log_progress(tracker)


async def run(args: argparse.Namespace) -> None:
    console.print(Rule("[bold cyan]Polymarket CLOB → Kafka[/bold cyan]"))

    asset_map = await select_markets(args)
    if not asset_map:
        return

    subscribe_msg = {"assets_ids": list(asset_map.keys()), "type": "market"}
    log_every = 50 if args.debug else 500

    console.print(f"\n[cyan]Subscribing to {len(asset_map)} tokens…[/cyan]")
    tracker = RateTracker()

    async with KafkaSink(args.kafka_bootstrap) as sink:
        console.print(f"[green]Kafka → {sink.bootstrap}[/green]")
        async with websockets.connect(
            CLOB_WS,
            ssl=ssl_context(),
            ping_interval=10,
            ping_timeout=15,
            max_size=10 * 1024 * 1024,
        ) as ws:
            await ws.send(json.dumps(subscribe_msg))
            console.print("[bold green]Streaming…[/bold green]")
            await stream(ws, asset_map, sink, tracker, log_every)


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

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopped.[/yellow]")


if __name__ == "__main__":
    main()
