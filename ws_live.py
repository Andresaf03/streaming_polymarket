#!/usr/bin/env python3
"""
ws_live.py — Polymarket CLOB WebSocket live feed validator.

Connects to the Polymarket CLOB WebSocket and streams real-time order book
and price events for high-volume markets (Bitcoin, crypto, elections, etc.)

Usage:
    python ws_live.py                        # top-20 markets by volume
    python ws_live.py --query bitcoin        # filter by keyword
    python ws_live.py --query trump --top 5  # top-5 Trump markets
    python ws_live.py --all                  # all active, no filter
"""

import asyncio
import json
import time
import argparse
from collections import defaultdict, deque
from datetime import datetime

import aiohttp
import websockets
from rich.console import Console
from rich.rule import Rule

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS_MARKET = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

console = Console()


async def fetch_top_markets(query: str | None, top_n: int) -> list[dict]:
    """Fetch active markets from Gamma API, sorted by volume."""
    async with aiohttp.ClientSession() as session:
        params = {
            "active": "true",
            "order": "volume",
            "ascending": "false",
            "limit": 200,
        }
        async with session.get(f"{GAMMA_URL}/markets", params=params) as resp:
            resp.raise_for_status()
            markets = await resp.json()

    # Filter by keyword if provided
    if query:
        q = query.lower()
        markets = [
            m for m in markets
            if q in m.get("question", "").lower()
            or q in m.get("slug", "").lower()
        ]

    # Only keep markets with CLOB token IDs
    markets = [m for m in markets if m.get("clobTokenIds")]
    return markets[:top_n]


def build_token_map(markets: list[dict]) -> dict[str, str]:
    """Map token_id → market question for display."""
    token_map = {}
    for m in markets:
        raw = m.get("clobTokenIds", [])
        try:
            ids = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            continue
        for tid in ids:
            token_map[str(tid)] = m.get("question", tid[:16])
    return token_map


def fmt_event(event: dict, token_map: dict, msg_rate: float, total: int) -> str | None:
    """Format a single CLOB event as a Rich string. Returns None to skip.

    Real event structures from CLOB WebSocket:
      price_change  → {market, price_changes: [{asset_id, price, size, side, best_bid, best_ask}]}
      book          → {asset_id, bids: [{price, size}], asks: [{price, size}], last_trade_price}
      last_trade_price → {asset_id, price, size, side, fee_rate_bps, transaction_hash}
    """
    etype = event.get("event_type", event.get("type", "?"))
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    meta = f"[dim]{msg_rate:.1f} msg/s  total={total}[/dim]"

    if etype == "price_change":
        # price_changes is a list; each item has its own asset_id
        lines = []
        for ch in event.get("price_changes", []):
            aid = str(ch.get("asset_id", ""))
            label = token_map.get(aid, aid[:20] or "?")[:52]
            price = ch.get("price", "?")
            side = ch.get("side", "?")
            size = ch.get("size", "?")
            best_bid = ch.get("best_bid", "—")
            best_ask = ch.get("best_ask", "—")
            color = "green" if side == "BUY" else "red"
            lines.append(
                f"[dim]{ts}[/dim]  [bold {color}]PRICE[/bold {color}]  "
                f"[bold]{label}[/bold]  "
                f"[{color}]{side} @ {price}  ×{size}[/{color}]  "
                f"[dim]bid={best_bid} ask={best_ask}[/dim]  {meta}"
            )
        return "\n".join(lines) if lines else None

    # For book and last_trade_price, asset_id is at top level
    asset_id = str(event.get("asset_id", ""))
    label = token_map.get(asset_id, asset_id[:20] if asset_id else "?")[:52]

    if etype == "book":
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        best_bid = bids[0].get("price", "—") if bids else "—"
        best_ask = asks[0].get("price", "—") if asks else "—"
        ltp = event.get("last_trade_price", "")
        ltp_str = f"  [dim]last={ltp}[/dim]" if ltp else ""
        return (
            f"[dim]{ts}[/dim]  [blue]BOOK [/blue]   "
            f"[bold]{label}[/bold]  "
            f"bid=[green]{best_bid}[/green]({len(bids)})  "
            f"ask=[red]{best_ask}[/red]({len(asks)}){ltp_str}  {meta}"
        )

    if etype == "last_trade_price":
        price = event.get("price", "?")
        size = event.get("size", "")
        side = event.get("side", "")
        color = "green" if side == "BUY" else "red" if side == "SELL" else "magenta"
        side_str = f"[{color}]{side}[/{color}] " if side else ""
        return (
            f"[dim]{ts}[/dim]  [magenta]TRADE[/magenta]  "
            f"[bold]{label}[/bold]  "
            f"{side_str}[magenta]@ {price}[/magenta]"
            f"{f'  ×{size}' if size else ''}  {meta}"
        )

    if etype in ("tick_size_change",):
        return None  # too noisy, skip

    # fallback for unknown events
    return (
        f"[dim]{ts}[/dim]  [yellow]{etype[:12].upper()}[/yellow]  "
        f"[bold]{label}[/bold]  {meta}"
    )


async def run(query: str | None, top_n: int):
    console.print()
    console.print(Rule("[bold cyan]Polymarket CLOB — live feed validator[/bold cyan]"))

    console.print(f"\n[cyan]Fetching markets (query={query!r}, top={top_n})…[/cyan]")
    markets = await fetch_top_markets(query=query, top_n=top_n)

    if not markets:
        console.print("[red]No markets found. Try a different --query.[/red]")
        return

    token_map = build_token_map(markets)
    if not token_map:
        console.print("[red]No CLOB token IDs in any of the matched markets.[/red]")
        return

    console.print(f"\n[green]Found {len(markets)} markets → {len(token_map)} tokens[/green]")
    for m in markets[:8]:
        vol = m.get("volume", 0)
        try:
            vol = f"${float(vol):,.0f}"
        except (TypeError, ValueError):
            vol = str(vol)
        console.print(f"  [dim]•[/dim] {m['question'][:75]}  [dim]{vol}[/dim]")
    if len(markets) > 8:
        console.print(f"  [dim]… and {len(markets) - 8} more[/dim]")

    # Polymarket CLOB WebSocket: send assets_ids + type, NO "channel" wrapper
    subscribe_msg = {
        "assets_ids": list(token_map.keys()),
        "type": "market",
    }

    console.print(f"\n[yellow]Connecting to {CLOB_WS_MARKET}…[/yellow]")

    # Metrics
    msg_times: deque[float] = deque(maxlen=2000)
    event_counts: dict[str, int] = defaultdict(int)
    total = 0

    async with websockets.connect(
        CLOB_WS_MARKET,
        ping_interval=10,
        ping_timeout=15,
        max_size=10 * 1024 * 1024,  # 10 MB per message
    ) as ws:
        await ws.send(json.dumps(subscribe_msg))
        console.print("[bold green]Connected. Streaming… (Ctrl+C to stop)[/bold green]\n")

        async for raw in ws:
            now = time.monotonic()
            msg_times.append(now)
            total += 1

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # CLOB can send a list of events or a single event
            events = data if isinstance(data, list) else [data]

            # Compute msg/s over the last 1-second window
            cutoff = now - 1.0
            rate = sum(1 for t in msg_times if t > cutoff)

            for event in events:
                etype = event.get("event_type", event.get("type", "?"))
                event_counts[etype] += 1

                line = fmt_event(event, token_map, rate, total)
                if line:
                    console.print(line)

            # Periodic stats every 100 messages
            if total % 100 == 0:
                console.print(
                    f"[dim]── stats: {dict(event_counts)}  "
                    f"rate={rate} msg/s  total={total} ──[/dim]"
                )


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket CLOB WebSocket live feed validator"
    )
    parser.add_argument(
        "--query",
        default=None,
        help="Keyword to filter markets (e.g. 'bitcoin', 'trump', 'fed'). "
             "Omit for top markets by volume.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Number of top markets to subscribe to (default: 20)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch top-100 markets regardless of query",
    )
    args = parser.parse_args()

    top_n = 100 if args.all else args.top
    query = None if args.all else args.query

    try:
        asyncio.run(run(query=query, top_n=top_n))
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Stopped.[/bold yellow]")


if __name__ == "__main__":
    main()
