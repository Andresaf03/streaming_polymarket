#!/usr/bin/env python3
"""
bitcoin_5m.py — Live feed for the current Bitcoin Up-or-Down 5-minute market.

Fetches the active btc-updown-5m-* market from Gamma API and streams
its WebSocket events in real time.

Usage:
    python bitcoin_5m.py           # BTC 5-min (default)
    python bitcoin_5m.py --window 15   # BTC 15-min variant
    python bitcoin_5m.py --asset eth   # ETH 5-min
"""

import asyncio
import json
import time
import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone

import aiohttp
import websockets
from rich.console import Console
from rich.rule import Rule

GAMMA_URL = "https://gamma-api.polymarket.com"
CLOB_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

console = Console()


def _slug_window_start(slug: str) -> datetime | None:
    """Extract window start from slug timestamp (e.g. btc-updown-5m-1776285000)."""
    try:
        ts = int(slug.rsplit("-", 1)[-1])
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except (ValueError, IndexError):
        return None


async def _fetch_by_slug(session: aiohttp.ClientSession, slug: str) -> dict | None:
    """Fetch a single market by exact slug. Returns None if not found."""
    async with session.get(f"{GAMMA_URL}/markets", params={"slug": slug}) as resp:
        resp.raise_for_status()
        data = await resp.json()
    markets = data if isinstance(data, list) else []
    return markets[0] if markets else None


async def find_market(asset: str, window: int) -> tuple[dict | None, bool]:
    """Find the active market for the given asset and window.

    The slug timestamp is always an exact multiple of window*60 seconds,
    so we compute it directly from the current Unix time — no pagination needed.

    Returns (market, is_live) where is_live=False means no window is live now.
    """
    import time as _time

    window_secs = window * 60
    now_ts = int(_time.time())
    current_start = (now_ts // window_secs) * window_secs
    next_start = current_start + window_secs

    async with aiohttp.ClientSession() as session:
        # Try current window first, then next
        for ts, live in [(current_start, True), (next_start, False)]:
            slug = f"{asset}-updown-{window}m-{ts}"
            market = await _fetch_by_slug(session, slug)
            if market:
                return market, live

    return None, False


def parse_token_ids(raw) -> list[str]:
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return [str(i) for i in ids]
    except (json.JSONDecodeError, TypeError):
        return []


def fmt_event(event: dict, label: str, rate: float, total: int) -> str | None:
    etype = event.get("event_type", event.get("type", "?"))
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    meta = f"[dim]{rate:.1f} msg/s  total={total}[/dim]"

    if etype == "price_change":
        lines = []
        for ch in event.get("price_changes", []):
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
        return None

    return (
        f"[dim]{ts}[/dim]  [yellow]{etype[:12].upper()}[/yellow]  "
        f"[bold]{label}[/bold]  {meta}"
    )


async def run(asset: str, window: int):
    console.print()
    console.print(Rule(f"[bold cyan]{asset.upper()} Up-or-Down {window}min — live[/bold cyan]"))

    console.print(f"\n[cyan]Looking for active {asset}-updown-{window}m-* market…[/cyan]")
    market, is_live = await find_market(asset, window)

    if not market:
        console.print(f"[red]No active {asset}-updown-{window}m market found.[/red]")
        return

    token_ids = parse_token_ids(market.get("clobTokenIds", []))
    if not token_ids:
        console.print("[red]No clobTokenIds in market.[/red]")
        return

    question = market.get("question", "?")
    end_date = market.get("endDate", "?")
    slug = market.get("slug", "?")
    condition_id = market.get("conditionId", "?")

    if not is_live:
        win_start = _slug_window_start(market.get("slug", ""))
        start_str = win_start.strftime("%Y-%m-%d %H:%M UTC") if win_start else end_date
        console.print(
            f"[yellow]No live market right now (outside trading hours).[/yellow]\n"
            f"Next window opens at [bold]{start_str}[/bold]. Showing it anyway — Ctrl+C to abort."
        )

    console.print(f"\n[green]Found:[/green] {question}")
    console.print(f"  slug:        [dim]{slug}[/dim]")
    console.print(f"  conditionId: [dim]{condition_id}[/dim]")
    console.print(f"  tokens:      [dim]{token_ids}[/dim]")
    console.print(f"  resolves:    [yellow]{end_date}[/yellow]")

    # Short label for display
    label = question[:55]

    subscribe_msg = {"assets_ids": token_ids, "type": "market"}

    console.print(f"\n[yellow]Connecting to WebSocket…[/yellow]")

    msg_times: deque[float] = deque(maxlen=2000)
    event_counts: dict[str, int] = defaultdict(int)
    total = 0

    async with websockets.connect(
        CLOB_WS,
        ping_interval=10,
        ping_timeout=15,
        max_size=10 * 1024 * 1024,
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

            events = data if isinstance(data, list) else [data]
            cutoff = now - 1.0
            rate = sum(1 for t in msg_times if t > cutoff)

            for event in events:
                etype = event.get("event_type", event.get("type", "?"))
                event_counts[etype] += 1
                line = fmt_event(event, label, rate, total)
                if line:
                    console.print(line)

            if total % 50 == 0:
                console.print(
                    f"[dim]── {dict(event_counts)}  rate={rate} msg/s  total={total} ──[/dim]"
                )


def main():
    parser = argparse.ArgumentParser(
        description="Stream the active Bitcoin (or other asset) Up-or-Down 5/15-min market"
    )
    parser.add_argument(
        "--asset",
        default="btc",
        choices=["btc", "eth", "sol", "xrp", "bnb", "doge", "hype"],
        help="Asset to watch (default: btc)",
    )
    parser.add_argument(
        "--window",
        type=int,
        default=5,
        choices=[5, 15],
        help="Window in minutes (default: 5)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run(asset=args.asset, window=args.window))
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Stopped.[/bold yellow]")


if __name__ == "__main__":
    main()
