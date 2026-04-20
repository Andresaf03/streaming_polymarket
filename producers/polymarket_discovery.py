"""Polymarket Gamma REST helpers — market discovery only.

Pure data-access layer. No WebSocket, no Kafka, no console output.
"""

from __future__ import annotations

import json
import time

import aiohttp

GAMMA_URL = "https://gamma-api.polymarket.com"


def parse_token_ids(raw) -> list[str]:
    try:
        ids = json.loads(raw) if isinstance(raw, str) else raw
        return [str(i) for i in ids]
    except (json.JSONDecodeError, TypeError):
        return []


async def fetch_top_markets(
    session: aiohttp.ClientSession, query: str | None, top_n: int
) -> list[dict]:
    """Top-N active markets by volume, optionally keyword-filtered on question/slug."""
    params = {"active": "true", "order": "volume", "ascending": "false", "limit": 200}
    async with session.get(f"{GAMMA_URL}/markets", params=params) as resp:
        resp.raise_for_status()
        markets = await resp.json()

    if query:
        q = query.lower()
        markets = [
            m for m in markets
            if q in m.get("question", "").lower() or q in m.get("slug", "").lower()
        ]
    markets = [m for m in markets if m.get("clobTokenIds")]
    return markets[:top_n]


async def fetch_updown_market(
    session: aiohttp.ClientSession, asset: str, window: int
) -> tuple[dict | None, bool]:
    """Find the active {asset}-updown-{window}m-* market.

    Slug timestamp is always an exact multiple of window*60 seconds, so we
    compute it directly from the current Unix time — no pagination needed.
    Returns (market, is_live). is_live=False means we fell back to the next window.
    """
    window_secs = window * 60
    now_ts = int(time.time())
    current_start = (now_ts // window_secs) * window_secs
    next_start = current_start + window_secs

    for ts, live in [(current_start, True), (next_start, False)]:
        slug = f"{asset}-updown-{window}m-{ts}"
        async with session.get(f"{GAMMA_URL}/markets", params={"slug": slug}) as resp:
            resp.raise_for_status()
            data = await resp.json()
        markets = data if isinstance(data, list) else []
        if markets:
            return markets[0], live
    return None, False


def build_asset_map(markets: list[dict]) -> dict[str, dict]:
    """asset_id → {question, slug}. One entry per clobTokenId across all markets."""
    asset_map: dict[str, dict] = {}
    for m in markets:
        meta = {"question": m.get("question", ""), "slug": m.get("slug", "")}
        for tid in parse_token_ids(m.get("clobTokenIds", [])):
            asset_map[tid] = meta
    return asset_map
