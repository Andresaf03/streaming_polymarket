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


def parse_outcomes(raw) -> list[str]:
    """Gamma API returns outcomes as a JSON-encoded string or a list, parallel
    to clobTokenIds. Binary markets are ['Yes', 'No']."""
    try:
        vals = json.loads(raw) if isinstance(raw, str) else raw
        return [str(v) for v in vals] if vals else []
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
    """asset_id → {question, slug, outcome}. One entry per clobTokenId.

    outcomes and clobTokenIds are parallel arrays in the Gamma response
    (['Yes', 'No'] for binary markets). Downstream uses outcome to identify
    the 'Yes' token when computing an implied probability.
    """
    asset_map: dict[str, dict] = {}
    for m in markets:
        token_ids = parse_token_ids(m.get("clobTokenIds", []))
        outcomes = parse_outcomes(m.get("outcomes", []))
        for i, tid in enumerate(token_ids):
            asset_map[tid] = {
                "question": m.get("question", ""),
                "slug": m.get("slug", ""),
                "outcome": outcomes[i] if i < len(outcomes) else None,
            }
    return asset_map
