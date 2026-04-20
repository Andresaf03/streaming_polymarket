"""Standard event envelope used by every producer.

Kept in one place so Spark can parse every topic with a single schema.
"""

from __future__ import annotations

import time
from typing import Any


def envelope(source: str, type_: str, payload: dict, **extra: Any) -> dict:
    """Build the canonical {source, type, recv_ts, payload, **extra} dict.

    extra goes at the top level so per-source identifiers (symbol, asset_id,
    market) stay flat instead of nested under payload.
    """
    return {
        "source": source,
        "type": type_,
        "recv_ts": time.time(),
        "payload": payload,
        **extra,
    }
