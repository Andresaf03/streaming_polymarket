"""Rolling-window rate tracker with labeled counts.

Used by producers for periodic "msg/s" log lines and by the throughput probe
for the final summary table.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque


class RateTracker:
    """Tracks cumulative total, rolling-window rate (msg/s), and per-label counts.

    O(n) rate() where n = number of timestamps in the window. Bounded by maxlen
    so high-rate streams don't allocate unboundedly.
    """

    def __init__(self, window_seconds: float = 1.0, maxlen: int = 10_000) -> None:
        self._window = window_seconds
        self._times: deque[float] = deque(maxlen=maxlen)
        self._counts: dict[str, int] = defaultdict(int)
        self._total = 0
        self._start = time.monotonic()

    def record(self, label: str | None = None) -> None:
        self._times.append(time.monotonic())
        self._total += 1
        if label is not None:
            self._counts[label] += 1

    def rate(self) -> int:
        cutoff = time.monotonic() - self._window
        return sum(1 for t in self._times if t > cutoff)

    @property
    def total(self) -> int:
        return self._total

    @property
    def counts(self) -> dict[str, int]:
        return dict(self._counts)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._start

    @property
    def avg_rate(self) -> float:
        elapsed = self.elapsed
        return self._total / elapsed if elapsed > 0 else 0.0
