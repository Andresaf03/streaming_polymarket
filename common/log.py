"""Shared logging helpers for producers and tools."""

from __future__ import annotations

from rich.console import Console

from .metrics import RateTracker


def log_progress(console: Console, tracker: RateTracker) -> None:
    """Print a one-line throughput snapshot using the caller's Rich Console."""
    console.print(
        f"[dim]rate={tracker.rate()}/s  avg={tracker.avg_rate:.1f}/s  "
        f"total={tracker.total}  counts={tracker.counts}[/dim]"
    )
