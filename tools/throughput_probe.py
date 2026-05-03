#!/usr/bin/env python3
"""
throughput_probe.py — measure sustained Kafka ingest rate across topics.

Consumes polymarket.events + binance.trades + binance.book for a fixed
window and reports per-topic msg/s and event-type distribution.

Validates the rubric's ≥4k msg/s target against the full local pipeline
(producers → Kafka → consumer), not just raw WebSocket throughput.

Usage:
    # bring up docker compose and start both producers first, then:
    python tools/throughput_probe.py                   # 10-minute probe
    python tools/throughput_probe.py --duration 60     # quick check
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

from aiokafka import AIOKafkaConsumer
from rich.console import Console
from rich.table import Table

from common import RateTracker

DEFAULT_TOPICS = ["polymarket.events", "binance.trades", "binance.book"]
RUBRIC_TARGET = 4000  # msg/s

console = Console(legacy_windows=False)


async def consume(
    consumer: AIOKafkaConsumer,
    duration: float,
    on_tick: callable | None = None,
) -> tuple[RateTracker, dict[str, RateTracker], dict[tuple[str, str], int]]:
    """Drain messages until duration elapses; return trackers + type breakdown."""
    total = RateTracker()
    per_topic: dict[str, RateTracker] = {}
    per_type: dict[tuple[str, str], int] = {}

    deadline = time.monotonic() + duration
    last_tick = time.monotonic()

    while True:
        remaining_ms = max(50, int((deadline - time.monotonic()) * 1000))
        if remaining_ms <= 0:
            break
        batch = await consumer.getmany(timeout_ms=min(1000, remaining_ms))

        for _tp, records in batch.items():
            for record in records:
                total.record()
                topic = record.topic
                per_topic.setdefault(topic, RateTracker()).record()
                v = record.value or {}
                key = (v.get("source", "?"), v.get("type", "?"))
                per_type[key] = per_type.get(key, 0) + 1

        now = time.monotonic()
        if on_tick and now - last_tick >= 1.0:
            on_tick(total, per_topic)
            last_tick = now
        if now >= deadline:
            break

    return total, per_topic, per_type


def print_tick(total: RateTracker, per_topic: dict[str, RateTracker]) -> None:
    by_topic = {t: tr.total for t, tr in per_topic.items()}
    console.print(
        f"[dim]t={total.elapsed:6.1f}s  rate_1s={total.rate():5d}/s  "
        f"avg={total.avg_rate:6.1f}/s  total={total.total:8d}  {by_topic}[/dim]"
    )


def render_summary(
    total: RateTracker,
    per_topic: dict[str, RateTracker],
    per_type: dict[tuple[str, str], int],
    threshold: int,
) -> None:
    elapsed = total.elapsed

    t1 = Table(title=f"Throughput summary ({elapsed:.1f}s)")
    t1.add_column("Topic")
    t1.add_column("Messages", justify="right")
    t1.add_column("avg msg/s", justify="right")
    for topic in sorted(per_topic):
        n = per_topic[topic].total
        t1.add_row(topic, f"{n:,}", f"{n / elapsed:,.1f}" if elapsed else "—")
    t1.add_row(
        "[bold]TOTAL[/bold]",
        f"[bold]{total.total:,}[/bold]",
        f"[bold]{total.avg_rate:,.1f}[/bold]",
    )
    console.print(t1)

    console.print()
    t2 = Table(title="Event-type breakdown")
    t2.add_column("Source")
    t2.add_column("Type")
    t2.add_column("Count", justify="right")
    t2.add_column("Share", justify="right")
    for (src, et), n in sorted(per_type.items(), key=lambda kv: -kv[1]):
        share = n / total.total * 100 if total.total else 0
        t2.add_row(src, et, f"{n:,}", f"{share:5.1f}%")
    console.print(t2)

    verdict = "[green]PASS[/green]" if total.avg_rate >= threshold else "[red]BELOW TARGET[/red]"
    console.print(
        f"\nSustained rate: [bold]{total.avg_rate:,.0f} msg/s[/bold]  "
        f"target ≥ {threshold}  {verdict}"
    )


async def main_async(bootstrap: str, topics: list[str], duration: float) -> None:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap,
        value_deserializer=lambda b: json.loads(b.decode()) if b else None,
        auto_offset_reset="latest",
        group_id=None,
    )
    await consumer.start()
    console.print(f"[green]Consumer connected → {bootstrap}[/green]")
    console.print(f"[cyan]Topics:[/cyan] {', '.join(topics)}   duration={duration:.0f}s\n")

    try:
        total, per_topic, per_type = await consume(consumer, duration, on_tick=print_tick)
    finally:
        await consumer.stop()

    console.print()
    render_summary(total, per_topic, per_type, RUBRIC_TARGET)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka throughput probe")
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
    )
    parser.add_argument("--topics", nargs="+", default=DEFAULT_TOPICS)
    parser.add_argument("--duration", type=float, default=600.0)
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args.kafka_bootstrap, args.topics, args.duration))
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted.[/yellow]")


if __name__ == "__main__":
    main()
