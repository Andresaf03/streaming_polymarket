"""Async Kafka JSON sink — thin wrapper around AIOKafkaProducer."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer


class KafkaSink:
    """Context-managed async Kafka producer with JSON value serialization.

    Usage:
        async with KafkaSink("localhost:9092") as sink:
            await sink.send("topic", {"hello": "world"}, key="k1")
    """

    def __init__(
        self,
        bootstrap: str,
        *,
        compression: str | None = None,
        linger_ms: int = 5,
        acks: int | str = "all",
        max_pending: int = 10_000,
        drain_every: int = 1_000,
    ) -> None:
        self._bootstrap = bootstrap
        self._max_pending = max_pending
        self._drain_every = drain_every
        self._since_drain = 0
        self._pending: set[asyncio.Future] = set()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks=acks,
            linger_ms=linger_ms,
            compression_type=compression,
        )

    @property
    def bootstrap(self) -> str:
        return self._bootstrap

    async def __aenter__(self) -> "KafkaSink":
        await self._producer.start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            await self.flush()
            await self._producer.flush()
        finally:
            await self._producer.stop()

    async def _drain_ready(self) -> None:
        done = {future for future in self._pending if future.done()}
        for future in done:
            self._pending.discard(future)
            future.result()

    async def _drain_one(self) -> None:
        if not self._pending:
            return
        done, _ = await asyncio.wait(self._pending, return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            self._pending.discard(future)
            future.result()

    async def flush(self) -> None:
        while self._pending:
            await self._drain_one()

    async def send(
        self,
        topic: str,
        value: dict,
        *,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        future = await self._producer.send(
            topic,
            value=value,
            key=key,
            partition=partition,
        )
        self._pending.add(future)
        self._since_drain += 1
        if self._since_drain >= self._drain_every:
            self._since_drain = 0
            await self._drain_ready()
        if len(self._pending) >= self._max_pending:
            await self._drain_one()

    async def send_and_wait(
        self,
        topic: str,
        value: dict,
        *,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        await self._producer.send_and_wait(
            topic,
            value=value,
            key=key,
            partition=partition,
        )


class NullSink:
    """Drop-in for KafkaSink that discards all messages. Used by --dry-run flags."""

    bootstrap = "dry-run"

    async def send(
        self,
        topic: str,
        value: dict,
        *,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        pass

    async def send_and_wait(
        self,
        topic: str,
        value: dict,
        *,
        key: str | None = None,
        partition: int | None = None,
    ) -> None:
        pass
