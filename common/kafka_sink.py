"""Async Kafka JSON sink — thin wrapper around AIOKafkaProducer."""

from __future__ import annotations

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
        acks: int | str = 1,
    ) -> None:
        self._bootstrap = bootstrap
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
            await self._producer.flush()
        finally:
            await self._producer.stop()

    async def send(self, topic: str, value: dict, *, key: str | None = None) -> None:
        await self._producer.send(topic, value=value, key=key)
