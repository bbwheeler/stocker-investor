"""Kafka signal ingestion.

Subscribes to stock signal topics, deserializes messages into
:class:`~stock_trading.models.StockSignal` instances, and routes them
into the trading engine. This module is stubbed for the project
foundation phase: the interface and lifecycle structure are in place,
but no messages are consumed yet.
"""

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from stock_trading.config import KafkaConfig
from stock_trading.logger import get_logger
from stock_trading.models import StockSignal

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "SignalConsumer is a stub: message ingestion is not yet implemented "
    "(see design doc roadmap phase 1)"
)


def deserialize_signal(raw: bytes | dict[str, Any]) -> StockSignal:
    """Deserialize a raw Kafka message into a :class:`StockSignal`.

    Args:
        raw: The raw message value from Kafka (JSON bytes or a decoded dict).

    Returns:
        The constructed signal.

    Raises:
        ValueError: If the payload is malformed or missing required fields.
    """
    raise NotImplementedError(_NOT_IMPLEMENTED)


class SignalConsumer:
    """Async Kafka consumer for stock signal topics.

    Lifecycle::

        consumer = SignalConsumer(config.kafka)
        await consumer.start()
        async for signal in consumer.consume():
            ...  # hand off to the trading engine
        await consumer.stop()
    """

    def __init__(self, config: KafkaConfig | None = None) -> None:
        self._config = config or KafkaConfig()
        self._running = False
        self._stopped: asyncio.Event | None = None

    @property
    def config(self) -> KafkaConfig:
        return self._config

    @property
    def running(self) -> bool:
        return self._running

    async def start(self) -> None:
        """Connect to the Kafka cluster and begin subscriptions."""
        log.warning("start() called on stubbed SignalConsumer", topics=self._config.topics)

    async def stop(self) -> None:
        """Commit offsets and disconnect cleanly from Kafka."""
        log.warning("stop() called on stubbed SignalConsumer")

    async def consume(self) -> AsyncIterator[StockSignal]:
        """Yield deserialized stock signals as they arrive.

        Yields:
            StockSignal instances in per-symbol order.
        """
        log.warning("consume() called on stubbed SignalConsumer")
        raise NotImplementedError(_NOT_IMPLEMENTED)
        yield  # pragma: no cover  (makes consume() an async generator)

    async def _on_message(self, raw: bytes | dict[str, Any]) -> StockSignal:
        """Deserialize a single message into a signal (internal)."""
        log.warning("_on_message() called on stubbed SignalConsumer")
        raise NotImplementedError(_NOT_IMPLEMENTED)
