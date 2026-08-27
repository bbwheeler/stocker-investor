"""IBKR TWS/Gateway order execution.

Places, monitors, and manages orders via the IBKR TWS API using
``ib_insync``. Supports MKT, LMT, PEG_BENCH, STP and TRAILING_STOP_LOSS
orders with retry logic and fill tracking. This module is stubbed for the
project foundation phase.
"""

from dataclasses import dataclass
from typing import Any

from stock_trading.config import IbkrConfig
from stock_trading.logger import get_logger
from stock_trading.models import OrderExecutionResult, OrderState, TradeOrder

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "IbkrExecutor is a stub: order execution is not yet implemented "
    "(see design doc roadmap phases 1 and 4)"
)

_RETRYABLE_STATUSES = frozenset({"rejected", "failed"})


@dataclass
class ExecutionStatus:
    """Internal order status tracking between submission and fill."""

    correlation_id: str
    state: OrderState
    filled_qty: int = 0
    avg_price: float = 0.0
    attempts: int = 0


class IbkrExecutor:
    """Async IBKR order executor built on ib_insync.

    Connection lifecycle::

        executor = IbkrExecutor(config.ibkr)
        await executor.connect()
        result = await executor.place_order(order)
        await executor.disconnect()
    """

    def __init__(self, config: IbkrConfig | None = None, max_retries: int = 3) -> None:
        self._config = config or IbkrConfig()
        self._max_retries = max_retries
        self._connected = False
        self._statuses: dict[str, ExecutionStatus] = {}

    @property
    def config(self) -> IbkrConfig:
        return self._config

    @property
    def connected(self) -> bool:
        return self._connected

    async def connect(self) -> None:
        """Open the TWS/Gateway connection and subscribe to market data."""
        log.warning(
            "connect() called on stubbed IbkrExecutor",
            host=self._config.host,
            port=self._config.port,
            paper=self._config.paper,
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def disconnect(self) -> None:
        """Close the TWS/Gateway connection cleanly."""
        log.warning("disconnect() called on stubbed IbkrExecutor")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def place_order(self, order: TradeOrder) -> OrderExecutionResult:
        """Submit an order to IBKR and wait for an execution update.

        Args:
            order: The validated trade order to submit.

        Returns:
            The :class:`OrderExecutionResult` once the order reaches a
            terminal or filled state.
        """
        log.warning(
            "place_order() called on stubbed IbkrExecutor",
            correlation_id=order.correlation_id,
            symbol=order.symbol,
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def cancel_order(self, correlation_id: str) -> bool:
        """Cancel a pending order by correlation id."""
        log.warning("cancel_order() called on stubbed IbkrExecutor", correlation_id=correlation_id)
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def get_order_status(self, correlation_id: str) -> ExecutionStatus | None:
        """Return the internal tracking status for an order, if any."""
        log.warning("get_order_status() called on stubbed IbkrExecutor", correlation_id=correlation_id)
        return self._statuses.get(correlation_id)

    async def _wait_for_fill(self, order: TradeOrder, timeout_s: float) -> dict[str, Any]:
        """Wait for IBKR fill updates, polling ExecDetails/OrderStatus (internal)."""
        log.warning("_wait_for_fill() called on stubbed IbkrExecutor")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _retry_with_backoff(self, order: TradeOrder) -> OrderExecutionResult:
        """Retry a failed order with exponential backoff (internal)."""
        log.warning("_retry_with_backoff() called on stubbed IbkrExecutor")
        raise NotImplementedError(_NOT_IMPLEMENTED)
