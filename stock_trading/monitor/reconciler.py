"""Order monitoring & reconciliation.

Tracks order lifecycle end-to-end and periodically reconciles internal
state against IBKR account summaries to catch drift. This module is
stubbed for the project foundation phase.
"""

from typing import Any

from stock_trading.config import PortfolioConfig
from stock_trading.logger import get_logger
from stock_trading.models import OrderState, OrderExecutionResult

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "OrderReconciler is a stub: reconciliation is not yet implemented "
    "(see design doc roadmap phase 5)"
)


class OrderReconciler:
    """Order lifecycle tracker and IBKR state reconciler."""

    def __init__(
        self,
        poll_interval_s: float = 30.0,
        portfolio_config: PortfolioConfig | None = None,
    ) -> None:
        self._poll_interval_s = poll_interval_s
        self._portfolio_config = portfolio_config or PortfolioConfig()
        self._running = False
        self._tracked: dict[str, OrderState] = {}

    @property
    def running(self) -> bool:
        return self._running

    def record_order(self, correlation_id: str, state: OrderState) -> None:
        """Record an order's current state in the internal tracker."""
        log.warning(
            "record_order() called on stubbed OrderReconciler",
            correlation_id=correlation_id,
            state=state.value,
        )
        self._tracked[correlation_id] = state

    async def start(self) -> None:
        """Begin the periodic reconciliation loop."""
        log.warning("start() called on stubbed OrderReconciler")
        self._running = True

    async def stop(self) -> None:
        """Stop the reconciliation loop."""
        log.warning("stop() called on stubbed OrderReconciler")
        self._running = False

    async def reconcile(self) -> list[dict[str, Any]]:
        """Pull account summary from IBKR and report any drift.

        Returns:
            A list of drift records (empty when state is consistent).
        """
        log.warning("reconcile() called on stubbed OrderReconciler")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _pull_ibkr_account_summary(self) -> dict[str, Any]:
        """Fetch the current IBKR account summary (internal)."""
        log.warning("_pull_ibkr_account_summary() called on stubbed OrderReconciler")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _compare_states(self) -> list[dict[str, Any]]:
        """Diff internal order states against IBKR confirmations (internal)."""
        log.warning("_compare_states() called on stubbed OrderReconciler")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _cleanup_terminal_orders(self) -> int:
        """Evict fully settled orders; returns the number removed (internal)."""
        log.warning("_cleanup_terminal_orders() called on stubbed OrderReconciler")
        raise NotImplementedError(_NOT_IMPLEMENTED)
