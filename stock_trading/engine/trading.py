"""Core trading engine orchestration.

Coordinates signal ingestion, strategy evaluation, portfolio checks and
order execution in a single async pipeline. State machine::

    IDLE -> STRATEGY_EVALUATING -> PORTFOLIO_CHECKING -> ORDER_PLACED
         -> FILL_WAITING -> COMPLETE

This module is stubbed for the project foundation phase.
"""

from enum import Enum
from typing import Any

from stock_trading.config import AppConfig
from stock_trading.executor.ibkr import IbkrExecutor
from stock_trading.logger import get_logger
from stock_trading.models import (
    OrderExecutionResult,
    StockSignal,
    TradeOrder,
    TradingDecision,
)
from stock_trading.portfolio.manager import PortfolioManager
from stock_trading.strategy.engine import StrategyEngine

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "TradingEngine is a stub: orchestration is not yet implemented "
    "(see design doc roadmap phases 1-4)"
)


class EngineState(str, Enum):
    """Internal trading engine states."""

    IDLE = "idle"
    STRATEGY_EVALUATING = "strategy_evaluating"
    PORTFOLIO_CHECKING = "portfolio_checking"
    ORDER_PLACED = "order_placed"
    FILL_WAITING = "fill_waiting"
    COMPLETE = "complete"
    ERROR = "error"


class TradingEngine:
    """The core orchestration layer for the trading service.

    Wires together the strategy engine, portfolio manager, and IBKR
    executor to process each signal end-to-end.
    """

    def __init__(
        self,
        config: AppConfig,
        strategy: StrategyEngine | None = None,
        portfolio: PortfolioManager | None = None,
        executor: IbkrExecutor | None = None,
    ) -> None:
        self._config = config
        self._strategy = strategy or StrategyEngine(config.strategy, config.portfolio)
        self._portfolio = portfolio or PortfolioManager(config.portfolio)
        self._executor = executor or IbkrExecutor(config.ibkr)
        self._state = EngineState.IDLE

    @property
    def state(self) -> EngineState:
        return self._state

    async def start(self) -> None:
        """Initialize all downstream components (connect IBKR, load positions)."""
        log.warning("start() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def stop(self) -> None:
        """Shut down all downstream components gracefully."""
        log.warning("stop() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def process_signal(self, signal: StockSignal) -> TradeOrder | None:
        """Process a single signal through the full pipeline.

        Args:
            signal: The incoming stock signal.

        Returns:
            The :class:`TradeOrder` that was produced (if any), else ``None``.
        """
        log.warning(
            "process_signal() called on stubbed TradingEngine",
            symbol=signal.symbol,
            event_type=signal.event_type,
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _evaluate_strategy(self, signal: StockSignal) -> TradingDecision | None:
        """Route the signal through the strategy engine (internal)."""
        log.warning("_evaluate_strategy() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _check_portfolio(self, decision: TradingDecision) -> bool:
        """Validate the decision against portfolio constraints (internal)."""
        log.warning("_check_portfolio() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _build_order(self, signal: StockSignal, decision: TradingDecision) -> TradeOrder:
        """Construct the final trade order from a decision (internal)."""
        log.warning("_build_order() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _execute_order(self, order: TradeOrder) -> OrderExecutionResult:
        """Submit the order to IBKR (internal)."""
        log.warning("_execute_order() called on stubbed TradingEngine")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _transition(self, new_state: EngineState) -> None:
        """Perform a state machine transition (internal)."""
        log.warning(
            "_transition() called on stubbed TradingEngine",
            from_state=self._state.value,
            to_state=new_state.value,
        )
        self._state = new_state
