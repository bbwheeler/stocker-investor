"""Strategy evaluation.

Evaluates incoming stock signals against configurable strategy rules
(thresholds, cooldowns, order sizing) and produces trading decisions.
This module is stubbed for the project foundation phase.
"""

from stock_trading.config import PortfolioConfig, StrategyConfig
from stock_trading.logger import get_logger
from stock_trading.models import StockSignal, TradingDecision

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "StrategyEngine is a stub: strategy evaluation is not yet implemented "
    "(see design doc roadmap phase 2)"
)


class StrategyEngine:
    """Rule-based strategy evaluation engine.

    Takes a stock signal and decides whether to BUY, SELL, or HOLD based on
    the configured thresholds and the portfolio's current state. Idempotency
    is enforced via a deduplication store keyed on ``(symbol, signal_id)``.
    """

    def __init__(
        self,
        strategy_config: StrategyConfig | None = None,
        portfolio_config: PortfolioConfig | None = None,
    ) -> None:
        self._strategy_config = strategy_config or StrategyConfig()
        self._portfolio_config = portfolio_config or PortfolioConfig()

    @property
    def strategy_config(self) -> StrategyConfig:
        return self._strategy_config

    async def evaluate(
        self, signal: StockSignal, current_position: int = 0
    ) -> TradingDecision | None:
        """Evaluate a signal and return a decision, or ``None`` to hold.

        Args:
            signal: The incoming stock signal.
            current_position: Current share count for the symbol.

        Returns:
            A :class:`TradingDecision` if the strategy triggers, else ``None``.
        """
        log.warning(
            "evaluate() called on stubbed StrategyEngine",
            symbol=signal.symbol,
            event_type=signal.event_type,
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _in_cooldown(self, symbol: str, now_ms: int) -> bool:
        """Check whether the per-symbol trade cooldown has elapsed (internal)."""
        log.warning("_in_cooldown() called on stubbed StrategyEngine", symbol=symbol)
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _size_position(
        self, signal: StockSignal, limit_price: float, current_position: int
    ) -> int:
        """Compute the trade quantity from signal + portfolio constraints (internal)."""
        log.warning(
            "_size_position() called on stubbed StrategyEngine", symbol=signal.symbol
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def _dedup_check(self, symbol: str, signal_id: str) -> bool:
        """Idempotency check keyed on (symbol, signal_id) (internal)."""
        log.warning("_dedup_check() called on stubbed StrategyEngine", symbol=symbol)
        raise NotImplementedError(_NOT_IMPLEMENTED)
