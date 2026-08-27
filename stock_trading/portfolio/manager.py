"""Portfolio management.

Tracks current positions, available cash, and portfolio constraints.
Enforces exposure limits and computes purchasing power in CAD and USD.
This module is stubbed for the project foundation phase.
"""

from dataclasses import dataclass
from typing import Any

from stock_trading.config import PortfolioConfig
from stock_trading.logger import get_logger

log = get_logger(__name__)

_NOT_IMPLEMENTED = (
    "PortfolioManager is a stub: position tracking is not yet implemented "
    "(see design doc roadmap phase 3)"
)


@dataclass
class PositionSnapshot:
    """A point-in-time view of a single held position."""

    symbol: str
    quantity: int
    avg_cost_cad: float
    market_value_cad: float

    def unrealized_pnl_cad(self) -> float:
        return (self.market_value_cad - self.avg_cost_cad * self.quantity)


@dataclass
class AccountSnapshot:
    """A point-in-time view of the whole account."""

    account_id: str
    cash_cad: float
    cash_usd: float
    positions: list[PositionSnapshot]

    def total_market_value_cad(self) -> float:
        return sum(p.market_value_cad for p in self.positions)


class PortfolioManager:
    """Position and constraint tracking for the trading service."""

    def __init__(self, config: PortfolioConfig | None = None) -> None:
        self._config = config or PortfolioConfig()
        self._snapshot: AccountSnapshot | None = None

    @property
    def config(self) -> PortfolioConfig:
        return self._config

    async def load_from_ibkr(self) -> AccountSnapshot:
        """Load initial position state from IBKR on startup (reconciliation)."""
        log.warning("load_from_ibkr() called on stubbed PortfolioManager")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def check_constraints(self, symbol: str, requested_cad: float) -> tuple[bool, str]:
        """Check whether a trade of ``requested_cad`` satisfies all constraints.

        Returns:
            ``(allowed, reason)`` tuple for logging/audit.
        """
        log.warning(
            "check_constraints() called on stubbed PortfolioManager",
            symbol=symbol,
            requested_cad=requested_cad,
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def update_position(
        self, symbol: str, filled_qty: int, avg_price_cad: float
    ) -> PositionSnapshot:
        """Update internal position state after a confirmed fill."""
        log.warning(
            "update_position() called on stubbed PortfolioManager", symbol=symbol
        )
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def get_position(self, symbol: str) -> PositionSnapshot | None:
        """Return the current snapshot for a symbol, or ``None`` if flat."""
        log.warning("get_position() called on stubbed PortfolioManager", symbol=symbol)
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def calculate_purchasing_power(self) -> dict[str, Any]:
        """Return purchasing power in CAD and USD after FX conversion."""
        log.warning("calculate_purchasing_power() called on stubbed PortfolioManager")
        raise NotImplementedError(_NOT_IMPLEMENTED)

    async def total_pnl_cad(self) -> float:
        """Return aggregate unrealized + realized P&L in CAD."""
        log.warning("total_pnl_cad() called on stubbed PortfolioManager")
        raise NotImplementedError(_NOT_IMPLEMENTED)
