"""Shared data models for the Stock Trading Service.

These dataclasses are the canonical types that flow through the pipeline:
Kafka signal -> strategy decision -> trade order -> execution result.
All models expose a :meth:`model_dump` helper for structured log ingestion.
"""

from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Any, Literal

EventType = Literal["price_change", "news_sentiment", "volume_spike", "financial_update"]
OrderAction = Literal["BUY", "SELL"]
OrderType = Literal["MKT", "LMT", "PEG_BENCH", "STP", "TRAILING_STOP_LOSS"]

_ORDER_ACTIONS = ("BUY", "SELL")
_ORDER_TYPES = ("MKT", "LMT", "PEG_BENCH", "STP", "TRAILING_STOP_LOSS")


def _serialize(value: Any) -> Any:
    """Recursively convert datetimes, dates and enums into JSON-safe values."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    return value


@dataclass
class StockSignal:
    """A deserialized stock signal message from a Kafka topic."""

    symbol: str
    event_type: EventType
    timestamp: datetime
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        """Validate signal contents, raising :class:`ValueError` on failure."""
        if not self.symbol or not self.symbol.strip():
            raise ValueError("StockSignal.symbol must be a non-empty string")
        if self.event_type not in (
            "price_change",
            "news_sentiment",
            "volume_spike",
            "financial_update",
        ):
            raise ValueError(f"Invalid event_type: {self.event_type!r}")
        if not isinstance(self.timestamp, datetime):
            raise ValueError("StockSignal.timestamp must be a datetime instance")

    def model_dump(self) -> dict[str, Any]:
        """Serialize the signal into a JSON-safe dict for log ingestion."""
        return _serialize(asdict(self))


@dataclass
class TradingDecision:
    """A strategy decision to trade, produced by the strategy engine."""

    symbol: str
    action: OrderAction
    quantity: int
    limit_price: float
    expiry_date: date

    def validate(self) -> None:
        if not self.symbol or not self.symbol.strip():
            raise ValueError("TradingDecision.symbol must be a non-empty string")
        if self.action not in _ORDER_ACTIONS:
            raise ValueError(f"Invalid action: {self.action!r}")
        if self.quantity <= 0:
            raise ValueError(f"quantity must be positive, got {self.quantity}")
        if self.limit_price <= 0:
            raise ValueError(f"limit_price must be positive, got {self.limit_price}")
        if not isinstance(self.expiry_date, date):
            raise ValueError("expiry_date must be a date instance")

    def model_dump(self) -> dict[str, Any]:
        return _serialize(asdict(self))


class OrderState(str, Enum):
    """Order lifecycle states tracked by the reconciler."""

    PENDING_SUBMIT = "pending_submit"
    PENDING_NEW = "pending_new"
    TRIGGERED = "triggered"
    FILLED_PARTIAL = "filled_partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

    @classmethod
    def all_states(cls) -> list[str]:
        return [state.value for state in cls]

    @classmethod
    def terminal_states(cls) -> list[str]:
        return [state.value for state in (cls.FILLED, cls.CANCELLED, cls.REJECTED)]


@dataclass
class TradeOrder:
    """An order ready for submission to IBKR."""

    correlation_id: str
    symbol: str
    action: str
    quantity: int
    order_type: OrderType
    limit_price: float | None
    tif: str
    expiry_date: date
    strategy_name: str
    signal_id: str

    def __init__(
        self,
        correlation_id: str,
        symbol: str,
        action: str,
        quantity: int,
        order_type: OrderType,
        limit_price: float | None,
        expiry_date: date,
        strategy_name: str,
        signal_id: str,
        tif: str = "Day",
    ) -> None:
        self.correlation_id = correlation_id
        self.symbol = symbol
        self.action = action
        self.quantity = quantity
        self.order_type = order_type
        self.limit_price = limit_price
        self.tif = tif
        self.expiry_date = expiry_date
        self.strategy_name = strategy_name
        self.signal_id = signal_id

    def validate(self) -> None:
        if not self.correlation_id or not self.correlation_id.strip():
            raise ValueError("TradeOrder.correlation_id must be a non-empty string")
        if not self.symbol or not self.symbol.strip():
            raise ValueError("TradeOrder.symbol must be a non-empty string")
        if self.action not in _ORDER_ACTIONS:
            raise ValueError(f"Invalid action: {self.action!r}")
        if self.quantity <= 0:
            raise ValueError(f"quantity must be positive, got {self.quantity}")
        if self.order_type not in _ORDER_TYPES:
            raise ValueError(f"Invalid order_type: {self.order_type!r}")
        if self.order_type in ("LMT", "PEG_BENCH", "STP", "TRAILING_STOP_LOSS") and (
            self.limit_price is None or self.limit_price <= 0
        ):
            raise ValueError(f"order_type {self.order_type!r} requires a positive limit_price")
        if not self.tif:
            raise ValueError("tif must not be empty")
        if not isinstance(self.expiry_date, date):
            raise ValueError("expiry_date must be a date instance")
        if not self.signal_id or not self.signal_id.strip():
            raise ValueError("TradeOrder.signal_id must be a non-empty string")

    def model_dump(self) -> dict[str, Any]:
        return _serialize(asdict(self))


@dataclass
class OrderExecutionResult:
    """The execution outcome of a submitted order."""

    correlation_id: str
    symbol: str
    status: str
    filled_qty: int
    avg_price: float
    commission: float

    def validate(self) -> None:
        if not self.correlation_id or not self.correlation_id.strip():
            raise ValueError("OrderExecutionResult.correlation_id must be a non-empty string")
        if self.filled_qty < 0:
            raise ValueError(f"filled_qty must be non-negative, got {self.filled_qty}")
        if self.filled_qty > 0 and self.avg_price <= 0:
            raise ValueError(f"avg_price must be positive when filled, got {self.avg_price}")
        if self.commission < 0:
            raise ValueError(f"commission must be non-negative, got {self.commission}")

    def model_dump(self) -> dict[str, Any]:
        return _serialize(asdict(self))
