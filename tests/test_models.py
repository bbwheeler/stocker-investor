"""Tests for stock_trading.models shared dataclasses."""

from datetime import date, datetime, timezone

import pytest

from stock_trading.models import (
    OrderExecutionResult,
    OrderState,
    StockSignal,
    TradeOrder,
    TradingDecision,
)


class TestStockSignal:
    def test_construction_and_fields(self, stock_signal):
        assert stock_signal.symbol == "SHOP.TO"
        assert stock_signal.event_type == "price_change"
        assert isinstance(stock_signal.timestamp, datetime)
        assert stock_signal.data["price"] == 542.18
        assert stock_signal.metadata["source"] == "financial_times"

    def test_model_dump_serializes(self, stock_signal):
        dumped = stock_signal.model_dump()
        assert dumped["symbol"] == "SHOP.TO"
        assert dumped["event_type"] == "price_change"
        assert isinstance(dumped["timestamp"], str)
        assert "T" in dumped["timestamp"]
        assert dumped["data"]["volume"] == 1_250_000
        assert dumped["metadata"]["prior_signal"] is None
        # Must be JSON-safe end to end.
        import json

        json.dumps(dumped)

    def test_validate_rejects_empty_symbol(self):
        signal = StockSignal(
            symbol="", event_type="price_change", timestamp=datetime.now(timezone.utc)
        )
        with pytest.raises(ValueError, match="symbol"):
            signal.validate()

    def test_validate_rejects_bad_event_type(self):
        signal = StockSignal(
            symbol="SHOP.TO", event_type="bogus_event", timestamp=datetime.now(timezone.utc)
        )
        with pytest.raises(ValueError, match="event_type"):
            signal.validate()

    def test_validate_rejects_bad_timestamp(self):
        signal = StockSignal(symbol="SHOP.TO", event_type="price_change", timestamp="not-a-dt")
        with pytest.raises(ValueError, match="timestamp"):
            signal.validate()

    def test_valid_event_types_accepted(self):
        for event_type in (
            "price_change",
            "news_sentiment",
            "volume_spike",
            "financial_update",
        ):
            signal = StockSignal(
                symbol="SHOP.TO", event_type=event_type, timestamp=datetime.now(timezone.utc)
            )
            signal.validate()


class TestTradingDecision:
    def test_construction_and_fields(self, trading_decision):
        assert trading_decision.symbol == "SHOP.TO"
        assert trading_decision.action == "BUY"
        assert trading_decision.quantity == 14
        assert trading_decision.limit_price == 518.0
        assert trading_decision.expiry_date == date(2099, 12, 31)

    def test_model_dump_serializes(self, trading_decision):
        dumped = trading_decision.model_dump()
        assert dumped["symbol"] == "SHOP.TO"
        assert dumped["action"] == "BUY"
        assert dumped["quantity"] == 14
        assert dumped["expiry_date"] == "2099-12-31"
        import json

        json.dumps(dumped)

    def test_validate_rejects_non_positive_quantity(self, trading_decision):
        trading_decision.quantity = 0
        with pytest.raises(ValueError, match="quantity"):
            trading_decision.validate()

    def test_validate_rejects_non_positive_price(self, trading_decision):
        trading_decision.limit_price = -5.0
        with pytest.raises(ValueError, match="limit_price"):
            trading_decision.validate()

    def test_validate_rejects_invalid_action(self, trading_decision):
        trading_decision.action = "HOLD"
        with pytest.raises(ValueError, match="action"):
            trading_decision.validate()

    def test_validate_rejects_bad_expiry_date(self, trading_decision):
        trading_decision.expiry_date = "2099-12-31"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="expiry_date"):
            trading_decision.validate()


class TestOrderState:
    def test_state_values(self):
        assert OrderState.PENDING_SUBMIT.value == "pending_submit"
        assert OrderState.PENDING_NEW.value == "pending_new"
        assert OrderState.TRIGGERED.value == "triggered"
        assert OrderState.FILLED_PARTIAL.value == "filled_partial"
        assert OrderState.FILLED.value == "filled"
        assert OrderState.CANCELLED.value == "cancelled"
        assert OrderState.REJECTED.value == "rejected"

    def test_str_enum(self):
        assert OrderState.FILLED == "filled"
        assert str(OrderState.PENDING_SUBMIT) == "pending_submit"

    def test_all_and_terminal_states(self):
        assert set(OrderState.all_states()) == {
            "pending_submit", "pending_new", "triggered",
            "filled_partial", "filled", "cancelled", "rejected",
        }
        assert set(OrderState.terminal_states()) == {"filled", "cancelled", "rejected"}


class TestTradeOrder:
    def test_construction_and_fields(self, trade_order):
        assert trade_order.correlation_id == "corr_abc123"
        assert trade_order.symbol == "SHOP.TO"
        assert trade_order.action == "BUY"
        assert trade_order.quantity == 14
        assert trade_order.order_type == "LMT"
        assert trade_order.limit_price == 520.00
        assert trade_order.tif == "Day"
        assert trade_order.strategy_name == "volume_breakout"
        assert trade_order.signal_id == "sig_xyz789"

    def test_default_tif(self, trading_decision):
        order = TradeOrder(
            correlation_id="corr_1",
            symbol="SHOP.TO",
            action="BUY",
            quantity=1,
            order_type="MKT",
            limit_price=None,
            expiry_date=date(2099, 12, 31),
            strategy_name="s",
            signal_id="sig",
        )
        assert order.tif == "Day"

    def test_model_dump_serializes(self, trade_order):
        dumped = trade_order.model_dump()
        assert dumped["correlation_id"] == "corr_abc123"
        assert dumped["order_type"] == "LMT"
        assert dumped["limit_price"] == 520.0
        assert dumped["expiry_date"] == "2099-12-31"
        import json

        json.dumps(dumped)

    def test_validate_rejects_empty_correlation_id(self, trade_order):
        trade_order.correlation_id = "  "
        with pytest.raises(ValueError, match="correlation_id"):
            trade_order.validate()

    def test_validate_rejects_invalid_action(self, trade_order):
        trade_order.action = "SHORT"
        with pytest.raises(ValueError, match="action"):
            trade_order.validate()

    def test_validate_rejects_invalid_order_type(self, trade_order):
        trade_order.order_type = "BOGUS"  # type: ignore[assignment]
        with pytest.raises(ValueError, match="order_type"):
            trade_order.validate()

    def test_validate_rejects_zero_quantity(self, trade_order):
        trade_order.quantity = 0
        with pytest.raises(ValueError, match="quantity"):
            trade_order.validate()

    def test_validate_rejects_missing_limit_for_lmt(self, trade_order):
        trade_order.order_type = "LMT"
        trade_order.limit_price = None
        with pytest.raises(ValueError, match="limit_price"):
            trade_order.validate()

    def test_market_order_allows_none_limit(self):
        order = TradeOrder(
            correlation_id="corr_1",
            symbol="SHOP.TO",
            action="SELL",
            quantity=5,
            order_type="MKT",
            limit_price=None,
            expiry_date=date(2099, 12, 31),
            strategy_name="s",
            signal_id="sig",
        )
        order.validate()


class TestOrderExecutionResult:
    def make_result(self) -> OrderExecutionResult:
        return OrderExecutionResult(
            correlation_id="corr_abc123",
            symbol="SHOP.TO",
            status="filled",
            filled_qty=14,
            avg_price=520.03,
            commission=1.4,
        )

    def test_construction_and_fields(self):
        result = self.make_result()
        assert result.symbol == "SHOP.TO"
        assert result.status == "filled"
        assert result.filled_qty == 14
        assert result.avg_price == 520.03
        assert result.commission == 1.4

    def test_model_dump_serializes(self):
        dumped = self.make_result().model_dump()
        assert dumped["status"] == "filled"
        assert dumped["filled_qty"] == 14
        assert dumped["avg_price"] == 520.03
        import json

        json.dumps(dumped)

    def test_validate_rejects_negative_filled_qty(self):
        result = self.make_result()
        result.filled_qty = -1
        with pytest.raises(ValueError, match="filled_qty"):
            result.validate()

    def test_validate_rejects_zero_avg_price_when_filled(self):
        result = self.make_result()
        result.avg_price = 0.0
        with pytest.raises(ValueError, match="avg_price"):
            result.validate()

    def test_validate_allows_zero_fill_with_zero_price(self):
        result = self.make_result()
        result.filled_qty = 0
        result.avg_price = 0.0
        result.validate()

    def test_validate_rejects_negative_commission(self):
        result = self.make_result()
        result.commission = -5
        with pytest.raises(ValueError, match="commission"):
            result.validate()
