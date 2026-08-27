"""Shared pytest fixtures and mocks for the Stock Trading Service test suite."""

import json
import os
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import patch

import pytest

from stock_trading.config import AppConfig, IbkrConfig, KafkaConfig, load_config
from stock_trading.logger import bind_correlation_id, clear_correlation_id, setup_logging
from stock_trading.models import StockSignal, TradeOrder, TradingDecision

SAMPLE_SYMBOL = "SHOP.TO"
SAMPLE_SIGNAL_ID = "sig_xyz789"
SAMPLE_CORRELATION_ID = "corr_abc123"


@pytest.fixture
def clean_env(monkeypatch):
    """Remove any STOCK_TRADING_* environment variables from the test environment."""
    prefix = "STOCK_TRADING_"
    for var in list(os.environ):
        if var.startswith(prefix):
            monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def sample_signal_payload() -> dict[str, Any]:
    """A well-formed Kafka signal payload, matching the design doc schema."""
    return {
        "symbol": SAMPLE_SYMBOL,
        "event_type": "price_change",
        "timestamp": "2026-08-10T14:30:00Z",
        "data": {
            "price": 542.18,
            "change_pct": -3.2,
            "volume": 1_250_000,
            "avg_volume_30d": 800_000,
        },
        "metadata": {
            "source": "financial_times",
            "prior_signal": None,
        },
    }


@pytest.fixture
def stock_signal(sample_signal_payload) -> StockSignal:
    return StockSignal(
        symbol=sample_signal_payload["symbol"],
        event_type=sample_signal_payload["event_type"],
        timestamp=datetime.fromisoformat(sample_signal_payload["timestamp"].replace("Z", "+00:00")),
        data=dict(sample_signal_payload["data"]),
        metadata=dict(sample_signal_payload["metadata"]),
    )


@pytest.fixture
def trading_decision() -> TradingDecision:
    return TradingDecision(
        symbol=SAMPLE_SYMBOL,
        action="BUY",
        quantity=14,
        limit_price=518.0,
        expiry_date=date(2099, 12, 31),
    )


@pytest.fixture
def trade_order(stock_signal) -> TradeOrder:
    return TradeOrder(
        correlation_id=SAMPLE_CORRELATION_ID,
        symbol=SAMPLE_SYMBOL,
        action="BUY",
        quantity=14,
        order_type="LMT",
        limit_price=520.00,
        tif="Day",
        expiry_date=date(2099, 12, 31),
        strategy_name="volume_breakout",
        signal_id=SAMPLE_SIGNAL_ID,
    )


@pytest.fixture
def default_config() -> AppConfig:
    """A fully-defaulted AppConfig (no file, no env vars)."""
    with patch.dict(os.environ, {}, clear=False):
        return load_config()


@pytest.fixture
def ibkr_config() -> IbkrConfig:
    return IbkrConfig(host="10.0.0.5", port=7497, client_id=42, paper=True, timeout_seconds=30)


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        bootstrap_servers=["broker-a:9092", "broker-b:9092"],
        consumer_group="test-group",
        topics=["stocks", "stocks-dlq"],
        max_poll_interval_ms=120_000,
    )


@pytest.fixture
def configured_logger():
    """Reset structlog to a clean JSON-configured state for each test."""
    setup_logging(level="INFO", format="json")
    yield
    setup_logging(level="DEBUG", format="json")


@pytest.fixture
def correlation_scoped(configured_logger):
    """Bind a correlation id for the duration of the using test."""
    token = bind_correlation_id(SAMPLE_CORRELATION_ID)
    yield SAMPLE_CORRELATION_ID
    clear_correlation_id(token)


def write_config_yaml(tmp_path, data: dict[str, Any]) -> str:
    """Write a config dict to a YAML file inside tmp_path and return its path."""
    import yaml

    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def parse_json_log_line(line: str) -> dict[str, Any]:
    """Parse a single JSON log line into a dict (raises on malformed JSON)."""
    return json.loads(line)


def make_env_overrides(**overrides) -> dict[str, str]:
    """Build a STOCK_TRADING_* environment mapping from keyword overrides."""
    return {
        name.upper() if name.startswith("STOCK_TRADING") else f"STOCK_TRADING_{name.upper()}": value
        for name, value in overrides.items()
    }
