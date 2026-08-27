"""Tests for stock_trading.config load_config."""

import os

import pytest

from stock_trading.config import AppConfig, load_config
from tests.conftest import write_config_yaml


def test_load_config_defaults(clean_env):
    cfg = load_config()
    assert cfg.name == "trading-service"
    assert cfg.environment == "dev"
    assert cfg.ibkr.host == "127.0.0.1"
    assert cfg.ibkr.port == 4002
    assert cfg.ibkr.client_id == 1
    assert cfg.ibkr.paper is True
    assert cfg.ibkr.timeout_seconds == 15
    assert cfg.kafka.bootstrap_servers == ["kafka-0.internal:9092"]
    assert cfg.kafka.consumer_group == "trading-service"
    assert cfg.kafka.topics == ["stocks"]
    assert cfg.kafka.max_poll_interval_ms == 300_000
    assert cfg.portfolio.base_currency == "CAD"
    assert cfg.portfolio.max_exposure_pct == 80.0
    assert cfg.portfolio.max_single_trade_cad == 10_000
    assert cfg.strategy.default_cooldown_ms == 60_000
    assert cfg.strategy.price_change_threshold_pct == 2.0
    assert cfg.logging.level == "INFO"
    assert cfg.logging.format == "json"


def test_load_config_from_yaml_file(tmp_path, clean_env):
    path = write_config_yaml(
        tmp_path,
        {
            "service": {"name": "custom-svc", "environment": "staging"},
            "ibkr": {"host": "10.0.0.5", "port": 7497, "paper": False},
            "kafka": {
                "bootstrap_servers": ["b1:9092", "b2:9092"],
                "consumer_group": "prod-group",
            },
            "portfolio": {"max_single_trade_cad": 20000},
            "strategy": {"default_cooldown_ms": 90000},
            "logging": {"level": "DEBUG", "format": "console"},
        },
    )
    cfg = load_config(path)

    assert cfg.name == "custom-svc"
    assert cfg.environment == "staging"
    assert cfg.ibkr.host == "10.0.0.5"
    assert cfg.ibkr.port == 7497
    assert cfg.ibkr.paper is False
    assert cfg.kafka.bootstrap_servers == ["b1:9092", "b2:9092"]
    assert cfg.kafka.consumer_group == "prod-group"
    assert cfg.portfolio.max_single_trade_cad == 20_000
    assert cfg.strategy.default_cooldown_ms == 90_000
    assert cfg.logging.level == "DEBUG"
    assert cfg.logging.format == "console"
    # Unset sections retain defaults.
    assert cfg.ibkr.client_id == 1
    assert cfg.kafka.topics == ["stocks"]


def test_env_override_takes_precedence(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {"ibkr": {"port": 4002, "paper": True}})
    env = {
        "STOCK_TRADING_IBKR_PORT": "7497",
        "STOCK_TRADING_IBKR_PAPER": "false",
        "STOCK_TRADING_KAFKA_BOOTSTRAP_SERVERS": "k1:9092,k2:9092",
        "STOCK_TRADING_ENVIRONMENT": "production",
        "STOCK_TRADING_PORTFOLIO_MAX_SINGLE_TRADE_CAD": "5000",
        "STOCK_TRADING_STRATEGY_PRICE_CHANGE_THRESHOLD_PCT": "5.5",
    }
    with __import__("unittest").mock.patch.dict(os.environ, env, clear=False):
        cfg = load_config(path)

    assert cfg.environment == "production"
    assert cfg.ibkr.port == 7497
    assert cfg.ibkr.paper is False
    assert cfg.kafka.bootstrap_servers == ["k1:9092", "k2:9092"]
    assert cfg.portfolio.max_single_trade_cad == 5_000
    assert cfg.strategy.price_change_threshold_pct == 5.5


def test_env_override_list_as_json(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {})
    with __import__("unittest").mock.patch.dict(
        os.environ,
        {"STOCK_TRADING_KAFKA_TOPICS": '["stocks", "stocks-dlq"]',
         "STOCK_TRADING_IBKR_CLIENT_ID": "7"},
        clear=False,
    ):
        cfg = load_config(path)

    assert cfg.kafka.topics == ["stocks", "stocks-dlq"]
    assert cfg.ibkr.client_id == 7


def test_config_file_env_var(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {"ibkr": {"port": 4001}})
    with __import__("unittest").mock.patch.dict(os.environ, {"STOCK_TRADING_CONFIG_FILE": path}):
        cfg = load_config()
    assert cfg.ibkr.port == 4001


def test_missing_config_file_raises(tmp_path, clean_env):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does-not-exist.yml")


def test_unknown_section_key_rejected(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {"ibkr": {"bogus_key": 123}})
    with pytest.raises(ValueError, match="Unknown key"):
        load_config(path)


def test_invalid_environment_rejected(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {"service": {"environment": "qa"}})
    with pytest.raises(ValueError, match="environment"):
        load_config(path)


def test_invalid_ibkr_port_rejected(tmp_path, clean_env):
    path = write_config_yaml(tmp_path, {"ibkr": {"port": 999_999}})
    with pytest.raises(ValueError):
        load_config(path)


def test_min_order_cannot_exceed_max_order(tmp_path, clean_env):
    path = write_config_yaml(
        tmp_path,
        {"portfolio": {"max_single_trade_cad": 500, "min_order_value_cad": 1000}},
    )
    with pytest.raises(ValueError):
        load_config(path)


def test_empty_yaml_file_uses_defaults(tmp_path, clean_env):
    path = tmp_path / "empty.yml"
    path.write_text("", encoding="utf-8")
    cfg = load_config(path)
    assert isinstance(cfg, AppConfig)
    assert cfg.ibkr.port == 4002


def test_example_config_file_loads():
    cfg = load_config("configs/config.example.yml")
    assert cfg.name == "trading-service"
    assert cfg.environment == "dev"
    assert cfg.portfolio.account_id == "U1234567"
    assert cfg.kafka.topics == ["stocks"]
