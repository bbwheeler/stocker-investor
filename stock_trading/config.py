"""Configuration loading for the Stock Trading Service.

Configuration is loaded from a YAML file and overridden by environment
variables. Environment variables take precedence and follow the pattern
``STOCK_TRADING_<SECTION>_<FIELD>``, for example::

    STOCK_TRADING_IBKR_PORT=7497
    STOCK_TRADING_KAFKA_BOOTSTRAP_SERVERS=kafka-0:9092,kafka-1:9092
    STOCK_TRADING_ENVIRONMENT=production

The config file location defaults to ``STOCK_TRADING_CONFIG_FILE`` when no
explicit path is passed to :func:`load_config`.
"""

import collections.abc
import dataclasses
import json
import os
import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

ENV_PREFIX = "STOCK_TRADING"
ENV_VAR_CONFIG_FILE = f"{ENV_PREFIX}_CONFIG_FILE"
_VALID_ENVIRONMENTS = ("dev", "staging", "production")


@dataclass
class IbkrConfig:
    """IBKR TWS/Gateway connection settings."""

    host: str = "127.0.0.1"
    port: int = 4002
    client_id: int = 1
    paper: bool = True
    timeout_seconds: int = 15

    def __post_init__(self) -> None:
        if self.port <= 0 or self.port > 65535:
            raise ValueError(f"IBKR port must be in range 1-65535, got {self.port}")
        if self.client_id <= 0:
            raise ValueError(f"IBKR client_id must be positive, got {self.client_id}")
        if self.timeout_seconds <= 0:
            raise ValueError(f"IBKR timeout_seconds must be positive, got {self.timeout_seconds}")


@dataclass
class KafkaConfig:
    """Kafka broker and consumer settings."""

    bootstrap_servers: list[str] = field(default_factory=lambda: ["kafka-0.internal:9092"])
    consumer_group: str = "trading-service"
    topics: list[str] = field(default_factory=lambda: ["stocks"])
    max_poll_interval_ms: int = 300_000

    def __post_init__(self) -> None:
        if not self.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers must not be empty")
        if not self.consumer_group:
            raise ValueError("Kafka consumer_group must not be empty")
        if not self.topics:
            raise ValueError("Kafka topics must not be empty")
        if self.max_poll_interval_ms <= 0:
            raise ValueError(
                f"Kafka max_poll_interval_ms must be positive, got {self.max_poll_interval_ms}"
            )


@dataclass
class PortfolioConfig:
    """Portfolio constraints and account settings."""

    account_id: str = ""
    base_currency: str = "CAD"
    max_exposure_pct: float = 80.0
    max_single_symbol_pct: float = 25.0
    max_single_trade_cad: float = 10_000
    min_order_value_cad: float = 100.0

    def __post_init__(self) -> None:
        if not (0.0 < self.max_exposure_pct <= 100.0):
            raise ValueError(f"max_exposure_pct must be in (0, 100], got {self.max_exposure_pct}")
        if not (0.0 < self.max_single_symbol_pct <= 100.0):
            raise ValueError(
                "max_single_symbol_pct must be in (0, 100], "
                f"got {self.max_single_symbol_pct}"
            )
        if self.max_single_trade_cad <= 0:
            raise ValueError(f"max_single_trade_cad must be positive, got {self.max_single_trade_cad}")
        if self.min_order_value_cad <= 0:
            raise ValueError(f"min_order_value_cad must be positive, got {self.min_order_value_cad}")
        if self.min_order_value_cad > self.max_single_trade_cad:
            raise ValueError(
                "min_order_value_cad must not exceed max_single_trade_cad, "
                f"got min={self.min_order_value_cad} max={self.max_single_trade_cad}"
            )


@dataclass
class StrategyConfig:
    """Strategy evaluation settings."""

    default_cooldown_ms: int = 60_000
    price_change_threshold_pct: float = 2.0
    order_delay_seconds: int = 3

    def __post_init__(self) -> None:
        if self.default_cooldown_ms < 0:
            raise ValueError(f"default_cooldown_ms must be non-negative, got {self.default_cooldown_ms}")
        if self.price_change_threshold_pct <= 0:
            raise ValueError(
                "price_change_threshold_pct must be positive, "
                f"got {self.price_change_threshold_pct}"
            )
        if self.order_delay_seconds < 0:
            raise ValueError(f"order_delay_seconds must be non-negative, got {self.order_delay_seconds}")


@dataclass
class LoggingConfig:
    """Logging settings."""

    level: str = "INFO"
    format: str = "json"

    def __post_init__(self) -> None:
        valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
        if self.level.upper() not in valid_levels:
            raise ValueError(f"logging level must be one of {valid_levels}, got {self.level}")
        if self.format not in ("json", "console", "text"):
            raise ValueError(f"logging format must be 'json', 'console' or 'text', got {self.format}")


@dataclass
class AppConfig:
    """Top-level application configuration."""

    name: str = "trading-service"
    environment: Literal["dev", "staging", "production"] = "dev"
    ibkr: IbkrConfig = field(default_factory=IbkrConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


def _as_dict(value: Any, context: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Config section {context!r} must be a mapping, got {type(value).__name__}")
    return value


def _base_type(annotation: Any) -> Any:
    """Resolve an annotation to its base Python type for coercion purposes."""
    if isinstance(annotation, type):
        return annotation
    origin = getattr(annotation, "__origin__", None)
    if origin in (list, tuple):
        return list
    if origin is getattr(types, "UnionType", None):
        args = [a for a in annotation.__args__ if a is not type(None)]
        if len(args) == 1:
            return _base_type(args[0])
        return str
    if origin is getattr(collections.abc, "Collection", None):
        return list
    return str


def _coerce(value: Any, target: Any, field_name: str) -> Any:
    """Coerce an environment variable string to the target field type."""
    base = _base_type(target)
    if base is bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on"}
    if base is int:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        try:
            return int(str(value).strip())
        except ValueError as exc:
            raise ValueError(f"Cannot coerce {field_name!r} to int: {value!r}") from exc
    if base is float:
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return float(value)
        try:
            return float(str(value).strip())
        except ValueError as exc:
            raise ValueError(f"Cannot coerce {field_name!r} to float: {value!r}") from exc
    if base is list:
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        text = str(value).strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except json.JSONDecodeError:
            pass
        return [item.strip() for item in text.split(",") if item.strip()]
    return str(value)


def _filtered_kwargs(cls: type, data: dict[str, Any], context: str) -> dict[str, Any]:
    valid = {f.name for f in dataclasses.fields(cls)}
    unknown = set(data) - valid
    if unknown:
        raise ValueError(f"Unknown key(s) in config section {context!r}: {sorted(unknown)}")
    return dict(data)


def _build_from_dict(raw: dict[str, Any]) -> AppConfig:
    service = _as_dict(raw.get("service"), "service")
    if service.get("environment") is not None:
        environment = str(service["environment"])
    else:
        environment = "dev"
    return AppConfig(
        name=str(service.get("name", "trading-service")),
        environment=environment,  # type: ignore[arg-type]
        ibkr=IbkrConfig(**_filtered_kwargs(IbkrConfig, _as_dict(raw.get("ibkr"), "ibkr"), "ibkr")),
        kafka=KafkaConfig(
            **_filtered_kwargs(KafkaConfig, _as_dict(raw.get("kafka"), "kafka"), "kafka")
        ),
        portfolio=PortfolioConfig(
            **_filtered_kwargs(
                PortfolioConfig, _as_dict(raw.get("portfolio"), "portfolio"), "portfolio"
            )
        ),
        strategy=StrategyConfig(
            **_filtered_kwargs(StrategyConfig, _as_dict(raw.get("strategy"), "strategy"), "strategy")
        ),
        logging=LoggingConfig(
            **_filtered_kwargs(LoggingConfig, _as_dict(raw.get("logging"), "logging"), "logging")
        ),
    )


_SECTIONS = ("ibkr", "kafka", "portfolio", "strategy", "logging")


def _apply_env_overrides(cfg: AppConfig) -> None:
    """Apply ``STOCK_TRADING_<SECTION>_<FIELD>`` environment variable overrides."""
    for section_name in _SECTIONS:
        section = getattr(cfg, section_name)
        prefix = f"{ENV_PREFIX}_{section_name.upper()}"
        for f in dataclasses.fields(section):
            raw = os.environ.get(f"{prefix}_{f.name.upper()}")
            if raw is not None:
                full_name = f"{prefix}_{f.name.upper()}"
                setattr(section, f.name, _coerce(raw, f.type, full_name))
    for f in dataclasses.fields(cfg):
        if dataclasses.is_dataclass(f.type):
            continue
        raw = os.environ.get(f"{ENV_PREFIX}_{f.name.upper()}")
        if raw is not None:
            full_name = f"{ENV_PREFIX}_{f.name.upper()}"
            setattr(cfg, f.name, _coerce(raw, f.type, full_name))


def _validate_environment(cfg: AppConfig) -> None:
    if cfg.environment not in _VALID_ENVIRONMENTS:
        raise ValueError(
            f"environment must be one of {_VALID_ENVIRONMENTS}, got {cfg.environment!r}"
        )


def load_config(config_path: str | os.PathLike[str] | None = None) -> AppConfig:
    """Load service configuration from YAML with environment variable overrides.

    Args:
        config_path: Explicit path to the YAML config file. When ``None``, the
            ``STOCK_TRADING_CONFIG_FILE`` environment variable is consulted;
            if that is unset, built-in defaults are used.

    Returns:
        A fully populated :class:`AppConfig` with environment overrides applied.

    Raises:
        FileNotFoundError: If a config file path is given but does not exist.
        ValueError: If the YAML structure or a config value is invalid.
    """
    if config_path is not None:
        path: Path | None = Path(config_path)
    else:
        env_path = os.environ.get(ENV_VAR_CONFIG_FILE)
        path = Path(env_path) if env_path else None

    raw: dict[str, Any] = {}
    if path is not None:
        if not path.is_file():
            raise FileNotFoundError(f"Config file not found: {path}")
        with path.open("r", encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh)
        if loaded is None:
            loaded = {}
        if not isinstance(loaded, dict):
            raise ValueError(f"Top-level of config file must be a mapping: {path}")
        raw = loaded

    cfg = _build_from_dict(raw)
    _apply_env_overrides(cfg)
    _validate_environment(cfg)
    return cfg
