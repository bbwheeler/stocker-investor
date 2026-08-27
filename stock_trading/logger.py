"""Structured JSON logging for the Stock Trading Service.

Uses ``structlog`` to produce JSON log entries (production) or console-friendly
entries (development). Every log frame is enriched with a ``correlation_id``
when one is bound in the current context, enabling end-to-end trace correlation
from Kafka signal ingestion through order execution.

Usage::

    from stock_trading.logger import get_logger, bind_correlation_id, clear_correlation_id

    corr = bind_correlation_id("corr_abc123")
    log = get_logger(__name__)
    log.info("order_placed", symbol="SHOP.TO", action="BUY")
    clear_correlation_id(corr)
"""

import logging
import logging.config
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator

import structlog

_CORRELATION_ID: ContextVar[str | None] = ContextVar("_CORRELATION_ID", default=None)
_configured = False


def _add_correlation_id(
    _logger: Any,
    _method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Attach the current correlation id (if any) to every log event."""
    corr = _CORRELATION_ID.get()
    if corr is not None:
        event_dict.setdefault("correlation_id", corr)
    return event_dict


def _shared_processors() -> list[Any]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        _add_correlation_id,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]


def setup_logging(level: str = "INFO", format: str = "json") -> None:
    """Configure structlog and the standard library logging pipeline.

    Args:
        level: Log level name (``DEBUG``, ``INFO``, ``WARNING``, ...).
        format: Renderer, ``json`` for production or ``console`` for dev.
    """
    global _configured
    log_level = getattr(logging, level.upper(), logging.INFO)
    renderer = (
        structlog.processors.JSONRenderer() if format == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[
            *_shared_processors(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "plain": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        renderer,
                    ],
                    "foreign_pre_chain": _shared_processors(),
                }
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "level": log_level,
                    "formatter": "plain",
                    "stream": "ext://sys.stdout",
                }
            },
            "root": {"level": log_level, "handlers": ["default"]},
            "loggers": {
                "aiokafka": {"level": logging.WARNING},
                "ib_insync": {"level": logging.INFO},
            },
        }
    )
    _configured = True


def get_logger(name: str | None = None, **initial_values: Any) -> Any:
    """Return a configured structured logger.

    The logger is configured on first use with the ``STOCK_TRADING_LOGGING_*``
    environment variables when no explicit setup has been performed.
    """
    if not _configured:
        import os

        setup_logging(
            level=os.environ.get("STOCK_TRADING_LOGGING_LEVEL", "INFO"),
            format=os.environ.get("STOCK_TRADING_LOGGING_FORMAT", "json"),
        )
    logger = structlog.get_logger(name) if name else structlog.get_logger()
    if initial_values:
        logger = logger.bind(**initial_values)
    return logger


def bind_correlation_id(correlation_id: str) -> Any:
    """Bind a correlation id to the current context and log contextvars.

    Returns a token usable with :func:`clear_correlation_id`.
    """
    token = _CORRELATION_ID.set(correlation_id)
    structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
    return token


def clear_correlation_id(token: Any) -> None:
    """Clear the correlation id bound by :func:`bind_correlation_id`."""
    _CORRELATION_ID.reset(token)
    structlog.contextvars.unbind_contextvars("correlation_id")


@contextmanager
def correlation_context(correlation_id: str) -> Iterator[None]:
    """Context manager that scopes a correlation id to a logical operation."""
    token = bind_correlation_id(correlation_id)
    try:
        yield
    finally:
        clear_correlation_id(token)
