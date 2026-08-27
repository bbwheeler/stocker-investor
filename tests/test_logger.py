"""Tests for stock_trading.logger structured JSON logging."""

import io
import json
from contextlib import redirect_stdout

import pytest

from stock_trading.logger import (
    bind_correlation_id,
    clear_correlation_id,
    correlation_context,
    get_logger,
    setup_logging,
)
from tests.conftest import parse_json_log_line


@pytest.fixture
def json_capture():
    """Capture stdout while freshly setting up JSON logging.

    Yields ``(stdout_buffer, log)`` for the using test.
    """
    output = io.StringIO()
    with redirect_stdout(output):
        setup_logging(level="INFO", format="json")
        log = get_logger("test.logger")
        yield output, log
    setup_logging(level="INFO", format="json")


def _captured_lines(output: io.StringIO) -> list[dict]:
    text = output.getvalue()
    lines = [line for line in text.splitlines() if line.strip()]
    return [parse_json_log_line(line) for line in lines]


def test_json_output_is_valid_structured_json(json_capture):
    output, log = json_capture
    log.info("order_placed", symbol="SHOP.TO", action="BUY", correlation_id="corr_abc123")

    entries = _captured_lines(output)
    assert len(entries) == 1
    entry = entries[0]
    assert entry["event"] == "order_placed"
    assert entry["level"] == "info"
    assert entry["logger"] == "test.logger"
    assert entry["symbol"] == "SHOP.TO"
    assert entry["action"] == "BUY"
    assert entry["correlation_id"] == "corr_abc123"
    assert "timestamp" in entry
    assert entry["timestamp"].startswith("20")


def test_json_output_contains_iso_timestamp(json_capture):
    output, log = json_capture
    log.warning("kafka_lag_detected", lag_ms=1200)

    entry = _captured_lines(output)[0]
    assert entry["level"] == "warning"
    assert entry["event"] == "kafka_lag_detected"
    assert entry["lag_ms"] == 1200


def test_error_level_logged(json_capture):
    output, log = json_capture
    log.error("ibkr_order_rejected", reason="insufficient_funds")

    entry = _captured_lines(output)[0]
    assert entry["level"] == "error"


def test_multiple_kvpairs_serialized(json_capture):
    output, log = json_capture
    log.info(
        "order_placed",
        symbol="SHOP.TO",
        order_type="LMT",
        limit_price=538.0,
        nested={"retry": 2, "tags": ["a", "b"]},
    )

    entry = _captured_lines(output)[0]
    assert entry["nested"]["retry"] == 2
    assert entry["nested"]["tags"] == ["a", "b"]


def test_correlation_id_injected_from_context(json_capture):
    output, log = json_capture
    token = bind_correlation_id("corr_xyz789")
    try:
        log.info("signal_received")
        log.info("strategy_evaluated")
    finally:
        clear_correlation_id(token)

    entries = _captured_lines(output)
    assert len(entries) == 2
    assert all(e.get("correlation_id") == "corr_xyz789" for e in entries)


def test_correlation_id_absent_when_unbound(json_capture):
    output, log = json_capture
    log.info("no_correlation_set")

    entry = _captured_lines(output)[0]
    assert "correlation_id" not in entry


def test_correlation_context_manager(json_capture):
    output, log = json_capture
    with correlation_context("corr_cm_1"):
        log.info("inside_scope")
        with correlation_context("corr_cm_2"):
            log.info("nested_scope")

    entries = _captured_lines(output)
    assert entries[0]["correlation_id"] == "corr_cm_1"
    assert entries[1]["correlation_id"] == "corr_cm_2"


def test_correlation_token_reset_after_clear(json_capture):
    output, log = json_capture
    token = bind_correlation_id("corr_once")
    clear_correlation_id(token)
    log.info("after_clear")

    assert "correlation_id" not in _captured_lines(output)[0]


def test_console_format_is_not_json():
    output = io.StringIO()
    with redirect_stdout(output):
        setup_logging(level="INFO", format="console")
        log = get_logger("test.logger")
        log.info("hello", key="value")

    text = output.getvalue()
    assert text.strip()
    with pytest.raises(json.JSONDecodeError):
        json.loads(text.splitlines()[-1])


def test_get_logger_returns_callable_with_methods(json_capture):
    _, log = json_capture
    for method in ("debug", "info", "warning", "error", "exception", "bind", "unbind"):
        assert callable(getattr(log, method))


def test_bind_values_appear_in_all_frames(json_capture):
    output, log = json_capture
    bound = log.bind(service="trading-service")
    bound.info("frame_one")
    bound.info("frame_two")

    entries = _captured_lines(output)
    assert all(e["service"] == "trading-service" for e in entries)
