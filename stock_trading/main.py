"""Application entry point and lifecycle management.

Wires together configuration, structured logging, and the (currently
stubbed) component group into a single async supervisor. Run via::

    python -m stock_trading
    # or
    stock-trading
"""

import asyncio
import contextlib
import os
import signal

from stock_trading import __version__
from stock_trading.config import AppConfig, load_config
from stock_trading.engine.trading import TradingEngine
from stock_trading.executor.ibkr import IbkrExecutor
from stock_trading.logger import (
    bind_correlation_id,
    clear_correlation_id,
    get_logger,
    setup_logging,
)
from stock_trading.kafka.consumer import SignalConsumer
from stock_trading.monitor.reconciler import OrderReconciler
from stock_trading.portfolio.manager import PortfolioManager
from stock_trading.strategy.engine import StrategyEngine

log = get_logger(__name__)


class ServiceContext:
    """Holds the constructed component graph for the running service."""

    def __init__(
        self,
        config: AppConfig,
        strategy: StrategyEngine,
        portfolio: PortfolioManager,
        executor: IbkrExecutor,
        consumer: SignalConsumer,
        reconciler: OrderReconciler,
        engine: TradingEngine,
    ) -> None:
        self.config = config
        self.strategy = strategy
        self.portfolio = portfolio
        self.executor = executor
        self.consumer = consumer
        self.reconciler = reconciler
        self.engine = engine


def build_context(config: AppConfig) -> ServiceContext:
    """Construct the full component graph from configuration."""
    strategy = StrategyEngine(config.strategy, config.portfolio)
    portfolio = PortfolioManager(config.portfolio)
    executor = IbkrExecutor(config.ibkr)
    consumer = SignalConsumer(config.kafka)
    reconciler = OrderReconciler(portfolio_config=config.portfolio)
    engine = TradingEngine(
        config, strategy=strategy, portfolio=portfolio, executor=executor
    )
    return ServiceContext(
        config=config,
        strategy=strategy,
        portfolio=portfolio,
        executor=executor,
        consumer=consumer,
        reconciler=reconciler,
        engine=engine,
    )


class StockTradingService:
    """Top-level service supervisor managing startup/shutdown lifecycle."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._ctx = build_context(config)
        self._stop_event: asyncio.Event = asyncio.Event()

    def request_stop(self) -> None:
        """Signal the service supervisor to shut down."""
        self._stop_event.set()

    async def start(self) -> None:
        """Bring up all components and begin consuming signals."""
        log.info(
            "service_starting",
            service=self._config.name,
            environment=self._config.environment,
            version=__version__,
            ibkr_paper=self._config.ibkr.paper,
            kafka_topics=self._config.kafka.topics,
        )
        token = bind_correlation_id("service-startup")
        try:
            await self._ctx.consumer.start()
            await self._ctx.reconciler.start()
            log.info("service_started", service=self._config.name)
        except Exception:
            log.exception("service_startup_failed")
            raise
        finally:
            clear_correlation_id(token)

    async def run(self) -> None:
        """Run the service until a stop is requested or it errors out."""
        await self.start()
        try:
            while not self._stop_event.is_set():
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Tear down all components in an order safe for in-flight work."""
        log.info("service_stopping", service=self._config.name)
        with contextlib.suppress(Exception):
            await self._ctx.consumer.stop()
        with contextlib.suppress(Exception):
            await self._ctx.reconciler.stop()
        with contextlib.suppress(Exception):
            await self._ctx.engine.stop()
        log.info("service_stopped", service=self._config.name)


def main(config_path: str | None = None) -> int:
    """Console/CLI entry point. Returns a process exit code."""
    config_file = config_path or os.environ.get("STOCK_TRADING_CONFIG_FILE")
    try:
        config = load_config(config_file)
    except (FileNotFoundError, ValueError) as exc:
        print(f"error: failed to load configuration: {exc}", flush=True)
        return 2

    setup_logging(level=config.logging.level, format=config.logging.format)
    service = StockTradingService(config)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _signal_handler() -> None:
        log.info("shutdown_signal_received")
        service.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(service.run())
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        loop.close()


if __name__ == "__main__":
    raise SystemExit(main())
