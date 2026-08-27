# Stock Trading Service

An async Python 3.12 service that consumes stock signal messages from Kafka
and executes buy/sell orders via Interactive Brokers' TWS/Gateway API using
`ib_insync`.

> **Status: project foundation (Phase 1).** The configuration layer, shared
> models, structured logging, and the full component interface surface are
> implemented. Signal ingestion, strategy evaluation, portfolio management,
> and order execution are stubbed with typed interfaces ready for
> implementation.

## Overview

The system transforms market data updates into trading decisions and places
orders through IBKR's low-latency, low-fee API infrastructure.

```
┌──────────────┐   ┌─────────────────┐   ┌───────────────────┐   ┌────────────────┐
│  Kafka       │──▶│  Signal         │──▶│  Trading           │──▶│  IBKR TWS /    │
│  Stock       │   │  Processor &    │   │  Engine            │   │  Gateway        │
│  Topics      │   │  Strategy Module│   │                     │   │  (Canada Inc.) │
└──────────────┘   └─────────────────┘   └───────────────────┘   └────────────────┘
                         │                        │
                         ▼                        ▼
                  ┌───────────────┐       ┌───────────────┐
                  │  Portfolio    │       │  Order         │
                  │  Manager      │       │  Executor      │
                  └───────────────┘       └───────────────┘
```

## Tech Stack

| Layer | Option |
|-------|--------|
| Language | Python 3.12+ |
| Kafka Client | `aiokafka` (async) |
| IBKR API | `ib_insync` (async wrapper over the TWS API) |
| Config | YAML + environment variable overrides |
| Logging | `structlog` (structured JSON) |
| Deployment | Docker Compose / Kubernetes |

## Quick Start

```bash
# Clone and create a virtual environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# Configure (copy the example, adjust values)
cp configs/config.example.yml configs/config.dev.yml

# Run the test suite
pytest

# Run the full stack locally (app + kafka + postgres, dev profile)
docker compose --profile dev up
```

Run the service directly:

```bash
export STOCK_TRADING_CONFIG_FILE=configs/config.dev.yml
python -m stock_trading
```

## Configuration

Configuration is loaded from a YAML file (see `configs/config.example.yml`)
with per-field environment variable overrides using the
`STOCK_TRADING_<SECTION>_<FIELD>` pattern, e.g.:

```bash
export STOCK_TRADING_IBKR_PORT=7497
export STOCK_TRADING_IBKR_PAPER=true
export STOCK_TRADING_KAFKA_BOOTSTRAP_SERVERS=kafka-0:9092,kafka-1:9092
```

## Project Layout

```
stock_trading/
├── config.py         # YAML + env config loading
├── models.py         # StockSignal, TradeOrder, OrderState, ...
├── logger.py         # structlog JSON logging + correlation ids
├── kafka/            # Signal ingestion
├── strategy/         # Strategy evaluation
├── portfolio/        # Position tracking
├── executor/         # IBKR TWS connection + order placement
├── monitor/          # Order reconciliation
├── engine/           # Orchestration layer
└── main.py           # Entry point / lifecycle
```

## Roadmap

See `stock_trading_service_design.md` for the full design document,
roadmap phases, and risk considerations.
