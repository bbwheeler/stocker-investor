# Stock Trading Service

A service that consumes stock signal messages from Kafka and executes buy/sell orders via the Interactive Brokers (IBKR) TWS/Gateway API.

## Quick Start

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run the service
python -m trading_service.main
```

## Architecture

See [stock_trading_service_design.md](./stock_trading_service_design.md) for detailed design.
