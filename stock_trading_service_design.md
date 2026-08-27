# Stock Trading Service Design

## Overview

A service that consumes stock signal messages from Kafka and executes buy/sell orders via the Interactive Brokers (IBKR) TWS/Gateway API. The system transforms market data updates into trading decisions and places orders through IBKR's low-latency, low-fee API infrastructure.

```
┌──────────────┐   ┌─────────────────┐   ┌───────────────────┐   ┌────────────────┐
│  Kafka       │──▶│  Signal         │──▶│  Trading            │──▶│  IBKR TWS /     │
│  Stock       │   │  Processor &    │   │  Engine             │   │  Gateway        │
│  Topics      │   │  Strategy Module│   │                     │   │  (Canada Inc.)  │
└──────────────┘   └─────────────────┘   └───────────────────┘   └────────────────┘
                          │                        │
                          ▼                        ▼
                   ┌───────────────┐       ┌───────────────┐
                   │  Portfolio    │       │  Order        │
                   │  Manager      │       │  Executor     │
                   └───────────────┘       └────────────────┘
```

---

## Architecture

### Components

#### 1. Kafka Consumer (Signal Ingestion)

- **Purpose:** Subscribe to stock-topic messages and deserialize them into a common event schema
- **Topics:** One topic per stock symbol, or a single `stocks` topic with `symbol` in the envelope
- **Consumer Group:** Single consumer group `trading-service` for ordered processing per symbol
- **Framework Options:** 
  - Node.js: `kafkajs` or `confluent-kafka-js`
  - Python: `aiokafka` (async) or `confluent-kafka-python`
  - Go: `segmentio/kafka-go` or `sarama`

#### 2. Signal Processor & Strategy Module

- **Purpose:** Evaluate incoming signals against a strategy configuration and decide whether to buy, sell, or hold
- **Strategy Input:** Rule-based (e.g., "if price changes >5% in last hour, buy $X worth") or configurable via YAML/JSON
- **Signal Schema (Kafka message format):**

```json
{
  "symbol": "SHOP.TO",
  "event_type": "price_change | news_sentiment | volume_spike | financial_update",
  "timestamp": "2026-08-10T14:30:00Z",
  "data": {
    "price": 542.18,
    "change_pct": -3.2,
    "volume": 1250000,
    "avg_volume_30d": 800000
  },
  "metadata": {
    "source": "financial_times | earnings_report | analyst_upgrade",
    "prior_signal": null
  }
}
```

- **Decision Thresholds:** Configurable per symbol/exchange:
  - `min_price_change_pct`: e.g., 2.0 (ignore smaller moves)
  - `max_position_size`: e.g., $10,000 CAD per trade
  - `cooldown_ms`: e.g., 60000 (prevent rapid fire trading)

#### 3. Portfolio Manager

- **Purpose:** Track current positions, available cash, and portfolio constraints before placing any order
- **Responsibilities:**
  - Maintain real-time position state (loaded on startup from IBKR + updated via internal cache)
  - Enforce max portfolio exposure per symbol/exchange
  - Calculate purchasing power in both CAD and USD after FX conversions
  - Track cost basis, unrealized gains, and total P&L

#### 4. Trading Engine

- **Purpose:** The core orchestration layer coordinating signals, strategy evaluation, portfolio checks, and order execution
- **Flow:**
  ```
  Signal arrives → Strategy evaluates → Check portfolio constraints → 
  If conditions met → Generate trade order → Pass to Order Executor
  ```
- **State Machine:** `IDLE → STRATEGY_EVALUATING → PORTFOLIO_CHECKING → ORDER_PLACED → FILL_WAITING → COMPLETE`

#### 5. Order Executor (IBKR Integration)

- **Purpose:** Place, monitor, and manage orders via IBKR TWS API
- **Connection:** TCP connection to IBKR Trader Workstation (TWS) or Gateway running on a server — use the `EClientSocket` client with the `IBKR Python SDK` (`ib_insync`) or Java/Go native bindings
- **Order Types Supported:**
  - `MKT` (market order)
  - `LMT` (limit order)
  - `PEG_BENCH` (pegged to benchmark)
  - `STP` (stop-loss) / `TRAILING_STOP_LOSS`
- **Execution Management:**
  - Poll for fills via IBKR's `ExecDetails` and `OrderStatus` events
  - Handle partial fills with remaining quantity re-submission or cancellation
  - Retry logic for failed orders (max retries: 3, backoff: exponential)

#### 6. Order Monitor & Reconciler

- **Purpose:** Track order lifecycle end-to-end and reconcile with IBKR confirmations
- **States tracked per order:**
  ```
  PENDING_SUBMIT → PENDING_NEW → TRIGGERED → FILLED_PARTIAL → FILLED → CANCELLED
  ```
- **Reconciliation:** Periodically pull account summary from IBKR API to catch any drift between internal state and actual positions

#### 7. Audit & Logging Layer

- Every signal, decision, and order logged with correlation IDs
- Structured JSON log entries for traceability:
  ```json
  {
    "event": "order_placed",
    "correlation_id": "corr_abc123",
    "symbol": "SHOP.TO",
    "action": "BUY",
    "quantity": 50,
    "order_type": "LMT",
    "limit_price": 538.00,
    "strategy": "volume_breakout",
    "signal_id": "sig_xyz789"
  }
  ```

---

## Data Flow

```
1. Kafka message arrives: {"symbol": "SHOP.TO", "change_pct": -4.1%, "timestamp": "..."}

2. Strategy module evaluates:
   - SHOP.TO is a TSX-listed stock (valid symbol)
   - Price drop > 3% threshold → trigger BUY signal
   - Cooldown elapsed since last SHOP.TO trade? Yes
   - Result: BUY decision generated

3. Portfolio Manager checks:
   - Current SHOP.TO position: 100 shares @ $570 avg cost
   - Available cash (CAD): $24,680
   - Max per-trade allocation (SHOP.TO): 30% portfolio → $7,500
   - Calculated quantity: floor($7,500 / ~$518) = 14 shares

4. Trading Engine constructs order:
   - symbol: SHOP.TO, action: BUY, qty: 14
   - order_type: LMT, limit_price: $520.00 (slight cushion above market)
   - tif: Day (time-in-force)

5. Order Executor sends to IBKR via TWS/Gateway API
   - Receives order_status: New, filled = 14 at avg price $520.03
   - Correlation ID stored for audit trail

6. Portfolio Manager updates internal state:
   - SHOP.TO position: 114 shares @ ~$521 avg cost
   - Cash remaining: $21,400
```

---

## IBKR Integration Details

### Connection Architecture

- **Deploy:** IBKR TWS or Gateway as a persistent process on the same host (or VPC) as the trading service
- **Recommended:** IBKR Gateway (lightweight headless version of TWS, no GUI overhead)
- **Port:** Default `4002` (Gateway) or `7496/7497` (TWS Paper/Live)
- **Protocol:** TCP with IBKR's custom message format — use the `ib_insync` Python library which wraps this cleanly with async/await syntax

### ib_insync Usage Pattern

```python
from ib_insync import IB, Order, Stock
from dataclasses import dataclass

@dataclass
class TradeSignal:
    symbol: str
    action: str        # "BUY" | "SELL"
    quantity: int
    limit_price: float
    exchange: str      # "TSX" | "NYSE" | "NASDAQ"

async def execute_signal(signal: TradeSignal):
    ib = IB()
    ib.connect('127.0.0.1', 4002, clientId=1)
    
    order = Order()
    order.action = signal.action
    order.totalQuantity = signal.quantity
    order.orderType = 'LMT'
    order.lmtPrice = signal.limit_price
    order.tif = 'Day'
    order.orderRef = f"trading-svc-{signal.symbol}"
    
    stock = Stock(signal.symbol, signal.exchange)
    trade = ib.placeOrder(stock, order)
    
    # Wait for fill
    ib.waitOnUpdate()
    
    return {
        'status': trade.orderStatus.status,
        'filled_qty': trade.filledQuantity,
        'avg_price': trade.avgFillPrice,
        'commission': trade.commission
    }
```

### Account Configuration

- **Account type:** IBKR Canada Inc. (Canadian entity)
- **Paper trading:** Available for development/testing — set `ib.connect(..., paper='1')` or use `IB.PAPER=1` environment variable
- **Funding:** CAD and USD sub-accounts — the service must specify the correct currency when pulling account summary
- **Market Data:** Subscribe to real-time quotes via IBKR before evaluating signals (delayed data causes stale order placements)

---

## Technology Stack

| Layer | Recommended Option | Alternatives |
|-------|-------------------|-------------|
| Language | Python 3.12+ | Node.js, Go, Rust |
| Kafka Client | `ib_insync` + `aiokafka` | confluent-kafka, kafkajs |
| IBKR API | `ib_insync` (async wrapper) | native TWS API (Java/Python), `ibapi` |
| State Storage | PostgreSQL | Redis (in-memory), SQLite |
| Position Cache | Redis | In-memory dict (single-process only) |
| Config | YAML + environment vars | HashiCorp Vault for secrets |
| Logging | Python structlog | Winston, logrus |
| Deployment | Docker Compose / Kubernetes | EC2 instance with systemd |

---

## Configuration Schema

```yaml
service:
  name: trading-service
  environment: dev                    # dev / staging / production
  
ibkr:
  host: 127.0.0.1
  port: 4002
  client_id: 1
  paper: true                         # Set to false for live trading
  timeout_seconds: 15

kafka:
  bootstrap_servers:
    - kafka-0.internal:9092
    - kafka-1.internal:9092
  consumer_group: trading-service
  topics:
    - stocks                          # Main stock updates topic
  max_poll_interval_ms: 300000        # 5 min — strategy evaluation can be slow

portfolio:
  account_id: U1234567               # IBKR account ID for this service
  base_currency: CAD
  max_exposure_pct: 80                # Max % of cash that can be deployed total
  max_single_symbol_pct: 25           # Max % per individual symbol
  max_single_trade_cad: 10000         # Max CAD per single trade
  min_order_value_cad: 100            # Skip trades below this value (too small after fees)

strategy:
  default_cooldown_ms: 60000          # Minimum interval between trades on same symbol
  price_change_threshold_pct: 2.0     # Must exceed this to trigger evaluation
  order_delay_seconds: 3              # Wait X seconds after signal before placing (confirm trend)
  
logging:
  level: INFO
  format: json
```

---

## Key Design Decisions

### Why ib_insync over the raw IBKR API?

- The native `ibapi` from IBKR is callback-based and blocks — it requires threading for async processing
- `ib_insync` provides a clean async/await interface built on asyncio, allowing Kafka consumers and IBKR calls to coexist in the same event loop
- Automatic error handling, reconnection logic, and type-safe dataclasses reduce boilerplate

### Why Python?

- Strongest community support for both Kafka (`aiokafka`, `confluent-kafka`) and IBKR (`ib_insync`)
- Strategy prototyping is fast — math and financial libraries (pandas, numpy) available
- Production-grade with asyncio for concurrency

### Paper trading first

- **All development must be done in paper mode** — no real money at risk while tuning strategy thresholds
- Switch to `paper: false` in config only after thorough backtesting and validation
- Monitor the transition closely — the first live trades are always the most dangerous

### Idempotency

- The service must never double-place an order. Use a deduplication store keyed on `(symbol, signal_id)` with TTL matching cooldown period
- IBKR's `orderRef` field carries the unique correlation ID for each order

---

## Order Lifecycle State Machine

```
                    ┌──────────┐
   Signal arrives  │          │
                 ─▶│          │──── Strategy rejects → DROP
                 │ │          │
                 │ │   IDLE   │
                 │ │          │──── Strategy accepts → CHECKING
                 │ │          │
                 │ └──────────┘
                 │      │
                 │      ▼
                 │  ┌───────────┐
                 │  │           │◀──── Retry (partial)
                 │  │   FILL    │──── Complete → COMPLETE
                 │  │   WAITING │
                 │  │           │──── Timeout / failure → CANCELLING
                 │  └───────────┘
                 │      │
                 │      ▼
                 │  ┌───────────┐
                 │  │           │
                 └──│ COMPLETE  │
                    │           │
                    └───────────┘
```

---

## Error Handling & Resilience

### Kafka Failures

- **Broker unavailable:** Consumer enters backoff mode (max retry with exponential delay); service health check fails
- **Deserialization error:** Dead-letter queue topic `stocks-dlq` for malformed messages; alert on DLQ volume spike
- **Consumer lag:** Monitor lag via Kafka consumer group offsets; alert if lag exceeds 1 minute

### IBKR Failures

- **TWS/Gateway disconnect:** Automatically retry connection with exponential backoff (max interval: 30s, max retries: unlimited until manual intervention)
- **Order rejected by exchange:** Log rejection reason from IBKR (`Status: Rejected`); do not retry — investigate root cause (e.g., halt on TSX stock)
- **Insufficient funds:** Return gracefully, skip the trade; alert only if this pattern persists across multiple signals
- **Fill timeout:** If no fill within `fill_timeout_seconds` (default 30s), cancel order and re-evaluate

### Market Conditions

- **Trading halted:** IBKR returns `Status: Inactive` — skip the symbol, resume monitoring
- **Extended hours:** Configure whether to trade outside regular exchange session (`OutsideRth: false`)
- **TSX-specific:** Canadian markets close at 4:00 PM ET; respect halt periods and avoid pre/post-hours trading if liquidity is poor

### Monitoring & Alerts

- Track per-symbol trade counts, order fill rate, and slippage (limit price vs. fill price)
- Alert on:
  - Service uptime < 99.9%
  - Order failure rate > 5% in any 1-hour window
  - Kafka consumer lag > 2 minutes
  - IBKR disconnect duration > 60 seconds

---

## Deployment

### Recommended Infrastructure

```
┌─────────────────────────────────────────────┐
│             Server / EC2 Instance            │
│                                              │
│  ┌──────────┐   ┌──────────────────────┐    │
│  │ IBKR     │   │  Trading Service      │    │
│  │ Gateway  │   │  (Docker container)   │    │
│  │ 4002     │◀─▶│  Kafka consumer       │    │
│  │          │   │  Strategy engine      │    │
│  └──────────┘   │  Portfolio manager    │    │
│                  │  Order executor       │    │
│                  │  Audit logger         │    │
│                  └──────────────────────┘    │
└─────────────────────────────────────────────┘
                    │
                    ▼
            ┌───────────────┐
            │  Kafka Cluster│
            └───────────────┘
```

- **Hosting:** Single dedicated instance (EC2, VPS) in the same region as IBKR data center (US East recommended for lowest latency to NYSE/NASDAQ; Toronto region for TSX latency sensitivity)
- **Process management:** Docker Compose for simple deployments; systemd service files for bare-metal
- **Backup:** No data backup needed — all state is either ephemeral or replicated from IBKR via account summaries

### Security

- Store IBKR credentials in secrets manager (AWS Secrets Manager, HashiCorp Vault), never in plain config files or environment variables at rest
- Network: Firewall restricts egress to only IBKR hosts and Kafka brokers
- TLS: Enable TLS for all Kafka connections (`security.protocol=SASL_SSL`)
- Client ID rotation: Use a unique `clientId` per trading service instance to avoid conflicts

---

## Development Roadmap

| Phase | Scope | Notes |
|-------|-------|-------|
| 1. Foundation | Kafka consumer + IBKR connection in paper mode | No trades executed, just verify data flow and connectivity |
| 2. Strategy Module | Rule-based strategy with configurable thresholds | Test against historical market data |
| 3. Portfolio Manager | Position tracking, cash calculations, constraints | Validate P&L accuracy against IBKR reports |
| 4. Paper Trading | Execute real paper trades via ib_insync in live paper account | Monitor fill rates and slippage in simulation |
| 5. Monitoring & Alerts | Health checks, metrics, alerting on failures | Set up Grafana dashboards (optional) |
| 6. Hardening | Error recovery tests, load testing with high signal volume | Stress test: simulate 100 signals/sec on hot stocks |
| 7. Production Deployment | Switch to live IBKR account | **Only after Phase 4 passes with >95% reliability** |

---

## Risk Considerations

1. **Market risk:** Automated trades incur real financial losses if strategy is flawed — limit order value and implement circuit breakers (stop all trading if P&L drops >X% in a day)
2. **Technical risk:** IBKR Gateway crash or network interruption could leave the service unaware of position state — always reconcile with IBKR account summary on startup
3. **Regulatory risk:** This system places orders directly into Canadian/US exchanges; ensure compliance with OSC/National Instrument 91-202 for automated trading practices
4. **Liquidity risk:** TSX-listed stocks may have wide bid-ask spreads — use limit orders exclusively (no market orders on low-volume symbols) to avoid slippage
