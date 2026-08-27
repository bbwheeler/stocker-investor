# Automated Stock Trading APIs Available in Canada

Research compiled: August 2, 2026

---

## The Challenge

Canadian securities regulations restrict Canadian brokers from allowing automated API trading on **Canadian exchanges** (TSX, etc.). This means most popular US-based APIs won't directly support TSX-listed stocks. However, several options exist for automating trades on **US/global exchanges** that are accessible to Canadians.

---

## Top Recommendations

### 1. Interactive Brokers (IBKR) -- Best overall for Canadian automated trading

- **Website:** https://www.interactivebrokers.ca
- **API:** TWS API / Gateway API / REST API
- **Markets:** US, Canadian (TSX), and 15+ global exchanges
- **Languages:** Python, Java, C#, C++, Rust, Go
- **Key details:**
  - Fully licensed in Canada through Interactive Brokers Canada Inc.
  - The only broker that supports **API trading on Canadian exchanges** via their TWS/Gateway platform
  - Full market data for TSX, NYSE, NASDAQ, and international exchanges
  - Supports stocks, options, futures, forex, bonds
  - Margin available with competitive rates
  - Paper trading account available for testing
  - Requires Interactive Brokers Trader Workstation (TWS) or API Gateway to be running as a local process that relays orders to the exchange
  - Not commission-free, but extremely low commissions (e.g. $0.005/share for US stocks)

**Verdict:** Best option if you need to trade Canadian stocks via API. This is the gold standard used by most algorithmic traders worldwide and in Canada.

---

### 2. Alpaca Markets -- Best commission-free REST API (US markets only)

- **Website:** https://alpaca.markets
- **API:** REST + WebSocket, streaming data
- **Markets:** US stocks, ETFs, options, crypto
- **Languages:** Python (`alpaca-py`), Node.js, Go, C#
- **Key details:**
  - SEC-registered, FINRA-regulated broker (US)
  - Commission-free trading for US stocks/ETFs (regulatory fees still apply)
  - Free paper trading account with real-time data for backtesting
  - Extremely developer-friendly REST API
  - SIPC insurance up to $500K
  - **Canadians can open accounts**, but can only trade **US-listed securities** and must fund in USD
  - No Canadian exchange support

**Verdict:** Best choice if you want commission-free, simple REST API access for US-market automation. Simplest developer experience. Must be funded with USD.

---

## Other Options Worth Considering

### 3. QuantConnect (LEAN Engine) -- Cloud-based algorithmic platform

- **Website:** https://www.quantconnect.com
- **Type:** Platform + brokerage integration (not a broker itself)
- **Key details:**
  - Open-source quantitative trading engine (LENA/LEAN)
  - Supports backtesting and live deployment across multiple brokers
  - Integrates with Interactive Brokers, Alpaca, TD Ameritrade/Schwab
  - Language support: Python, C#
  - Free tier available for research/backtesting
  - Not a broker itself -- you still need a brokerage connection (IBKR or Alpaca recommended)

**Verdict:** If your strategy is in Python or C#, QuantConnect provides a full backtesting infrastructure and can deploy to IBKR or Alpaca.

---

### 4. Tradier API -- Simple REST API for US markets

- **Website:** https://www.tradier.com
- **API:** REST + WebSocket streaming
- **Markets:** US equities, options
- **Key details:**
  - Low-cost trading on US stocks and options
  - Clean REST API with good documentation
  - Supports market data (real-time, delayed, historical)
  - Primarily designed for developers building investment applications
  - May require a US bank account for funding
  - No Canadian exchange support

---

### 5. Questrade + TD Direct Investing -- Limited API access (Canadian brokerages with REST APIs)

- **Questrade:** https://www.questrade.com/webapi (REST API, but only available to select partners/institutions)
- **TD Direct Investing:** No public API for retail users

**Key details:**
  - Questrade has a REST API that can place orders, retrieve quotes, and manage portfolios
  - However, access is limited -- primarily offered through partnership programs or existing relationships; not available for open self-service signup
  - TD does not offer a public API for retail automation
  - These support **Canadian exchanges** (TSX) but the API barrier to entry makes them impractical for individual developers

---

## Comparison Table

| Feature | Interactive Brokers | Alpaca | Tradier | QuantConnect + IBKR |
|---|---|---|---|---|
| Canadian account | Yes (Canadian entity) | Yes (as US non-resident) | May require US bank | Depends on connected broker |
| Canadian exchange (TSX) API | Yes | No | No | Via IBKR bridge |
| US market API | Yes | Yes | Yes | Via IBKR bridge |
| Commission-free | No (~$0.005/share) | Yes (US stocks) | Low commissions | Depends on broker |
| Paper / sandbox trading | Yes | Yes | No | Yes (LENA engine) |
| REST API | Yes (plus TWS protocol) | Yes | Yes | n/a (platform wrapper) |
| WebSocket streaming | Yes | Yes | Yes | Via underlying broker |
| Options support | Yes | Yes | Yes | Via IBKR bridge |
| Developer friendliness | Moderate | Excellent | Good | Excellent (platform) |
| Minimum deposit | $0 (cash) / $2,000 (margin) | $0 | $0 | Varies by broker |

---

## Summary Recommendation

For most Canadian developers building an automated stock trading system:

1. **If you need to trade TSX/Canadian stocks:** Use **Interactive Brokers Canada** with their TWS API. This is the only practical option for Canadian exchange automation via API. Set up a local API Gateway and connect your system to it.

2. **If US-market automation is sufficient:** Use **Alpaca**. It's commission-free, has the best developer experience, supports paper trading out of the box, and Canadians can open accounts as non-residents. Fund with USD and trade any US-listed stocks/ETFs/options.

3. **For building/testing strategies before going live:** Use **QuantConnect's LEAN engine** (free) for backtesting, then deploy to your chosen broker (IBKR or Alpaca).

---

## Important Notes

- All of these services require you to hold real money in the brokerage account to execute live trades
- Paper / simulated trading is available on all top options for free
- Tax reporting for US-sourced capital gains as a Canadian tax resident: consult a CPA, as capital gains rules differ between countries
- You'll need USD funding for Alpaca/Tradier (open a currency exchange account or use Wise/Paysend to transfer funds)
