# Stocker Investor

A self-hostable Go service that consumes stock signals from Kafka, pulls context
from the sibling `stocker-store` gRPC service, computes an investor decision
(BUY / SELL / HOLD with a position size and confidence), and publishes the
decision back to Kafka.

**Tech:** Go 1.25 · Kafka (segmentio/kafka-go) · gRPC/protobuf · `log/slog` JSON
logging · Podman Quadlets

```
┌──────────────┐   protobuf        ┌──────────────────────────────┐
│ kafka        │ ────▶             │ stocker-investor             │
│ brokers      │   StockUpdate     │ consume → decide → publish   │
│ (in topic)   │                   │ per-symbol cooldown          │
└──────────────┘                   └──────────────────────────────┘
        ▲                                      │
        │ protobuf (out topic)                 │ gRPC GetStock /
        │                                      ▼ GetStocks
┌──────────────┐                   ┌──────────────────────────────┐
│ kafka        │ ◀────             │ stocker-store (gRPC :3500)   │
│ brokers      │   StockUpdate     │ StockStore service           │
└──────────────┘                   └──────────────────────────────┘
```

This is **not** an order executor: it never talks to a broker API. It is a
compute-and-publish loop only. The earlier Python/IBKR design
(`stock_trading_service_design.md`) is historical and not carried over.

## Overview

The service is a single Go binary. It consumes `kafkastockv1.StockUpdate`
messages (raw scored stock events) from `KAFKA_IN_TOPIC`, optionally reads
context for the symbol via `stockstore.v1.StockStore.GetStock`, applies a
single configurable decision rule, and publishes the decision as a
`StockUpdate` on `KAFKA_OUT_TOPIC` carrying reserved `stocker_investor.*`
score keys. Kafka output is a **no-op unless `KAFKA_OUT_TOPIC` is set**, and
`INVESTOR_PAPER=1` disables publishing entirely (log-only paper mode).

### Decision rule (v1)

Reads `scores["momentum"]` (falling back to the store context when the signal
carries none):

- BUY when `momentum >= INVESTOR_MIN_MOMENTUM` and the per-symbol cooldown has elapsed
- SELL when `momentum <= INVESTOR_MAX_MOMENTUM` and the per-symbol cooldown has elapsed
- HOLD otherwise

`position_size` is `INVESTOR_MAX_POSITION × |momentum|` (clamped); `confidence`
is `momentum` clamped to `[-1, 1]`. A rationale string is logged but not carried
in the message.

### Reserved output score keys

| Key | Value | Meaning |
|---|---|---|
| `stocker_investor.action` | `-1.0` / `0.0` / `+1.0` | SELL / HOLD / BUY |
| `stocker_investor.position_size` | `[0.0, 1.0]` | Fraction of max allowed position |
| `stocker_investor.confidence` | `[-1.0, 1.0]` | Decision confidence |
| `stocker_investor.decided_at_unix_ms` | `> 0` (as `double`) | Unix-millis timestamp |

> **Known deviation:** the implementation currently publishes the *sized* value
> (`INVESTOR_MAX_POSITION × |momentum|`) under `stocker_investor.position_size`
> instead of the `[0.0, 1.0]` fraction documented above, and
> `stocker_investor.decided_at_unix_ms` is a millisecond timestamp. Both fall
> outside the `[-1.0, 1.0]` range that `stocker-store` enforces on ingested
> scores, so do not point a `stocker-store` Kafka subscriber at this output
> topic as-is.

## Configuration

All configuration is via environment variables. The operator creates
`~/.config/stocker-investor/.env.podman` (a sample lives at
`deploy/env/.env.podman.sample`).

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `KAFKA_IN_BROKERS` | Yes | — | Comma-separated Kafka bootstrap servers for the **input** topic |
| `KAFKA_IN_TOPIC` | Yes | — | Topic for `kafkastockv1.StockUpdate` events to consume |
| `KAFKA_IN_GROUP_ID` | No | `stocker-investor` | Consumer group ID |
| `KAFKA_OUT_BROKERS` | No | falls back to `KAFKA_IN_BROKERS` | Bootstrap servers for the **output** topic |
| `KAFKA_OUT_TOPIC` | No | — | Topic to publish decisions on; **no-op when unset** |
| `STORE_GRPC_ADDR` | Yes | — | gRPC target for `stockstorev1.StockStore` (host:port) |
| `STORE_GRPC_TIMEOUT` | No | `2s` | Deadline per gRPC call |
| `INVESTOR_COOLDOWN_MS` | No | `60000` | Per-symbol minimum interval between decisions (ms) |
| `INVESTOR_MAX_POSITION` | No | `10000` | Max position value (CAD) for sizing |
| `INVESTOR_MIN_MOMENTUM` | No | `0.2` | Threshold for BUY on a momentum-up signal |
| `INVESTOR_MAX_MOMENTUM` | No | `-0.2` | Threshold for SELL on a momentum-down signal |
| `INVESTOR_PAPER` | No | `0` | `1` = paper mode (log only, no outbound publish); `0` = normal |
| `LOG_LEVEL` | No | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |

The store client is best-effort: if `STORE_GRPC_ADDR` is unreachable, signals
are still decided from their own scores and the failure is logged.

## Build, test, run

```bash
make build          # go build -o bin/stocker-investor ./cmd/main.go
make test           # go test ./...
go vet ./...        # static checks
go test -race ./... # race detector
gofmt -l .          # should print nothing

# Run with a temp env file (paper mode, no Kafka writes)
env $(grep -v '^#' /tmp/investor.env | xargs) ./bin/stocker-investor
```

A minimal env file for a paper-mode smoke run:

```bash
KAFKA_IN_BROKERS=localhost:9092
KAFKA_IN_TOPIC=stockers
STORE_GRPC_ADDR=localhost:3500
INVESTOR_PAPER=1
```

## Deployment

### Container

```bash
podman build -t stocker-investor .
podman run --rm --env-file ~/.config/stocker-investor/.env.podman stocker-investor
```

The multi-stage `Containerfile` builds with `golang:1.25-bookworm` and runs on
`gcr.io/distroless/static-debian12`. There is no `protoc` step: protos are
pre-generated and committed (see below).

### Push to registry

```bash
./deploy/push.sh   # builds git.wheeli.ca/brian/stocker-investor:latest and pushes
```

### Quadlet (self-host)

1. Write `~/.config/stocker-investor/.env.podman` (mirror `deploy/env/.env.podman.sample`).
2. `./deploy/push.sh`.
3. `mkdir -p ~/.config/containers/systemd && cp deploy/quadlet/stocker-investor.container ~/.config/containers/systemd/`.
4. `systemctl --user enable --now stocker-investor.container`.
5. `systemctl --user status stocker-investor.container` and `podman --rootless logs stocker-investor`.

The unit uses `Network=host` so the gRPC client can reach a host-local
`stocker-store` without NAT; drop that line if not needed.

## Development

### Repository layout

```
cmd/main.go                 # entrypoint: config → observability → gRPC client → Kafka → handler loop
internal/config/            # env-var config parsing + validation
internal/observability/     # slog JSON logging + correlation_id
internal/storeclient/       # gRPC client for stockstorev1.StockStore
internal/investor/          # pure decision logic + per-symbol cooldown
internal/consumer/          # Kafka consumer (kafkastockv1 decode)
internal/producer/          # Kafka producer (kafkastockv1 encode; no-op when disabled/paper)
proto/v1/                   # pre-generated protobufs (copied from stocker-store)
deploy/                     # push.sh, quadlet unit, .env.podman sample
```

### Protobuf provenance

All message types come from the shared `stocker-store` contract, snapshotted
verbatim (no `protoc` run in this repo). See `proto/README.md` for the pinned
tag and the update procedure. Do not hand-edit the generated files.

## Further reading

- `STOCK_TRADING_V2_IMPLEMENTATION_PLAN.md` — the implementation plan (source of truth for intended behavior).
- `stock_trading_service_design.md` — historical design doc (Python/IBKR era; kept for reference).
- Sibling repo: https://git.wheeli.ca/brian/stocker-store