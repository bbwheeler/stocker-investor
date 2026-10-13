# Stocker-Investor — Implementation Plan (Go)

## Summary

`stocker-investor` is a **Go service** in the same family as `stocker-store`. It consumes
stock signals (protobuf `stockstorev1.Stock`) from a Kafka topic, pulls context (scores,
state) from the sibling `stockstore.v1.StockStore` gRPC service, computes an investor
decision (BUY / SELL / HOLD, a position size, and a confidence), and publishes the
decision to another Kafka topic as a `stockstorev1.Stock` carrying our reserved
`ScoreEntry` keys.

The service is a single Go binary. Config is entirely via environment variables.
Deployment is a multi-stage `Containerfile` (golang → distroless) built and pushed to
`git.wheeli.ca/brian/stocker-investor:latest`, and run as a rootless Podman Quadlet.

All message types — input and output — come from the shared `.proto` contract in
`stocker-store@main` (see [Protobuf Integration](#protobuf-integration)). No local
ad-hoc JSON structs are used for family messages.

**This is a reimplementation.** The Python implementation in `stock_trading/`, `tests/`,
`configs/`, `requirements*.txt`, `pyproject.toml`, `Dockerfile`, and `docker-compose.yml`
is removed in one step (step 12) and replaced by the Go tree in steps 1–11. There is no
long-lived hybrid; on any given commit the repo is either Python or Go.

## Migration note (contract correction)

The Go implementation already in this repo was written against the **`0.1.0` tag** of
`stocker-store` (a separate `proto/v1/kafka/` proto with `kafkastockv1.StockUpdate`, and
a `ScoreEntry` without `updated_at`). **`stocker-store@main` has no `kafka/` proto** —
the Kafka message is `stockstore.v1.Stock` itself. This plan is a **correction, not a
new feature**: the existing 0.1.0-based code (proto tree, `internal/consumer`,
`internal/producer`, `internal/investor`, `cmd/main.go`) must be updated to main's
contract per steps 4–9. The migration is a single PR on top of the current
`feat/stock-trading-service-v2` branch.

---

## What this service is / is not

### It is
- A Kafka **consumer** of `stockstorev1.Stock` messages (raw scored stock events).
- A gRPC **client** of `stockstore.v1.StockStore` (read context: current scores,
  recent history — via `GetStock` / `GetStocks`).
- A pure **compute** step that derives an investor decision + position size + confidence.
- A Kafka **producer** that publishes the decision on a reserved-topic as a
  `stockstorev1.Stock` with our reserved `ScoreEntry` keys.
- A self-hostable single binary in the same family as `stocker-store`.

### It is not
- **Not an order executor.** It does not talk to IBKR / TWS / Gateway / any broker API.
  The earlier `ib_insync` Python design is not carried over.
- **Not a gRPC server.** It has no outbound gRPC endpoint of its own; it is only a
  consumer + producer + gRPC client. (If a `service Investor {...}` is required later,
  that is a future change — do not design for it in this plan.)
- **Not a stateful store.** No database. No position cache in v1. The decision logic is
  stateless on a per-message basis, with a configurable per-symbol cooldown backed by an
  in-memory map keyed on `symbol` (see step 3 / step 6).
- **Not a strategy / research platform.** One decision rule, configurable by env vars.

---

## Functional scope (from the design doc, carried over)

The design doc described a broader trading service. The scope that fits the family and
this plan is the **compute-and-publish loop**:

1. **Consume** a `stockstorev1.Stock` on `KAFKA_IN_TOPIC`
   - `symbol` (required, non-empty)
   - `exchange` (required, non-empty)
   - `scores` (optional `repeated ScoreEntry`; each `value` in `[-1.0, 1.0]`)
2. **Read context** (optional) for the symbol/exchange via
   `stockstore.v1.StockStore.GetStock` / `GetStocks`.
3. **Compute** a decision:
   - `action` ∈ {HOLD, BUY, SELL}
   - `position_size` (a dollar amount up to `INVESTOR_MAX_POSITION`; normalized to
     `[0.0, 1.0]` before publishing)
   - `confidence` ∈ `[-1.0, 1.0]` (matches the `ScoreEntry.value` range)
   - `rationale` (string) — logged only, not carried in the message
4. **Publish** the decision as a `stockstorev1.Stock` on `KAFKA_OUT_TOPIC`, with our
   reserved `ScoreEntry` keys (all values in `[-1.0, 1.0]`):
   - `stocker_investor.action` → `-1.0` (SELL), `0.0` (HOLD), `+1.0` (BUY)
   - `stocker_investor.position_size` → `[0.0, 1.0]` (normalized fraction)
   - `stocker_investor.confidence` → `[-1.0, 1.0]`
   - (no timestamp key — see [Reserved output score keys](#reserved-output-score-keys))
5. **Log** every signal, decision, and outbound message as structured JSON, with a
   per-signal `correlation_id`.

The rule in step 3 (the *decision function* itself) is one configurable rule driven by
env vars (e.g. "BUY when `momentum` score in `[-1, 1]` is a new local max above
`INVESTOR_MIN_MOMENTUM`, SELL when it is a new local minimum below
`INVESTOR_MAX_MOMENTUM`, otherwise HOLD"). The exact rule is a v1 choice; the point of
the plan is the loop shape and the I/O contract.

---

## Family conventions (from `stocker-store`)

Everything below is **the exact convention from `stocker-store@main`** as of this
writing. We mirror it.

| Concern | `stocker-store` convention | `stocker-investor` follows |
|---|---|---|
| Language / Go version | Go 1.25 (`go 1.25.0`) | Go 1.25 |
| Module path | `stocker-store` (see `go.mod`) | `stocker-investor` |
| Entrypoint | `cmd/main.go`, single binary | `cmd/main.go`, single binary |
| Internal layout | `internal/{grpc,kafka,store}/` + tests alongside | `internal/{config,observability,storeclient,investor,consumer,producer}/` + tests alongside |
| Config | env vars only, no YAML/TOML file | env vars only |
| Structured logging | standard `log` (in stocker-store); `log/slog` JSON in stocker-investor | `log/slog` JSON |
| Error handling | Go `error` returns, `context.Context` cancellation | Go `error` returns, `context.Context` |
| Build | `make build` → `go build -o bin/stocker-store ./cmd/main.go` | `make build` → `go build -o bin/stocker-investor ./cmd/main.go` |
| Containerfile | multi-stage: `golang:1.25-bookworm` → `gcr.io/distroless/static-debian12`, `ENTRYPOINT` | same, but **no `protoc` step** (protos are pre-generated and copied) |
| Push | `podman build -t git.wheeli.ca/brian/stocker-store:latest .` then `podman push` | `podman build -t git.wheeli.ca/brian/stocker-investor:latest .` then `podman push` |
| Quadlet | one `.container` unit: `AutoUpdate=registry`, `EnvironmentFile=%h/.config/<name>/.env.podman`, `Restart=always`, `RestartSec=10` | one `.container` unit with `Image=git.wheeli.ca/brian/stocker-investor:latest` and `EnvironmentFile=%h/.config/stocker-investor/.env.podman` |
| Pre-generated protos | committed in `proto/v1/` (no `kafka/` subdir on main) | **committed in `proto/v1/` (copied from stocker-store@main)** |
| Message contract | all messages in `stockstore.v1` | all messages in `stockstore.v1` (Kafka and gRPC both use `Stock`; no local ad-hoc types) |
| Tests alongside code | `internal/{...}/{...}_test.go` | same |
| Makefile | `build` / `test` / `run` / `clean` | same |

---

## Module layout

```
stocker-investor/
├── cmd/
│   └── main.go                          # entrypoint: env config, client, consumer/producer wire-up
├── internal/
│   ├── config/
│   │   ├── config.go                    # env-var parsing + validation
│   │   └── config_test.go
│   ├── observability/
│   │   ├── logging.go                   # slog setup (JSON), correlation_id binding
│   │   ├── correlation.go               # With(ctx) / Clear(ctx)
│   │   └── (tests)
│   ├── storeclient/
│   │   ├── client.go                    # gRPC client over stockstorev1.StockStoreClient
│   │   └── client_test.go               # in-process fake server (bufconn)
│   ├── investor/
│   │   ├── decision.go                  # pure Decider: Decision, decide(sig, ctx, cfg)
│   │   └── decision_test.go             # table-driven; no I/O
│   ├── consumer/
│   │   ├── consumer.go                  # segmentio/kafka-go Reader; stockstorev1 decode
│   │   └── consumer_test.go             # mock Reader
│   └── producer/
│       ├── producer.go                  # segmentio/kafka-go Writer; stockstorev1 encode
│       └── producer_test.go             # mock Writer
├── proto/
│   ├── v1/
│   │   ├── stock_store.proto            # (copied verbatim from stocker-store@main)
│   │   ├── stock_store.pb.go            # (copied verbatim from stocker-store@main)
│   │   └── stock_store_grpc.pb.go       # (copied verbatim from stocker-store@main)
│   └── README.md                        # provenance note (brian/stocker-store@main)
├── deploy/
│   ├── quadlet/
│   │   └── stocker-investor.container
│   └── push.sh
├── Containerfile                        # multi-stage (golang → distroless)
├── Makefile                             # build / test / run / clean
├── go.mod                               # module stocker-investor; go 1.25.0
├── go.sum
├── AGENTS.md                            # 3–5 lines: "model after stocker-store"
├── README.md                            # overview (rewrite)
├── STOCK_TRADING_V2_IMPLEMENTATION_PLAN.md   # this file
└── stock_trading_service_design.md            # historical (keep for reference; do not edit)

REMOVED (step 12):
├── stock_trading/                       # entire Python package
├── tests/                               # entire Python test tree
├── configs/                             # YAML config examples
├── requirements.txt                     # Python deps
├── requirements-dev.txt                 # Python dev deps
├── pyproject.toml                       # Python build metadata
├── Dockerfile                           # Python image
├── docker-compose.yml                   # Python compose
└── .dockerignore                        # Python excludes
```

---

## Protobuf Integration

**Rule:** all Kafka and gRPC message types used by this service MUST come from the
shared `.proto` contract in **`stocker-store@main`**:

- `proto/v1/stock_store.proto` → package `stockstore.v1`, Go package `stockstorev1`

There is **no `kafka/` proto on `stocker-store@main`.** The Kafka message type is
`stockstore.v1.Stock` itself: both the inbound signal and the outbound decision are a
`*stockstorev1.Stock` carrying `repeated ScoreEntry scores`. The older
`stockerstore.kafka.v1.StockUpdate` (`map<string, double> scores`) existed only on the
`0.1.0` tag and is **not** part of main's contract.

**No** local ad-hoc JSON structs, hand-rolled Go structs, or locally-defined `.proto`
messages may be used for family messages. Our *reserved score keys* — e.g.
`stocker_investor.action` — are `ScoreEntry`s under the shared `Stock.scores` field,
not new message types.

### How to obtain the types

**Do not re-run `protoc`.** Copy the pre-generated files from `stocker-store@main`
verbatim into this repo's `proto/v1/` tree. (`stocker-store@main`'s
`proto/v1/README.md` prefers consuming via a Go module dependency —
`go get git.wheeli.ca/brian/stocker-store@latest` — but this repo snapshots the files
so it stays self-contained, matching the family's "copy the protos" convention.)
`proto/v1/` on main contains only `stock_store.proto`, `stock_store.pb.go`,
`stock_store_grpc.pb.go`, and `README.md` — no `kafka/` subdirectory.

Step-by-step:

1. From `stocker-store@main`, copy these three files verbatim into this repo:
   - `proto/v1/stock_store.proto` → `proto/v1/stock_store.proto`
   - `proto/v1/stock_store.pb.go` → `proto/v1/stock_store.pb.go`
   - `proto/v1/stock_store_grpc.pb.go` → `proto/v1/stock_store_grpc.pb.go`
   These replace the existing 0.1.0 files `proto/v1/stockstorev1.pb.go` and
   `proto/v1/stockstorev1_grpc.pb.go` (delete the old files so the `stockstorev1`
   package is defined once). The Go package name remains `stockstorev1` for all three.
2. **Remove the `proto/v1/kafka/` tree** (`stock_message.proto`, `kafkastockv1.pb.go`,
   `README.md`). It does not exist on main and its `StockUpdate` type is gone.
3. Update `proto/README.md` provenance to `brian/stocker-store@main` and drop the
   `v1/kafka/` rows from its Contents table (see step 4).
4. `go mod tidy` — adds/keeps `google.golang.org/protobuf` and
   `google.golang.org/grpc` (the `.pb.go` files import them). The `go_package` option
   in the `.proto` only affects where `protoc` puts output, not the module; the file on
   disk is a normal Go package `stockstorev1` that this repo imports at
   `stocker-investor/proto/v1`.
5. `go build ./...` — succeeds; the `stockstorev1` package compiles.

### Updating the contract

If the shared `.proto` changes in `stocker-store`, the update is a PR against
`stocker-store` followed by a re-copy here (steps 1–5 above). There is no submodule
relationship and no shared Go module dependency; the contract is a snapshot.

### Reserved output score keys

On `stocker-store@main`, `Stock.scores` is `repeated ScoreEntry{category, value,
updated_at}` and `ScoreEntry.value` MUST be in `[-1.0, 1.0]` — main's Kafka consumer
**hard-rejects** (drops) any `Stock` carrying a `ScoreEntry` outside that range. Our
published decision is therefore encoded as `ScoreEntry`s, and **every value we publish
must be within `[-1.0, 1.0]`**:

| `ScoreEntry.category` | `ScoreEntry.value` | Meaning |
|---|---|---|
| `stocker_investor.action` | `-1.0` / `0.0` / `+1.0` | SELL / HOLD / BUY |
| `stocker_investor.position_size` | `[0.0, 1.0]` | Fraction of max allowed position (see normalization below) |
| `stocker_investor.confidence` | `[-1.0, 1.0]` | Decision confidence |

`position_size` normalization: the decider sizes a **dollar** position
(`Decision.PositionSize`, up to `INVESTOR_MAX_POSITION` CAD). Before publishing, the
entrypoint normalizes it to a `[0, 1]` fraction:
`clamp(Decision.PositionSize / INVESTOR_MAX_POSITION, 0.0, 1.0)`.

`ScoreEntry.updated_at` is **advisory** — `stocker-store` stamps its own clock on write
and discards what we send. We may set it to the decision time
(`timestamppb.New(d.DecidedAt)`); it carries no contract weight.

The decision timestamp is **not** published as a score. An earlier draft used a
`stocker_investor.decided_at_unix_ms` key, but a Unix-millis value is far outside
`[-1.0, 1.0]` and would be rejected by `stocker-store`. **Drop that key**; the timestamp
is already in the structured log line (and, optionally, in `ScoreEntry.updated_at`).

Rationale (a string) is **not** carried in the message (scores are numeric
`ScoreEntry`s); it is logged (step 2 / step 9).

---

## Config (env vars)

All config is via environment variables, mirroring `stocker-store`'s convention.
The operator creates `~/.config/stocker-investor/.env.podman` (not shipped in the repo).

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `KAFKA_IN_BROKERS` | Yes | — | Comma-separated Kafka bootstrap servers for the **input** topic |
| `KAFKA_IN_TOPIC` | Yes | — | Topic for `stockstorev1.Stock` events to consume |
| `KAFKA_IN_GROUP_ID` | No | `stocker-investor` | Consumer group ID |
| `KAFKA_OUT_BROKERS` | No | falls back to `KAFKA_IN_BROKERS` | Bootstrap servers for the **output** topic |
| `KAFKA_OUT_TOPIC` | No | `<input>_decisions` | Topic to publish `Stock` decisions on; **no-op when unset** |
| `STORE_GRPC_ADDR` | Yes | — | gRPC target for `stockstorev1.StockStore` (host:port) |
| `STORE_GRPC_TIMEOUT` | No | `2s` | Deadline per gRPC call |
| `INVESTOR_COOLDOWN_MS` | No | `60000` | Per-symbol minimum interval between decisions (ms) |
| `INVESTOR_MAX_POSITION` | No | `10000` | Max position value (CAD) for sizing |
| `INVESTOR_MIN_MOMENTUM` | No | `0.2` | Threshold for BUY on a momentum-up signal |
| `INVESTOR_MAX_MOMENTUM` | No | `-0.2` | Threshold for SELL on a momentum-down signal |
| `INVESTOR_PAPER` | No | `0` | `1` = paper mode (log only, no outbound publish); `0` = normal |
| `LOG_LEVEL` | No | `INFO` | `DEBUG` / `INFO` / `WARN` / `ERROR` |

Rule (mirror stocker-store): **Kafka output is a no-op unless `KAFKA_OUT_TOPIC` is set.**
The service runs as consumer-only when output is not configured.

---

## Deployment Strategy

The deployment mirrors `stocker-store` exactly. Three files, all shipped in the repo:
`Containerfile`, `deploy/push.sh`, and
`deploy/quadlet/stocker-investor.container`. The env file is created by the operator,
not shipped.

### A. `Containerfile` (root of repo)

Multi-stage build using the same bases as stocker-store, but **no `protoc` step**
(protos are pre-generated and copied in step 4):

```dockerfile
# Build stage
FROM docker.io/golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/stocker-investor ./cmd/main.go

# Runtime stage
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/stocker-investor /stocker-investor
ENTRYPOINT ["/stocker-investor"]
```

Notes:
- No `EXPOSE` (the service binds on the env-configured gRPC target for the client;
  it does not itself listen).
- `CGO_ENABLED=0` for a fully static binary; safe on distroless static.

### B. `deploy/push.sh` (root of the `deploy/` dir)

Build and push the image to the same Forgejo registry as stocker-store, with the
same tag convention (`<name>:latest`):

```bash
#!/usr/bin/env bash
set -euo pipefail

REGISTRY="git.wheeli.ca/brian"
IMAGE_NAME="stocker-investor:latest"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Building image for $REGISTRY"
podman build -t "${REGISTRY}/${IMAGE_NAME}" "$PROJECT_DIR"

echo "==> Pushing image to $REGISTRY"
podman push "${REGISTRY}/${IMAGE_NAME}"

echo "Done. Image pushed to ${REGISTRY}/${IMAGE_NAME}"
```

### C. `deploy/quadlet/stocker-investor.container`

Single Quadlet unit (stocker-store has only a `.container`; no `.build` in-tree) that
pulls from the registry, enables autoupdate from the registry, and loads env from the
operator-created file. `Network=host` is added so the gRPC client can reach host-local
services (e.g. a same-host StockStore) without NAT; drop the line if not needed.

```ini
[Unit]
After=network-online.target

[Container]
Image=git.wheeli.ca/brian/stocker-investor:latest
EnvironmentFile=%h/.config/stocker-investor/.env.podman
AutoUpdate=registry
Network=host

[Service]
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

### D. Operator-created env file (NOT shipped)

The operator writes `~/.config/stocker-investor/.env.podman`:

```bash
# — Required — input (Kafka).
KAFKA_IN_BROKERS=kafka-0.internal:9092,kafka-1.internal:9092
KAFKA_IN_TOPIC=stockers
KAFKA_IN_GROUP_ID=stocker-investor
STORE_GRPC_ADDR=localhost:3500

# — Optional — output (publish decisions). No-op when unset.
# KAFKA_OUT_BROKERS=kafka-0.internal:9092
# KAFKA_OUT_TOPIC=stocker_investor_decisions

# — Optional — investor knobs.
# INVESTOR_COOLDOWN_MS=60000
# INVESTOR_MAX_POSITION=10000
# INVESTOR_MIN_MOMENTUM=0.2
# INVESTOR_MAX_MOMENTUM=-0.2
# INVESTOR_PAPER=1

# — Optional — logging.
# LOG_LEVEL=INFO
```

### E. End-to-end quickstart (Quadlets)

1. Operator writes `~/.config/stocker-investor/.env.podman` (see D).
2. Build + push: `./deploy/push.sh`.
3. Copy the Quadlet into the user's Quadlet dir:
   `mkdir -p ~/.config/containers/systemd && cp deploy/quadlet/stocker-investor.container ~/.config/containers/systemd/`.
4. Enable & start: `systemctl --user enable --now stocker-investor.container`.
5. Verify: `systemctl --user status stocker-investor.container` and
   `podman --rootless logs stocker-investor`.

---

## Numbered implementation steps

Each number is ONE self-contained, delegable developer task. Each step has:
**Goal**, **Files**, **Verify** (the gate that must pass before the step is done).
Later steps may read or import code from earlier steps, but a developer working on a
later step is given everything they need to know from this file.

### Step 1 — Go module skeleton

**Goal:** a minimal, buildable Go module with a stub `main` and the Makefile / AGENTS.md.

**Files:**
- `go.mod` — `module stocker-investor`; `go 1.25.0`.
- `cmd/main.go` — a minimal `package main` with a `func main()` that prints
  `"stocker-investor: stub"` to stdout and returns. Enough to prove the module compiles.
- `Makefile` — four targets, mirroring stocker-store (build / test / run / clean),
  with binary name `stocker-investor` and entrypoint `./cmd/main.go`.
- `AGENTS.md` — 3–5 lines: "This is `stocker-investor`, a Go service in the same family
  as `stocker-store`. Model after stocker-store: env-var config, structured logging,
  single binary, pre-generated protobufs in `proto/v1/`. Model after
  `stocker-store`'s `cmd/main.go`, `Makefile`, and `Containerfile` directly."

**Verify:** from clean tree, `make build` produces `bin/stocker-investor`, `go vet ./...`
passes, `make test` passes (no tests yet → 0 tests, exit 0).

### Step 2 — Observability (`log/slog` + correlation_id)

**Goal:** structured JSON logging with a per-signal `correlation_id`, matching the
design doc's "Audit & Logging Layer" requirement.

**Files:**
- `internal/observability/logging.go` — `Init(level string) *slog.Logger` building a
  JSON handler to stdout; a package-level `log.Logger` accessible from other
  sub-packages.
- `internal/observability/correlation.go` — `With(ctx, id string) context.Context`,
  `From(ctx) string`, `Clear(ctx) context.Context`; a helper `logWithContext(ctx)`
  that returns a `*slog.Logger` with the `correlation_id` attribute bound.
- `internal/observability/correlation_test.go` — roundtrip tests.

**Verify:** `go vet ./... && go test ./...` green.

### Step 3 — Config (env-var parsing)

**Goal:** parse all config from env vars into a typed `Config` struct, with validation
errors for missing required variables.

**Files:**
- `internal/config/config.go` — `Config` struct (fields per the [Config section above]),
  `FromEnv() (Config, error)`, and helper `envList(name) []string` (comma-split).
  Defaults and validation rules exactly as in the [Config section above].
- `internal/config/config_test.go` — table-driven: valid / missing-required /
  invalid-value cases; each test uses an isolated env var set (set / unset via
  `t.Setenv`).

**Verify:** `go vet ./... && go test ./...` green.

### Step 4 — Protobufs (copy from stocker-store@main)

**Goal:** bring the shared `stockstorev1` Go package into this repo, verbatim, from
`stocker-store@main`; remove the 0.1.0 `kafka/` proto tree; update provenance. No local
ad-hoc types.

**Files:**
- Copy from `stocker-store@main` `proto/v1/` (main has only these three files plus a
  `README.md`; there is **no `kafka/` subdirectory**):
  - `proto/v1/stock_store.proto` → `proto/v1/stock_store.proto`
  - `proto/v1/stock_store.pb.go` → `proto/v1/stock_store.pb.go`
  - `proto/v1/stock_store_grpc.pb.go` → `proto/v1/stock_store_grpc.pb.go`
  These replace the existing 0.1.0 files `proto/v1/stockstorev1.pb.go` and
  `proto/v1/stockstorev1_grpc.pb.go` — **delete the old files** so the `stockstorev1`
  package is defined once. The Go package name stays `stockstorev1`.
- **Delete the entire `proto/v1/kafka/` tree** (`stock_message.proto`,
  `kafkastockv1.pb.go`, `README.md`). It does not exist on main; its `StockUpdate` type
  is gone.
- Update `proto/README.md`:
  - First line: "Copied from `brian/stocker-store@main`."
  - Contents table: keep only the `v1/stock_store.proto`, `v1/stock_store.pb.go`,
    `v1/stock_store_grpc.pb.go` rows (package `stockstorev1`); **remove the
    `v1/kafka/...` rows**.
  - Updating section: "Make the change in `brian/stocker-store`, merge to `main`."
- `go mod tidy` — adds/keeps `google.golang.org/protobuf` and
  `google.golang.org/grpc` (the `.pb.go` files import them). **Do not** run `protoc`.
  **Do not** hand-edit the copied files.

**Verify:** `go build ./...` succeeds. `go vet ./...` and `go test ./...` green.
`grep -r "kafkastockv1\|StockUpdate" proto/` returns nothing. (At this point no code
imports the protos; the packages are compiled but unused — which is fine.)

### Step 5 — gRPC client for `stockstore.v1.StockStore`

**Goal:** a thin client wrapper that calls `GetStock` and `GetStocks` with main's
signatures, with a deadline and an interface for testing.

**Files:**
- `internal/storeclient/client.go` —
  - `Client` interface (request-message signatures per `stocker-store@main`):
    - `GetStock(ctx context.Context, req *stockstorev1.GetStockRequest) (*stockstorev1.Stock, error)`
    - `GetStocks(ctx context.Context, req *stockstorev1.GetStocksRequest) (*stockstorev1.StockList, error)`
  - `Client` struct implementing the interface, holding a `stockstorev1.StockStoreClient`
    and a default `context.Context` deadline.
  - `New(addr string, timeout time.Duration) (*Client, error)` — uses
    `grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))`.
  - Note: on main, `GetStockRequest.exchange` is `optional string` (Go `*string`) and
    `Stock.scores` is `repeated ScoreEntry` (`[]*stockstorev1.ScoreEntry` with
    `Category`, `Value`, `UpdatedAt`). The wrapper just forwards the request; callers
    build it (see step 9).
- `internal/storeclient/client_test.go` — in-process fake server on
  `bufconn` returning canned `Stock` / `StockList` (with `ScoreEntry` scores); verify
  decode and that an unreachable target times out with a wrapped error within
  `~2× deadline`.

**Verify:** `go vet ./... && go test ./...` green.

### Step 6 — Pure investor decision logic

**Goal:** the decision function, fully testable, no I/O, no gRPC, no Kafka.

**Files:**
- `internal/investor/decision.go` —
  - `Action` type (typed `int8`: `HOLD Action = 0`, `BUY Action = 1`, `SELL Action = -1`)
  - `Decision` struct: `Action`, `PositionSize float64` (a **dollar** amount, up to
    `cfg.MaxPosition`), `Confidence float64`, `Rationale string`, `DecidedAt time.Time`.
  - `Config` struct: a subset of `internal/config.Config` (cooldown, max position,
    min/max momentum, paper).
  - `Decider` interface: `Decide(ctx context.Context, sig *stockstorev1.Stock,
    stockCtx *stockstorev1.Stock) Decision`. Both the signal and the store context are
    now the **same** type (`*stockstorev1.Stock`); the signal is the consumed Kafka
    message and `stockCtx` is the optional gRPC lookup (may be `nil`).
  - `NewDecider(cfg) Decider`.
  - Momentum lookup helper: `momentumScore(sig, stockCtx *stockstorev1.Stock) (float64,
    bool)` — scan `sig.GetScores()` for the `ScoreEntry` whose `Category == "momentum"`
    and return its `Value`; fall back to `stockCtx`'s `"momentum"` entry when the signal
    has none. (There is no `scores` map anymore — it is a `repeated ScoreEntry`.)
  - The default implementation: a single rule — "BUY when
    `momentum >= cfg.MinMomentum` and the cooldown has elapsed; SELL when
    `momentum <= cfg.MaxMomentum` and the cooldown has elapsed; otherwise HOLD."
    `PositionSize` is scaled by `cfg.MaxPosition` and a simple `abs(momentum)` factor
    (this is a dollar amount; step 9 normalizes it for publishing); `Confidence` is the
    normalized `momentum` value clamped to `[-1, 1]`. (The exact rule is a v1 choice per
    the [Functional scope section]; swap the rule body freely — the interface is what
    matters.)
- `internal/investor/decision_test.go` — table-driven over (sig, store context, config)
  → expected (Action, PositionSize, Confidence, Rationale prefix). Build signals as
  `&stockstorev1.Stock{Symbol: ..., Exchange: ..., Scores: []*stockstorev1.ScoreEntry{{Category: "momentum", Value: ...}}}`.
- `internal/investor/cooldown.go` — a small in-memory per-symbol cooldown map keyed on
  `symbol`, `CheckAndSet(symbol string, now time.Time) bool`.
- `internal/investor/cooldown_test.go` — basic roundtrip.

**Verify:** `go vet ./... && go test ./...` green.

### Step 7 — Kafka consumer

**Goal:** consume `stockstorev1.Stock` messages from `KAFKA_IN_TOPIC`; hand valid ones
to a callback; log-and-skip malformed ones (matching stocker-store's family style of
dropping invalid messages rather than routing to a DLQ in v1).

**Files:**
- `internal/consumer/consumer.go` —
  - `Handler func(ctx context.Context, sig *stockstorev1.Stock) error`.
  - `Config` struct: `Brokers []string`, `Topic string`, `GroupID string`.
  - `Consumer` struct wrapping a `*kafka.Reader` (from `segmentio/kafka-go`).
  - `New(cfg Config) *Consumer` — builds the reader.
  - `Run(ctx context.Context, h Handler) error` — loop:
    ```
    for {
      m, err := c.reader.FetchMessage(ctx)
      if err == context.Canceled { return nil }
      if err != nil { log; continue / backoff }
      sig := &stockstorev1.Stock{}                 // main's Kafka message IS Stock
      if err := proto.Unmarshal(m.Value, sig); err != nil { log warn "malformed"; c.reader.CommitMessages(m); continue }
      if err := h(ctx, sig); err != nil { log warn "handler"; c.reader.CommitMessages(m); continue }
      c.reader.CommitMessages(m)
    }
    ```
- `internal/consumer/consumer_test.go` — mock the `Reader` (interface) — feed one good
  message (assert `Handler` called once with correct `sig`), one `proto.Unmarshal`
  error (assert `Handler` not called, message committed), and one `ctx` cancellation
  (assert `Run` returns `nil` with `context.Canceled`). Marshal test payloads with
  `proto.Marshal(&stockstorev1.Stock{...})`.

**Verify:** `go vet ./... && go test ./...` green.

### Step 8 — Kafka producer

**Goal:** publish a `stockstorev1.Stock` (with our reserved `ScoreEntry` keys) to
`KAFKA_OUT_TOPIC`, or be a no-op when unconfigured.

**Files:**
- `internal/producer/producer.go` —
  - `Config` struct: `Brokers []string`, `Topic string`, `Enabled bool`.
  - `Producer` struct wrapping a `*kafka.Writer` (from `segmentio/kafka-go`).
  - `New(cfg Config) *Producer` — if `cfg.Enabled == false` or `Topic == ""`, set
    `p.noop = true` and do not construct a writer.
  - `Publish(ctx context.Context, sig *stockstorev1.Stock) error` —
    `proto.Marshal` then write `kafka.Message{Topic, Key: []byte(sig.GetSymbol()),
    Value: ...}`; if `p.noop`, log `debug` and return `nil` without sending.
  - `Close() error` — close the writer if constructed.
- `internal/producer/producer_test.go` — mock the `Writer` (interface), assert:
  - enabled: correct topic/key/value bytes (unmarshal the value back to a
    `stockstorev1.Stock` and compare);
  - disabled: `Publish` returns `nil`, mock is never called.

**Verify:** `go vet ./... && go test ./...` green.

### Step 9 — Wire the entrypoint (`cmd/main.go`)

**Goal:** replace the step-1 stub with the full wire-up: config, observability, client,
consumer, producer, and the signal → decide → publish handler loop. The published
message is a `stockstorev1.Stock` whose `ScoreEntry` values are **all in `[-1.0, 1.0]`**.

**Files:**
- `cmd/main.go` —
  - Parse `internal/config.FromEnv()`. On error, log `ERROR` and exit 2 (matching
    the design doc's "config failed" failure path).
  - `obs.Init(cfg.LogLevel)`.
  - Build `storeclient.New(cfg.StoreGRPCAddr, cfg.StoreGRPCTimeout)`. On failure →
    log `ERROR`, continue (the decision function must still work without context).
  - Build `consumer.New(...)` and `producer.New(...)`.
  - Build `investor.NewDecider(cfg)`.
  - `ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`.
  - Define `handler(ctx, sig) error` (note `sig` is now `*stockstorev1.Stock`):
    ```
    ctx = obs.With(ctx, correlationIDFor(sig))          // new per-signal id
    log := obs.LoggerWithContext(ctx)
    log.Debug("signal_received", "symbol", sig.GetSymbol(), "exchange", sig.GetExchange())
    var stockCtx *stockstorev1.Stock
    if client != nil {
      exch := sig.GetExchange()
      s, err := client.GetStock(ctx, &stockstorev1.GetStockRequest{
        Symbol: sig.GetSymbol(), Exchange: &exch,
      })
      if err != nil { stockCtx = nil } else { stockCtx = s }       // tolerate errors
    }
    d := decider.Decide(ctx, sig, stockCtx)
    log.Info("decision",
      "symbol", sig.GetSymbol(), "exchange", sig.GetExchange(),
      "action", d.Action, "position_size", d.PositionSize,
      "confidence", d.Confidence, "decided_at", d.DecidedAt, "rationale", d.Rationale)
    if err := producer.Publish(ctx, buildOutput(sig, d, cfg.INVESTOR_MAX_POSITION)); err != nil {
      return fmt.Errorf("publish decision: %w", err)
    }
    return nil
    ```
    (In paper mode, `producer.Publish` is a no-op returning `nil` — already handled by
    the producer in step 8.)
  - `buildOutput(sig *stockstorev1.Stock, d investor.Decision, maxPosition float64)
    *stockstorev1.Stock` — encodes the decision as reserved `ScoreEntry`s, **normalizing
    the dollar position size to a `[0,1]` fraction** and dropping the timestamp key:
    ```
    func buildOutput(sig *stockstorev1.Stock, d investor.Decision, maxPosition float64) *stockstorev1.Stock {
      frac := 0.0
      if maxPosition > 0 { frac = d.PositionSize / maxPosition }
      frac = clamp01(frac)
      ts := timestamppb.New(d.DecidedAt)   // advisory; server stamps its own clock
      return &stockstorev1.Stock{
        Symbol:   sig.GetSymbol(),
        Exchange: sig.GetExchange(),
        Scores: []*stockstorev1.ScoreEntry{
          {Category: "stocker_investor.action",        Value: float64(d.Action), UpdatedAt: ts},
          {Category: "stocker_investor.position_size", Value: frac,              UpdatedAt: ts},
          {Category: "stocker_investor.confidence",    Value: d.Confidence,      UpdatedAt: ts},
        },
      }
    }

    func clamp01(v float64) float64 {
      if v < 0 { return 0 }
      if v > 1 { return 1 }
      return v
    }
    ```
    Every published `Value` is in `[-1.0, 1.0]` (`action` ∈ {-1,0,1}, `position_size`
    ∈ [0,1], `confidence` ∈ [-1,1]). **No `decided_at_unix_ms` key** — a Unix-millis
    value would be rejected by `stocker-store`; the timestamp is logged instead (and
    carried in `ScoreEntry.updated_at`).
  - `correlationIDFor(sig *stockstorev1.Stock) string` — derive from `sig.GetSymbol()`
    and the current time.
  - `consumer.Run(ctx, handler)` in a goroutine, then `<-ctx.Done()`, close the
    client, cancel, exit 0.

**Verify:** `go vet ./... && go test ./...` green. **End-to-end:** with a fake
`KAFKA_IN_BROKERS` / `KAFKA_OUT_BROKERS` (or a `INVESTOR_PAPER=1` env file), an
operator can run the binary against a fake Kafka broker and a fake StockStore
(`--addr` for the gRPC client) and observe the log output (a received signal, a
decision, and either a publish or a paper-mode skip). This is the acceptance gate
for step 9.

### Step 10 — `Containerfile`

**Goal:** the multi-stage container build for this repo, replacing the old Python
`Dockerfile`.

**Files:**
- `Containerfile` — the exact contents in [Deployment Strategy A](#a-containerfile-root-of-repo).

**Verify:** `make build` still succeeds (unchanged). `podman build -t stocker-investor:latest .`
succeeds. `podman run --rm stocker-investor:latest` prints `stocker-investor: stub`
(or the equivalent of the step-1 stub `main` — it will print the real startup logs
from step 9 instead, and exit when `ctx` is canceled).

### Step 11 — `deploy/push.sh` + Quadlet + env file sample

**Goal:** the push script and the Quadlet unit, plus a documented sample env file
that the operator will mirror into `~/.config/stocker-investor/.env.podman`.

**Files:**
- `deploy/push.sh` — the exact contents in [Deployment Strategy B](#b-deploypushsh-root-of-the-deploy-dir); `chmod +x`.
- `deploy/quadlet/stocker-investor.container` — the exact contents in [Deployment Strategy C](#c-deployquadletstocker-investorcontainer).
- `deploy/env/.env.podman.sample` — the exact contents in [Deployment Strategy D](#d-operator-created-env-file-not-shipped); **this is a sample** that the operator copies to `~/.config/stocker-investor/.env.podman`. It is not the file loaded at runtime (that's operator-created).

**Verify:** `bash -n deploy/push.sh` (syntax) passes. Visual diff against
`stocker-store/deploy/push.sh` and `stocker-store/deploy/quadlet/stocker-store.container`
shows only the name substitutions (stocker-store → stocker-investor, same tag,
same registry, same `AutoUpdate=registry`, same `Restart=always / RestartSec=10`).

### Step 12 — Remove the Python codebase

**Goal:** one clean, self-contained removal; no files left that would confuse a build
or a new reader.

**Files to delete (all at once, in one commit):**
- `stock_trading/` (entire package, including `__pycache__`)
- `tests/` (entire Python test tree, including `__pycache__`)
- `configs/` (YAML examples)
- `requirements.txt`
- `requirements-dev.txt`
- `pyproject.toml`
- `Dockerfile` (replaced by the Go `Containerfile` in step 10)
- `docker-compose.yml`
- `.dockerignore` (replaced by a new one if step 10 requires it — for now, none)

**Files to keep:** `README.md` (to be updated in step 13),
`stock_trading_service_design.md` (historical reference; mark it as such),
`STOCK_TRADING_V2_IMPLEMENTATION_PLAN.md` (this file), `AGENTS.md`, `Makefile`,
`go.mod`, `go.sum`, `Containerfile`, `cmd/`, `internal/`, `proto/`, `deploy/`.

**Verify:** `ls -F` shows the Go tree and nothing else from the Python era;
`git status` shows a single staged commit "chore: remove python implementation";
`make build && go vet ./... && go test ./...` still all green;
`podman build -t stocker-investor:latest .` still succeeds; the binary still runs.

### Step 13 — Final gate

**Goal:** from a clean checkout, the whole project builds, vets, tests, and runs —
in one command chain.

**Files:** none (gate only).

**Verify (run in this order, from a clean clone):**
1. `make clean && make build` — produces `bin/stocker-investor`.
2. `go vet ./...` — zero findings.
3. `go test ./...` — all tests pass.
4. `podman build -t stocker-investor .` — image builds.
5. `podman run --rm --env-file ~/.config/stocker-investor/.env.podman stocker-investor`
   — runs with the operator's env file; on SIGTERM (Ctrl-C), exits cleanly with code 0.

If all five are green, the plan is **done**.

---

## Assumptions / blockers

- **Registry access.** Steps 10–11 assume `podman` is configured to log in to
  `git.wheeli.ca` and that the `brian` namespace is writable (or the image lands in a
  shared `brian` project). If it is not, the push step will fail with a 401/403 —
  the operator must `podman login git.wheeli.ca` first.
- **Pre-generated protos must exist in stocker-store@main.** Steps 4 and onward assume
  `stocker-store@main` has `proto/v1/stock_store.pb.go` and
  `proto/v1/stock_store_grpc.pb.go` committed alongside `proto/v1/stock_store.proto`
  (per the stocker-store README, they are). There is **no `proto/v1/kafka/`** on main.
  If stocker-store drops the generated files, the plan's "copy the protos" step needs to
  be changed to "prune + re-run `protoc`" (see the
  [Containerfile](#a-containerfile-root-of-repo) in `stocker-store` for the codegen
  command).
- **IBKR is out of scope.** The earlier `stock_trading_service_design.md` describes
  order execution to IBKR via `ib_insync`. That is not carried over. If it is
  required later, it is a separate plan; this plan deliberately does not design for
  it.
- **Paper mode is the safe default for v1.** `INVESTOR_PAPER=1` disables outbound
  publishing; the operator should run the first real deployment in paper mode.
- **No database.** The service in this plan is stateless per message (with an
  in-memory cooldown map). If state across restarts is required (e.g. a persisted
  cooldown store, an audit table), that is a separate plan.
- **Kafka out topic is a no-op when unset.** This matches stocker-store's "Kafka is
  optional" convention. A deployment that only wants to *consume* is fully functional
  without an output topic.

---

## What was cut, and why (from the old 85KB plan)

The previous `STOCK_TRADING_V2_IMPLEMENTATION_PLAN.md` was 2,116 lines and was a plan
for the **Python** service. The Python skeleton it was extending is entirely stubs —
the real `TradingEngine.process_signal`, `IbkrExecutor.place_order`, and
`SignalConsumer.consume` are all `NotImplementedError`. Cutting them is cutting the
whole plan.

What was cut:

- **The `ib_insync` design** — IBKR is a custom-protocol broker API and is the single
  thing that did not fit the Go family convention. Replacing it with a gRPC +
  protobuf consumer/producer loop is what this plan does.
- **The 7-phase roadmap** (foundation, strategy, portfolio, paper, monitoring,
  hardening, production) — replaced by 13 delegable steps.
- **The Kafka DLQ, alerting, and Grafana dashboards** (phases 5–6) — out of scope
  for v1; the service is a single binary, the operator adds dashboards if they want.
- **The 2,116 lines of Python test fixtures** around `ib_insync` mocks — the new tree
  replaces them with 6 Go test files.
- **The `configs/*.yml`** — replaced by env vars (matching family convention).
- **`Dockerfile` (Python)** and **`docker-compose.yml`** — replaced by
  `Containerfile` (Go) and the Quadlet.
- **`pyproject.toml`, `requirements*.txt`** — no longer needed.

The one piece of the old plan that is **retained** is the *functional contract*:
Kafka in → decision → Kafka out, with a gRPC service for context, structured JSON
logging with correlation IDs, and a per-symbol cooldown. Everything around that
contract (the strategy rule, the exact I/O shape) is now written in Go against the
shared protobuf, and every detail is in one place in this file.
