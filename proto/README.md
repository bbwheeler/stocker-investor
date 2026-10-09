# Protos

Copied from `brian/stocker-store@0.1.0`. Do not hand-edit. To update, PR against
stocker-store and re-copy.

These are the pre-generated Go packages shared across the `stocker-*` family. They are
snapshotted here verbatim (no `protoc` run in this repo) so this service uses the same
message contract as `stocker-store`.

## Contents

| File | Package | Notes |
|---|---|---|
| `v1/stock_store.proto` | `stockstore.v1` | Source of `stockstorev1`. |
| `v1/stockstorev1.pb.go` | `stockstorev1` | Generated messages. |
| `v1/stockstorev1_grpc.pb.go` | `stockstorev1` | Generated gRPC client/server. |
| `v1/kafka/stock_message.proto` | `stockerstore.kafka.v1` | Source of `kafkastockv1`. |
| `v1/kafka/kafkastockv1.pb.go` | `kafkastockv1` | Generated messages (upstream filename `stock_message.pb.go`; renamed to match the Go package). |
| `v1/kafka/README.md` | — | Upstream-provenance README. |

## Updating

1. Make the change in `brian/stocker-store`, tag it.
2. Re-copy the files above into this tree, byte-for-byte.
3. Update the tag in the first line of this file.
4. `go mod tidy && go build ./...`.
