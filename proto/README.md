# Protos

Copied from `brian/stocker-store@main`. Do not hand-edit. To update, make the
change in `brian/stocker-store`, merge to `main`, and re-copy.

These are the pre-generated Go packages shared across the `stocker-*` family. They are
snapshotted here verbatim (no `protoc` run in this repo) so this service uses the same
message contract as `stocker-store`.

## Contents

| File | Package | Notes |
|---|---|---|
| `v1/stock_store.proto` | `stockstore.v1` | Source of `stockstorev1`. |
| `v1/stock_store.pb.go` | `stockstorev1` | Generated messages. |
| `v1/stock_store_grpc.pb.go` | `stockstorev1` | Generated gRPC client/server. |

## Updating

1. Make the change in `brian/stocker-store`, merge to `main`.
2. Re-copy the files above into this tree, byte-for-byte.
3. `go mod tidy && go build ./...`.