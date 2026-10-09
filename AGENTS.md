# Stocker Investor

This is `stocker-investor`, a Go service in the same family as `stocker-store`.

* Convention: env-var config, structured logging with correlation IDs, single binary, message types come from `proto/v1/`.
* When in doubt, model after the sibling repo (https://git.wheeli.ca/brian/stocker-store): its `cmd/main.go`, `Makefile`, `Containerfile`, and `deploy/`.
