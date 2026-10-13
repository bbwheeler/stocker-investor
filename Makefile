.PHONY: build test clean run

build:
	go build -o bin/stocker-investor ./cmd/main.go

test:
	go test ./...

clean:
	rm -rf bin/

run: build
	./bin/stocker-investor
