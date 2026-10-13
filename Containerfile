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