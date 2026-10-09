// Package observability provides structured logging and correlation ID utilities.
package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

var Logger *slog.Logger

// Init initializes the global structured logger with JSON output to stdout.
func Init(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToUpper(level) {
	case "DEBUG":
		lvl = slog.LevelDebug
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	case "INFO":
		fallthrough
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	Logger = slog.New(handler)
	return Logger
}

// LoggerWithContext returns a logger with correlation_id bound from context.
func LoggerWithContext(ctx context.Context) *slog.Logger {
	if Logger == nil {
		Init("INFO")
	}
	cid := From(ctx)
	if cid == "" {
		return Logger
	}
	return Logger.With("correlation_id", cid)
}

// logWithContext returns a logger with correlation_id bound from context.
func logWithContext(ctx context.Context) *slog.Logger {
	return LoggerWithContext(ctx)
}
