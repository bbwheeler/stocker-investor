package observability

import (
	"context"
	"log/slog"
	"testing"
)

func TestInitLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"DEBUG", slog.LevelDebug},
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"bogus", slog.LevelInfo},
		{"", slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			l := Init(tt.level)
			if l == nil {
				t.Fatal("Init returned nil logger")
			}
			if !l.Enabled(context.Background(), tt.want) {
				t.Errorf("logger for level %q should be enabled at %v", tt.level, tt.want)
			}
		})
	}
}

func TestLoggerWithContext(t *testing.T) {
	Init("INFO")

	plain := LoggerWithContext(context.Background())
	if plain == nil {
		t.Fatal("LoggerWithContext(background) returned nil")
	}

	ctx := With(context.Background(), "cid-123")
	bound := LoggerWithContext(ctx)
	if bound == nil {
		t.Fatal("LoggerWithContext(with cid) returned nil")
	}
}
