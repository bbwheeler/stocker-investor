// Package observability provides structured logging and correlation ID utilities.
package observability

import (
	"context"
	"testing"
)

func TestCorrelationRoundtrip(t *testing.T) {
	ctx := With(context.Background(), "test-correlation-id")
	if got := From(ctx); got != "test-correlation-id" {
		t.Errorf("From(ctx) = %q, want %q", got, "test-correlation-id")
	}

	cleared := Clear(ctx)
	if got := From(cleared); got != "" {
		t.Errorf("From(cleared) = %q, want empty string", got)
	}
}
