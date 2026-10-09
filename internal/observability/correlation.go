// Package observability provides structured logging and correlation ID utilities.
package observability

import "context"

type ctxKey struct{}

var correlationKey = ctxKey{}

// With adds a correlation ID to the context.
func With(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, correlationKey, id)
}

// From retrieves the correlation ID from the context.
func From(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(correlationKey).(string); ok {
		return v
	}
	return ""
}

// Clear removes the correlation ID from the context.
func Clear(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, correlationKey, nil)
}
