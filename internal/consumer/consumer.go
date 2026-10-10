// Package consumer consumes kafkastockv1.StockUpdate messages from a Kafka
// topic and hands valid ones to a Handler. Malformed messages and handler
// failures are logged and skipped (committed) rather than routed to a DLQ.
package consumer

import (
	"context"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"stocker-investor/internal/observability"
	kafkastockv1 "stocker-investor/proto/v1/kafka"
)

// Handler processes a decoded stock update. A non-nil error is logged and the
// offending message is committed and skipped; the consumer keeps running.
type Handler func(ctx context.Context, sig *kafkastockv1.StockUpdate) error

// Config holds the settings required to consume the stock topic.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string
}

// reader is the subset of *kafka.Reader used by Consumer. It is an interface so
// tests can substitute a scripted implementation.
type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Consumer reads messages from a Kafka topic and dispatches them to a Handler.
type Consumer struct {
	reader reader
}

// New builds a Consumer backed by a segmentio/kafka-go reader.
func New(cfg Config) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  cfg.Brokers,
			Topic:    cfg.Topic,
			GroupID:  cfg.GroupID,
			MinBytes: 1e3,
			MaxBytes: 1e6,
		}),
	}
}

// Run consumes messages until ctx is canceled, invoking h for every
// successfully decoded message. Malformed messages, handler errors, and
// transient fetch errors are logged and the loop continues; it returns nil on
// cancellation.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	log := observability.LoggerWithContext(ctx)

	for {
		m, err := c.reader.FetchMessage(ctx)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.Warn("kafka: fetch message", "error", err)
			continue
		}

		sig := &kafkastockv1.StockUpdate{}
		if err := proto.Unmarshal(m.Value, sig); err != nil {
			log.Warn("kafka: malformed message", "topic", m.Topic, "offset", m.Offset, "error", err)
			c.commit(ctx, m, log)
			continue
		}

		if err := h(ctx, sig); err != nil {
			log.Warn("kafka: handler error", "topic", m.Topic, "offset", m.Offset, "error", err)
			c.commit(ctx, m, log)
			continue
		}

		c.commit(ctx, m, log)
	}
}

// Close releases the underlying Kafka reader. It should be called after Run
// has returned.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// commit advances the consumer group offset past m, logging (but not failing)
// on error so a commit problem never crashes the loop.
func (c *Consumer) commit(ctx context.Context, m kafka.Message, log *slog.Logger) {
	if err := c.reader.CommitMessages(ctx, m); err != nil {
		log.Warn("kafka: commit message", "topic", m.Topic, "offset", m.Offset, "error", err)
	}
}
