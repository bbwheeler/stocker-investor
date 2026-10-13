// Package producer publishes stockstorev1.Stock messages to a Kafka topic.
// When Kafka output is unconfigured (disabled or no topic), the producer is a
// no-op: Publish logs at debug level and returns nil without connecting.
package producer

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	"stocker-investor/internal/observability"
	stockstorev1 "stocker-investor/proto/v1"
)

// Config holds the settings required to publish to the stock decision topic.
type Config struct {
	Brokers []string
	Topic   string
	Enabled bool
	Paper   bool
}

// writer is the subset of *kafka.Writer used by Producer. It is an interface so
// tests can substitute a recording implementation.
type writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// Producer publishes stock updates to a Kafka topic. A disabled or
// topic-less producer is a no-op.
type Producer struct {
	w     writer
	topic string
	noop  bool
}

// New builds a Producer. If the producer is disabled or has no topic, it
// returns a no-op Producer without constructing a Kafka writer.
func New(cfg Config) *Producer {
	if !cfg.Enabled || cfg.Topic == "" || cfg.Paper {
		return &Producer{topic: cfg.Topic, noop: true}
	}
	return &Producer{
		w: &kafka.Writer{
			Addr:  kafka.TCP(cfg.Brokers...),
			Topic: cfg.Topic,
		},
		topic: cfg.Topic,
	}
}

// Publish marshals sig and writes it to the configured topic, keyed by symbol.
// A no-op producer logs at debug level and returns nil without sending.
func (p *Producer) Publish(ctx context.Context, sig *stockstorev1.Stock) error {
	log := observability.LoggerWithContext(ctx)

	if p.noop {
		log.Debug("kafka: publish skipped (producer disabled)", "symbol", sig.GetSymbol(), "topic", p.topic)
		return nil
	}

	value, err := proto.Marshal(sig)
	if err != nil {
		return fmt.Errorf("marshal stock: %w", err)
	}

	msg := kafka.Message{Topic: p.topic, Key: []byte(sig.GetSymbol()), Value: value}
	if err := p.w.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	log.Debug("kafka: published", "symbol", sig.GetSymbol(), "topic", p.topic)
	return nil
}

// Close releases the underlying writer if one was constructed.
func (p *Producer) Close() error {
	if p.w == nil {
		return nil
	}
	return p.w.Close()
}
