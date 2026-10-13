package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	stockstorev1 "stocker-investor/proto/v1"
)

// fakeWriter is a test double for the writer interface. It records every
// WriteMessages call and whether Close was invoked.
type fakeWriter struct {
	calls  int
	msgs   []kafka.Message
	err    error
	closed bool
}

func (f *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	f.calls++
	f.msgs = append(f.msgs, msgs...)
	return f.err
}

func (f *fakeWriter) Close() error {
	f.closed = true
	return nil
}

// TestPublishSendsMarshaledMessage verifies an enabled producer marshals the
// update and writes exactly one message with the configured topic and symbol key.
func TestPublishSendsMarshaledMessage(t *testing.T) {
	fw := &fakeWriter{}
	p := &Producer{w: fw, topic: "decisions"}

	sig := &stockstorev1.Stock{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   []*stockstorev1.ScoreEntry{{Category: "investor_score", Value: 0.5}},
	}
	if err := p.Publish(context.Background(), sig); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}

	if fw.calls != 1 {
		t.Fatalf("WriteMessages called %d times, want 1", fw.calls)
	}
	if len(fw.msgs) != 1 {
		t.Fatalf("wrote %d messages, want 1", len(fw.msgs))
	}
	m := fw.msgs[0]
	if m.Topic != "decisions" {
		t.Errorf("message topic = %q, want %q", m.Topic, "decisions")
	}
	if string(m.Key) != "AAPL" {
		t.Errorf("message key = %q, want %q", m.Key, "AAPL")
	}

	var got stockstorev1.Stock
	if err := proto.Unmarshal(m.Value, &got); err != nil {
		t.Fatalf("proto.Unmarshal(value) error = %v", err)
	}
	if got.Symbol != sig.Symbol || got.Exchange != sig.Exchange {
		t.Errorf("decoded %q/%q, want %q/%q", got.Symbol, got.Exchange, sig.Symbol, sig.Exchange)
	}
	if len(got.Scores) != 1 || got.Scores[0].Category != "investor_score" || got.Scores[0].Value != 0.5 {
		t.Errorf("decoded scores = %v, want investor_score=0.5", got.Scores)
	}
}

// TestPublishPropagatesWriterError verifies a write failure is returned to the caller.
func TestPublishPropagatesWriterError(t *testing.T) {
	wantErr := errors.New("broker down")
	p := &Producer{w: &fakeWriter{err: wantErr}, topic: "decisions"}

	err := p.Publish(context.Background(), &stockstorev1.Stock{Symbol: "AAPL"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Publish() error = %v, want %v", err, wantErr)
	}
}

// TestPublishNoopWhenDisabled verifies a no-op producer returns nil and never
// touches the writer.
func TestPublishNoopWhenDisabled(t *testing.T) {
	fw := &fakeWriter{}
	p := &Producer{w: fw, topic: "decisions", noop: true}

	if err := p.Publish(context.Background(), &stockstorev1.Stock{Symbol: "AAPL"}); err != nil {
		t.Fatalf("Publish() error = %v, want nil", err)
	}
	if fw.calls != 0 {
		t.Errorf("WriteMessages called %d times, want 0 when noop", fw.calls)
	}
}

// TestNewDisabledDoesNotBuildWriter verifies New marks the producer no-op and
// leaves the writer nil when disabled.
func TestNewDisabledDoesNotBuildWriter(t *testing.T) {
	p := New(Config{Brokers: []string{"localhost:9092"}, Topic: "decisions", Enabled: false})
	if !p.noop {
		t.Errorf("noop = false, want true when disabled")
	}
	if p.w != nil {
		t.Errorf("writer = %v, want nil when disabled", p.w)
	}
}

// TestNewEmptyTopicDoesNotBuildWriter verifies an empty topic also yields a no-op.
func TestNewEmptyTopicDoesNotBuildWriter(t *testing.T) {
	p := New(Config{Brokers: []string{"localhost:9092"}, Topic: "", Enabled: true})
	if !p.noop {
		t.Errorf("noop = false, want true when topic empty")
	}
	if p.w != nil {
		t.Errorf("writer = %v, want nil when topic empty", p.w)
	}
}

// TestNewEnabledBuildsWriter verifies an enabled, configured producer carries a
// writer and is not a no-op.
func TestNewEnabledBuildsWriter(t *testing.T) {
	p := New(Config{Brokers: []string{"localhost:9092"}, Topic: "decisions", Enabled: true})
	if p.noop {
		t.Errorf("noop = true, want false when enabled with topic")
	}
	if p.w == nil {
		t.Fatalf("writer = nil, want a constructed writer")
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

// TestCloseNoopReturnsNil verifies Close is safe when no writer was constructed.
func TestCloseNoopReturnsNil(t *testing.T) {
	p := New(Config{Enabled: false})
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
}

// TestCloseClosesWriter verifies Close delegates to the writer.
func TestCloseClosesWriter(t *testing.T) {
	fw := &fakeWriter{}
	p := &Producer{w: fw, topic: "decisions"}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if !fw.closed {
		t.Errorf("writer.Close() not called")
	}
}

func TestProducer_PaperMode(t *testing.T) {
	p := New(Config{Brokers: []string{"b:9092"}, Topic: "out", Enabled: true, Paper: true})
	if !p.noop {
		t.Errorf("noop = false, want true in paper mode")
	}
	if p.w != nil {
		t.Errorf("writer = %v, want nil in paper mode", p.w)
	}
	if err := p.Publish(context.Background(), &stockstorev1.Stock{Symbol: "AAPL"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
