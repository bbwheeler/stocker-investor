package consumer

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	kafkastockv1 "stocker-investor/proto/v1/kafka"
)

// fetchResult is one scripted FetchMessage outcome.
type fetchResult struct {
	msg kafka.Message
	err error
}

// fakeReader is a scripted test double for the reader interface: it returns
// queued results in order, then context.Canceled once the queue is exhausted,
// and records every committed message.
type fakeReader struct {
	results   []fetchResult
	idx       int
	commits   []kafka.Message
	commitErr error
}

func (f *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	if f.idx >= len(f.results) {
		return kafka.Message{}, context.Canceled
	}
	r := f.results[f.idx]
	f.idx++
	return r.msg, r.err
}

func (f *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	f.commits = append(f.commits, msgs...)
	return f.commitErr
}

func (f *fakeReader) Close() error { return nil }

// TestRunHandsValidMessageToHandler verifies a well-formed protobuf is decoded
// and passed to the handler exactly once, then committed.
func TestRunHandsValidMessageToHandler(t *testing.T) {
	want := &kafkastockv1.StockUpdate{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   map[string]float64{"momentum": 0.5},
	}
	raw, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	r := &fakeReader{results: []fetchResult{{msg: kafka.Message{Value: raw}}}}
	c := &Consumer{reader: r}

	var got []*kafkastockv1.StockUpdate
	err = c.Run(context.Background(), func(_ context.Context, sig *kafkastockv1.StockUpdate) error {
		got = append(got, sig)
		return nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if len(got) != 1 {
		t.Fatalf("handler called %d times, want 1", len(got))
	}
	if got[0].Symbol != want.Symbol || got[0].Exchange != want.Exchange {
		t.Errorf("handler got %q/%q, want %q/%q", got[0].Symbol, got[0].Exchange, want.Symbol, want.Exchange)
	}
	if got[0].Scores["momentum"] != want.Scores["momentum"] {
		t.Errorf("handler got momentum %v, want %v", got[0].Scores["momentum"], want.Scores["momentum"])
	}
	if len(r.commits) != 1 {
		t.Errorf("committed %d messages, want 1", len(r.commits))
	}
}

// TestRunSkipsMalformedMessage verifies undecodable bytes are committed and
// skipped without invoking the handler.
func TestRunSkipsMalformedMessage(t *testing.T) {
	r := &fakeReader{results: []fetchResult{{msg: kafka.Message{Value: []byte{0x01, 0x02, 0xFF, 0xFE}}}}}
	c := &Consumer{reader: r}

	called := 0
	err := c.Run(context.Background(), func(_ context.Context, _ *kafkastockv1.StockUpdate) error {
		called++
		return nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if called != 0 {
		t.Errorf("handler called %d times, want 0 for malformed message", called)
	}
	if len(r.commits) != 1 {
		t.Errorf("committed %d messages, want 1 (malformed message must be committed)", len(r.commits))
	}
}

// TestRunCommitsAndContinuesOnHandlerError verifies a handler failure is
// swallowed: the message is committed and the loop keeps running.
func TestRunCommitsAndContinuesOnHandlerError(t *testing.T) {
	raw, err := proto.Marshal(&kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"})
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}
	r := &fakeReader{results: []fetchResult{{msg: kafka.Message{Value: raw}}}}
	c := &Consumer{reader: r}

	called := 0
	err = c.Run(context.Background(), func(_ context.Context, _ *kafkastockv1.StockUpdate) error {
		called++
		return errors.New("boom")
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if called != 1 {
		t.Errorf("handler called %d times, want 1", called)
	}
	if len(r.commits) != 1 {
		t.Errorf("committed %d messages, want 1", len(r.commits))
	}
}

// TestRunContinuesAfterFetchError verifies a transient fetch error is logged
// and the loop continues (it does not abort the consumer).
func TestRunContinuesAfterFetchError(t *testing.T) {
	r := &fakeReader{results: []fetchResult{{err: errors.New("temporary")}}}
	c := &Consumer{reader: r}

	var got []*kafkastockv1.StockUpdate
	err := c.Run(context.Background(), func(_ context.Context, sig *kafkastockv1.StockUpdate) error {
		got = append(got, sig)
		return nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("handler called %d times, want 0", len(got))
	}
	if len(r.commits) != 0 {
		t.Errorf("committed %d messages, want 0", len(r.commits))
	}
}

// TestRunReturnsNilOnContextCancellation verifies cancellation from FetchMessage
// ends the loop cleanly.
func TestRunReturnsNilOnContextCancellation(t *testing.T) {
	r := &fakeReader{results: []fetchResult{{err: context.Canceled}}}
	c := &Consumer{reader: r}

	err := c.Run(context.Background(), func(_ context.Context, _ *kafkastockv1.StockUpdate) error {
		return nil
	})

	if err != nil {
		t.Fatalf("Run() error = %v, want nil on context cancellation", err)
	}
}
