package main

import (
	"strings"
	"testing"
	"time"

	"stocker-investor/internal/investor"
	stockstorev1 "stocker-investor/proto/v1"
)

// TestBuildOutputValuesInRange is the interop gate: stocker-store rejects any
// ScoreEntry.value outside [-1.0, 1.0], so every published value must be in
// range no matter how extreme the decision is.
func TestBuildOutputValuesInRange(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name        string
		d           investor.Decision
		maxPosition float64
		wantAction  float64
		wantPos     float64
		wantConf    float64
	}{
		{
			name:        "buy normalizes position to fraction",
			d:           investor.Decision{Action: investor.BUY, PositionSize: 5000, Confidence: 0.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  1,
			wantPos:     0.5,
			wantConf:    0.5,
		},
		{
			name:        "sell maps to minus one",
			d:           investor.Decision{Action: investor.SELL, PositionSize: 5000, Confidence: -0.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  -1,
			wantPos:     0.5,
			wantConf:    -0.5,
		},
		{
			name:        "hold maps to zero",
			d:           investor.Decision{Action: investor.HOLD, PositionSize: 0, Confidence: 0.1, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  0,
			wantPos:     0,
			wantConf:    0.1,
		},
		{
			name:        "position above max clamps to one",
			d:           investor.Decision{Action: investor.BUY, PositionSize: 99999, Confidence: 0.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  1,
			wantPos:     1,
			wantConf:    0.5,
		},
		{
			name:        "negative position clamps to zero",
			d:           investor.Decision{Action: investor.BUY, PositionSize: -50, Confidence: 0.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  1,
			wantPos:     0,
			wantConf:    0.5,
		},
		{
			name:        "zero max position avoids divide by zero",
			d:           investor.Decision{Action: investor.BUY, PositionSize: 5000, Confidence: 0.5, DecidedAt: now},
			maxPosition: 0,
			wantAction:  1,
			wantPos:     0,
			wantConf:    0.5,
		},
		{
			name:        "out of range confidence clamps",
			d:           investor.Decision{Action: investor.BUY, PositionSize: 5000, Confidence: 7.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  1,
			wantPos:     0.5,
			wantConf:    1,
		},
		{
			name:        "out of range negative confidence clamps",
			d:           investor.Decision{Action: investor.SELL, PositionSize: 5000, Confidence: -7.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  -1,
			wantPos:     0.5,
			wantConf:    -1,
		},
		{
			name:        "out of range action clamps",
			d:           investor.Decision{Action: investor.Action(42), PositionSize: 5000, Confidence: 0.5, DecidedAt: now},
			maxPosition: 10000,
			wantAction:  1,
			wantPos:     0.5,
			wantConf:    0.5,
		},
	}

	sig := &stockstorev1.Stock{Symbol: "AAPL", Exchange: "NASDAQ"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := buildOutput(sig, tt.d, tt.maxPosition)
			got := scoresByCategory(out)

			for _, cat := range []string{
				"stocker_investor.action",
				"stocker_investor.position_size",
				"stocker_investor.confidence",
			} {
				v, ok := got[cat]
				if !ok {
					t.Fatalf("missing score category %q", cat)
				}
				if v < -1 || v > 1 {
					t.Errorf("%s = %v, out of [-1, 1]", cat, v)
				}
			}

			if got["stocker_investor.action"] != tt.wantAction {
				t.Errorf("action = %v, want %v", got["stocker_investor.action"], tt.wantAction)
			}
			if got["stocker_investor.position_size"] != tt.wantPos {
				t.Errorf("position_size = %v, want %v", got["stocker_investor.position_size"], tt.wantPos)
			}
			if got["stocker_investor.confidence"] != tt.wantConf {
				t.Errorf("confidence = %v, want %v", got["stocker_investor.confidence"], tt.wantConf)
			}
		})
	}
}

func TestBuildOutputPreservesIdentityAndDropsTimestampKey(t *testing.T) {
	sig := &stockstorev1.Stock{Symbol: "MSFT", Exchange: "NASDAQ"}
	out := buildOutput(sig, investor.Decision{Action: investor.HOLD, Confidence: 0, DecidedAt: time.Now()}, 10000)

	if out.GetSymbol() != "MSFT" || out.GetExchange() != "NASDAQ" {
		t.Errorf("identity = %q/%q, want MSFT/NASDAQ", out.GetSymbol(), out.GetExchange())
	}
	if len(out.GetScores()) != 3 {
		t.Fatalf("published %d scores, want 3", len(out.GetScores()))
	}
	for _, s := range out.GetScores() {
		if s.GetCategory() == "stocker_investor.decided_at_unix_ms" {
			t.Error("decided_at_unix_ms must not be published (out of range)")
		}
		if s.UpdatedAt == nil {
			t.Errorf("score %q has nil updated_at", s.GetCategory())
		}
	}
}

func TestClampRange(t *testing.T) {
	tests := []struct {
		v, lo, hi, want float64
	}{
		{5, -1, 1, 1},
		{-5, -1, 1, -1},
		{0.5, -1, 1, 0.5},
		{0, 0, 1, 0},
		{2, 0, 1, 1},
		{-2, 0, 1, 0},
	}
	for _, tt := range tests {
		if got := clampRange(tt.v, tt.lo, tt.hi); got != tt.want {
			t.Errorf("clampRange(%v, %v, %v) = %v, want %v", tt.v, tt.lo, tt.hi, got, tt.want)
		}
	}
}

func TestCorrelationIDFor(t *testing.T) {
	sig := &stockstorev1.Stock{Symbol: "AAPL"}
	a := correlationIDFor(sig)
	b := correlationIDFor(sig)
	if a == "" || b == "" {
		t.Fatal("correlation ID must not be empty")
	}
	if !strings.HasPrefix(a, "AAPL-") {
		t.Errorf("correlation ID = %q, want AAPL- prefix", a)
	}
	if a == b {
		t.Errorf("correlation ID is not unique across calls: %q", a)
	}
}

func scoresByCategory(s *stockstorev1.Stock) map[string]float64 {
	out := make(map[string]float64, len(s.GetScores()))
	for _, e := range s.GetScores() {
		out[e.GetCategory()] = e.GetValue()
	}
	return out
}
