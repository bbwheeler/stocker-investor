// Package investor implements the pure decision logic for stocker-investor:
// given a stock signal and optional store context, it produces a Decision.
// It performs no I/O and depends on no Kafka or gRPC transports.
package investor

import (
	"context"
	"math"
	"time"

	stockstorev1 "stocker-investor/proto/v1"
)

// MomentumKey is the score key the decision rule reads.
const MomentumKey = "momentum"

// Action is the decision action.
type Action int8

const (
	// SELL indicates a sell decision.
	SELL Action = -1
	// HOLD indicates no action.
	HOLD Action = 0
	// BUY indicates a buy decision.
	BUY Action = 1
)

// Decision is the output of the decision rule.
type Decision struct {
	Action       Action
	PositionSize float64
	Confidence   float64
	Rationale    string
	DecidedAt    time.Time
}

// Config is the subset of internal/config.Config used by the decision logic.
type Config struct {
	CooldownMS  int64
	MaxPosition float64
	MinMomentum float64
	MaxMomentum float64
	Paper       bool
}

// Decider computes a Decision for a signal, optionally informed by store context.
type Decider interface {
	Decide(ctx context.Context, sig *stockstorev1.Stock, stockCtx *stockstorev1.Stock) Decision
}

type decider struct {
	cfg      Config
	cooldown *Cooldown
}

// NewDecider returns the default Decider implementing the v1 rule:
//
//	BUY  when the "momentum" ScoreEntry >= MinMomentum and the cooldown has elapsed;
//	SELL when the "momentum" ScoreEntry <= MaxMomentum and the cooldown has elapsed;
//	HOLD otherwise.
func NewDecider(cfg Config) Decider {
	return &decider{
		cfg:      cfg,
		cooldown: NewCooldown(time.Duration(cfg.CooldownMS) * time.Millisecond),
	}
}

func (d *decider) Decide(_ context.Context, sig *stockstorev1.Stock, stockCtx *stockstorev1.Stock) Decision {
	now := time.Now()

	symbol := ""
	if sig != nil {
		symbol = sig.Symbol
	}
	momentum, ok := momentumScore(sig, stockCtx)
	if !ok {
		return Decision{Action: HOLD, Confidence: 0, Rationale: "hold: no momentum score", DecidedAt: now}
	}

	confidence := clamp(momentum, -1, 1)
	hold := Decision{Action: HOLD, PositionSize: 0, Confidence: confidence, DecidedAt: now}

	switch {
	case momentum >= d.cfg.MinMomentum:
		if !d.cooldown.CheckAndSet(symbol, now) {
			hold.Rationale = "hold: buy signal within cooldown"
			return hold
		}
		return Decision{
			Action:       BUY,
			PositionSize: d.positionSize(momentum),
			Confidence:   confidence,
			Rationale:    "buy: momentum at or above minimum",
			DecidedAt:    now,
		}
	case momentum <= d.cfg.MaxMomentum:
		if !d.cooldown.CheckAndSet(symbol, now) {
			hold.Rationale = "hold: sell signal within cooldown"
			return hold
		}
		return Decision{
			Action:       SELL,
			PositionSize: d.positionSize(momentum),
			Confidence:   confidence,
			Rationale:    "sell: momentum at or below maximum",
			DecidedAt:    now,
		}
	default:
		hold.Rationale = "hold: momentum within thresholds"
		return hold
	}
}

// momentumScore reads the momentum score from the signal, falling back to the
// store context when the signal carries none.
func momentumScore(sig *stockstorev1.Stock, stockCtx *stockstorev1.Stock) (float64, bool) {
	if sig != nil {
		for _, s := range sig.GetScores() {
			if s != nil && s.Category == MomentumKey {
				return s.Value, true
			}
		}
	}
	if stockCtx != nil {
		for _, s := range stockCtx.GetScores() {
			if s != nil && s.Category == MomentumKey {
				return s.Value, true
			}
		}
	}
	return 0, false
}

// positionSize scales the max position by the magnitude of momentum, clamped to
// the [0, MaxPosition] range.
func (d *decider) positionSize(momentum float64) float64 {
	size := d.cfg.MaxPosition * math.Abs(momentum)
	return clamp(size, 0, d.cfg.MaxPosition)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
