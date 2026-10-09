package investor

import (
	"context"
	"math"
	"testing"

	stockstorev1 "stocker-investor/proto/v1"
	kafkastockv1 "stocker-investor/proto/v1/kafka"
)

func TestActionValues(t *testing.T) {
	if SELL != -1 || HOLD != 0 || BUY != 1 {
		t.Fatalf("unexpected action values: SELL=%d HOLD=%d BUY=%d", SELL, HOLD, BUY)
	}
}

func TestDecider_Decide(t *testing.T) {
	baseCfg := Config{
		CooldownMS:  60000,
		MaxPosition: 10000,
		MinMomentum: 0.2,
		MaxMomentum: -0.2,
	}

	tests := []struct {
		name       string
		sig        *kafkastockv1.StockUpdate
		stockCtx   *stockstorev1.Stock
		cfg        Config
		wantAction Action
		wantPos    float64
		wantConf   float64
		wantPrefix string
	}{
		{
			name:       "buy above min momentum",
			sig:        signal("AAPL", 0.5),
			cfg:        baseCfg,
			wantAction: BUY,
			wantPos:    5000,
			wantConf:   0.5,
			wantPrefix: "buy:",
		},
		{
			name:       "buy at min momentum threshold",
			sig:        signal("AAPL", 0.2),
			cfg:        baseCfg,
			wantAction: BUY,
			wantPos:    2000,
			wantConf:   0.2,
			wantPrefix: "buy:",
		},
		{
			name:       "sell below max momentum",
			sig:        signal("AAPL", -0.5),
			cfg:        baseCfg,
			wantAction: SELL,
			wantPos:    5000,
			wantConf:   -0.5,
			wantPrefix: "sell:",
		},
		{
			name:       "sell at max momentum threshold",
			sig:        signal("AAPL", -0.2),
			cfg:        baseCfg,
			wantAction: SELL,
			wantPos:    2000,
			wantConf:   -0.2,
			wantPrefix: "sell:",
		},
		{
			name:       "hold within thresholds",
			sig:        signal("AAPL", 0.1),
			cfg:        baseCfg,
			wantAction: HOLD,
			wantPos:    0,
			wantConf:   0.1,
			wantPrefix: "hold:",
		},
		{
			name:       "confidence clamped above one",
			sig:        signal("AAPL", 1.5),
			cfg:        baseCfg,
			wantAction: BUY,
			wantPos:    10000,
			wantConf:   1.0,
			wantPrefix: "buy:",
		},
		{
			name:       "confidence clamped below minus one",
			sig:        signal("AAPL", -1.5),
			cfg:        baseCfg,
			wantAction: SELL,
			wantPos:    10000,
			wantConf:   -1.0,
			wantPrefix: "sell:",
		},
		{
			name:       "hold when no momentum score",
			sig:        &kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"},
			cfg:        baseCfg,
			wantAction: HOLD,
			wantPos:    0,
			wantConf:   0,
			wantPrefix: "hold:",
		},
		{
			name:       "nil signal holds",
			sig:        nil,
			cfg:        baseCfg,
			wantAction: HOLD,
			wantPos:    0,
			wantConf:   0,
			wantPrefix: "hold:",
		},
		{
			name:       "momentum from store context fallback",
			sig:        &kafkastockv1.StockUpdate{Symbol: "AAPL", Exchange: "NASDAQ"},
			stockCtx:   storeContext(0.5),
			cfg:        baseCfg,
			wantAction: BUY,
			wantPos:    5000,
			wantConf:   0.5,
			wantPrefix: "buy:",
		},
		{
			name:       "signal momentum takes precedence over store context",
			sig:        signal("AAPL", -0.5),
			stockCtx:   storeContext(0.9),
			cfg:        baseCfg,
			wantAction: SELL,
			wantPos:    5000,
			wantConf:   -0.5,
			wantPrefix: "sell:",
		},
		{
			name:       "custom thresholds",
			sig:        signal("AAPL", 0.3),
			cfg:        Config{CooldownMS: 0, MaxPosition: 1000, MinMomentum: 0.25, MaxMomentum: -0.25},
			wantAction: BUY,
			wantPos:    300,
			wantConf:   0.3,
			wantPrefix: "buy:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDecider(tt.cfg)
			got := d.Decide(context.Background(), tt.sig, tt.stockCtx)

			if got.Action != tt.wantAction {
				t.Errorf("Action = %d, want %d", got.Action, tt.wantAction)
			}
			if !almostEqual(got.PositionSize, tt.wantPos) {
				t.Errorf("PositionSize = %v, want %v", got.PositionSize, tt.wantPos)
			}
			if !almostEqual(got.Confidence, tt.wantConf) {
				t.Errorf("Confidence = %v, want %v", got.Confidence, tt.wantConf)
			}
			if len(got.Rationale) < len(tt.wantPrefix) || got.Rationale[:len(tt.wantPrefix)] != tt.wantPrefix {
				t.Errorf("Rationale = %q, want prefix %q", got.Rationale, tt.wantPrefix)
			}
			if got.DecidedAt.IsZero() {
				t.Error("DecidedAt is zero")
			}
		})
	}
}

func TestDecider_Cooldown(t *testing.T) {
	d := NewDecider(Config{CooldownMS: 60000, MaxPosition: 10000, MinMomentum: 0.2, MaxMomentum: -0.2})

	first := d.Decide(context.Background(), signal("AAPL", 0.5), nil)
	if first.Action != BUY {
		t.Fatalf("first decision = %d, want BUY", first.Action)
	}

	second := d.Decide(context.Background(), signal("AAPL", 0.5), nil)
	if second.Action != HOLD {
		t.Fatalf("second decision = %d, want HOLD (cooldown)", second.Action)
	}
	if second.PositionSize != 0 {
		t.Fatalf("second PositionSize = %v, want 0", second.PositionSize)
	}

	// A different symbol is unaffected by AAPL's cooldown.
	other := d.Decide(context.Background(), signal("MSFT", 0.5), nil)
	if other.Action != BUY {
		t.Fatalf("other symbol decision = %d, want BUY", other.Action)
	}
}

func TestDecider_CooldownDisabled(t *testing.T) {
	d := NewDecider(Config{CooldownMS: 0, MaxPosition: 10000, MinMomentum: 0.2, MaxMomentum: -0.2})

	for i := 0; i < 3; i++ {
		got := d.Decide(context.Background(), signal("AAPL", 0.5), nil)
		if got.Action != BUY {
			t.Fatalf("decision %d = %d, want BUY with cooldown disabled", i, got.Action)
		}
	}
}

func signal(symbol string, momentum float64) *kafkastockv1.StockUpdate {
	return &kafkastockv1.StockUpdate{
		Symbol:   symbol,
		Exchange: "NASDAQ",
		Scores:   map[string]float64{MomentumKey: momentum},
	}
}

func storeContext(momentum float64) *stockstorev1.Stock {
	return &stockstorev1.Stock{
		Symbol:   "AAPL",
		Exchange: "NASDAQ",
		Scores:   []*stockstorev1.ScoreEntry{{Category: MomentumKey, Value: momentum}},
	}
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
