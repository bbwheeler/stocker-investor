package investor

import (
	"testing"
	"time"
)

func TestCooldown_CheckAndSet(t *testing.T) {
	c := NewCooldown(time.Minute)
	now := time.Now()

	if !c.CheckAndSet("AAPL", now) {
		t.Fatal("first call should be allowed")
	}
	if c.CheckAndSet("AAPL", now) {
		t.Fatal("immediate second call should be denied")
	}
	if c.CheckAndSet("AAPL", now.Add(30*time.Second)) {
		t.Fatal("call within window should be denied")
	}
	if !c.CheckAndSet("AAPL", now.Add(time.Minute)) {
		t.Fatal("call at window boundary should be allowed")
	}
}

func TestCooldown_IndependentSymbols(t *testing.T) {
	c := NewCooldown(time.Minute)
	now := time.Now()

	if !c.CheckAndSet("AAPL", now) {
		t.Fatal("AAPL first call should be allowed")
	}
	if !c.CheckAndSet("MSFT", now) {
		t.Fatal("MSFT first call should be allowed")
	}
	if c.CheckAndSet("AAPL", now) {
		t.Fatal("AAPL second call should be denied")
	}
}

func TestCooldown_ZeroWindow(t *testing.T) {
	c := NewCooldown(0)
	now := time.Now()

	for i := 0; i < 3; i++ {
		if !c.CheckAndSet("AAPL", now) {
			t.Fatalf("call %d should be allowed with zero window", i)
		}
	}
}

func TestCooldown_NegativeWindow(t *testing.T) {
	c := NewCooldown(-time.Second)
	now := time.Now()

	for i := 0; i < 3; i++ {
		if !c.CheckAndSet("AAPL", now) {
			t.Fatalf("call %d should be allowed with negative window", i)
		}
	}
}
