package config

import (
	"testing"
	"time"
)

func TestFromEnv_ValidConfig(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka1:9092,kafka2:9092")
	t.Setenv("KAFKA_IN_TOPIC", "stock-updates")
	t.Setenv("KAFKA_IN_GROUP_ID", "investor-group")
	t.Setenv("KAFKA_OUT_BROKERS", "kafka-out:9092")
	t.Setenv("KAFKA_OUT_TOPIC", "decisions")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")
	t.Setenv("STORE_GRPC_TIMEOUT", "500ms")
	t.Setenv("INVESTOR_COOLDOWN_MS", "30000")
	t.Setenv("INVESTOR_MAX_POSITION", "5000")
	t.Setenv("INVESTOR_MIN_MOMENTUM", "0.3")
	t.Setenv("INVESTOR_MAX_MOMENTUM", "-0.3")
	t.Setenv("INVESTOR_PAPER", "1")
	t.Setenv("LOG_LEVEL", "DEBUG")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.KAFKA_IN_BROKERS) != 2 {
		t.Fatalf("expected 2 in brokers, got %d", len(cfg.KAFKA_IN_BROKERS))
	}
	if cfg.KAFKA_IN_TOPIC != "stock-updates" {
		t.Fatalf("unexpected topic: %s", cfg.KAFKA_IN_TOPIC)
	}
	if cfg.KAFKA_IN_GROUP_ID != "investor-group" {
		t.Fatalf("unexpected group: %s", cfg.KAFKA_IN_GROUP_ID)
	}
	if len(cfg.KAFKA_OUT_BROKERS) != 1 {
		t.Fatalf("expected 1 out broker, got %d", len(cfg.KAFKA_OUT_BROKERS))
	}
	if cfg.KAFKA_OUT_TOPIC != "decisions" {
		t.Fatalf("unexpected out topic: %s", cfg.KAFKA_OUT_TOPIC)
	}
	if cfg.STORE_GRPC_ADDR != "store:9090" {
		t.Fatalf("unexpected grpc addr: %s", cfg.STORE_GRPC_ADDR)
	}
	if cfg.STORE_GRPC_TIMEOUT != 500*time.Millisecond {
		t.Fatalf("unexpected timeout: %v", cfg.STORE_GRPC_TIMEOUT)
	}
	if cfg.INVESTOR_COOLDOWN_MS != 30000 {
		t.Fatalf("unexpected cooldown: %d", cfg.INVESTOR_COOLDOWN_MS)
	}
	if cfg.INVESTOR_MAX_POSITION != 5000 {
		t.Fatalf("unexpected max pos: %f", cfg.INVESTOR_MAX_POSITION)
	}
	if cfg.INVESTOR_MIN_MOMENTUM != 0.3 {
		t.Fatalf("unexpected min mom: %f", cfg.INVESTOR_MIN_MOMENTUM)
	}
	if cfg.INVESTOR_MAX_MOMENTUM != -0.3 {
		t.Fatalf("unexpected max mom: %f", cfg.INVESTOR_MAX_MOMENTUM)
	}
	if cfg.INVESTOR_PAPER != 1 {
		t.Fatalf("unexpected paper: %d", cfg.INVESTOR_PAPER)
	}
	if cfg.LOG_LEVEL != "DEBUG" {
		t.Fatalf("unexpected log level: %s", cfg.LOG_LEVEL)
	}
}

func TestFromEnv_Defaults(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_IN_TOPIC", "updates")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.KAFKA_IN_GROUP_ID != "stocker-investor" {
		t.Fatalf("unexpected group default: %s", cfg.KAFKA_IN_GROUP_ID)
	}
	if len(cfg.KAFKA_OUT_BROKERS) != 1 || cfg.KAFKA_OUT_BROKERS[0] != "kafka:9092" {
		t.Fatalf("out brokers should fallback to in brokers: %v", cfg.KAFKA_OUT_BROKERS)
	}
	if cfg.KAFKA_OUT_TOPIC != "" {
		t.Fatalf("out topic should be empty by default: %s", cfg.KAFKA_OUT_TOPIC)
	}
	if cfg.STORE_GRPC_TIMEOUT != 2*time.Second {
		t.Fatalf("unexpected timeout default: %v", cfg.STORE_GRPC_TIMEOUT)
	}
	if cfg.INVESTOR_COOLDOWN_MS != 60000 {
		t.Fatalf("unexpected cooldown default: %d", cfg.INVESTOR_COOLDOWN_MS)
	}
	if cfg.INVESTOR_MAX_POSITION != 10000 {
		t.Fatalf("unexpected max pos default: %f", cfg.INVESTOR_MAX_POSITION)
	}
	if cfg.INVESTOR_MIN_MOMENTUM != 0.2 {
		t.Fatalf("unexpected min mom default: %f", cfg.INVESTOR_MIN_MOMENTUM)
	}
	if cfg.INVESTOR_MAX_MOMENTUM != -0.2 {
		t.Fatalf("unexpected max mom default: %f", cfg.INVESTOR_MAX_MOMENTUM)
	}
	if cfg.INVESTOR_PAPER != 0 {
		t.Fatalf("unexpected paper default: %d", cfg.INVESTOR_PAPER)
	}
	if cfg.LOG_LEVEL != "INFO" {
		t.Fatalf("unexpected log level default: %s", cfg.LOG_LEVEL)
	}
}

func TestFromEnv_MissingKAFKA_IN_BROKERS(t *testing.T) {
	t.Setenv("KAFKA_IN_TOPIC", "updates")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")
	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error for missing KAFKA_IN_BROKERS")
	}
}

func TestFromEnv_MissingKAFKA_IN_TOPIC(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka:9092")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")
	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error for missing KAFKA_IN_TOPIC")
	}
}

func TestFromEnv_MissingSTORE_GRPC_ADDR(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_IN_TOPIC", "updates")
	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error for missing STORE_GRPC_ADDR")
	}
}

func TestFromEnv_KAFKA_OUT_BROKERS_Fallback(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka1:9092,kafka2:9092")
	t.Setenv("KAFKA_IN_TOPIC", "updates")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")
	t.Setenv("KAFKA_OUT_BROKERS", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.KAFKA_OUT_BROKERS) != 2 {
		t.Fatalf("expected fallback to in brokers, got %v", cfg.KAFKA_OUT_BROKERS)
	}
}

func TestFromEnv_BadDuration(t *testing.T) {
	t.Setenv("KAFKA_IN_BROKERS", "kafka:9092")
	t.Setenv("KAFKA_IN_TOPIC", "updates")
	t.Setenv("STORE_GRPC_ADDR", "store:9090")
	t.Setenv("STORE_GRPC_TIMEOUT", "notaduration")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected error for bad duration")
	}
}

func TestFromEnv_BadNumericValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{name: "cooldown", key: "INVESTOR_COOLDOWN_MS", val: "notanint"},
		{name: "max position", key: "INVESTOR_MAX_POSITION", val: "notafloat"},
		{name: "min momentum", key: "INVESTOR_MIN_MOMENTUM", val: "notafloat"},
		{name: "max momentum", key: "INVESTOR_MAX_MOMENTUM", val: "notafloat"},
		{name: "paper", key: "INVESTOR_PAPER", val: "notanint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KAFKA_IN_BROKERS", "kafka:9092")
			t.Setenv("KAFKA_IN_TOPIC", "updates")
			t.Setenv("STORE_GRPC_ADDR", "store:9090")
			t.Setenv(tt.key, tt.val)

			_, err := FromEnv()
			if err == nil {
				t.Fatalf("expected error for invalid %s", tt.key)
			}
		})
	}
}
