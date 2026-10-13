// Package config provides environment-based configuration for stocker-investor.
package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the service.
type Config struct {
	KAFKA_IN_BROKERS      []string
	KAFKA_IN_TOPIC        string
	KAFKA_IN_GROUP_ID     string
	KAFKA_OUT_BROKERS     []string
	KAFKA_OUT_TOPIC       string
	STORE_GRPC_ADDR       string
	STORE_GRPC_TIMEOUT    time.Duration
	INVESTOR_COOLDOWN_MS  int64
	INVESTOR_MAX_POSITION float64
	INVESTOR_MIN_MOMENTUM float64
	INVESTOR_MAX_MOMENTUM float64
	INVESTOR_PAPER        int
	LOG_LEVEL             string
}

// FromEnv parses configuration from environment variables.
func FromEnv() (Config, error) {
	var cfg Config

	// Required
	cfg.KAFKA_IN_BROKERS = envList("KAFKA_IN_BROKERS")
	if len(cfg.KAFKA_IN_BROKERS) == 0 {
		return cfg, errors.New("KAFKA_IN_BROKERS is required")
	}

	cfg.KAFKA_IN_TOPIC = strings.TrimSpace(os.Getenv("KAFKA_IN_TOPIC"))
	if cfg.KAFKA_IN_TOPIC == "" {
		return cfg, errors.New("KAFKA_IN_TOPIC is required")
	}

	cfg.STORE_GRPC_ADDR = strings.TrimSpace(os.Getenv("STORE_GRPC_ADDR"))
	if cfg.STORE_GRPC_ADDR == "" {
		return cfg, errors.New("STORE_GRPC_ADDR is required")
	}

	// Optional with defaults
	cfg.KAFKA_IN_GROUP_ID = strings.TrimSpace(os.Getenv("KAFKA_IN_GROUP_ID"))
	if cfg.KAFKA_IN_GROUP_ID == "" {
		cfg.KAFKA_IN_GROUP_ID = "stocker-investor"
	}

	cfg.KAFKA_OUT_BROKERS = envList("KAFKA_OUT_BROKERS")
	if len(cfg.KAFKA_OUT_BROKERS) == 0 {
		cfg.KAFKA_OUT_BROKERS = cfg.KAFKA_IN_BROKERS
	}

	cfg.KAFKA_OUT_TOPIC = strings.TrimSpace(os.Getenv("KAFKA_OUT_TOPIC"))

	// STORE_GRPC_TIMEOUT
	if v := strings.TrimSpace(os.Getenv("STORE_GRPC_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return cfg, err
		}
		cfg.STORE_GRPC_TIMEOUT = d
	} else {
		cfg.STORE_GRPC_TIMEOUT = 2 * time.Second
	}

	// INVESTOR_COOLDOWN_MS
	if v := strings.TrimSpace(os.Getenv("INVESTOR_COOLDOWN_MS")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cfg, err
		}
		cfg.INVESTOR_COOLDOWN_MS = n
	} else {
		cfg.INVESTOR_COOLDOWN_MS = 60000
	}

	// INVESTOR_MAX_POSITION
	if v := strings.TrimSpace(os.Getenv("INVESTOR_MAX_POSITION")); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return cfg, err
		}
		cfg.INVESTOR_MAX_POSITION = n
	} else {
		cfg.INVESTOR_MAX_POSITION = 10000
	}

	// INVESTOR_MIN_MOMENTUM
	if v := strings.TrimSpace(os.Getenv("INVESTOR_MIN_MOMENTUM")); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return cfg, err
		}
		cfg.INVESTOR_MIN_MOMENTUM = n
	} else {
		cfg.INVESTOR_MIN_MOMENTUM = 0.2
	}

	// INVESTOR_MAX_MOMENTUM
	if v := strings.TrimSpace(os.Getenv("INVESTOR_MAX_MOMENTUM")); v != "" {
		n, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return cfg, err
		}
		cfg.INVESTOR_MAX_MOMENTUM = n
	} else {
		cfg.INVESTOR_MAX_MOMENTUM = -0.2
	}

	// INVESTOR_PAPER
	if v := strings.TrimSpace(os.Getenv("INVESTOR_PAPER")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, err
		}
		cfg.INVESTOR_PAPER = n
	} else {
		cfg.INVESTOR_PAPER = 0
	}

	// LOG_LEVEL
	cfg.LOG_LEVEL = strings.TrimSpace(os.Getenv("LOG_LEVEL"))
	if cfg.LOG_LEVEL == "" {
		cfg.LOG_LEVEL = "INFO"
	}

	return cfg, nil
}

// envList splits a comma-separated environment variable into a list of trimmed, non-empty strings.
func envList(name string) []string {
	v := os.Getenv(name)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	var out []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
