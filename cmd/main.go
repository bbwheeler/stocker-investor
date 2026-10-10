// Package main is the entry point for the stocker-investor service.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"stocker-investor/internal/config"
	"stocker-investor/internal/consumer"
	"stocker-investor/internal/investor"
	obs "stocker-investor/internal/observability"
	"stocker-investor/internal/producer"
	"stocker-investor/internal/storeclient"
	stockstorev1 "stocker-investor/proto/v1"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		obs.Init("ERROR").Error("config: invalid", "error", err)
		os.Exit(2)
	}
	log := obs.Init(cfg.LOG_LEVEL)

	// The store client is best-effort: a failure to build it must not stop the
	// decision loop, which still works without store context.
	var client storeclient.Client
	if c, err := storeclient.New(cfg.STORE_GRPC_ADDR, cfg.STORE_GRPC_TIMEOUT); err != nil {
		log.Error("store: init client", "addr", cfg.STORE_GRPC_ADDR, "error", err)
	} else {
		client = c
	}

	cons := consumer.New(consumer.Config{
		Brokers: cfg.KAFKA_IN_BROKERS,
		Topic:   cfg.KAFKA_IN_TOPIC,
		GroupID: cfg.KAFKA_IN_GROUP_ID,
	})

	prod := producer.New(producer.Config{
		Brokers: cfg.KAFKA_OUT_BROKERS,
		Topic:   cfg.KAFKA_OUT_TOPIC,
		Enabled: cfg.KAFKA_OUT_TOPIC != "",
		Paper:   cfg.INVESTOR_PAPER == 1,
	})

	decider := investor.NewDecider(investor.Config{
		CooldownMS:  cfg.INVESTOR_COOLDOWN_MS,
		MaxPosition: cfg.INVESTOR_MAX_POSITION,
		MinMomentum: cfg.INVESTOR_MIN_MOMENTUM,
		MaxMomentum: cfg.INVESTOR_MAX_MOMENTUM,
		Paper:       cfg.INVESTOR_PAPER == 1,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Info("stocker-investor starting",
		"in_topic", cfg.KAFKA_IN_TOPIC,
		"out_topic", cfg.KAFKA_OUT_TOPIC,
		"paper", cfg.INVESTOR_PAPER == 1,
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := cons.Run(ctx, newHandler(client, decider, prod, cfg.INVESTOR_MAX_POSITION)); err != nil {
			log.Error("consumer: run", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Wait for the consumer to stop before closing the producer: the handler
	// publishes on the same goroutine, so closing first could race an in-flight
	// write.
	<-done
	if err := cons.Close(); err != nil {
		log.Error("consumer: close", "error", err)
	}
	if err := prod.Close(); err != nil {
		log.Error("producer: close", "error", err)
	}
}

// newHandler wires the signal → decide → publish pipeline for each consumed
// stock signal. Store context is optional: a lookup failure degrades to a
// context-free decision rather than dropping the signal.
func newHandler(client storeclient.Client, decider investor.Decider, prod *producer.Producer, maxPosition float64) consumer.Handler {
	return func(ctx context.Context, sig *stockstorev1.Stock) error {
		ctx = obs.With(ctx, correlationIDFor(sig))
		log := obs.LoggerWithContext(ctx)
		log.Debug("signal_received", "symbol", sig.GetSymbol(), "exchange", sig.GetExchange())

		var ctxStock *stockstorev1.Stock
		if client != nil {
			exch := sig.GetExchange()
			s, err := client.GetStock(ctx, &stockstorev1.GetStockRequest{
				Symbol:   sig.GetSymbol(),
				Exchange: &exch,
			})
			if err != nil {
				log.Warn("store: get stock", "symbol", sig.GetSymbol(), "exchange", sig.GetExchange(), "error", err)
				ctxStock = nil
			} else {
				ctxStock = s
			}
		}

		d := decider.Decide(ctx, sig, ctxStock)
		log.Info("decision",
			"symbol", sig.GetSymbol(),
			"exchange", sig.GetExchange(),
			"action", d.Action,
			"position_size", d.PositionSize,
			"confidence", d.Confidence,
			"decided_at", d.DecidedAt,
			"rationale", d.Rationale,
		)

		if err := prod.Publish(ctx, buildOutput(sig, d, maxPosition)); err != nil {
			return fmt.Errorf("publish decision: %w", err)
		}
		return nil
	}
}

// buildOutput encodes a Decision into a Stock carrying the reserved
// stocker_investor ScoreEntry keys. Every published value is in [-1.0, 1.0]:
// action ∈ {-1,0,1}, position_size ∈ [0,1], confidence ∈ [-1,1]. The decision
// timestamp is logged (and carried in ScoreEntry.updated_at as advisory), not
// published as a score.
func buildOutput(sig *stockstorev1.Stock, d investor.Decision, maxPosition float64) *stockstorev1.Stock {
	frac := 0.0
	if maxPosition > 0 {
		frac = clamp01(d.PositionSize / maxPosition)
	}
	ts := timestamppb.New(d.DecidedAt)
	return &stockstorev1.Stock{
		Symbol:   sig.GetSymbol(),
		Exchange: sig.GetExchange(),
		Scores: []*stockstorev1.ScoreEntry{
			{Category: "stocker_investor.action", Value: float64(d.Action), UpdatedAt: ts},
			{Category: "stocker_investor.position_size", Value: frac, UpdatedAt: ts},
			{Category: "stocker_investor.confidence", Value: d.Confidence, UpdatedAt: ts},
		},
	}
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// correlationIDFor returns a per-signal correlation ID derived from the symbol
// and the current time.
func correlationIDFor(sig *stockstorev1.Stock) string {
	return fmt.Sprintf("%s-%d", sig.GetSymbol(), time.Now().UnixNano())
}
