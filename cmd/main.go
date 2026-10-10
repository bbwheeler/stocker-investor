// Package main is the entry point for the stocker-investor service.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"stocker-investor/internal/config"
	"stocker-investor/internal/consumer"
	"stocker-investor/internal/investor"
	obs "stocker-investor/internal/observability"
	"stocker-investor/internal/producer"
	"stocker-investor/internal/storeclient"
	stockstorev1 "stocker-investor/proto/v1"
	kafkastockv1 "stocker-investor/proto/v1/kafka"
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

	go func() {
		if err := cons.Run(ctx, newHandler(client, decider, prod)); err != nil {
			log.Error("consumer: run", "error", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	if err := prod.Close(); err != nil {
		log.Error("producer: close", "error", err)
	}
	cancel()
}

// newHandler wires the signal → decide → publish pipeline for each consumed
// stock update. Store context is optional: a lookup failure degrades to a
// context-free decision rather than dropping the signal.
func newHandler(client storeclient.Client, decider investor.Decider, prod *producer.Producer) consumer.Handler {
	return func(ctx context.Context, sig *kafkastockv1.StockUpdate) error {
		ctx = obs.With(ctx, correlationIDFor(sig))
		log := obs.LoggerWithContext(ctx)
		log.Debug("signal_received", "symbol", sig.GetSymbol(), "exchange", sig.GetExchange())

		var ctxStock *stockstorev1.Stock
		if client != nil {
			s, err := client.GetStock(ctx, sig.GetSymbol(), sig.GetExchange())
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
			"rationale", d.Rationale,
		)

		if err := prod.Publish(ctx, buildOutput(sig, d)); err != nil {
			return fmt.Errorf("publish decision: %w", err)
		}
		return nil
	}
}

// buildOutput encodes a Decision into a StockUpdate carrying the reserved
// stocker_investor score keys.
func buildOutput(sig *kafkastockv1.StockUpdate, d investor.Decision) *kafkastockv1.StockUpdate {
	return &kafkastockv1.StockUpdate{
		Symbol:   sig.GetSymbol(),
		Exchange: sig.GetExchange(),
		Scores: map[string]float64{
			"stocker_investor.action":             float64(d.Action),
			"stocker_investor.position_size":      d.PositionSize,
			"stocker_investor.confidence":         d.Confidence,
			"stocker_investor.decided_at_unix_ms": float64(d.DecidedAt.UnixMilli()),
		},
	}
}

// correlationIDFor returns a per-signal correlation ID derived from the symbol
// and the current time.
func correlationIDFor(sig *kafkastockv1.StockUpdate) string {
	return fmt.Sprintf("%s-%d", sig.GetSymbol(), time.Now().UnixNano())
}
