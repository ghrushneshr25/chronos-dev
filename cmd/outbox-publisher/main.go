package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ghrushneshr25/chronos-dev/internal/app"
	"github.com/ghrushneshr25/chronos-dev/internal/outbox"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/kafka"
	"github.com/ghrushneshr25/nexus"
)

func main() {
	log := app.Logger("outbox-publisher")
	cfg, _, err := app.Register("outbox-publisher")
	if err != nil {
		log.Error("wire failed", "err", err)
		os.Exit(1)
	}

	store := nexus.MustGet(app.JobStoreContract)
	// ensure event topics exist
	if len(cfg.Kafka.Brokers) > 0 {
		_ = kafka.EnsureTopic(cfg.Kafka.Brokers, cfg.Kafka.JobEventsTopic, 3)
		_ = kafka.EnsureTopic(cfg.Kafka.Brokers, cfg.Kafka.WorkerEventsTopic, 3)
		_ = kafka.EnsureTopic(cfg.Kafka.Brokers, cfg.Kafka.SchedulerEventsTopic, 3)
	}

	pub := outbox.New(store, cfg.Kafka, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("outbox publisher starting")
	if err := pub.Run(ctx); err != nil && err != context.Canceled {
		log.Error("outbox publisher stopped", "err", err)
		os.Exit(1)
	}
	_ = store.Close()
}
