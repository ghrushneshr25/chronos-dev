package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
	"github.com/ghrushneshr25/chronos-dev/internal/app"
	"github.com/ghrushneshr25/chronos-dev/internal/config"
)

func main() {
	log := app.Logger("analytics-consumer")
	cfg, err := config.Load("analytics-consumer")
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(1)
	}

	var writer *analytics.ClickHouseWriter
	for i := 0; i < 30; i++ {
		writer, err = analytics.NewClickHouse(cfg.ClickHouse.DSN, cfg.ClickHouse.Database)
		if err == nil {
			break
		}
		log.Warn("clickhouse not ready, retrying", "err", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Error("clickhouse connect failed — analytics offline (ops plane unaffected)", "err", err)
		os.Exit(1)
	}
	defer writer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := writer.EnsureSchema(ctx); err != nil {
		log.Error("ensure schema", "err", err)
		os.Exit(1)
	}

	consumer := analytics.NewConsumer(cfg.Kafka, writer, log)
	log.Info("analytics-consumer starting",
		"brokers", cfg.Kafka.Brokers,
		"group", cfg.Kafka.ConsumerGroup,
		"clickhouse", cfg.ClickHouse.Database,
	)
	if err := consumer.Run(ctx); err != nil && err != context.Canceled {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}
