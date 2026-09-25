package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/app"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/kafka"
	"github.com/ghrushneshr25/chronos-dev/internal/scheduler"
	"github.com/ghrushneshr25/nexus"
)

func main() {
	log := app.Logger("scheduler")
	cfg, authenticator, err := app.Register("scheduler")
	if err != nil {
		log.Error("wire failed", "err", err)
		os.Exit(1)
	}

	store := nexus.MustGet(app.JobStoreContract)
	q := nexus.MustGet(app.QueueContract)
	pub := nexus.MustGet(app.EventPublisherContract)

	// best-effort topic create for local kafka
	if q.Name() == "kafka" && len(cfg.Queue.KafkaBrokers) > 0 {
		_ = kafka.EnsureTopic(cfg.Queue.KafkaBrokers, cfg.Queue.KafkaTopic, 3)
	}

	sched := scheduler.NewService(cfg.Scheduler, store, q, pub, log)
	grpcSrv := scheduler.NewGRPCServer(sched, authenticator, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 2)
	go func() {
		log.Info("scheduler starting", "id", cfg.Scheduler.ID, "queue", q.Name())
		errCh <- sched.Run(ctx)
	}()
	go func() {
		errCh <- grpcSrv.Serve(ctx, cfg.Scheduler.ListenAddr)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			log.Error("scheduler error", "err", err)
		}
		stop()
	}

	time.Sleep(200 * time.Millisecond)
	_ = q.Close()
	_ = store.Close()
	log.Info("scheduler stopped")
}
