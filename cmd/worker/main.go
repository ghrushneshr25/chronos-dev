package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ghrushneshr25/chronos-dev/internal/app"
	"github.com/ghrushneshr25/chronos-dev/internal/execution"
	"github.com/ghrushneshr25/chronos-dev/internal/worker"
	"github.com/ghrushneshr25/nexus"
)

func main() {
	log := app.Logger("worker")
	cfg, authenticator, err := app.Register("worker")
	if err != nil {
		log.Error("wire failed", "err", err)
		os.Exit(1)
	}
	_ = authenticator

	// worker only needs executor; avoid requiring queue/postgres if possible.
	// Register still wires store — for M1 worker still connects to scheduler only.
	// If postgres is down, Register fails; workers need shared config only.
	// For docker, postgres is up. Close unused store.
	if store, err := nexus.Get(app.JobStoreContract); err == nil {
		defer store.Close()
	}

	exec := execution.NewExecutor(cfg.Worker.HTTPClientTimeout)
	if je, err := nexus.Get(app.JobExecutorContract); err == nil {
		if e, ok := je.(*execution.Executor); ok {
			exec = e
		}
	}

	token := "dev-worker-token-change-me"
	if len(cfg.Auth.WorkerTokens) > 0 {
		token = cfg.Auth.WorkerTokens[0]
	}

	w := worker.New(cfg.Worker, token, exec, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("worker starting", "id", cfg.Worker.ID, "listen", cfg.Worker.ListenAddr, "scheduler", cfg.Worker.SchedulerAddr)
	if err := w.Run(ctx); err != nil && err != context.Canceled {
		log.Error("worker stopped", "err", err)
		os.Exit(1)
	}
}
