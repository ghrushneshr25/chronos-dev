package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
	"github.com/ghrushneshr25/chronos-dev/internal/api"
	"github.com/ghrushneshr25/chronos-dev/internal/app"
	"github.com/ghrushneshr25/nexus"
)

func main() {
	log := app.Logger("api")
	cfg, authenticator, err := app.Register("api")
	if err != nil {
		log.Error("wire failed", "err", err)
		os.Exit(1)
	}

	store := nexus.MustGet(app.JobStoreContract)
	q := nexus.MustGet(app.QueueContract)
	pub := nexus.MustGet(app.EventPublisherContract)

	srv := api.NewServer(store, q, pub, authenticator)

	// Optional analytics plane — never blocks ops startup
	if dsn := cfg.ClickHouse.DSN; dsn != "" && os.Getenv("CHRONOS_ANALYTICS_DISABLED") != "1" {
		ch, err := analytics.NewClickHouse(dsn, cfg.ClickHouse.Database)
		if err != nil {
			log.Warn("analytics disabled (clickhouse unreachable)", "err", err)
		} else {
			if err := ch.EnsureSchema(context.Background()); err != nil {
				log.Warn("analytics schema ensure failed", "err", err)
			}
			srv.SetAnalytics(ch)
			defer ch.Close()
			log.Info("analytics enabled", "database", cfg.ClickHouse.Database)
		}
	}

	httpSrv := &http.Server{
		Addr:              cfg.API.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("api listening", "addr", cfg.API.HTTPAddr, "queue", q.Name())
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
	_ = q.Close()
	_ = store.Close()
	log.Info("api stopped")
}
