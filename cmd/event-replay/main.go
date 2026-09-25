package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/segmentio/kafka-go"
)

// event-replay re-reads Kafka topics from the beginning into ClickHouse (or stdout).
// Usage:
//
//	event-replay -topic chronos.job-events -to clickhouse
//	event-replay -topic chronos.job-events -to stdout -max 100
func main() {
	topic := flag.String("topic", "chronos.job-events", "kafka topic")
	to := flag.String("to", "stdout", "stdout | clickhouse")
	max := flag.Int("max", 0, "max events (0 = all available until idle)")
	idle := flag.Duration("idle", 3*time.Second, "stop after no messages for this long")
	flag.Parse()

	cfg, err := config.Load("event-replay")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var ch *analytics.ClickHouseWriter
	if *to == "clickhouse" {
		ch, err = analytics.NewClickHouse(cfg.ClickHouse.DSN, cfg.ClickHouse.Database)
		if err != nil {
			fmt.Fprintln(os.Stderr, "clickhouse:", err)
			os.Exit(1)
		}
		defer ch.Close()
		_ = ch.EnsureSchema(ctx)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.Kafka.Brokers,
		Topic:       *topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10e6,
	})
	defer reader.Close()

	var batch []domain.Event
	count := 0
	lastMsg := time.Now()

	for {
		if *max > 0 && count >= *max {
			break
		}
		mctx, cancel := context.WithTimeout(ctx, *idle)
		msg, err := reader.ReadMessage(mctx)
		cancel()
		if err != nil {
			if time.Since(lastMsg) >= *idle || ctx.Err() != nil {
				break
			}
			continue
		}
		lastMsg = time.Now()
		var ev domain.Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}
		count++
		switch *to {
		case "stdout":
			b, _ := json.Marshal(ev)
			fmt.Println(string(b))
		case "clickhouse":
			batch = append(batch, ev)
			if len(batch) >= 100 {
				if err := ch.InsertEvents(ctx, batch); err != nil {
					fmt.Fprintln(os.Stderr, "insert:", err)
					os.Exit(1)
				}
				batch = batch[:0]
			}
		}
	}
	if *to == "clickhouse" && len(batch) > 0 {
		if err := ch.InsertEvents(ctx, batch); err != nil {
			fmt.Fprintln(os.Stderr, "insert:", err)
			os.Exit(1)
		}
	}
	fmt.Fprintf(os.Stderr, "replayed %d events from %s → %s\n", count, *topic, *to)
}
