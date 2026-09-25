package analytics

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/segmentio/kafka-go"
)

// Consumer reads Chronos domain events from Kafka and writes them to ClickHouse.
// Failures here must never affect the operational plane.
type Consumer struct {
	cfg    config.KafkaConfig
	writer *ClickHouseWriter
	log    *slog.Logger
	batch  int
}

func NewConsumer(cfg config.KafkaConfig, w *ClickHouseWriter, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{cfg: cfg, writer: w, log: log, batch: 100}
}

func (c *Consumer) Run(ctx context.Context) error {
	topics := []string{
		c.cfg.JobEventsTopic,
		c.cfg.WorkerEventsTopic,
		c.cfg.SchedulerEventsTopic,
	}
	errCh := make(chan error, len(topics))
	for _, topic := range topics {
		topic := topic
		go func() {
			if err := c.consumeTopic(ctx, topic); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (c *Consumer) consumeTopic(ctx context.Context, topic string) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        c.cfg.Brokers,
		Topic:          topic,
		GroupID:        c.cfg.ConsumerGroup,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()
	c.log.Info("analytics consumer started", "topic", topic, "group", c.cfg.ConsumerGroup)

	buf := make([]domain.Event, 0, c.batch)
	msgs := make([]kafka.Message, 0, c.batch)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := c.writer.InsertEvents(ctx, buf); err != nil {
			return err
		}
		if err := reader.CommitMessages(ctx, msgs...); err != nil {
			return err
		}
		c.log.Debug("ingested events", "topic", topic, "count", len(buf))
		buf = buf[:0]
		msgs = msgs[:0]
		return nil
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = flush()
			return ctx.Err()
		case <-ticker.C:
			if err := flush(); err != nil {
				c.log.Error("flush", "topic", topic, "err", err)
				// do not commit — retry after brief pause; ops plane unaffected
				time.Sleep(time.Second)
			}
		default:
		}

		mctx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := reader.FetchMessage(mctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
		var ev domain.Event
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			c.log.Warn("poison event skipped", "topic", topic, "err", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if ev.EventID == "" {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		buf = append(buf, ev)
		msgs = append(msgs, msg)
		if len(buf) >= c.batch {
			if err := flush(); err != nil {
				c.log.Error("flush", "topic", topic, "err", err)
				time.Sleep(time.Second)
			}
		}
	}
}
