package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/segmentio/kafka-go"
)

// Publisher drains the transactional outbox to Kafka event topics.
type Publisher struct {
	store  ports.JobStore
	cfg    config.KafkaConfig
	log    *slog.Logger
	writer *kafka.Writer
}

func New(store ports.JobStore, cfg config.KafkaConfig, log *slog.Logger) *Publisher {
	if log == nil {
		log = slog.Default()
	}
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
		Async:        false,
	}
	return &Publisher{store: store, cfg: cfg, log: log, writer: w}
}

func (p *Publisher) Run(ctx context.Context) error {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = p.writer.Close()
			return ctx.Err()
		case <-t.C:
			if err := p.flush(ctx); err != nil {
				p.log.Error("outbox flush", "err", err)
			}
		}
	}
}

func (p *Publisher) flush(ctx context.Context) error {
	rows, err := p.store.FetchOutboxBatch(ctx, 100)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	var msgs []kafka.Message
	var ids []string
	for _, r := range rows {
		topic := p.topicFor(r.EventType)
		key := r.AggregateID
		if key == "" {
			key = r.ID
		}
		msgs = append(msgs, kafka.Message{
			Topic: topic,
			Key:   []byte(key),
			Value: r.Payload,
			Time:  r.CreatedAt,
		})
		ids = append(ids, r.ID)
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return err
	}
	return p.store.MarkOutboxPublished(ctx, ids)
}

func (p *Publisher) topicFor(eventType string) string {
	var et domain.EventType = domain.EventType(eventType)
	switch {
	case len(eventType) >= 4 && eventType[:4] == "JOB_":
		return p.cfg.JobEventsTopic
	case len(eventType) >= 7 && eventType[:7] == "WORKER_":
		return p.cfg.WorkerEventsTopic
	case len(eventType) >= 10 && eventType[:10] == "SCHEDULER_":
		return p.cfg.SchedulerEventsTopic
	default:
		_ = et
		return p.cfg.JobEventsTopic
	}
}

// DecodeEvent helper for consumers.
func DecodeEvent(b []byte) (domain.Event, error) {
	var e domain.Event
	err := json.Unmarshal(b, &e)
	return e, err
}
