package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers []string
	Topic   string
	Group   string
}

// Queue implements domain.Queue using Kafka.
// Priority is encoded in message headers; true priority ordering across partitions
// is best-effort (single partition recommended for strict priority in M1).
type Queue struct {
	cfg    Config
	writer *kafka.Writer
	mu     sync.Mutex
	closed bool
}

func New(cfg Config) (*Queue, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka queue: brokers required")
	}
	if cfg.Topic == "" {
		cfg.Topic = "chronos.jobs"
	}
	if cfg.Group == "" {
		cfg.Group = "chronos-scheduler"
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Compression:  kafka.Snappy,
	}

	return &Queue{cfg: cfg, writer: w}, nil
}

func (q *Queue) Name() string { return "kafka" }

func (q *Queue) Enqueue(ctx context.Context, job domain.QueuedJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return fmt.Errorf("kafka queue closed")
	}
	if job.EnqueuedAt.IsZero() {
		job.EnqueuedAt = time.Now().UTC()
	}
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	msg := kafka.Message{
		Key:   []byte(job.JobID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "priority", Value: []byte(fmt.Sprintf("%d", job.Priority))},
			{Key: "job_id", Value: []byte(job.JobID)},
		},
		Time: time.Now().UTC(),
	}
	return q.writer.WriteMessages(ctx, msg)
}

func (q *Queue) EnqueueDelay(ctx context.Context, job domain.QueuedJob, delay time.Duration) error {
	// Kafka has no native delay; scheduler should hold delayed jobs in Postgres
	// and enqueue when due. For interface completeness we sleep in a goroutine
	// only for short delays in dev — production path uses store + poller.
	if delay <= 0 {
		return q.Enqueue(ctx, job)
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = q.Enqueue(context.Background(), job)
		case <-ctx.Done():
		}
	}()
	return nil
}

func (q *Queue) StartConsumer(ctx context.Context, handler func(ctx context.Context, job domain.QueuedJob) error) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        q.cfg.Brokers,
		Topic:          q.cfg.Topic,
		GroupID:        q.cfg.Group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commit only — never auto-ack before handler succeeds
		StartOffset:    kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		var job domain.QueuedJob
		if err := json.Unmarshal(msg.Value, &job); err != nil {
			// skip poison message
			_ = reader.CommitMessages(ctx, msg)
			continue
		}
		if err := handler(ctx, job); err != nil {
			// do not commit — redelivery (at-least-once)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	if q.writer != nil {
		return q.writer.Close()
	}
	return nil
}

// EnsureTopic creates the jobs topic if missing (dev convenience).
func EnsureTopic(brokers []string, topic string, partitions int) error {
	if partitions < 1 {
		partitions = 3
	}
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
