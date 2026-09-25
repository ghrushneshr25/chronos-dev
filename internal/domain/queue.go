package domain

import (
	"context"
	"time"
)

// QueuedJob is the message placed on the work queue for delivery to the scheduler/workers.
type QueuedJob struct {
	JobID       string    `json:"job_id"`
	ExecutionID string    `json:"execution_id"`
	Name        string    `json:"name"`
	Type        JobType   `json:"type"`
	Payload     []byte    `json:"payload"`
	Priority    int       `json:"priority"`
	Attempt     int       `json:"attempt"`
	Timeout     time.Duration `json:"timeout"`
	EnqueuedAt  time.Time `json:"enqueued_at"`
	TraceID     string    `json:"trace_id,omitempty"`
}

// Queue is the pluggable job delivery mechanism between scheduler and the ready-work pipeline.
// Implementations: kafka (default), future: postgres, redis, nats, memory.
//
// Selection is via CHRONOS_QUEUE_BACKEND env (kafka|memory|postgres|redis).
type Queue interface {
	// Enqueue adds a job to the ready queue. Higher priority should be preferred by consumers.
	Enqueue(ctx context.Context, job QueuedJob) error

	// EnqueueDelay schedules a job to become visible after delay.
	EnqueueDelay(ctx context.Context, job QueuedJob, delay time.Duration) error

	// StartConsumer begins consuming ready jobs and invokes handler for each.
	// Handler must return nil to ack; non-nil may trigger redelivery depending on backend.
	StartConsumer(ctx context.Context, handler func(ctx context.Context, job QueuedJob) error) error

	// Close releases backend resources.
	Close() error

	// Name returns the backend identifier (e.g. "kafka").
	Name() string
}

// QueueFactory constructs a Queue from runtime config.
type QueueFactory func(cfg QueueConfig) (Queue, error)

type QueueConfig struct {
	Backend string // kafka | memory | postgres | redis

	// Kafka
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroup   string

	// Postgres (future)
	PostgresDSN string

	// Redis (future)
	RedisAddr string
}
