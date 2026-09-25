package analytics

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/google/uuid"
)

// Querier is the analytics read API used by HTTP handlers.
type Querier interface {
	Throughput(ctx context.Context, tr TimeRange, grain string) (ThroughputResult, error)
	Latency(ctx context.Context, tr TimeRange) (LatencyResult, error)
	Failures(ctx context.Context, tr TimeRange) (FailureResult, error)
	Workers(ctx context.Context, tr TimeRange) (WorkersResult, error)
	Queue(ctx context.Context, tr TimeRange) (QueueResult, error)
	Capacity(ctx context.Context, tr TimeRange, targetPerWorker float64) (CapacityResult, error)
	Anomalies(ctx context.Context, current, baseline TimeRange) (AnomaliesResult, error)
	Scheduler(ctx context.Context, tr TimeRange) (map[string]any, error)
	Ping(ctx context.Context) error
}

type ClickHouseWriter struct {
	conn driver.Conn
	db   string
}

func NewClickHouse(dsn, database string) (*ClickHouseWriter, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		opts = &clickhouse.Options{
			Addr: []string{dsn},
			Auth: clickhouse.Auth{Database: database},
		}
	}
	if database != "" {
		opts.Auth.Database = database
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("clickhouse ping: %w", err)
	}
	return &ClickHouseWriter{conn: conn, db: database}, nil
}

func (w *ClickHouseWriter) Close() error {
	if w.conn != nil {
		return w.conn.Close()
	}
	return nil
}

func (w *ClickHouseWriter) Ping(ctx context.Context) error {
	return w.conn.Ping(ctx)
}

func (w *ClickHouseWriter) Conn() driver.Conn { return w.conn }

func (w *ClickHouseWriter) InsertEvents(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `
INSERT INTO chronos_events (
  event_id, event_type, event_version, event_time, ingested_at,
  job_id, execution_id, worker_id, scheduler_id, attempt, trace_id, payload
)`)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, e := range events {
		id, err := uuid.Parse(e.EventID)
		if err != nil {
			id = uuid.New()
		}
		payload := "{}"
		if len(e.Payload) > 0 {
			payload = string(e.Payload)
		}
		et := e.EventTime.UTC()
		if et.IsZero() {
			et = now
		}
		if err := batch.Append(
			id,
			string(e.EventType),
			uint16(e.EventVersion),
			et,
			now,
			e.JobID,
			e.ExecutionID,
			e.WorkerID,
			e.SchedulerID,
			uint32(e.Attempt),
			e.TraceID,
			payload,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *ClickHouseWriter) CountEvents(ctx context.Context) (uint64, error) {
	var n uint64
	err := w.conn.QueryRow(ctx, `SELECT count() FROM chronos_events FINAL`).Scan(&n)
	return n, err
}

func (w *ClickHouseWriter) CountByType(ctx context.Context, eventType string) (uint64, error) {
	var n uint64
	err := w.conn.QueryRow(ctx, `SELECT count() FROM chronos_events FINAL WHERE event_type = ?`, eventType).Scan(&n)
	return n, err
}

var _ Querier = (*ClickHouseWriter)(nil)
