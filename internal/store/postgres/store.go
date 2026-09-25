package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Store struct {
	db *sql.DB
}

func New(dsn string, maxOpen, maxIdle int, maxLifetime time.Duration) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(maxLifetime)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) CreateJob(ctx context.Context, job *domain.Job) error {
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now
	if job.State == "" {
		job.State = domain.JobStateCreated
	}
	if job.Priority == 0 {
		job.Priority = 5
	}
	meta, _ := json.Marshal(job.Metadata)
	payload := []byte(job.Payload)
	if payload == nil {
		payload = []byte("{}")
	}
	retry, _ := json.Marshal(job.RetryPolicy)
	sched, _ := json.Marshal(job.Schedule)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (
			id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, idempotency_key, attempt, max_attempts, metadata, created_at, updated_at, scheduled_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''
		DO NOTHING
	`, job.ID, job.Name, string(job.Type), payload, job.Priority, string(job.State),
		sched, retry, job.Timeout.Milliseconds(), nullStr(job.IdempotencyKey),
		job.Attempt, job.MaxAttempts, meta, job.CreatedAt, job.UpdatedAt, job.ScheduledAt)
	if err != nil {
		return err
	}
	// if idempotency conflict, load existing
	if job.IdempotencyKey != "" {
		existing, err := s.GetJobByIdempotencyKey(ctx, job.IdempotencyKey)
		if err == nil && existing.ID != job.ID {
			*job = *existing
			return nil
		}
	}
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
			COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
			scheduled_at, queued_at, started_at, completed_at, next_retry_at,
			created_at, updated_at, COALESCE(metadata,'{}')
		FROM jobs WHERE id = $1
	`, id)
	return scanJob(row)
}

func (s *Store) GetJobByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
			COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
			scheduled_at, queued_at, started_at, completed_at, next_retry_at,
			created_at, updated_at, COALESCE(metadata,'{}')
		FROM jobs WHERE idempotency_key = $1
	`, key)
	return scanJob(row)
}

func (s *Store) UpdateJobState(ctx context.Context, id string, state domain.JobState, fields map[string]any) error {
	return s.UpdateJobStateIf(ctx, id, state, fields, nil)
}

// UpdateJobStateIf updates job state only when current state is one of onlyFrom (if non-empty).
func (s *Store) UpdateJobStateIf(ctx context.Context, id string, state domain.JobState, fields map[string]any, onlyFrom []domain.JobState) error {
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if len(onlyFrom) > 0 {
		ok := false
		for _, st := range onlyFrom {
			if job.State == st {
				ok = true
				break
			}
		}
		if !ok {
			return nil // stale transition ignored
		}
	}
	job.State = state
	job.UpdatedAt = time.Now().UTC()
	if v, ok := fields["worker_id"].(string); ok {
		job.WorkerID = v
	}
	if v, ok := fields["execution_id"].(string); ok {
		job.ExecutionID = v
	}
	if v, ok := fields["last_error"].(string); ok {
		job.LastError = v
	}
	if v, ok := fields["attempt"].(int); ok {
		job.Attempt = v
	}
	if v, ok := fields["queued_at"].(*time.Time); ok {
		job.QueuedAt = v
	}
	if v, ok := fields["started_at"].(*time.Time); ok {
		job.StartedAt = v
	}
	if v, ok := fields["completed_at"].(*time.Time); ok {
		job.CompletedAt = v
	}
	if v, ok := fields["next_retry_at"].(*time.Time); ok {
		job.NextRetryAt = v
	}
	if v, ok := fields["scheduled_at"].(*time.Time); ok {
		job.ScheduledAt = v
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE jobs SET state=$2, worker_id=$3, execution_id=$4, last_error=$5, attempt=$6,
			queued_at=$7, started_at=$8, completed_at=$9, next_retry_at=$10, scheduled_at=$11, updated_at=$12
		WHERE id=$1
	`, job.ID, string(job.State), nullStr(job.WorkerID), nullStr(job.ExecutionID), nullStr(job.LastError),
		job.Attempt, job.QueuedAt, job.StartedAt, job.CompletedAt, job.NextRetryAt, job.ScheduledAt, job.UpdatedAt)
	return err
}

func (s *Store) ListJobs(ctx context.Context, state string, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if state == "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, name, type, payload, priority, state, schedule, retry_policy,
				timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
				COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
				scheduled_at, queued_at, started_at, completed_at, next_retry_at,
				created_at, updated_at, COALESCE(metadata,'{}')
			FROM jobs ORDER BY created_at DESC LIMIT $1
		`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, name, type, payload, priority, state, schedule, retry_policy,
				timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
				COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
				scheduled_at, queued_at, started_at, completed_at, next_retry_at,
				created_at, updated_at, COALESCE(metadata,'{}')
			FROM jobs WHERE state=$1 ORDER BY created_at DESC LIMIT $2
		`, state, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ListDueScheduled returns jobs ready to move to QUEUED.
func (s *Store) ListDueScheduled(ctx context.Context, limit int) ([]*domain.Job, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
			COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
			scheduled_at, queued_at, started_at, completed_at, next_retry_at,
			created_at, updated_at, COALESCE(metadata,'{}')
		FROM jobs
		WHERE (state = 'SCHEDULED' AND scheduled_at <= NOW())
		   OR (state = 'RETRYING' AND next_retry_at <= NOW())
		ORDER BY priority DESC, scheduled_at ASC NULLS FIRST
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) ListCancelRequested(ctx context.Context, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
			COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
			scheduled_at, queued_at, started_at, completed_at, next_retry_at,
			created_at, updated_at, COALESCE(metadata,'{}')
		FROM jobs WHERE state = 'CANCEL_REQUESTED'
		ORDER BY updated_at ASC LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) ListTimedOutRunning(ctx context.Context, now time.Time, limit int) ([]*domain.Job, error) {
	if limit <= 0 {
		limit = 50
	}
	// timeout_ms is stored; job is overdue when started_at + timeout_ms < now
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, type, payload, priority, state, schedule, retry_policy,
			timeout_ms, COALESCE(idempotency_key,''), attempt, max_attempts,
			COALESCE(worker_id,''), COALESCE(execution_id::text,''), COALESCE(last_error,''),
			scheduled_at, queued_at, started_at, completed_at, next_retry_at,
			created_at, updated_at, COALESCE(metadata,'{}')
		FROM jobs
		WHERE state IN ('RUNNING', 'ASSIGNED')
		  AND started_at IS NOT NULL
		  AND started_at + (timeout_ms || ' milliseconds')::interval < $1
		ORDER BY started_at ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) CreateExecution(ctx context.Context, ex *domain.JobExecution) error {
	if ex.ID == "" {
		ex.ID = uuid.NewString()
	}
	ex.CreatedAt = time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO job_executions (id, job_id, worker_id, attempt, state, created_at)
		VALUES ($1,$2,$3,$4,$5,$6)
	`, ex.ID, ex.JobID, ex.WorkerID, ex.Attempt, string(ex.State), ex.CreatedAt)
	return err
}

func (s *Store) FinishExecution(ctx context.Context, id string, state domain.ExecutionState, result domain.ExecutionResult) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE job_executions SET state=$2, finished_at=$3, duration_ms=$4, error=$5, output=$6, retryable=$7
		WHERE id=$1
	`, id, string(state), now, result.Duration.Milliseconds(), nullStr(result.Error), result.Output, result.Retryable)
	return err
}

// Workers

func (s *Store) UpsertWorker(ctx context.Context, w *domain.Worker) error {
	caps, _ := json.Marshal(w.Capabilities)
	labels, _ := json.Marshal(w.Labels)
	meta, _ := json.Marshal(w.Metadata)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workers (id, address, status, capacity, active_jobs, capabilities, labels, last_heartbeat, registered_at, metadata)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (id) DO UPDATE SET
			address=EXCLUDED.address, status=EXCLUDED.status, capacity=EXCLUDED.capacity,
			active_jobs=EXCLUDED.active_jobs, capabilities=EXCLUDED.capabilities, labels=EXCLUDED.labels,
			last_heartbeat=EXCLUDED.last_heartbeat, metadata=EXCLUDED.metadata
	`, w.ID, w.Address, string(w.Status), w.Capacity, w.ActiveJobs, caps, labels, w.LastHeartbeat, w.RegisteredAt, meta)
	return err
}

func (s *Store) ListHealthyWorkers(ctx context.Context) ([]*domain.Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, address, status, capacity, active_jobs, capabilities, COALESCE(labels,'{}'),
			last_heartbeat, registered_at, COALESCE(metadata,'{}')
		FROM workers WHERE status = 'HEALTHY'
		ORDER BY active_jobs ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) ListWorkers(ctx context.Context) ([]*domain.Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, address, status, capacity, active_jobs, capabilities, COALESCE(labels,'{}'),
			last_heartbeat, registered_at, COALESCE(metadata,'{}')
		FROM workers ORDER BY registered_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*domain.Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

func (s *Store) MarkStaleWorkers(ctx context.Context, heartbeatTimeout, grace time.Duration) (suspected, unavailable []string, err error) {
	suspectBefore := time.Now().UTC().Add(-heartbeatTimeout)
	deadBefore := time.Now().UTC().Add(-(heartbeatTimeout + grace))

	// Only newly transitioned workers — avoids re-firing recovery every poll.
	rows, err := s.db.QueryContext(ctx, `
		UPDATE workers SET status='UNAVAILABLE'
		WHERE status IN ('HEALTHY','SUSPECTED') AND last_heartbeat < $1
		RETURNING id
	`, deadBefore)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, nil, err
		}
		unavailable = append(unavailable, id)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rows2, err := s.db.QueryContext(ctx, `
		UPDATE workers SET status='SUSPECTED'
		WHERE status='HEALTHY' AND last_heartbeat < $1 AND last_heartbeat >= $2
		RETURNING id
	`, suspectBefore, deadBefore)
	if err != nil {
		return nil, unavailable, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var id string
		if err := rows2.Scan(&id); err != nil {
			return nil, unavailable, err
		}
		suspected = append(suspected, id)
	}
	return suspected, unavailable, rows2.Err()
}

// Outbox

func (s *Store) InsertOutbox(ctx context.Context, tx *sql.Tx, event domain.Event) error {
	payload, _ := json.Marshal(event)
	exec := s.db.ExecContext
	if tx != nil {
		exec = tx.ExecContext
	}
	_, err := exec(ctx, `
		INSERT INTO outbox (id, event_type, aggregate_id, payload, created_at)
		VALUES ($1,$2,$3,$4,$5)
	`, event.EventID, string(event.EventType), event.JobID, payload, event.EventTime)
	return err
}

func (s *Store) FetchOutboxBatch(ctx context.Context, limit int) ([]OutboxRow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, event_type, aggregate_id, payload, created_at
		FROM outbox WHERE published_at IS NULL
		ORDER BY created_at ASC LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.ID, &r.EventType, &r.AggregateID, &r.Payload, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) MarkOutboxPublished(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		_, err := s.db.ExecContext(ctx, `UPDATE outbox SET published_at=NOW() WHERE id=$1`, id)
		if err != nil {
			return err
		}
	}
	return nil
}

type OutboxRow struct {
	ID          string
	EventType   string
	AggregateID string
	Payload     []byte
	CreatedAt   time.Time
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (*domain.Job, error) {
	var j domain.Job
	var typ, state string
	var payload, sched, retry, meta []byte
	var timeoutMs int64
	var scheduledAt, queuedAt, startedAt, completedAt, nextRetryAt sql.NullTime
	err := row.Scan(
		&j.ID, &j.Name, &typ, &payload, &j.Priority, &state, &sched, &retry,
		&timeoutMs, &j.IdempotencyKey, &j.Attempt, &j.MaxAttempts,
		&j.WorkerID, &j.ExecutionID, &j.LastError,
		&scheduledAt, &queuedAt, &startedAt, &completedAt, &nextRetryAt,
		&j.CreatedAt, &j.UpdatedAt, &meta,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	j.Type = domain.JobType(typ)
	j.State = domain.JobState(state)
	j.Payload = payload
	j.Timeout = time.Duration(timeoutMs) * time.Millisecond
	_ = json.Unmarshal(sched, &j.Schedule)
	_ = json.Unmarshal(retry, &j.RetryPolicy)
	_ = json.Unmarshal(meta, &j.Metadata)
	if scheduledAt.Valid {
		t := scheduledAt.Time.UTC()
		j.ScheduledAt = &t
	}
	if queuedAt.Valid {
		t := queuedAt.Time.UTC()
		j.QueuedAt = &t
	}
	if startedAt.Valid {
		t := startedAt.Time.UTC()
		j.StartedAt = &t
	}
	if completedAt.Valid {
		t := completedAt.Time.UTC()
		j.CompletedAt = &t
	}
	if nextRetryAt.Valid {
		t := nextRetryAt.Time.UTC()
		j.NextRetryAt = &t
	}
	return &j, nil
}

func scanWorker(row scannable) (*domain.Worker, error) {
	var w domain.Worker
	var status string
	var caps, labels, meta []byte
	err := row.Scan(&w.ID, &w.Address, &status, &w.Capacity, &w.ActiveJobs, &caps, &labels, &w.LastHeartbeat, &w.RegisteredAt, &meta)
	if err != nil {
		return nil, err
	}
	w.Status = domain.WorkerStatus(status)
	_ = json.Unmarshal(caps, &w.Capabilities)
	_ = json.Unmarshal(labels, &w.Labels)
	_ = json.Unmarshal(meta, &w.Metadata)
	return &w, nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
