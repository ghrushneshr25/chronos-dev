package memory

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
	"github.com/google/uuid"
)

// Store is an in-process JobStore for unit and e2e tests (no Postgres).
type Store struct {
	mu         sync.RWMutex
	jobs       map[string]*domain.Job
	byIdem     map[string]string
	executions map[string]*domain.JobExecution
	workers    map[string]*domain.Worker
	outbox     []postgres.OutboxRow
}

func New() *Store {
	return &Store{
		jobs:       make(map[string]*domain.Job),
		byIdem:     make(map[string]string),
		executions: make(map[string]*domain.JobExecution),
		workers:    make(map[string]*domain.Worker),
	}
}

var _ ports.JobStore = (*Store)(nil)

func (s *Store) DB() *sql.DB { return nil }
func (s *Store) Close() error { return nil }

func cloneJob(j *domain.Job) *domain.Job {
	if j == nil {
		return nil
	}
	c := *j
	if j.ScheduledAt != nil {
		t := *j.ScheduledAt
		c.ScheduledAt = &t
	}
	if j.QueuedAt != nil {
		t := *j.QueuedAt
		c.QueuedAt = &t
	}
	if j.StartedAt != nil {
		t := *j.StartedAt
		c.StartedAt = &t
	}
	if j.CompletedAt != nil {
		t := *j.CompletedAt
		c.CompletedAt = &t
	}
	if j.NextRetryAt != nil {
		t := *j.NextRetryAt
		c.NextRetryAt = &t
	}
	if j.Metadata != nil {
		c.Metadata = map[string]string{}
		for k, v := range j.Metadata {
			c.Metadata[k] = v
		}
	}
	return &c
}

func (s *Store) CreateJob(ctx context.Context, job *domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if job.ID == "" {
		job.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	job.CreatedAt = now
	job.UpdatedAt = now
	if job.IdempotencyKey != "" {
		if id, ok := s.byIdem[job.IdempotencyKey]; ok {
			*job = *cloneJob(s.jobs[id])
			return nil
		}
		s.byIdem[job.IdempotencyKey] = job.ID
	}
	s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (s *Store) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	return cloneJob(j), nil
}

func (s *Store) GetJobByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byIdem[key]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	return cloneJob(s.jobs[id]), nil
}

func (s *Store) UpdateJobState(ctx context.Context, id string, state domain.JobState, fields map[string]any) error {
	return s.UpdateJobStateIf(ctx, id, state, fields, nil)
}

func (s *Store) UpdateJobStateIf(ctx context.Context, id string, state domain.JobState, fields map[string]any, onlyFrom []domain.JobState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return postgres.ErrNotFound
	}
	if len(onlyFrom) > 0 {
		allowed := false
		for _, st := range onlyFrom {
			if j.State == st {
				allowed = true
				break
			}
		}
		if !allowed {
			return nil
		}
	}
	j.State = state
	j.UpdatedAt = time.Now().UTC()
	if v, ok := fields["worker_id"].(string); ok {
		j.WorkerID = v
	}
	if v, ok := fields["execution_id"].(string); ok {
		j.ExecutionID = v
	}
	if v, ok := fields["last_error"].(string); ok {
		j.LastError = v
	}
	if v, ok := fields["attempt"].(int); ok {
		j.Attempt = v
	}
	if v, ok := fields["queued_at"].(*time.Time); ok {
		j.QueuedAt = v
	}
	if v, ok := fields["started_at"].(*time.Time); ok {
		j.StartedAt = v
	}
	if v, ok := fields["completed_at"].(*time.Time); ok {
		j.CompletedAt = v
	}
	if v, ok := fields["next_retry_at"].(*time.Time); ok {
		j.NextRetryAt = v
	}
	if v, ok := fields["scheduled_at"].(*time.Time); ok {
		j.ScheduledAt = v
	}
	return nil
}

func (s *Store) ListJobs(ctx context.Context, state string, limit int) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	out := make([]*domain.Job, 0)
	for _, j := range s.jobs {
		if state != "" && string(j.State) != state {
			continue
		}
		out = append(out, cloneJob(j))
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) ListDueScheduled(ctx context.Context, limit int) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	out := make([]*domain.Job, 0)
	for _, j := range s.jobs {
		switch j.State {
		case domain.JobStateScheduled:
			if j.ScheduledAt != nil && !j.ScheduledAt.After(now) {
				out = append(out, cloneJob(j))
			}
		case domain.JobStateRetrying:
			if j.NextRetryAt != nil && !j.NextRetryAt.After(now) {
				out = append(out, cloneJob(j))
			}
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *Store) ListCancelRequested(ctx context.Context, limit int) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	out := make([]*domain.Job, 0)
	for _, j := range s.jobs {
		if j.State == domain.JobStateCancelRequested {
			out = append(out, cloneJob(j))
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) ListTimedOutRunning(ctx context.Context, now time.Time, limit int) ([]*domain.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	out := make([]*domain.Job, 0)
	for _, j := range s.jobs {
		if j.State != domain.JobStateRunning && j.State != domain.JobStateAssigned {
			continue
		}
		if j.StartedAt == nil {
			continue
		}
		to := j.Timeout
		if to <= 0 {
			to = 30 * time.Second
		}
		if j.StartedAt.Add(to).Before(now) {
			out = append(out, cloneJob(j))
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) CreateExecution(ctx context.Context, ex *domain.JobExecution) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ex.ID == "" {
		ex.ID = uuid.NewString()
	}
	ex.CreatedAt = time.Now().UTC()
	cp := *ex
	s.executions[ex.ID] = &cp
	return nil
}

func (s *Store) FinishExecution(ctx context.Context, id string, state domain.ExecutionState, result domain.ExecutionResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ex, ok := s.executions[id]
	if !ok {
		return postgres.ErrNotFound
	}
	now := time.Now().UTC()
	ex.State = state
	ex.FinishedAt = &now
	ex.DurationMs = result.Duration.Milliseconds()
	ex.Error = result.Error
	ex.Output = result.Output
	ex.Retryable = result.Retryable
	return nil
}

func (s *Store) UpsertWorker(ctx context.Context, w *domain.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *w
	s.workers[w.ID] = &cp
	return nil
}

func (s *Store) ListHealthyWorkers(ctx context.Context) ([]*domain.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Worker
	for _, w := range s.workers {
		if w.Status == domain.WorkerStatusHealthy {
			cp := *w
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]*domain.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*domain.Worker
	for _, w := range s.workers {
		cp := *w
		out = append(out, &cp)
	}
	return out, nil
}

func (s *Store) MarkStaleWorkers(ctx context.Context, heartbeatTimeout, grace time.Duration) (suspected, unavailable []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	deadBefore := now.Add(-(heartbeatTimeout + grace))
	suspectBefore := now.Add(-heartbeatTimeout)
	for id, w := range s.workers {
		if w.LastHeartbeat.Before(deadBefore) && (w.Status == domain.WorkerStatusHealthy || w.Status == domain.WorkerStatusSuspected) {
			w.Status = domain.WorkerStatusUnavailable
			unavailable = append(unavailable, id)
		} else if w.LastHeartbeat.Before(suspectBefore) && w.Status == domain.WorkerStatusHealthy {
			w.Status = domain.WorkerStatusSuspected
			suspected = append(suspected, id)
		}
	}
	return suspected, unavailable, nil
}

func (s *Store) InsertOutbox(ctx context.Context, tx *sql.Tx, event domain.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outbox = append(s.outbox, postgres.OutboxRow{
		ID:          event.EventID,
		EventType:   string(event.EventType),
		AggregateID: event.JobID,
		CreatedAt:   event.EventTime,
	})
	return nil
}

func (s *Store) FetchOutboxBatch(ctx context.Context, limit int) ([]postgres.OutboxRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit > len(s.outbox) {
		limit = len(s.outbox)
	}
	out := make([]postgres.OutboxRow, limit)
	copy(out, s.outbox[:limit])
	return out, nil
}

func (s *Store) MarkOutboxPublished(ctx context.Context, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]struct{}{}
	for _, id := range ids {
		set[id] = struct{}{}
	}
	remaining := s.outbox[:0]
	for _, r := range s.outbox {
		if _, ok := set[r.ID]; !ok {
			remaining = append(remaining, r)
		}
	}
	s.outbox = remaining
	return nil
}

func (s *Store) OutboxLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.outbox)
}
