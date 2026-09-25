package ports

import (
	"context"
	"database/sql"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
)

// JobStore is the operational persistence contract.
type JobStore interface {
	CreateJob(ctx context.Context, job *domain.Job) error
	GetJob(ctx context.Context, id string) (*domain.Job, error)
	GetJobByIdempotencyKey(ctx context.Context, key string) (*domain.Job, error)
	UpdateJobState(ctx context.Context, id string, state domain.JobState, fields map[string]any) error
	UpdateJobStateIf(ctx context.Context, id string, state domain.JobState, fields map[string]any, onlyFrom []domain.JobState) error
	ListJobs(ctx context.Context, state string, limit int) ([]*domain.Job, error)
	ListDueScheduled(ctx context.Context, limit int) ([]*domain.Job, error)
	ListCancelRequested(ctx context.Context, limit int) ([]*domain.Job, error)
	ListTimedOutRunning(ctx context.Context, now time.Time, limit int) ([]*domain.Job, error)
	CreateExecution(ctx context.Context, ex *domain.JobExecution) error
	FinishExecution(ctx context.Context, id string, state domain.ExecutionState, result domain.ExecutionResult) error
	UpsertWorker(ctx context.Context, w *domain.Worker) error
	ListHealthyWorkers(ctx context.Context) ([]*domain.Worker, error)
	ListWorkers(ctx context.Context) ([]*domain.Worker, error)
	MarkStaleWorkers(ctx context.Context, heartbeatTimeout, grace time.Duration) (suspected, unavailable []string, err error)
	InsertOutbox(ctx context.Context, tx *sql.Tx, event domain.Event) error
	FetchOutboxBatch(ctx context.Context, limit int) ([]postgres.OutboxRow, error)
	MarkOutboxPublished(ctx context.Context, ids []string) error
	DB() *sql.DB
	Close() error
}

// EventPublisher emits domain events (typically via transactional outbox).
type EventPublisher interface {
	Emit(ctx context.Context, eventType domain.EventType, jobID, executionID, workerID string, attempt int, payload any) error
}

// JobExecutor runs a single job payload.
type JobExecutor interface {
	Execute(ctx context.Context, jobType domain.JobType, payload []byte) domain.ExecutionResult
}
