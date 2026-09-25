package scheduler_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/events"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
	"github.com/ghrushneshr25/chronos-dev/internal/scheduler"
	memstore "github.com/ghrushneshr25/chronos-dev/internal/store/memory"
)

func newTestScheduler(t *testing.T) (*scheduler.Service, *memstore.Store) {
	t.Helper()
	store := memstore.New()
	q := memory.New()
	t.Cleanup(func() { _ = q.Close() })
	cfg := config.SchedulerConfig{
		ID:                 "sched-test",
		PollInterval:       50 * time.Millisecond,
		AssignBatchSize:    10,
		HeartbeatTimeout:   time.Second,
		SuspectGracePeriod: time.Second,
	}
	pub := events.NoopPublisher{}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return scheduler.NewService(cfg, store, q, pub, log), store
}

func TestHandleResultSuccess(t *testing.T) {
	svc, store := newTestScheduler(t)
	job := &domain.Job{
		Name:        "ok",
		State:       domain.JobStateRunning,
		MaxAttempts: 3,
		Attempt:     1,
	}
	_ = store.CreateJob(context.Background(), job)
	ex := &domain.JobExecution{JobID: job.ID, WorkerID: "w1", Attempt: 1, State: domain.ExecutionRunning}
	_ = store.CreateExecution(context.Background(), ex)

	err := svc.HandleResult(context.Background(), job.ID, ex.ID, "w1", 1, domain.ExecutionResult{
		Success:  true,
		Duration: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateCompleted {
		t.Fatalf("state=%s", got.State)
	}
}

func TestHandleResultRetryable(t *testing.T) {
	svc, store := newTestScheduler(t)
	job := &domain.Job{
		Name:        "retry",
		State:       domain.JobStateRunning,
		MaxAttempts: 3,
		Attempt:     1,
		RetryPolicy: domain.DefaultRetryPolicy(),
	}
	_ = store.CreateJob(context.Background(), job)
	ex := &domain.JobExecution{JobID: job.ID, WorkerID: "w1", Attempt: 1, State: domain.ExecutionRunning}
	_ = store.CreateExecution(context.Background(), ex)

	err := svc.HandleResult(context.Background(), job.ID, ex.ID, "w1", 1, domain.ExecutionResult{
		Success:   false,
		Error:     "temp",
		Retryable: true,
		Duration:  time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateRetrying {
		t.Fatalf("state=%s", got.State)
	}
	if got.NextRetryAt == nil {
		t.Fatal("next_retry_at required")
	}
}

func TestHandleResultTimeoutRetry(t *testing.T) {
	svc, store := newTestScheduler(t)
	job := &domain.Job{
		Name:        "to",
		State:       domain.JobStateRunning,
		MaxAttempts: 3,
		Attempt:     1,
		RetryPolicy: domain.DefaultRetryPolicy(),
	}
	_ = store.CreateJob(context.Background(), job)
	ex := &domain.JobExecution{JobID: job.ID, WorkerID: "w1", Attempt: 1, State: domain.ExecutionRunning}
	_ = store.CreateExecution(context.Background(), ex)

	err := svc.HandleResult(context.Background(), job.ID, ex.ID, "w1", 1, domain.ExecutionResult{
		Success: false, Error: "timeout", Retryable: true, Reason: domain.ReasonTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateRetrying {
		t.Fatalf("state=%s", got.State)
	}
}

func TestHandleResultCancelled(t *testing.T) {
	svc, store := newTestScheduler(t)
	job := &domain.Job{
		Name:   "c",
		State:  domain.JobStateCancelRequested,
		Attempt: 1,
	}
	_ = store.CreateJob(context.Background(), job)
	ex := &domain.JobExecution{JobID: job.ID, WorkerID: "w1", Attempt: 1, State: domain.ExecutionRunning}
	_ = store.CreateExecution(context.Background(), ex)

	err := svc.HandleResult(context.Background(), job.ID, ex.ID, "w1", 1, domain.ExecutionResult{
		Success: false, Error: "cancelled", Reason: domain.ReasonCancelled,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateCancelled {
		t.Fatalf("state=%s", got.State)
	}
}

func TestHandleResultExhausted(t *testing.T) {
	svc, store := newTestScheduler(t)
	job := &domain.Job{
		Name:        "fail",
		State:       domain.JobStateRunning,
		MaxAttempts: 2,
		Attempt:     2,
		RetryPolicy: domain.DefaultRetryPolicy(),
	}
	_ = store.CreateJob(context.Background(), job)
	ex := &domain.JobExecution{JobID: job.ID, WorkerID: "w1", Attempt: 2, State: domain.ExecutionRunning}
	_ = store.CreateExecution(context.Background(), ex)

	err := svc.HandleResult(context.Background(), job.ID, ex.ID, "w1", 2, domain.ExecutionResult{
		Success:   false,
		Error:     "done",
		Retryable: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateFailed {
		t.Fatalf("state=%s", got.State)
	}
}
