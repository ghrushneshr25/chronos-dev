package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/store/memory"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
)

func TestCreateGetJob(t *testing.T) {
	s := memory.New()
	job := &domain.Job{
		Name:  "t",
		Type:  domain.JobTypeBuiltin,
		State: domain.JobStateScheduled,
	}
	if err := s.CreateJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" {
		t.Fatal("id assigned")
	}
	got, err := s.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "t" {
		t.Fatalf("%+v", got)
	}
}

func TestIdempotencyKey(t *testing.T) {
	s := memory.New()
	j1 := &domain.Job{Name: "a", IdempotencyKey: "pay-1", State: domain.JobStateCreated}
	j2 := &domain.Job{Name: "b", IdempotencyKey: "pay-1", State: domain.JobStateCreated}
	_ = s.CreateJob(context.Background(), j1)
	_ = s.CreateJob(context.Background(), j2)
	if j1.ID != j2.ID {
		t.Fatalf("expected same id %s vs %s", j1.ID, j2.ID)
	}
}

func TestUpdateJobStateIf(t *testing.T) {
	s := memory.New()
	job := &domain.Job{Name: "x", State: domain.JobStateCompleted}
	_ = s.CreateJob(context.Background(), job)

	err := s.UpdateJobStateIf(context.Background(), job.ID, domain.JobStateQueued, nil,
		[]domain.JobState{domain.JobStateScheduled, domain.JobStateRetrying})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateCompleted {
		t.Fatalf("stale transition should be ignored, got %s", got.State)
	}
}

func TestListDueScheduled(t *testing.T) {
	s := memory.New()
	past := time.Now().UTC().Add(-time.Minute)
	future := time.Now().UTC().Add(time.Hour)
	due := &domain.Job{Name: "due", State: domain.JobStateScheduled, ScheduledAt: &past}
	later := &domain.Job{Name: "later", State: domain.JobStateScheduled, ScheduledAt: &future}
	_ = s.CreateJob(context.Background(), due)
	_ = s.CreateJob(context.Background(), later)

	list, err := s.ListDueScheduled(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "due" {
		t.Fatalf("%+v", list)
	}
}

func TestNotFound(t *testing.T) {
	s := memory.New()
	_, err := s.GetJob(context.Background(), "missing")
	if err != postgres.ErrNotFound {
		t.Fatalf("got %v", err)
	}
}

func TestWorkerLifecycle(t *testing.T) {
	s := memory.New()
	w := &domain.Worker{
		ID:            "w1",
		Address:       "localhost:1",
		Status:        domain.WorkerStatusHealthy,
		Capacity:      2,
		LastHeartbeat: time.Now().UTC().Add(-time.Hour),
	}
	_ = s.UpsertWorker(context.Background(), w)
	_, unavail, err := s.MarkStaleWorkers(context.Background(), time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(unavail) != 1 || unavail[0] != "w1" {
		t.Fatalf("%v", unavail)
	}
}

func TestOutbox(t *testing.T) {
	s := memory.New()
	_ = s.InsertOutbox(context.Background(), nil, domain.Event{EventID: "e1", EventType: domain.EventJobCreated, JobID: "j1", EventTime: time.Now().UTC()})
	if s.OutboxLen() != 1 {
		t.Fatal("outbox len")
	}
	rows, err := s.FetchOutboxBatch(context.Background(), 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %v", rows, err)
	}
	_ = s.MarkOutboxPublished(context.Background(), []string{"e1"})
	if s.OutboxLen() != 0 {
		t.Fatal("expected drained")
	}
}
