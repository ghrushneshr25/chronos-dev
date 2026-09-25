package domain_test

import (
	"testing"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

func TestWorkerAvailableSlots(t *testing.T) {
	w := &domain.Worker{Capacity: 5, ActiveJobs: 2, Status: domain.WorkerStatusHealthy}
	if w.AvailableSlots() != 3 {
		t.Fatalf("slots=%d", w.AvailableSlots())
	}
	w.ActiveJobs = 10
	if w.AvailableSlots() != 0 {
		t.Fatal("over capacity should clamp to 0")
	}
}

func TestWorkerIsAssignable(t *testing.T) {
	w := &domain.Worker{Capacity: 1, ActiveJobs: 0, Status: domain.WorkerStatusHealthy}
	if !w.IsAssignable() {
		t.Fatal("expected assignable")
	}
	w.Status = domain.WorkerStatusSuspected
	if w.IsAssignable() {
		t.Fatal("suspected not assignable")
	}
	w.Status = domain.WorkerStatusHealthy
	w.ActiveJobs = 1
	if w.IsAssignable() {
		t.Fatal("full capacity not assignable")
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := domain.DefaultRetryPolicy()
	if p.MaxAttempts != 3 || p.BackoffStrategy != "exponential" {
		t.Fatalf("%+v", p)
	}
}
