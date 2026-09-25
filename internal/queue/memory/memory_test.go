package memory_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
)

func TestEnqueuePriorityOrder(t *testing.T) {
	q := memory.New()
	defer q.Close()

	_ = q.Enqueue(context.Background(), domain.QueuedJob{JobID: "low", Priority: 1})
	_ = q.Enqueue(context.Background(), domain.QueuedJob{JobID: "high", Priority: 10})
	_ = q.Enqueue(context.Background(), domain.QueuedJob{JobID: "mid", Priority: 5})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []string
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		_ = q.StartConsumer(ctx, func(ctx context.Context, job domain.QueuedJob) error {
			mu.Lock()
			got = append(got, job.JobID)
			n := len(got)
			mu.Unlock()
			if n == 3 {
				close(done)
				cancel()
			}
			return nil
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{"high", "mid", "low"}
	if len(got) != 3 {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order got %v want %v", got, want)
		}
	}
}

func TestEnqueueDelay(t *testing.T) {
	q := memory.New()
	defer q.Close()

	start := time.Now()
	_ = q.EnqueueDelay(context.Background(), domain.QueuedJob{JobID: "d", Priority: 5}, 150*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var when time.Time
	err := q.StartConsumer(ctx, func(ctx context.Context, job domain.QueuedJob) error {
		when = time.Now()
		cancel()
		return nil
	})
	if err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	if when.IsZero() || when.Sub(start) < 100*time.Millisecond {
		t.Fatalf("delay not respected: elapsed=%v", when.Sub(start))
	}
}

func TestConsumerRedeliverOnError(t *testing.T) {
	q := memory.New()
	defer q.Close()
	_ = q.Enqueue(context.Background(), domain.QueuedJob{JobID: "j1", Priority: 5})

	var attempts atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = q.StartConsumer(ctx, func(ctx context.Context, job domain.QueuedJob) error {
		n := attempts.Add(1)
		if n < 3 {
			return context.DeadlineExceeded
		}
		cancel()
		return nil
	})

	if attempts.Load() < 3 {
		t.Fatalf("expected redelivery, attempts=%d", attempts.Load())
	}
}

func TestName(t *testing.T) {
	if memory.New().Name() != "memory" {
		t.Fatal("name")
	}
}
