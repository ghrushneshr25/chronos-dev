package memory

import (
	"context"
	"sync"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

// Queue is an in-process priority queue for tests and local dev without Kafka.
type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []domain.QueuedJob
	closed   bool
	delayed  []delayedItem
}

type delayedItem struct {
	job       domain.QueuedJob
	available time.Time
}

func New() *Queue {
	q := &Queue{}
	q.cond = sync.NewCond(&q.mu)
	go q.releaseDelayed()
	return q
}

func (q *Queue) Name() string { return "memory" }

func (q *Queue) Enqueue(ctx context.Context, job domain.QueuedJob) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return context.Canceled
	}
	if job.EnqueuedAt.IsZero() {
		job.EnqueuedAt = time.Now().UTC()
	}
	q.insertLocked(job)
	q.cond.Signal()
	return nil
}

func (q *Queue) EnqueueDelay(ctx context.Context, job domain.QueuedJob, delay time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return context.Canceled
	}
	if delay <= 0 {
		q.insertLocked(job)
		q.cond.Signal()
		return nil
	}
	q.delayed = append(q.delayed, delayedItem{job: job, available: time.Now().UTC().Add(delay)})
	return nil
}

func (q *Queue) insertLocked(job domain.QueuedJob) {
	// higher priority first; stable by EnqueuedAt
	i := 0
	for i < len(q.items) {
		if job.Priority > q.items[i].Priority {
			break
		}
		if job.Priority == q.items[i].Priority && job.EnqueuedAt.Before(q.items[i].EnqueuedAt) {
			break
		}
		i++
	}
	q.items = append(q.items, domain.QueuedJob{})
	copy(q.items[i+1:], q.items[i:])
	q.items[i] = job
}

func (q *Queue) releaseDelayed() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		now := time.Now().UTC()
		remaining := q.delayed[:0]
		for _, d := range q.delayed {
			if !d.available.After(now) {
				q.insertLocked(d.job)
				q.cond.Signal()
			} else {
				remaining = append(remaining, d)
			}
		}
		q.delayed = remaining
		q.mu.Unlock()
	}
}

func (q *Queue) StartConsumer(ctx context.Context, handler func(ctx context.Context, job domain.QueuedJob) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		q.mu.Lock()
		for len(q.items) == 0 && !q.closed {
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					q.cond.Broadcast()
				case <-done:
				}
			}()
			q.cond.Wait()
			close(done)
			if ctx.Err() != nil {
				q.mu.Unlock()
				return ctx.Err()
			}
		}
		if q.closed {
			q.mu.Unlock()
			return nil
		}
		job := q.items[0]
		q.items = q.items[1:]
		q.mu.Unlock()

		if err := handler(ctx, job); err != nil {
			// requeue on failure
			_ = q.Enqueue(ctx, job)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
	return nil
}
