package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/config"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/ghrushneshr25/chronos-dev/internal/workerclient"
	chronosv1 "github.com/ghrushneshr25/chronos-dev/proto/chronos/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var ErrNoWorker = errors.New("no healthy worker with capacity")

// Service is the single-node scheduler control plane.
type Service struct {
	cfg    config.SchedulerConfig
	store  ports.JobStore
	queue  domain.Queue
	events ports.EventPublisher
	log    *slog.Logger

	mu      sync.RWMutex
	workers map[string]*workerConn
	// pendingCancels: worker_id -> set of job_ids awaiting cancel confirmation
	pendingCancels map[string]map[string]struct{}
}

type workerConn struct {
	info   *domain.Worker
	conn   *grpc.ClientConn
	client *workerclient.Client
}

func NewService(cfg config.SchedulerConfig, store ports.JobStore, q domain.Queue, pub ports.EventPublisher, log *slog.Logger) *Service {
	if log == nil {
		log = slog.Default()
	}
	return &Service{
		cfg:            cfg,
		store:          store,
		queue:          q,
		events:         pub,
		log:            log,
		workers:        make(map[string]*workerConn),
		pendingCancels: make(map[string]map[string]struct{}),
	}
}

func (s *Service) Run(ctx context.Context) error {
	_ = s.events.Emit(ctx, domain.EventSchedulerStarted, "", "", "", 0, map[string]string{"scheduler_id": s.cfg.ID})

	errCh := make(chan error, 1)

	go func() {
		t := time.NewTicker(s.cfg.PollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.promoteDueJobs(ctx); err != nil {
					s.log.Error("promote due jobs", "err", err)
				}
				if err := s.pushPendingCancels(ctx); err != nil {
					s.log.Error("push cancels", "err", err)
				}
				if err := s.watchdogTimeouts(ctx); err != nil {
					s.log.Error("timeout watchdog", "err", err)
				}
				if err := s.checkWorkers(ctx); err != nil {
					s.log.Error("check workers", "err", err)
				}
			}
		}
	}()

	go func() {
		s.log.Info("queue consumer starting", "backend", s.queue.Name())
		if err := s.queue.StartConsumer(ctx, s.handleQueuedJob); err != nil && ctx.Err() == nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		_ = s.events.Emit(context.Background(), domain.EventSchedulerStopped, "", "", "", 0, nil)
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Service) promoteDueJobs(ctx context.Context) error {
	jobs, err := s.store.ListDueScheduled(ctx, s.cfg.AssignBatchSize)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		qj := domain.QueuedJob{
			JobID:      job.ID,
			Name:       job.Name,
			Type:       job.Type,
			Payload:    job.Payload,
			Priority:   job.Priority,
			Attempt:    job.Attempt,
			Timeout:    job.Timeout,
			EnqueuedAt: time.Now().UTC(),
		}
		if err := s.queue.Enqueue(ctx, qj); err != nil {
			s.log.Error("enqueue", "job_id", job.ID, "err", err)
			continue
		}
		now := time.Now().UTC()
		_ = s.store.UpdateJobStateIf(ctx, job.ID, domain.JobStateQueued, map[string]any{"queued_at": &now},
			[]domain.JobState{domain.JobStateScheduled, domain.JobStateRetrying})
		_ = s.events.Emit(ctx, domain.EventJobQueued, job.ID, "", "", job.Attempt, nil)
	}
	return nil
}

func (s *Service) handleQueuedJob(ctx context.Context, job domain.QueuedJob) error {
	// Skip if job already left the queue (duplicate delivery / race with promote).
	existing, err := s.store.GetJob(ctx, job.JobID)
	if err != nil {
		s.log.Error("get job for assign", "job_id", job.JobID, "err", err)
		return err
	}
	switch existing.State {
	case domain.JobStateQueued, domain.JobStateScheduled, domain.JobStateRetrying:
		// proceed
	case domain.JobStateCancelRequested, domain.JobStateCancelled:
		s.log.Info("skip cancelled job", "job_id", job.JobID, "state", existing.State)
		return nil
	default:
		s.log.Info("skip job not queueable", "job_id", job.JobID, "state", existing.State)
		return nil
	}

	w := s.pickWorker()
	if w == nil {
		time.Sleep(200 * time.Millisecond)
		return ErrNoWorker
	}

	executionID := uuid.NewString()
	ex := &domain.JobExecution{
		ID:       executionID,
		JobID:    job.JobID,
		WorkerID: w.info.ID,
		Attempt:  job.Attempt + 1,
		State:    domain.ExecutionPending,
	}
	if err := s.store.CreateExecution(ctx, ex); err != nil {
		s.log.Error("create execution", "job_id", job.JobID, "err", err)
		return err
	}
	now := time.Now().UTC()
	if err := s.store.UpdateJobState(ctx, job.JobID, domain.JobStateAssigned, map[string]any{
		"worker_id":    w.info.ID,
		"execution_id": executionID,
		"attempt":      ex.Attempt,
	}); err != nil {
		s.log.Error("assign job", "job_id", job.JobID, "err", err)
		return err
	}
	_ = s.events.Emit(ctx, domain.EventJobAssigned, job.JobID, executionID, w.info.ID, ex.Attempt, nil)

	job.Attempt = ex.Attempt
	job.ExecutionID = executionID
	if err := w.client.ExecuteJob(ctx, job, executionID); err != nil {
		s.log.Error("push job failed", "job_id", job.JobID, "worker", w.info.ID, "err", err)
		return err
	}

	if err := s.store.UpdateJobState(ctx, job.JobID, domain.JobStateRunning, map[string]any{"started_at": &now}); err != nil {
		s.log.Error("mark running", "job_id", job.JobID, "err", err)
		return err
	}
	_ = s.events.Emit(ctx, domain.EventJobStarted, job.JobID, executionID, w.info.ID, ex.Attempt, nil)

	s.mu.Lock()
	w.info.ActiveJobs++
	s.mu.Unlock()
	s.log.Info("job assigned", "job_id", job.JobID, "worker", w.info.ID, "execution_id", executionID)
	return nil
}

func (s *Service) pickWorker() *workerConn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *workerConn
	for _, w := range s.workers {
		if w.info == nil || !w.info.IsAssignable() {
			continue
		}
		if best == nil || w.info.ActiveJobs < best.info.ActiveJobs {
			best = w
		}
	}
	return best
}

func (s *Service) RegisterWorker(ctx context.Context, w *domain.Worker) error {
	w.Status = domain.WorkerStatusHealthy
	w.LastHeartbeat = time.Now().UTC()
	if w.RegisteredAt.IsZero() {
		w.RegisteredAt = w.LastHeartbeat
	}
	if err := s.store.UpsertWorker(ctx, w); err != nil {
		return err
	}

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, w.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if old, ok := s.workers[w.ID]; ok && old.conn != nil {
		_ = old.conn.Close()
	}
	s.workers[w.ID] = &workerConn{
		info:   w,
		conn:   conn,
		client: workerclient.New(conn),
	}
	s.mu.Unlock()

	_ = s.events.Emit(ctx, domain.EventWorkerRegistered, "", "", w.ID, 0, map[string]any{
		"address": w.Address, "capacity": w.Capacity,
	})
	s.log.Info("worker registered", "worker_id", w.ID, "address", w.Address, "capacity", w.Capacity)
	return nil
}

func (s *Service) Heartbeat(ctx context.Context, workerID string, active, capacity int) ([]string, error) {
	s.mu.Lock()
	wc, ok := s.workers[workerID]
	if ok && wc.info != nil {
		wc.info.ActiveJobs = active
		wc.info.Capacity = capacity
		wc.info.LastHeartbeat = time.Now().UTC()
		wc.info.Status = domain.WorkerStatusHealthy
	}
	var cancelIDs []string
	if set, exists := s.pendingCancels[workerID]; exists {
		for id := range set {
			cancelIDs = append(cancelIDs, id)
		}
	}
	s.mu.Unlock()
	if !ok {
		return cancelIDs, nil
	}
	return cancelIDs, s.store.UpsertWorker(ctx, wc.info)
}

// RequestCancel marks a job for cancellation and notifies its worker when known.
func (s *Service) RequestCancel(ctx context.Context, jobID string) error {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job.WorkerID != "" {
		s.mu.Lock()
		if s.pendingCancels[job.WorkerID] == nil {
			s.pendingCancels[job.WorkerID] = make(map[string]struct{})
		}
		s.pendingCancels[job.WorkerID][jobID] = struct{}{}
		wc := s.workers[job.WorkerID]
		s.mu.Unlock()
		if wc != nil && wc.client != nil {
			_ = wc.client.CancelJob(ctx, jobID, job.ExecutionID)
		}
	}
	return nil
}

func (s *Service) clearPendingCancel(workerID, jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if set, ok := s.pendingCancels[workerID]; ok {
		delete(set, jobID)
		if len(set) == 0 {
			delete(s.pendingCancels, workerID)
		}
	}
}

func (s *Service) pushPendingCancels(ctx context.Context) error {
	jobs, err := s.store.ListCancelRequested(ctx, s.cfg.AssignBatchSize)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.WorkerID == "" {
			// never assigned — confirm cancel immediately
			now := time.Now().UTC()
			_ = s.store.UpdateJobState(ctx, job.ID, domain.JobStateCancelled, map[string]any{"completed_at": &now})
			_ = s.events.Emit(ctx, domain.EventJobCancelled, job.ID, job.ExecutionID, "", job.Attempt, nil)
			continue
		}
		s.mu.Lock()
		if s.pendingCancels[job.WorkerID] == nil {
			s.pendingCancels[job.WorkerID] = make(map[string]struct{})
		}
		s.pendingCancels[job.WorkerID][job.ID] = struct{}{}
		wc := s.workers[job.WorkerID]
		s.mu.Unlock()
		if wc != nil && wc.client != nil {
			if err := wc.client.CancelJob(ctx, job.ID, job.ExecutionID); err != nil {
				s.log.Warn("cancel push failed", "job_id", job.ID, "err", err)
			}
		}
	}
	return nil
}

func (s *Service) watchdogTimeouts(ctx context.Context) error {
	now := time.Now().UTC()
	jobs, err := s.store.ListTimedOutRunning(ctx, now, s.cfg.AssignBatchSize)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		// Ask worker to stop; mark TIMEOUT if still running (worker may not report).
		if job.WorkerID != "" {
			s.mu.RLock()
			wc := s.workers[job.WorkerID]
			s.mu.RUnlock()
			if wc != nil && wc.client != nil {
				_ = wc.client.CancelJob(ctx, job.ID, job.ExecutionID)
			}
		}
		res := domain.ExecutionResult{
			Success: false, Error: "timeout", Retryable: true,
			Reason: domain.ReasonTimeout, Duration: job.Timeout,
		}
		if err := s.HandleResult(ctx, job.ID, job.ExecutionID, job.WorkerID, job.Attempt, res); err != nil {
			s.log.Error("watchdog timeout handle", "job_id", job.ID, "err", err)
		}
	}
	return nil
}

func (s *Service) Deregister(ctx context.Context, workerID string) error {
	s.mu.Lock()
	if wc, ok := s.workers[workerID]; ok {
		if wc.conn != nil {
			_ = wc.conn.Close()
		}
		delete(s.workers, workerID)
	}
	s.mu.Unlock()
	_ = s.events.Emit(ctx, domain.EventWorkerDeregistered, "", "", workerID, 0, nil)
	return nil
}

func (s *Service) HandleResult(ctx context.Context, jobID, executionID, workerID string, attempt int, result domain.ExecutionResult) error {
	s.mu.Lock()
	if wc, ok := s.workers[workerID]; ok && wc.info != nil && wc.info.ActiveJobs > 0 {
		wc.info.ActiveJobs--
	}
	s.mu.Unlock()
	s.clearPendingCancel(workerID, jobID)

	if result.Reason == "" {
		if result.Success {
			result.Reason = domain.ReasonSuccess
		} else {
			result.Reason = domain.ReasonFailed
		}
	}

	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	switch job.State {
	case domain.JobStateCompleted, domain.JobStateFailed, domain.JobStateCancelled, domain.JobStateTimeout, domain.JobStateExpired:
		return nil // already terminal
	}
	now := time.Now().UTC()

	// Cancel path takes priority over success/fail if cancel was requested
	// or the worker reported cancellation.
	if result.Reason == domain.ReasonCancelled || job.State == domain.JobStateCancelRequested {
		if executionID != "" {
			_ = s.store.FinishExecution(ctx, executionID, domain.ExecutionCancelled, result)
		}
		_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateCancelled, map[string]any{
			"completed_at": &now,
			"last_error":   result.Error,
		})
		_ = s.events.Emit(ctx, domain.EventJobCancelled, jobID, executionID, workerID, attempt, map[string]any{
			"reason": result.Reason,
		})
		return nil
	}

	if result.Success {
		if executionID != "" {
			_ = s.store.FinishExecution(ctx, executionID, domain.ExecutionCompleted, result)
		}
		_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateCompleted, map[string]any{"completed_at": &now})
		_ = s.events.Emit(ctx, domain.EventJobCompleted, jobID, executionID, workerID, attempt, map[string]any{
			"duration_ms": result.Duration.Milliseconds(),
		})
		s.maybeRescheduleRecurring(ctx, job, now)
		return nil
	}

	// Timeout
	if result.Reason == domain.ReasonTimeout {
		if executionID != "" {
			_ = s.store.FinishExecution(ctx, executionID, domain.ExecutionTimeout, result)
		}
		_ = s.events.Emit(ctx, domain.EventJobTimeout, jobID, executionID, workerID, attempt, map[string]any{
			"error": result.Error,
		})
		if attempt < job.MaxAttempts {
			delay := computeBackoff(job.RetryPolicy, attempt)
			next := now.Add(delay)
			_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateRetrying, map[string]any{
				"next_retry_at": &next,
				"last_error":    "timeout",
				"worker_id":     "",
				"execution_id":  "",
			})
			_ = s.events.Emit(ctx, domain.EventJobRetryScheduled, jobID, executionID, workerID, attempt, map[string]any{
				"next_retry_at": next, "reason": "timeout",
			})
			return nil
		}
		_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateTimeout, map[string]any{
			"completed_at": &now,
			"last_error":   "timeout",
		})
		return nil
	}

	// Generic failure
	if executionID != "" {
		_ = s.store.FinishExecution(ctx, executionID, domain.ExecutionFailed, result)
	}
	_ = s.events.Emit(ctx, domain.EventJobFailed, jobID, executionID, workerID, attempt, map[string]any{
		"error": result.Error, "retryable": result.Retryable,
	})

	if result.Retryable && attempt < job.MaxAttempts {
		delay := computeBackoff(job.RetryPolicy, attempt)
		next := now.Add(delay)
		_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateRetrying, map[string]any{
			"next_retry_at": &next,
			"last_error":    result.Error,
			"worker_id":     "",
			"execution_id":  "",
		})
		_ = s.events.Emit(ctx, domain.EventJobRetryScheduled, jobID, executionID, workerID, attempt, map[string]any{
			"next_retry_at": next, "delay_ms": delay.Milliseconds(),
		})
		return nil
	}
	_ = s.store.UpdateJobState(ctx, jobID, domain.JobStateFailed, map[string]any{
		"completed_at": &now,
		"last_error":   result.Error,
	})
	return nil
}

func (s *Service) maybeRescheduleRecurring(ctx context.Context, job *domain.Job, from time.Time) {
	if job.Schedule.Type != domain.ScheduleRecurring {
		return
	}
	next, err := domain.NextRunAt(job.Schedule, from)
	if err != nil {
		s.log.Error("recurring next run", "job_id", job.ID, "err", err)
		return
	}
	// Spawn a new occurrence (keeps history of the completed run intact).
	child := &domain.Job{
		Name:           job.Name,
		Type:           job.Type,
		Payload:        job.Payload,
		Priority:       job.Priority,
		State:          domain.JobStateScheduled,
		Schedule:       job.Schedule,
		RetryPolicy:    job.RetryPolicy,
		Timeout:        job.Timeout,
		MaxAttempts:    job.MaxAttempts,
		ScheduledAt:    &next,
		Metadata:       job.Metadata,
		IdempotencyKey: "",
	}
	if err := s.store.CreateJob(ctx, child); err != nil {
		s.log.Error("create recurring occurrence", "parent", job.ID, "err", err)
		return
	}
	_ = s.events.Emit(ctx, domain.EventJobScheduled, child.ID, "", "", 0, map[string]any{
		"parent_job_id": job.ID,
		"scheduled_at":  next,
		"recurring":     true,
	})
	s.log.Info("recurring job scheduled", "parent", job.ID, "child", child.ID, "run_at", next)
}

func (s *Service) checkWorkers(ctx context.Context) error {
	_, unavailable, err := s.store.MarkStaleWorkers(ctx, s.cfg.HeartbeatTimeout, s.cfg.SuspectGracePeriod)
	if err != nil {
		return err
	}
	for _, id := range unavailable {
		s.mu.Lock()
		delete(s.workers, id)
		s.mu.Unlock()
		_ = s.events.Emit(ctx, domain.EventWorkerUnavailable, "", "", id, 0, nil)
		if err := s.recoverWorkerJobs(ctx, id); err != nil {
			s.log.Error("recover worker jobs", "worker_id", id, "err", err)
		}
	}
	return nil
}

func (s *Service) recoverWorkerJobs(ctx context.Context, workerID string) error {
	jobs, err := s.store.ListJobs(ctx, string(domain.JobStateRunning), 1000)
	if err != nil {
		return err
	}
	assigned, err := s.store.ListJobs(ctx, string(domain.JobStateAssigned), 1000)
	if err != nil {
		return err
	}
	cancelReq, err := s.store.ListJobs(ctx, string(domain.JobStateCancelRequested), 1000)
	if err != nil {
		return err
	}
	jobs = append(jobs, assigned...)
	jobs = append(jobs, cancelReq...)
	now := time.Now().UTC()
	for _, j := range jobs {
		if j.WorkerID != workerID {
			continue
		}
		if j.ExecutionID != "" {
			_ = s.store.FinishExecution(ctx, j.ExecutionID, domain.ExecutionFailed, domain.ExecutionResult{
				Success: false, Error: "worker lost", Reason: domain.ReasonFailed,
			})
		}
		if j.State == domain.JobStateCancelRequested {
			_ = s.store.UpdateJobState(ctx, j.ID, domain.JobStateCancelled, map[string]any{
				"completed_at": &now,
				"last_error":   "worker lost during cancel",
			})
			_ = s.events.Emit(ctx, domain.EventJobCancelled, j.ID, j.ExecutionID, workerID, j.Attempt, map[string]string{
				"reason": "worker_lost",
			})
			continue
		}
		if j.Attempt < j.MaxAttempts {
			next := now
			_ = s.store.UpdateJobState(ctx, j.ID, domain.JobStateRetrying, map[string]any{
				"next_retry_at": &next,
				"last_error":    "worker lost",
				"worker_id":     "",
				"execution_id":  "",
			})
			_ = s.events.Emit(ctx, domain.EventJobRetryScheduled, j.ID, j.ExecutionID, workerID, j.Attempt, map[string]string{
				"reason": "worker_lost",
			})
		} else {
			_ = s.store.UpdateJobState(ctx, j.ID, domain.JobStateFailed, map[string]any{
				"completed_at": &now,
				"last_error":   "worker lost, max attempts exceeded",
			})
			_ = s.events.Emit(ctx, domain.EventJobFailed, j.ID, j.ExecutionID, workerID, j.Attempt, map[string]string{
				"reason": "worker_lost",
			})
		}
	}
	s.mu.Lock()
	delete(s.pendingCancels, workerID)
	s.mu.Unlock()
	return nil
}

func computeBackoff(p domain.RetryPolicy, attempt int) time.Duration {
	delay := p.InitialDelay
	if delay <= 0 {
		delay = time.Second
	}
	if p.BackoffStrategy == "exponential" {
		mult := p.Multiplier
		if mult < 1 {
			mult = 2
		}
		delay = time.Duration(float64(delay) * math.Pow(mult, float64(attempt-1)))
	}
	if p.MaxDelay > 0 && delay > p.MaxDelay {
		delay = p.MaxDelay
	}
	if p.Jitter {
		j := 0.5 + rand.Float64()
		delay = time.Duration(float64(delay) * j)
	}
	return delay
}

// Ensure *Service implements generated server via adapter in grpc_server.go
var _ chronosv1.SchedulerServiceServer = (*GRPCServer)(nil)
