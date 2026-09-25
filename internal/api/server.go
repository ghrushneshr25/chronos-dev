package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
	"github.com/ghrushneshr25/chronos-dev/internal/auth"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/ghrushneshr25/chronos-dev/internal/queue"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

type Server struct {
	store     ports.JobStore
	queue     domain.Queue
	events    ports.EventPublisher
	auth      *auth.Authenticator
	analytics analytics.Querier // optional; nil = ops-only
}

func NewServer(store ports.JobStore, q domain.Queue, pub ports.EventPublisher, a *auth.Authenticator) *Server {
	return &Server{store: store, queue: q, events: pub, auth: a}
}

// SetAnalytics attaches ClickHouse-backed analytics. Safe to leave nil.
func (s *Server) SetAnalytics(q analytics.Querier) {
	s.analytics = q
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)

	r.Group(func(r chi.Router) {
		r.Use(s.auth.HTTPMiddleware)
		r.Post("/v1/jobs", s.createJob)
		r.Get("/v1/jobs", s.listJobs)
		r.Get("/v1/jobs/{id}", s.getJob)
		r.Post("/v1/jobs/{id}/cancel", s.cancelJob)
		r.Get("/v1/workers", s.listWorkers)
		r.Get("/v1/queue/backends", s.queueBackends)

		r.Get("/v1/analytics/throughput", s.analyticsThroughput)
		r.Get("/v1/analytics/latency", s.analyticsLatency)
		r.Get("/v1/analytics/failures", s.analyticsFailures)
		r.Get("/v1/analytics/retries", s.analyticsFailures)
		r.Get("/v1/analytics/workers", s.analyticsWorkers)
		r.Get("/v1/analytics/queue", s.analyticsQueue)
		r.Get("/v1/analytics/scheduler", s.analyticsScheduler)
		r.Get("/v1/analytics/capacity", s.analyticsCapacity)
		r.Get("/v1/analytics/anomalies", s.analyticsAnomalies)
	})
	return r
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if db := s.store.DB(); db != nil {
		if err := db.PingContext(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "error": err.Error()})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "queue": s.queue.Name()})
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req domain.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name required")
		return
	}
	if req.Type == "" {
		req.Type = domain.JobTypeBuiltin
	}
	if req.Type != domain.JobTypeBuiltin && req.Type != domain.JobTypeWebhook {
		writeErr(w, http.StatusBadRequest, "type must be builtin or webhook")
		return
	}
	if req.Priority == 0 {
		req.Priority = 5
	}
	if req.Priority < 1 || req.Priority > 10 {
		writeErr(w, http.StatusBadRequest, "priority must be 1-10")
		return
	}
	if req.Schedule.Type == "" {
		req.Schedule.Type = domain.ScheduleImmediate
	}
	if err := domain.ValidateSchedule(req.Schedule); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}

	retry := domain.DefaultRetryPolicy()
	if req.RetryPolicy != nil {
		retry = *req.RetryPolicy
	}
	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	now := time.Now().UTC()
	job := &domain.Job{
		ID:             uuid.NewString(),
		Name:           req.Name,
		Type:           req.Type,
		Payload:        req.Payload,
		Priority:       req.Priority,
		State:          domain.JobStateCreated,
		Schedule:       req.Schedule,
		RetryPolicy:    retry,
		Timeout:        timeout,
		IdempotencyKey: req.IdempotencyKey,
		MaxAttempts:    retry.MaxAttempts,
		Metadata:       req.Metadata,
	}
	if job.Payload == nil {
		job.Payload = json.RawMessage(`{}`)
	}

	switch req.Schedule.Type {
	case domain.ScheduleImmediate:
		job.State = domain.JobStateScheduled
		job.ScheduledAt = &now
	case domain.ScheduleDelayed:
		runAt := now.Add(req.Schedule.Delay)
		job.State = domain.JobStateScheduled
		job.ScheduledAt = &runAt
	case domain.ScheduleOneTime:
		if req.Schedule.RunAt == nil {
			writeErr(w, http.StatusBadRequest, "run_at required for one_time")
			return
		}
		job.State = domain.JobStateScheduled
		job.ScheduledAt = req.Schedule.RunAt
	case domain.ScheduleRecurring:
		job.State = domain.JobStateScheduled
		next, err := domain.NextRunAt(req.Schedule, now.Add(-time.Nanosecond))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if !next.After(now) {
			next, err = domain.NextRunAt(req.Schedule, now)
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		job.ScheduledAt = &next
	default:
		writeErr(w, http.StatusBadRequest, "unknown schedule type")
		return
	}

	if err := s.store.CreateJob(r.Context(), job); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	_ = s.events.Emit(r.Context(), domain.EventJobCreated, job.ID, "", "", 0, map[string]any{
		"name": job.Name, "type": job.Type, "priority": job.Priority,
	})
	_ = s.events.Emit(r.Context(), domain.EventJobScheduled, job.ID, "", "", 0, map[string]any{
		"scheduled_at": job.ScheduledAt,
	})

	if job.ScheduledAt != nil && !job.ScheduledAt.After(now) {
		if err := s.enqueueJob(r.Context(), job); err != nil {
			writeErr(w, http.StatusInternalServerError, "enqueue failed: "+err.Error())
			return
		}
	}

	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) enqueueJob(ctx context.Context, job *domain.Job) error {
	now := time.Now().UTC()
	qj := domain.QueuedJob{
		JobID:      job.ID,
		Name:       job.Name,
		Type:       job.Type,
		Payload:    job.Payload,
		Priority:   job.Priority,
		Attempt:    job.Attempt,
		Timeout:    job.Timeout,
		EnqueuedAt: now,
	}
	if err := s.queue.Enqueue(ctx, qj); err != nil {
		return err
	}
	_ = s.events.Emit(ctx, domain.EventJobQueued, job.ID, "", "", job.Attempt, nil)
	if err := s.store.UpdateJobState(ctx, job.ID, domain.JobStateQueued, map[string]any{
		"queued_at": &now,
	}); err != nil {
		return err
	}
	job.State = domain.JobStateQueued
	job.QueuedAt = &now
	return nil
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		if err == postgres.ErrNotFound {
			writeErr(w, http.StatusNotFound, "job not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	jobs, err := s.store.ListJobs(r.Context(), state, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if jobs == nil {
		jobs = []*domain.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		if err == postgres.ErrNotFound {
			writeErr(w, http.StatusNotFound, "job not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	terminal := map[domain.JobState]bool{
		domain.JobStateCompleted: true,
		domain.JobStateCancelled: true,
		domain.JobStateExpired:   true,
	}
	if terminal[job.State] {
		writeErr(w, http.StatusConflict, "job already terminal")
		return
	}
	if job.State == domain.JobStateRunning || job.State == domain.JobStateAssigned {
		_ = s.store.UpdateJobState(r.Context(), id, domain.JobStateCancelRequested, nil)
		_ = s.events.Emit(r.Context(), domain.EventJobCancelRequested, id, job.ExecutionID, job.WorkerID, job.Attempt, nil)
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "CANCEL_REQUESTED"})
		return
	}
	now := time.Now().UTC()
	_ = s.store.UpdateJobState(r.Context(), id, domain.JobStateCancelled, map[string]any{"completed_at": &now})
	_ = s.events.Emit(r.Context(), domain.EventJobCancelled, id, "", "", job.Attempt, nil)
	writeJSON(w, http.StatusOK, map[string]string{"status": "CANCELLED"})
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request) {
	workers, err := s.store.ListWorkers(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if workers == nil {
		workers = []*domain.Worker{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"workers": workers})
}

func (s *Server) queueBackends(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"active":     s.queue.Name(),
		"registered": queue.List(),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
