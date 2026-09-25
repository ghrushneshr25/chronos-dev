package domain

import (
	"encoding/json"
	"time"
)

type JobState string

const (
	JobStateCreated         JobState = "CREATED"
	JobStateScheduled       JobState = "SCHEDULED"
	JobStateQueued          JobState = "QUEUED"
	JobStateAssigned        JobState = "ASSIGNED"
	JobStateRunning         JobState = "RUNNING"
	JobStateCompleted       JobState = "COMPLETED"
	JobStateFailed          JobState = "FAILED"
	JobStateTimeout         JobState = "TIMEOUT"
	JobStateRetrying        JobState = "RETRYING"
	JobStateCancelRequested JobState = "CANCEL_REQUESTED"
	JobStateCancelled       JobState = "CANCELLED"
	JobStateExpired         JobState = "EXPIRED"
)

type ScheduleType string

const (
	ScheduleImmediate ScheduleType = "immediate"
	ScheduleDelayed   ScheduleType = "delayed"
	ScheduleOneTime   ScheduleType = "one_time"
	ScheduleRecurring ScheduleType = "recurring"
)

type JobType string

const (
	JobTypeBuiltin  JobType = "builtin"
	JobTypeWebhook  JobType = "webhook"
)

type RetryPolicy struct {
	MaxAttempts     int           `json:"max_attempts"`
	BackoffStrategy string        `json:"backoff_strategy"` // fixed | exponential
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	Multiplier      float64       `json:"multiplier"`
	Jitter          bool          `json:"jitter"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:     3,
		BackoffStrategy: "exponential",
		InitialDelay:    time.Second,
		MaxDelay:        5 * time.Minute,
		Multiplier:      2.0,
		Jitter:          true,
	}
}

type Schedule struct {
	Type       ScheduleType `json:"type"`
	RunAt      *time.Time   `json:"run_at,omitempty"`
	Delay      time.Duration `json:"delay,omitempty"`
	CronExpr   string       `json:"cron_expr,omitempty"`
	Timezone   string       `json:"timezone,omitempty"`
}

type Job struct {
	ID              string          `json:"job_id"`
	Name            string          `json:"name"`
	Type            JobType         `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Priority        int             `json:"priority"`
	State           JobState        `json:"state"`
	Schedule        Schedule        `json:"schedule"`
	RetryPolicy     RetryPolicy     `json:"retry_policy"`
	Timeout         time.Duration   `json:"timeout"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	Attempt         int             `json:"attempt"`
	MaxAttempts     int             `json:"max_attempts"`
	WorkerID        string          `json:"worker_id,omitempty"`
	ExecutionID     string          `json:"execution_id,omitempty"`
	LastError       string          `json:"last_error,omitempty"`
	ScheduledAt     *time.Time      `json:"scheduled_at,omitempty"`
	QueuedAt        *time.Time      `json:"queued_at,omitempty"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
	NextRetryAt     *time.Time      `json:"next_retry_at,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

type CreateJobRequest struct {
	Name           string          `json:"name"`
	Type           JobType         `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Schedule       Schedule        `json:"schedule"`
	RetryPolicy    *RetryPolicy    `json:"retry_policy,omitempty"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type BuiltinPayload struct {
	Handler string          `json:"handler"` // sleep | echo | fail | cpu_burn
	Args    json.RawMessage `json:"args,omitempty"`
}

type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

type ResultReason string

const (
	ReasonSuccess   ResultReason = "success"
	ReasonFailed    ResultReason = "failed"
	ReasonTimeout   ResultReason = "timeout"
	ReasonCancelled ResultReason = "cancelled"
)

type ExecutionResult struct {
	Success   bool            `json:"success"`
	Output    json.RawMessage `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
	Retryable bool            `json:"retryable"`
	Duration  time.Duration   `json:"duration"`
	Reason    ResultReason    `json:"reason,omitempty"`
}
