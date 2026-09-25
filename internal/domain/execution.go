package domain

import (
	"encoding/json"
	"time"
)

type ExecutionState string

const (
	ExecutionPending   ExecutionState = "PENDING"
	ExecutionRunning   ExecutionState = "RUNNING"
	ExecutionCompleted ExecutionState = "COMPLETED"
	ExecutionFailed    ExecutionState = "FAILED"
	ExecutionTimeout   ExecutionState = "TIMEOUT"
	ExecutionCancelled ExecutionState = "CANCELLED"
)

type JobExecution struct {
	ID          string          `json:"execution_id"`
	JobID       string          `json:"job_id"`
	WorkerID    string          `json:"worker_id"`
	Attempt     int             `json:"attempt"`
	State       ExecutionState  `json:"state"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
	DurationMs  int64           `json:"duration_ms,omitempty"`
	Error       string          `json:"error,omitempty"`
	Output      json.RawMessage `json:"output,omitempty"`
	Retryable   bool            `json:"retryable"`
	CreatedAt   time.Time       `json:"created_at"`
}
