package domain

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventJobCreated         EventType = "JOB_CREATED"
	EventJobScheduled       EventType = "JOB_SCHEDULED"
	EventJobQueued          EventType = "JOB_QUEUED"
	EventJobAssigned        EventType = "JOB_ASSIGNED"
	EventJobStarted         EventType = "JOB_STARTED"
	EventJobCompleted       EventType = "JOB_COMPLETED"
	EventJobFailed          EventType = "JOB_FAILED"
	EventJobRetryScheduled  EventType = "JOB_RETRY_SCHEDULED"
	EventJobCancelRequested EventType = "JOB_CANCEL_REQUESTED"
	EventJobCancelled       EventType = "JOB_CANCELLED"
	EventJobTimeout         EventType = "JOB_TIMEOUT"
	EventJobExpired         EventType = "JOB_EXPIRED"

	EventWorkerRegistered      EventType = "WORKER_REGISTERED"
	EventWorkerHeartbeat       EventType = "WORKER_HEARTBEAT"
	EventWorkerCapacityChanged EventType = "WORKER_CAPACITY_CHANGED"
	EventWorkerUnavailable     EventType = "WORKER_UNAVAILABLE"
	EventWorkerRecovered       EventType = "WORKER_RECOVERED"
	EventWorkerDeregistered    EventType = "WORKER_DEREGISTERED"

	EventSchedulerStarted        EventType = "SCHEDULER_STARTED"
	EventSchedulerStopped        EventType = "SCHEDULER_STOPPED"
	EventSchedulerLeaderAcquired EventType = "SCHEDULER_LEADER_ACQUIRED"
	EventSchedulerLeaderLost     EventType = "SCHEDULER_LEADER_LOST"
	EventSchedulerRebalanced     EventType = "SCHEDULER_REBALANCED"
)

type Event struct {
	EventID      string          `json:"event_id"`
	EventType    EventType       `json:"event_type"`
	EventVersion int             `json:"event_version"`
	EventTime    time.Time       `json:"event_time"`
	IngestedAt   *time.Time      `json:"ingested_at,omitempty"`
	JobID        string          `json:"job_id,omitempty"`
	ExecutionID  string          `json:"execution_id,omitempty"`
	WorkerID     string          `json:"worker_id,omitempty"`
	SchedulerID  string          `json:"scheduler_id,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	TraceID      string          `json:"trace_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
}
