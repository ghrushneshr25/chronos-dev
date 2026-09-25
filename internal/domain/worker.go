package domain

import "time"

type WorkerStatus string

const (
	WorkerStatusHealthy     WorkerStatus = "HEALTHY"
	WorkerStatusSuspected   WorkerStatus = "SUSPECTED"
	WorkerStatusUnavailable WorkerStatus = "UNAVAILABLE"
	WorkerStatusDraining    WorkerStatus = "DRAINING"
)

type WorkerCapabilities struct {
	Builtin bool `json:"builtin"`
	Webhook bool `json:"webhook"`
}

type Worker struct {
	ID           string             `json:"worker_id"`
	Address      string             `json:"address"` // gRPC address host:port
	Status       WorkerStatus       `json:"status"`
	Capacity     int                `json:"capacity"`
	ActiveJobs   int                `json:"active_jobs"`
	Capabilities WorkerCapabilities `json:"capabilities"`
	Labels       map[string]string  `json:"labels,omitempty"`
	LastHeartbeat time.Time         `json:"last_heartbeat"`
	RegisteredAt time.Time          `json:"registered_at"`
	Metadata     map[string]string  `json:"metadata,omitempty"`
}

func (w *Worker) AvailableSlots() int {
	slots := w.Capacity - w.ActiveJobs
	if slots < 0 {
		return 0
	}
	return slots
}

func (w *Worker) IsAssignable() bool {
	return w.Status == WorkerStatusHealthy && w.AvailableSlots() > 0
}
