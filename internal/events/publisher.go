package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
	"github.com/ghrushneshr25/chronos-dev/internal/store/postgres"
	"github.com/google/uuid"
)

// Publisher writes domain events to the transactional outbox.
type Publisher struct {
	store       *postgres.Store
	schedulerID string
}

func NewPublisher(store *postgres.Store, schedulerID string) *Publisher {
	return &Publisher{store: store, schedulerID: schedulerID}
}

var _ ports.EventPublisher = (*Publisher)(nil)

func (p *Publisher) Emit(ctx context.Context, eventType domain.EventType, jobID, executionID, workerID string, attempt int, payload any) error {
	if p == nil || p.store == nil {
		return nil
	}
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		raw = b
	}
	ev := domain.Event{
		EventID:      uuid.NewString(),
		EventType:    eventType,
		EventVersion: 1,
		EventTime:    time.Now().UTC(),
		JobID:        jobID,
		ExecutionID:  executionID,
		WorkerID:     workerID,
		SchedulerID:  p.schedulerID,
		Attempt:      attempt,
		Payload:      raw,
	}
	return p.store.InsertOutbox(ctx, nil, ev)
}
