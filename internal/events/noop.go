package events

import (
	"context"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/ports"
)

type NoopPublisher struct{}

func (NoopPublisher) Emit(context.Context, domain.EventType, string, string, string, int, any) error {
	return nil
}

var _ ports.EventPublisher = NoopPublisher{}
