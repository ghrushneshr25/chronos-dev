package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

type Executor struct {
	builtin *BuiltinExecutor
	webhook *WebhookExecutor
}

func NewExecutor(httpTimeout time.Duration) *Executor {
	return &Executor{
		builtin: NewBuiltin(),
		webhook: NewWebhook(httpTimeout),
	}
}

func (e *Executor) Execute(ctx context.Context, jobType domain.JobType, payload []byte) domain.ExecutionResult {
	switch jobType {
	case domain.JobTypeBuiltin:
		return e.builtin.Execute(ctx, payload)
	case domain.JobTypeWebhook:
		return e.webhook.Execute(ctx, payload)
	default:
		return domain.ExecutionResult{
			Success:   false,
			Error:     fmt.Sprintf("unsupported job type %q", jobType),
			Retryable: false,
		}
	}
}
