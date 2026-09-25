package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

type BuiltinExecutor struct{}

func NewBuiltin() *BuiltinExecutor { return &BuiltinExecutor{} }

type sleepArgs struct {
	DurationMs int `json:"duration_ms"`
}

type echoArgs struct {
	Message string `json:"message"`
}

type failArgs struct {
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

type cpuBurnArgs struct {
	DurationMs int `json:"duration_ms"`
}

func (e *BuiltinExecutor) Execute(ctx context.Context, payload json.RawMessage) domain.ExecutionResult {
	start := time.Now()
	var p domain.BuiltinPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return domain.ExecutionResult{
			Success: false, Error: fmt.Sprintf("invalid builtin payload: %v", err),
			Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed,
		}
	}

	switch p.Handler {
	case "sleep":
		var args sleepArgs
		_ = json.Unmarshal(p.Args, &args)
		if args.DurationMs <= 0 {
			args.DurationMs = 100
		}
		t := time.NewTimer(time.Duration(args.DurationMs) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctxResult(ctx, start)
		case <-t.C:
			out, _ := json.Marshal(map[string]any{"slept_ms": args.DurationMs})
			return domain.ExecutionResult{Success: true, Output: out, Duration: time.Since(start), Reason: domain.ReasonSuccess}
		}

	case "echo":
		var args echoArgs
		_ = json.Unmarshal(p.Args, &args)
		out, _ := json.Marshal(map[string]string{"echo": args.Message})
		return domain.ExecutionResult{Success: true, Output: out, Duration: time.Since(start), Reason: domain.ReasonSuccess}

	case "fail":
		var args failArgs
		_ = json.Unmarshal(p.Args, &args)
		if args.Message == "" {
			args.Message = "intentional failure"
		}
		return domain.ExecutionResult{
			Success: false, Error: args.Message, Retryable: args.Retryable,
			Duration: time.Since(start), Reason: domain.ReasonFailed,
		}

	case "cpu_burn":
		var args cpuBurnArgs
		_ = json.Unmarshal(p.Args, &args)
		if args.DurationMs <= 0 {
			args.DurationMs = 50
		}
		deadline := time.Now().Add(time.Duration(args.DurationMs) * time.Millisecond)
		x := 0
		for time.Now().Before(deadline) {
			select {
			case <-ctx.Done():
				return ctxResult(ctx, start)
			default:
				x++
			}
		}
		out, _ := json.Marshal(map[string]any{"iterations": x})
		return domain.ExecutionResult{Success: true, Output: out, Duration: time.Since(start), Reason: domain.ReasonSuccess}

	default:
		return domain.ExecutionResult{
			Success: false, Error: fmt.Sprintf("unknown builtin handler %q", p.Handler),
			Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed,
		}
	}
}

func ctxResult(ctx context.Context, start time.Time) domain.ExecutionResult {
	err := ctx.Err()
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.ExecutionResult{
			Success: false, Error: "timeout", Retryable: true,
			Duration: time.Since(start), Reason: domain.ReasonTimeout,
		}
	}
	return domain.ExecutionResult{
		Success: false, Error: "cancelled", Retryable: false,
		Duration: time.Since(start), Reason: domain.ReasonCancelled,
	}
}
