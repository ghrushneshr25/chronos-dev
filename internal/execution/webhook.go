package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

type WebhookExecutor struct {
	client *http.Client
}

func NewWebhook(timeout time.Duration) *WebhookExecutor {
	return &WebhookExecutor{
		client: &http.Client{Timeout: timeout},
	}
}

func (e *WebhookExecutor) Execute(ctx context.Context, payload json.RawMessage) domain.ExecutionResult {
	start := time.Now()
	var p domain.WebhookPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return domain.ExecutionResult{
			Success: false, Error: fmt.Sprintf("invalid webhook payload: %v", err),
			Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed,
		}
	}
	if p.URL == "" {
		return domain.ExecutionResult{Success: false, Error: "webhook url required", Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed}
	}
	if !strings.HasPrefix(p.URL, "http://") && !strings.HasPrefix(p.URL, "https://") {
		return domain.ExecutionResult{Success: false, Error: "webhook url must be http(s)", Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed}
	}
	method := p.Method
	if method == "" {
		method = http.MethodPost
	}

	var body io.Reader
	if len(p.Body) > 0 {
		body = bytes.NewReader(p.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.URL, body)
	if err != nil {
		return domain.ExecutionResult{Success: false, Error: err.Error(), Retryable: false, Duration: time.Since(start), Reason: domain.ReasonFailed}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "chronos-worker/1.0")
	for k, v := range p.Headers {
		if strings.EqualFold(k, "Host") {
			continue
		}
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return domain.ExecutionResult{
				Success: false, Error: "timeout", Retryable: true,
				Duration: time.Since(start), Reason: domain.ReasonTimeout,
			}
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return domain.ExecutionResult{
				Success: false, Error: "cancelled", Retryable: false,
				Duration: time.Since(start), Reason: domain.ReasonCancelled,
			}
		}
		return domain.ExecutionResult{Success: false, Error: err.Error(), Retryable: true, Duration: time.Since(start), Reason: domain.ReasonFailed}
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	out, _ := json.Marshal(map[string]any{
		"status_code": resp.StatusCode,
		"body":        string(respBody),
	})
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return domain.ExecutionResult{Success: true, Output: out, Duration: time.Since(start), Reason: domain.ReasonSuccess}
	}
	retryable := resp.StatusCode >= 500 || resp.StatusCode == 429
	return domain.ExecutionResult{
		Success: false, Output: out,
		Error: fmt.Sprintf("webhook status %d", resp.StatusCode),
		Retryable: retryable, Duration: time.Since(start), Reason: domain.ReasonFailed,
	}
}
