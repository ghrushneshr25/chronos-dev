package execution_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/execution"
)

func TestWebhookSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ex := execution.NewWebhook(2 * time.Second)
	payload, _ := json.Marshal(map[string]any{
		"url":    srv.URL,
		"method": "POST",
		"body":   map[string]string{"a": "b"},
	})
	res := ex.Execute(context.Background(), payload)
	if !res.Success {
		t.Fatalf("expected success: %s", res.Error)
	}
}

func TestWebhookServerErrorRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	ex := execution.NewWebhook(2 * time.Second)
	payload, _ := json.Marshal(map[string]any{"url": srv.URL})
	res := ex.Execute(context.Background(), payload)
	if res.Success {
		t.Fatal("expected failure")
	}
	if !res.Retryable {
		t.Fatal("5xx should be retryable")
	}
}

func TestWebhookClientErrorNotRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	ex := execution.NewWebhook(2 * time.Second)
	payload, _ := json.Marshal(map[string]any{"url": srv.URL})
	res := ex.Execute(context.Background(), payload)
	if res.Success || res.Retryable {
		t.Fatal("4xx should not be retryable")
	}
}

func TestWebhookRejectsNonHTTP(t *testing.T) {
	ex := execution.NewWebhook(time.Second)
	payload, _ := json.Marshal(map[string]any{"url": "ftp://example.com"})
	res := ex.Execute(context.Background(), payload)
	if res.Success {
		t.Fatal("expected reject")
	}
}

func TestWebhookMissingURL(t *testing.T) {
	ex := execution.NewWebhook(time.Second)
	payload, _ := json.Marshal(map[string]any{"method": "GET"})
	res := ex.Execute(context.Background(), payload)
	if res.Success {
		t.Fatal("expected missing url failure")
	}
}
