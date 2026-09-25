package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
	"github.com/ghrushneshr25/chronos-dev/internal/api"
	"github.com/ghrushneshr25/chronos-dev/internal/auth"
	"github.com/ghrushneshr25/chronos-dev/internal/events"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
	memstore "github.com/ghrushneshr25/chronos-dev/internal/store/memory"
)

type fakeAnalytics struct{}

func (fakeAnalytics) Throughput(ctx context.Context, tr analytics.TimeRange, grain string) (analytics.ThroughputResult, error) {
	return analytics.ThroughputResult{Grain: grain, Range: tr}, nil
}
func (fakeAnalytics) Latency(ctx context.Context, tr analytics.TimeRange) (analytics.LatencyResult, error) {
	return analytics.LatencyResult{Range: tr, P95: 12}, nil
}
func (fakeAnalytics) Failures(ctx context.Context, tr analytics.TimeRange) (analytics.FailureResult, error) {
	return analytics.FailureResult{Range: tr, SuccessRate: 0.99}, nil
}
func (fakeAnalytics) Workers(ctx context.Context, tr analytics.TimeRange) (analytics.WorkersResult, error) {
	return analytics.WorkersResult{Range: tr}, nil
}
func (fakeAnalytics) Queue(ctx context.Context, tr analytics.TimeRange) (analytics.QueueResult, error) {
	return analytics.QueueResult{Range: tr}, nil
}
func (fakeAnalytics) Capacity(ctx context.Context, tr analytics.TimeRange, target float64) (analytics.CapacityResult, error) {
	return analytics.CapacityResult{Range: tr, RecommendedWorkers: 2, TargetPerWorker: target}, nil
}
func (fakeAnalytics) Anomalies(ctx context.Context, c, b analytics.TimeRange) (analytics.AnomaliesResult, error) {
	return analytics.AnomaliesResult{Window: c, Baseline: b}, nil
}
func (fakeAnalytics) Scheduler(ctx context.Context, tr analytics.TimeRange) (map[string]any, error) {
	return map[string]any{"ok": true}, nil
}
func (fakeAnalytics) Ping(ctx context.Context) error { return nil }

func TestAnalyticsUnavailableWithoutCH(t *testing.T) {
	store := memstore.New()
	q := memory.New()
	t.Cleanup(func() { _ = q.Close() })
	a := auth.New([]string{"k"}, nil, false)
	srv := api.NewServer(store, q, events.NoopPublisher{}, a)
	h := srv.Handler()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/analytics/throughput", nil)
	req.Header.Set("X-API-Key", "k")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", rr.Code)
	}
}

func TestAnalyticsWithFake(t *testing.T) {
	store := memstore.New()
	q := memory.New()
	t.Cleanup(func() { _ = q.Close() })
	a := auth.New([]string{"k"}, nil, false)
	srv := api.NewServer(store, q, events.NoopPublisher{}, a)
	srv.SetAnalytics(fakeAnalytics{})
	h := srv.Handler()

	paths := []string{
		"/v1/analytics/throughput",
		"/v1/analytics/latency",
		"/v1/analytics/failures",
		"/v1/analytics/workers",
		"/v1/analytics/queue",
		"/v1/analytics/capacity",
		"/v1/analytics/anomalies",
		"/v1/analytics/scheduler",
	}
	for _, p := range paths {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, p, nil)
		req.Header.Set("X-API-Key", "k")
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("%s -> %d %s", p, rr.Code, rr.Body.String())
		}
	}
	_ = time.Now
}
