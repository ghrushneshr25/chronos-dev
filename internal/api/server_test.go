package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/api"
	"github.com/ghrushneshr25/chronos-dev/internal/auth"
	"github.com/ghrushneshr25/chronos-dev/internal/domain"
	"github.com/ghrushneshr25/chronos-dev/internal/events"
	"github.com/ghrushneshr25/chronos-dev/internal/queue/memory"
	memstore "github.com/ghrushneshr25/chronos-dev/internal/store/memory"
)

func setupAPI(t *testing.T) (http.Handler, *memstore.Store, *memory.Queue) {
	t.Helper()
	store := memstore.New()
	q := memory.New()
	t.Cleanup(func() { _ = q.Close() })
	a := auth.New([]string{"test-key"}, nil, false)
	srv := api.NewServer(store, q, events.NoopPublisher{}, a)
	return srv.Handler(), store, q
}

func TestHealthz(t *testing.T) {
	h, _, _ := setupAPI(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("%d", rr.Code)
	}
}

func TestCreateJobUnauthorized(t *testing.T) {
	h, _, _ := setupAPI(t)
	body := `{"name":"x","type":"builtin","payload":{"handler":"echo","args":{"message":"a"}}}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("%d", rr.Code)
	}
}

func TestCreateJobImmediate(t *testing.T) {
	h, store, _ := setupAPI(t)
	body := `{"name":"echo-job","type":"builtin","priority":8,"payload":{"handler":"echo","args":{"message":"hi"}}}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	var job domain.Job
	if err := json.Unmarshal(rr.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" || job.State != domain.JobStateQueued {
		t.Fatalf("%+v", job)
	}
	got, err := store.GetJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != domain.JobStateQueued {
		t.Fatalf("store state=%s", got.State)
	}
}

func TestCreateJobValidation(t *testing.T) {
	h, _, _ := setupAPI(t)
	cases := []struct {
		name string
		body string
		code int
	}{
		{"missing name", `{"type":"builtin","payload":{}}`, http.StatusBadRequest},
		{"bad type", `{"name":"x","type":"shell","payload":{}}`, http.StatusBadRequest},
		{"bad priority", `{"name":"x","type":"builtin","priority":99,"payload":{}}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-API-Key", "test-key")
			h.ServeHTTP(rr, req)
			if rr.Code != tc.code {
				t.Fatalf("got %d want %d body=%s", rr.Code, tc.code, rr.Body.String())
			}
		})
	}
}

func TestGetAndListJobs(t *testing.T) {
	h, _, _ := setupAPI(t)
	body := `{"name":"list-me","type":"builtin","payload":{"handler":"echo","args":{"message":"z"}}}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	var created domain.Job
	_ = json.Unmarshal(rr.Body.Bytes(), &created)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/jobs/"+created.ID, nil)
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/jobs?limit=10", nil)
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%d", rr.Code)
	}
}

func TestCancelJob(t *testing.T) {
	h, store, _ := setupAPI(t)
	job := &domain.Job{Name: "c", State: domain.JobStateScheduled, Type: domain.JobTypeBuiltin}
	_ = store.CreateJob(context.Background(), job)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateCancelled {
		t.Fatalf("state=%s", got.State)
	}
}

func TestCancelRunningRequests(t *testing.T) {
	h, store, _ := setupAPI(t)
	job := &domain.Job{Name: "r", State: domain.JobStateRunning, Type: domain.JobTypeBuiltin, WorkerID: "w1"}
	_ = store.CreateJob(context.Background(), job)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs/"+job.ID+"/cancel", nil)
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("%d", rr.Code)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.State != domain.JobStateCancelRequested {
		t.Fatalf("state=%s", got.State)
	}
}

func TestCreateDelayedJob(t *testing.T) {
	h, store, _ := setupAPI(t)
	// delay is duration nanoseconds in JSON number for time.Duration
	body := `{"name":"later","type":"builtin","schedule":{"type":"delayed","delay":60000000000},"payload":{"handler":"echo","args":{"message":"x"}}}`
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}
	var job domain.Job
	_ = json.Unmarshal(rr.Body.Bytes(), &job)
	if job.State != domain.JobStateScheduled {
		t.Fatalf("delayed should stay scheduled, got %s", job.State)
	}
	got, _ := store.GetJob(context.Background(), job.ID)
	if got.ScheduledAt == nil || !got.ScheduledAt.After(time.Now().UTC()) {
		t.Fatalf("expected future schedule %+v", got.ScheduledAt)
	}
}

func TestQueueBackends(t *testing.T) {
	h, _, _ := setupAPI(t)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/queue/backends", nil)
	req.Header.Set("X-API-Key", "test-key")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("%d", rr.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body["active"] != "memory" {
		t.Fatalf("%v", body)
	}
}
