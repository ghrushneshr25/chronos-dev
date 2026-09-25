package e2e_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

func httpNewRequest(h *harness, method, path string, body []byte) (*http.Request, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, h.apiURL+path, r)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", h.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

func httpDo(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

func TestE2E_TimeoutThenRetryExhaust(t *testing.T) {
	h := startHarness(t)
	// sleep longer than job timeout → timeout reason → retry until max
	body := `{
		"name":"e2e-timeout",
		"type":"builtin",
		"priority":9,
		"timeout_seconds":1,
		"retry_policy":{"max_attempts":2,"backoff_strategy":"fixed","initial_delay":20000000,"max_delay":100000000,"multiplier":1,"jitter":false},
		"payload":{"handler":"sleep","args":{"duration_ms":5000}}
	}`
	job := h.postJob(t, body)
	done := h.waitState(t, job.ID, domain.JobStateTimeout, 15*time.Second)
	if done.Attempt < 2 {
		t.Fatalf("expected retries before terminal timeout, attempt=%d", done.Attempt)
	}
}

func TestE2E_CancelRunningJob(t *testing.T) {
	h := startHarness(t)
	body := `{
		"name":"e2e-cancel-run",
		"type":"builtin",
		"priority":9,
		"timeout_seconds":30,
		"payload":{"handler":"sleep","args":{"duration_ms":10000}}
	}`
	job := h.postJob(t, body)

	// wait until running
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j := h.getJob(t, job.ID)
		if j.State == domain.JobStateRunning || j.State == domain.JobStateAssigned {
			break
		}
		if j.State == domain.JobStateCompleted {
			t.Fatal("job finished before cancel")
		}
		time.Sleep(20 * time.Millisecond)
	}

	req, _ := httpNewRequest(h, "POST", "/v1/jobs/"+job.ID+"/cancel", nil)
	resp, err := httpDo(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 202 && resp.StatusCode != 200 {
		t.Fatalf("cancel status %d", resp.StatusCode)
	}

	done := h.waitState(t, job.ID, domain.JobStateCancelled, 10*time.Second)
	if done.State != domain.JobStateCancelled {
		t.Fatalf("state=%s", done.State)
	}
}

func TestE2E_ScheduleValidationRecurring(t *testing.T) {
	h := startHarness(t)
	// invalid cron
	body := `{"name":"bad-cron","type":"builtin","schedule":{"type":"recurring","cron_expr":"not a cron"},"payload":{"handler":"echo","args":{"message":"x"}}}`
	req, _ := httpNewRequest(h, "POST", "/v1/jobs", []byte(body))
	resp, err := httpDo(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 got %d", resp.StatusCode)
	}
}
