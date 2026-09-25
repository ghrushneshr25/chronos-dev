package e2e_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

// Opt-in smoke against a running docker compose stack:
//
//	CHRONOS_E2E_DOCKER=1 go test ./test/e2e -run Docker -count=1 -v
func TestDocker_SmokeJobLifecycle(t *testing.T) {
	if os.Getenv("CHRONOS_E2E_DOCKER") == "" {
		t.Skip("set CHRONOS_E2E_DOCKER=1 with stack running (make start)")
	}
	base := envOr("CHRONOS_API_URL", "http://localhost:8080")
	key := envOr("CHRONOS_API_KEYS", "dev-api-key-change-me")
	// if multiple keys CSV, take first
	if i := indexByte(key, ','); i >= 0 {
		key = key[:i]
	}

	client := &http.Client{Timeout: 10 * time.Second}

	// health
	resp, err := client.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz status %d", resp.StatusCode)
	}

	body := `{"name":"docker-smoke","type":"builtin","priority":9,"payload":{"handler":"echo","args":{"message":"docker-e2e"}}}`
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", key)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d %s", resp.StatusCode, raw)
	}
	var created struct {
		ID string `json:"job_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
		t.Fatalf("parse create: %s", raw)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		req, _ = http.NewRequest(http.MethodGet, base+"/v1/jobs/"+created.ID, nil)
		req.Header.Set("X-API-Key", key)
		resp, err = client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		var job struct {
			State string `json:"state"`
		}
		_ = json.Unmarshal(raw, &job)
		if job.State == "COMPLETED" {
			return
		}
		if job.State == "FAILED" || job.State == "CANCELLED" {
			t.Fatalf("unexpected terminal state %s body=%s", job.State, raw)
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for COMPLETED: %s", raw)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
