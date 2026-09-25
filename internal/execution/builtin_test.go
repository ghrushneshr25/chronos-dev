package execution_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/execution"
)

func TestBuiltinEcho(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{
		"handler": "echo",
		"args":    map[string]string{"message": "hi"},
	})
	res := ex.Execute(context.Background(), payload)
	if !res.Success {
		t.Fatalf("expected success: %s", res.Error)
	}
	var out map[string]string
	if err := json.Unmarshal(res.Output, &out); err != nil {
		t.Fatal(err)
	}
	if out["echo"] != "hi" {
		t.Fatalf("got %v", out)
	}
}

func TestBuiltinSleep(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{
		"handler": "sleep",
		"args":    map[string]int{"duration_ms": 50},
	})
	start := time.Now()
	res := ex.Execute(context.Background(), payload)
	if !res.Success {
		t.Fatalf("expected success: %s", res.Error)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("sleep too short")
	}
}

func TestBuiltinSleepCancel(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{
		"handler": "sleep",
		"args":    map[string]int{"duration_ms": 5000},
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	res := ex.Execute(ctx, payload)
	if res.Success {
		t.Fatal("expected cancel failure")
	}
	if res.Reason != "cancelled" {
		t.Fatalf("reason=%s", res.Reason)
	}
}

func TestBuiltinSleepTimeout(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{
		"handler": "sleep",
		"args":    map[string]int{"duration_ms": 5000},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	res := ex.Execute(ctx, payload)
	if res.Success || res.Reason != "timeout" {
		t.Fatalf("expected timeout, got success=%v reason=%s", res.Success, res.Reason)
	}
	if !res.Retryable {
		t.Fatal("timeout should be retryable")
	}
}

func TestBuiltinFail(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{
		"handler": "fail",
		"args":    map[string]any{"message": "boom", "retryable": true},
	})
	res := ex.Execute(context.Background(), payload)
	if res.Success {
		t.Fatal("expected failure")
	}
	if !res.Retryable {
		t.Fatal("expected retryable")
	}
	if res.Error != "boom" {
		t.Fatalf("error=%q", res.Error)
	}
}

func TestBuiltinUnknown(t *testing.T) {
	ex := execution.NewBuiltin()
	payload, _ := json.Marshal(map[string]any{"handler": "nope"})
	res := ex.Execute(context.Background(), payload)
	if res.Success || res.Retryable {
		t.Fatal("unknown handler should hard-fail")
	}
}

func TestBuiltinInvalidJSON(t *testing.T) {
	ex := execution.NewBuiltin()
	res := ex.Execute(context.Background(), []byte(`{`))
	if res.Success {
		t.Fatal("expected failure")
	}
}

func TestExecutorDispatch(t *testing.T) {
	e := execution.NewExecutor(time.Second)
	payload, _ := json.Marshal(map[string]any{
		"handler": "echo",
		"args":    map[string]string{"message": "x"},
	})
	res := e.Execute(context.Background(), "builtin", payload)
	if !res.Success {
		t.Fatal(res.Error)
	}
	res = e.Execute(context.Background(), "unknown", payload)
	if res.Success {
		t.Fatal("expected unsupported type failure")
	}
}
