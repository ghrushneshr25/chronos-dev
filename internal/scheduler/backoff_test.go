package scheduler

import (
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

func TestComputeBackoffFixed(t *testing.T) {
	p := domain.RetryPolicy{
		BackoffStrategy: "fixed",
		InitialDelay:    2 * time.Second,
		MaxDelay:        10 * time.Second,
		Jitter:          false,
	}
	d := computeBackoff(p, 1)
	if d != 2*time.Second {
		t.Fatalf("got %v", d)
	}
	d = computeBackoff(p, 5)
	if d != 2*time.Second {
		t.Fatalf("fixed should stay constant, got %v", d)
	}
}

func TestComputeBackoffExponential(t *testing.T) {
	p := domain.RetryPolicy{
		BackoffStrategy: "exponential",
		InitialDelay:    time.Second,
		MaxDelay:        10 * time.Second,
		Multiplier:      2,
		Jitter:          false,
	}
	d1 := computeBackoff(p, 1)
	d2 := computeBackoff(p, 2)
	d3 := computeBackoff(p, 3)
	if d1 != time.Second {
		t.Fatalf("attempt1=%v", d1)
	}
	if d2 != 2*time.Second {
		t.Fatalf("attempt2=%v", d2)
	}
	if d3 != 4*time.Second {
		t.Fatalf("attempt3=%v", d3)
	}
}

func TestComputeBackoffMaxDelay(t *testing.T) {
	p := domain.RetryPolicy{
		BackoffStrategy: "exponential",
		InitialDelay:    time.Second,
		MaxDelay:        3 * time.Second,
		Multiplier:      10,
		Jitter:          false,
	}
	d := computeBackoff(p, 5)
	if d != 3*time.Second {
		t.Fatalf("expected cap at max, got %v", d)
	}
}

func TestComputeBackoffJitterRange(t *testing.T) {
	p := domain.RetryPolicy{
		BackoffStrategy: "fixed",
		InitialDelay:    100 * time.Millisecond,
		Jitter:          true,
	}
	seen := map[time.Duration]bool{}
	for i := 0; i < 30; i++ {
		d := computeBackoff(p, 1)
		if d < 50*time.Millisecond || d > 150*time.Millisecond {
			t.Fatalf("jitter out of range: %v", d)
		}
		seen[d] = true
	}
	if len(seen) < 2 {
		t.Fatal("expected jitter variation")
	}
}
