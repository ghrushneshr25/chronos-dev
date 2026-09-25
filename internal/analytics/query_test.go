package analytics_test

import (
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/analytics"
)

func TestParseRangeDefault(t *testing.T) {
	tr, err := analytics.ParseRange("", "", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if tr.To.Sub(tr.From) < 50*time.Minute {
		t.Fatalf("%v", tr)
	}
}

func TestParseRangeExplicit(t *testing.T) {
	from := "2026-01-01T00:00:00Z"
	to := "2026-01-01T01:00:00Z"
	tr, err := analytics.ParseRange(from, to, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if tr.From.Hour() != 0 || tr.To.Hour() != 1 {
		t.Fatalf("%v", tr)
	}
}

func TestParseRangeInvalid(t *testing.T) {
	_, err := analytics.ParseRange("nope", "", time.Hour)
	if err == nil {
		t.Fatal("expected error")
	}
	_, err = analytics.ParseRange("2026-01-01T02:00:00Z", "2026-01-01T01:00:00Z", time.Hour)
	if err == nil {
		t.Fatal("expected to-after-from error")
	}
}
