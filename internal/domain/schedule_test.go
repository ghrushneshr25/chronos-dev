package domain_test

import (
	"testing"
	"time"

	"github.com/ghrushneshr25/chronos-dev/internal/domain"
)

func TestNextRunAtCron(t *testing.T) {
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	s := domain.Schedule{Type: domain.ScheduleRecurring, CronExpr: "0 * * * *"} // top of hour
	next, err := domain.NextRunAt(s, from)
	if err != nil {
		t.Fatal(err)
	}
	if next.Minute() != 0 || next.Before(from) || !next.After(from) {
		t.Fatalf("next=%v", next)
	}
}

func TestValidateScheduleRecurring(t *testing.T) {
	if err := domain.ValidateSchedule(domain.Schedule{Type: domain.ScheduleRecurring}); err == nil {
		t.Fatal("expected error without cron")
	}
	if err := domain.ValidateSchedule(domain.Schedule{Type: domain.ScheduleRecurring, CronExpr: "*/5 * * * *"}); err != nil {
		t.Fatal(err)
	}
}
