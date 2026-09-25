package domain

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// NextRunAt computes the next fire time for a schedule after `from` (exclusive for cron).
func NextRunAt(s Schedule, from time.Time) (time.Time, error) {
	switch s.Type {
	case ScheduleImmediate:
		return from, nil
	case ScheduleDelayed:
		d := s.Delay
		if d <= 0 {
			d = 0
		}
		return from.Add(d), nil
	case ScheduleOneTime:
		if s.RunAt == nil {
			return time.Time{}, fmt.Errorf("run_at required")
		}
		return s.RunAt.UTC(), nil
	case ScheduleRecurring:
		if s.CronExpr == "" {
			return time.Time{}, fmt.Errorf("cron_expr required for recurring")
		}
		loc := time.UTC
		if s.Timezone != "" {
			var err error
			loc, err = time.LoadLocation(s.Timezone)
			if err != nil {
				return time.Time{}, fmt.Errorf("timezone: %w", err)
			}
		}
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		sched, err := parser.Parse(s.CronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("cron_expr: %w", err)
		}
		return sched.Next(from.In(loc)).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unknown schedule type %q", s.Type)
	}
}

func ValidateSchedule(s Schedule) error {
	switch s.Type {
	case "", ScheduleImmediate:
		return nil
	case ScheduleDelayed:
		return nil
	case ScheduleOneTime:
		if s.RunAt == nil {
			return fmt.Errorf("run_at required for one_time")
		}
		return nil
	case ScheduleRecurring:
		if s.CronExpr == "" {
			return fmt.Errorf("cron_expr required for recurring")
		}
		_, err := NextRunAt(s, time.Now().UTC())
		return err
	default:
		return fmt.Errorf("unknown schedule type %q", s.Type)
	}
}
