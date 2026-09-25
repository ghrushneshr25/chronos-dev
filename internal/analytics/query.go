package analytics

import (
	"context"
	"fmt"
	"math"
	"time"
)

func cleanF(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

type TimeRange struct {
	From time.Time
	To   time.Time
}

func ParseRange(from, to string, defaultWindow time.Duration) (TimeRange, error) {
	now := time.Now().UTC()
	tr := TimeRange{To: now, From: now.Add(-defaultWindow)}
	if from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			return tr, fmt.Errorf("invalid from: %w", err)
		}
		tr.From = t.UTC()
	}
	if to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			return tr, fmt.Errorf("invalid to: %w", err)
		}
		tr.To = t.UTC()
	}
	if !tr.To.After(tr.From) {
		return tr, fmt.Errorf("to must be after from")
	}
	return tr, nil
}

type ThroughputPoint struct {
	Bucket    time.Time `json:"bucket"`
	Completed uint64    `json:"completed"`
	Failed    uint64    `json:"failed"`
	Created   uint64    `json:"created"`
	RatePerSec float64  `json:"rate_per_sec"`
}

type ThroughputResult struct {
	Range  TimeRange         `json:"range"`
	Grain  string            `json:"grain"`
	Points []ThroughputPoint `json:"points"`
	Totals struct {
		Completed uint64  `json:"completed"`
		Failed    uint64  `json:"failed"`
		Created   uint64  `json:"created"`
		AvgRate   float64 `json:"avg_rate_per_sec"`
	} `json:"totals"`
}

type LatencyResult struct {
	Range TimeRange `json:"range"`
	P50   float64   `json:"p50_ms"`
	P90   float64   `json:"p90_ms"`
	P95   float64   `json:"p95_ms"`
	P99   float64   `json:"p99_ms"`
	Avg   float64   `json:"avg_ms"`
	Count uint64    `json:"count"`
	Series []struct {
		Bucket time.Time `json:"bucket"`
		P50    float64   `json:"p50_ms"`
		P95    float64   `json:"p95_ms"`
		P99    float64   `json:"p99_ms"`
		Count  uint64    `json:"count"`
	} `json:"series"`
}

type FailureResult struct {
	Range TimeRange `json:"range"`
	ByType map[string]uint64 `json:"by_type"`
	SuccessRate float64 `json:"success_rate"`
	FailureRate float64 `json:"failure_rate"`
	RetryRate   float64 `json:"retry_rate"`
	TimeoutRate float64 `json:"timeout_rate"`
	CancelRate  float64 `json:"cancel_rate"`
	Series []struct {
		Bucket time.Time `json:"bucket"`
		Failed uint64    `json:"failed"`
		Timeout uint64   `json:"timeout"`
		Retry  uint64    `json:"retry"`
	} `json:"series"`
}

type WorkerStats struct {
	WorkerID   string  `json:"worker_id"`
	Completed  uint64  `json:"completed"`
	Failed     uint64  `json:"failed"`
	AvgMs      float64 `json:"avg_duration_ms"`
	Share      float64 `json:"throughput_share"`
}

type WorkersResult struct {
	Range   TimeRange     `json:"range"`
	Workers []WorkerStats `json:"workers"`
}

type QueueResult struct {
	Range TimeRange `json:"range"`
	// Approximate from event timings: QUEUED → STARTED delta when both exist
	AvgWaitMs float64 `json:"avg_queue_wait_ms"`
	P95WaitMs float64 `json:"p95_queue_wait_ms"`
	Queued    uint64  `json:"queued_events"`
	Started   uint64  `json:"started_events"`
	DepthEstimate float64 `json:"depth_estimate"` // queued - started in window (rough)
}

type CapacityResult struct {
	Range              TimeRange `json:"range"`
	Completed          uint64    `json:"completed"`
	PeakPerMinute      uint64    `json:"peak_completed_per_minute"`
	AvgPerMinute       float64   `json:"avg_completed_per_minute"`
	ActiveWorkersHint  int       `json:"active_workers_hint"`
	// Required workers to keep peak under target jobs/min per worker
	RecommendedWorkers int     `json:"recommended_workers"`
	TargetPerWorker    float64 `json:"target_jobs_per_worker_per_min"`
	Notes              string  `json:"notes"`
}

type Anomaly struct {
	Metric     string  `json:"metric"`
	Baseline   float64 `json:"baseline"`
	Current    float64 `json:"current"`
	Ratio      float64 `json:"ratio"`
	Severity   string  `json:"severity"` // info | warn | critical
	Message    string  `json:"message"`
}

type AnomaliesResult struct {
	Window    TimeRange `json:"window"`
	Baseline  TimeRange `json:"baseline_window"`
	Anomalies []Anomaly `json:"anomalies"`
}

func (w *ClickHouseWriter) Throughput(ctx context.Context, tr TimeRange, grain string) (ThroughputResult, error) {
	out := ThroughputResult{Range: tr, Grain: grain, Points: []ThroughputPoint{}}
	bucketExpr := "toStartOfMinute(event_time)"
	sec := 60.0
	if grain == "hour" {
		bucketExpr = "toStartOfHour(event_time)"
		sec = 3600.0
	}
	q := fmt.Sprintf(`
SELECT
  %s AS bucket,
  countIf(event_type = 'JOB_COMPLETED') AS completed,
  countIf(event_type = 'JOB_FAILED') AS failed,
  countIf(event_type = 'JOB_CREATED') AS created
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
GROUP BY bucket
ORDER BY bucket
`, bucketExpr)

	rows, err := w.conn.Query(ctx, q, tr.From, tr.To)
	if err != nil {
		return out, err
	}
	defer rows.Close()

	var totalC, totalF, totalCr uint64
	for rows.Next() {
		var p ThroughputPoint
		if err := rows.Scan(&p.Bucket, &p.Completed, &p.Failed, &p.Created); err != nil {
			return out, err
		}
		p.RatePerSec = float64(p.Completed) / sec
		out.Points = append(out.Points, p)
		totalC += p.Completed
		totalF += p.Failed
		totalCr += p.Created
	}
	out.Totals.Completed = totalC
	out.Totals.Failed = totalF
	out.Totals.Created = totalCr
	dur := tr.To.Sub(tr.From).Seconds()
	if dur > 0 {
		out.Totals.AvgRate = float64(totalC) / dur
	}
	return out, rows.Err()
}

func (w *ClickHouseWriter) Latency(ctx context.Context, tr TimeRange) (LatencyResult, error) {
	out := LatencyResult{Range: tr}
	err := w.conn.QueryRow(ctx, `
SELECT
  toFloat64(ifNull(quantileTDigest(0.5)(d), 0)),
  toFloat64(ifNull(quantileTDigest(0.9)(d), 0)),
  toFloat64(ifNull(quantileTDigest(0.95)(d), 0)),
  toFloat64(ifNull(quantileTDigest(0.99)(d), 0)),
  toFloat64(ifNull(avg(d), 0)),
  count()
FROM (
  SELECT toFloat64(JSONExtractUInt(payload, 'duration_ms')) AS d
  FROM chronos_events FINAL
  WHERE event_type = 'JOB_COMPLETED'
    AND event_time >= ? AND event_time < ?
    AND JSONExtractUInt(payload, 'duration_ms') > 0
)
`, tr.From, tr.To).Scan(&out.P50, &out.P90, &out.P95, &out.P99, &out.Avg, &out.Count)
	if err != nil {
		return out, err
	}
	out.P50, out.P90, out.P95, out.P99, out.Avg = cleanF(out.P50), cleanF(out.P90), cleanF(out.P95), cleanF(out.P99), cleanF(out.Avg)

	rows, err := w.conn.Query(ctx, `
SELECT
  toStartOfMinute(event_time) AS bucket,
  toFloat64(quantileTDigest(0.5)(toFloat64(JSONExtractUInt(payload, 'duration_ms')))),
  toFloat64(quantileTDigest(0.95)(toFloat64(JSONExtractUInt(payload, 'duration_ms')))),
  toFloat64(quantileTDigest(0.99)(toFloat64(JSONExtractUInt(payload, 'duration_ms')))),
  count()
FROM chronos_events FINAL
WHERE event_type = 'JOB_COMPLETED'
  AND event_time >= ? AND event_time < ?
  AND JSONExtractUInt(payload, 'duration_ms') > 0
GROUP BY bucket
ORDER BY bucket
`, tr.From, tr.To)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var b time.Time
		var p50, p95, p99 float64
		var c uint64
		if err := rows.Scan(&b, &p50, &p95, &p99, &c); err != nil {
			return out, err
		}
		out.Series = append(out.Series, struct {
			Bucket time.Time `json:"bucket"`
			P50    float64   `json:"p50_ms"`
			P95    float64   `json:"p95_ms"`
			P99    float64   `json:"p99_ms"`
			Count  uint64    `json:"count"`
		}{b, cleanF(p50), cleanF(p95), cleanF(p99), c})
	}
	return out, rows.Err()
}

func (w *ClickHouseWriter) Failures(ctx context.Context, tr TimeRange) (FailureResult, error) {
	out := FailureResult{Range: tr, ByType: map[string]uint64{}}
	rows, err := w.conn.Query(ctx, `
SELECT event_type, count()
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
  AND event_type IN ('JOB_COMPLETED','JOB_FAILED','JOB_TIMEOUT','JOB_CANCELLED','JOB_RETRY_SCHEDULED')
GROUP BY event_type
`, tr.From, tr.To)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	var completed, failed, timeout, cancel, retry uint64
	for rows.Next() {
		var et string
		var c uint64
		if err := rows.Scan(&et, &c); err != nil {
			return out, err
		}
		out.ByType[et] = c
		switch et {
		case "JOB_COMPLETED":
			completed = c
		case "JOB_FAILED":
			failed = c
		case "JOB_TIMEOUT":
			timeout = c
		case "JOB_CANCELLED":
			cancel = c
		case "JOB_RETRY_SCHEDULED":
			retry = c
		}
	}
	term := float64(completed + failed + timeout + cancel)
	if term > 0 {
		out.SuccessRate = float64(completed) / term
		out.FailureRate = float64(failed) / term
		out.TimeoutRate = float64(timeout) / term
		out.CancelRate = float64(cancel) / term
	}
	attempts := completed + failed + timeout
	if attempts > 0 {
		out.RetryRate = float64(retry) / float64(attempts+retry)
	}

	srows, err := w.conn.Query(ctx, `
SELECT
  toStartOfMinute(event_time) AS bucket,
  countIf(event_type = 'JOB_FAILED'),
  countIf(event_type = 'JOB_TIMEOUT'),
  countIf(event_type = 'JOB_RETRY_SCHEDULED')
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
GROUP BY bucket
ORDER BY bucket
`, tr.From, tr.To)
	if err != nil {
		return out, err
	}
	defer srows.Close()
	for srows.Next() {
		var b time.Time
		var f, to, r uint64
		if err := srows.Scan(&b, &f, &to, &r); err != nil {
			return out, err
		}
		out.Series = append(out.Series, struct {
			Bucket  time.Time `json:"bucket"`
			Failed  uint64    `json:"failed"`
			Timeout uint64    `json:"timeout"`
			Retry   uint64    `json:"retry"`
		}{b, f, to, r})
	}
	return out, srows.Err()
}

func (w *ClickHouseWriter) Workers(ctx context.Context, tr TimeRange) (WorkersResult, error) {
	out := WorkersResult{Range: tr, Workers: []WorkerStats{}}
	rows, err := w.conn.Query(ctx, `
SELECT
  worker_id,
  countIf(event_type = 'JOB_COMPLETED') AS completed,
  countIf(event_type = 'JOB_FAILED') AS failed,
  toFloat64(ifNull(avgIf(toFloat64(JSONExtractUInt(payload, 'duration_ms')), event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0), 0)) AS avg_ms
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
  AND worker_id != ''
GROUP BY worker_id
ORDER BY completed DESC
LIMIT 100
`, tr.From, tr.To)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	var total uint64
	var list []WorkerStats
	for rows.Next() {
		var ws WorkerStats
		if err := rows.Scan(&ws.WorkerID, &ws.Completed, &ws.Failed, &ws.AvgMs); err != nil {
			return out, err
		}
		total += ws.Completed
		list = append(list, ws)
	}
	for i := range list {
		if total > 0 {
			list[i].Share = float64(list[i].Completed) / float64(total)
		}
	}
	out.Workers = list
	return out, rows.Err()
}

func (w *ClickHouseWriter) Queue(ctx context.Context, tr TimeRange) (QueueResult, error) {
	out := QueueResult{Range: tr}
	// Pair QUEUED and STARTED by job_id within window (simple join)
	err := w.conn.QueryRow(ctx, `
SELECT
  toFloat64(ifNull(avg(wait_ms), 0)),
  toFloat64(ifNull(quantileTDigest(0.95)(wait_ms), 0)),
  count()
FROM (
  SELECT
    q.job_id,
    dateDiff('millisecond', q.event_time, s.event_time) AS wait_ms
  FROM (
    SELECT job_id, min(event_time) AS event_time
    FROM chronos_events FINAL
    WHERE event_type = 'JOB_QUEUED' AND event_time >= ? AND event_time < ? AND job_id != ''
    GROUP BY job_id
  ) q
  INNER JOIN (
    SELECT job_id, min(event_time) AS event_time
    FROM chronos_events FINAL
    WHERE event_type = 'JOB_STARTED' AND event_time >= ? AND event_time < ? AND job_id != ''
    GROUP BY job_id
  ) s ON q.job_id = s.job_id
  WHERE s.event_time >= q.event_time
)
`, tr.From, tr.To, tr.From, tr.To).Scan(&out.AvgWaitMs, &out.P95WaitMs, &out.Started)
	if err != nil {
		out.AvgWaitMs = 0
		out.P95WaitMs = 0
	}
	out.AvgWaitMs, out.P95WaitMs = cleanF(out.AvgWaitMs), cleanF(out.P95WaitMs)
	_ = w.conn.QueryRow(ctx, `
SELECT countIf(event_type='JOB_QUEUED'), countIf(event_type='JOB_STARTED')
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
`, tr.From, tr.To).Scan(&out.Queued, &out.Started)
	out.DepthEstimate = math.Max(0, float64(out.Queued)-float64(out.Started))
	return out, nil
}

func (w *ClickHouseWriter) Capacity(ctx context.Context, tr TimeRange, targetPerWorker float64) (CapacityResult, error) {
	if targetPerWorker <= 0 {
		targetPerWorker = 30 // jobs/min/worker default assumption
	}
	out := CapacityResult{Range: tr, TargetPerWorker: targetPerWorker}
	err := w.conn.QueryRow(ctx, `
SELECT
  toUInt64(ifNull(sum(c), 0)),
  toUInt64(ifNull(max(c), 0)),
  toFloat64(ifNull(avg(c), 0))
FROM (
  SELECT toStartOfMinute(event_time) AS m, countIf(event_type='JOB_COMPLETED') AS c
  FROM chronos_events FINAL
  WHERE event_time >= ? AND event_time < ?
  GROUP BY m
)
`, tr.From, tr.To).Scan(&out.Completed, &out.PeakPerMinute, &out.AvgPerMinute)
	if err != nil {
		return out, err
	}
	var workers uint64
	_ = w.conn.QueryRow(ctx, `
SELECT uniqExact(worker_id)
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ? AND worker_id != '' AND event_type = 'JOB_COMPLETED'
`, tr.From, tr.To).Scan(&workers)
	out.ActiveWorkersHint = int(workers)

	if targetPerWorker > 0 {
		out.RecommendedWorkers = int(math.Ceil(float64(out.PeakPerMinute) / targetPerWorker))
		if out.RecommendedWorkers < 1 && out.PeakPerMinute > 0 {
			out.RecommendedWorkers = 1
		}
	}
	out.Notes = "Recommended workers = ceil(peak_completed_per_minute / target_jobs_per_worker_per_min). Tune target from measured worker throughput."
	return out, nil
}

func (w *ClickHouseWriter) Anomalies(ctx context.Context, current, baseline TimeRange) (AnomaliesResult, error) {
	out := AnomaliesResult{Window: current, Baseline: baseline, Anomalies: []Anomaly{}}

	type snap struct {
		p95, failRate, throughput float64
	}
	load := func(tr TimeRange) (snap, error) {
		var s snap
		var completed, failed uint64
		var p95 float64
		err := w.conn.QueryRow(ctx, `
SELECT
  countIf(event_type='JOB_COMPLETED'),
  countIf(event_type='JOB_FAILED'),
  toFloat64(ifNull(quantileTDigest(0.95)(if(event_type='JOB_COMPLETED', toFloat64(JSONExtractUInt(payload,'duration_ms')), NULL)), 0))
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
`, tr.From, tr.To).Scan(&completed, &failed, &p95)
		if err != nil {
			return s, err
		}
		s.p95 = cleanF(p95)
		term := float64(completed + failed)
		if term > 0 {
			s.failRate = float64(failed) / term
		}
		dur := tr.To.Sub(tr.From).Seconds()
		if dur > 0 {
			s.throughput = float64(completed) / dur
		}
		return s, nil
	}

	cur, err := load(current)
	if err != nil {
		return out, err
	}
	base, err := load(baseline)
	if err != nil {
		return out, err
	}

	check := func(metric string, b, c float64, warn, crit float64, higherBad bool) {
		if b <= 0 && c <= 0 {
			return
		}
		ratio := 0.0
		if b > 0 {
			ratio = c / b
		} else if c > 0 {
			ratio = math.Inf(1)
		}
		sev := ""
		if higherBad {
			if ratio >= crit {
				sev = "critical"
			} else if ratio >= warn {
				sev = "warn"
			}
		} else {
			// drop is bad (throughput)
			if b > 0 && c/b <= 1/crit {
				sev = "critical"
			} else if b > 0 && c/b <= 1/warn {
				sev = "warn"
			}
		}
		if sev == "" {
			return
		}
		out.Anomalies = append(out.Anomalies, Anomaly{
			Metric: metric, Baseline: b, Current: c, Ratio: ratio, Severity: sev,
			Message: fmt.Sprintf("%s baseline=%.2f current=%.2f ratio=%.2f", metric, b, c, ratio),
		})
	}

	check("p95_latency_ms", base.p95, cur.p95, 2.0, 4.0, true)
	check("failure_rate", base.failRate, cur.failRate, 2.0, 5.0, true)
	check("throughput_per_sec", base.throughput, cur.throughput, 2.0, 4.0, false)
	return out, nil
}

// SchedulerEvents returns leader/start/stop counts for the window.
func (w *ClickHouseWriter) Scheduler(ctx context.Context, tr TimeRange) (map[string]any, error) {
	rows, err := w.conn.Query(ctx, `
SELECT event_type, count()
FROM chronos_events FINAL
WHERE event_time >= ? AND event_time < ?
  AND startsWith(event_type, 'SCHEDULER_')
GROUP BY event_type
`, tr.From, tr.To)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	by := map[string]uint64{}
	for rows.Next() {
		var et string
		var c uint64
		if err := rows.Scan(&et, &c); err != nil {
			return nil, err
		}
		by[et] = c
	}
	return map[string]any{"range": tr, "by_type": by}, rows.Err()
}
