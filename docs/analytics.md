# Analytics Platform (M7–M8)

## ClickHouse physical design

| Object | Engine | TTL | Purpose |
|--------|--------|-----|---------|
| `chronos_events` | ReplacingMergeTree | 30d | Raw domain events + bloom indexes + projections |
| `job_minute_stats` | AggregatingMergeTree | 90d | Hot throughput / latency digests |
| `worker_minute_stats` | SummingMergeTree | 90d | Per-worker counters |
| `failure_minute_stats` | SummingMergeTree | 90d | Fail/timeout/retry/cancel |
| `job_hourly_stats` | SummingMergeTree | 1y | Historical rollup |
| `job_daily_stats` | SummingMergeTree | long | Capacity trends |

### Projections (on raw events)

- `proj_worker_time` — `(worker_id, event_time)`
- `proj_type_time` — `(event_type, event_time)`

### Skipping indexes

- bloom: `job_id`, `trace_id`, `execution_id`, `worker_id`
- set: `event_type`

## Analytics API

All require `X-API-Key`. Optional query params: `from`, `to` (RFC3339).

| Endpoint | Description |
|----------|-------------|
| `GET /v1/analytics/throughput?grain=minute\|hour` | Completions / failures / create rate |
| `GET /v1/analytics/latency` | P50/P90/P95/P99 execution duration |
| `GET /v1/analytics/failures` | Rates + series |
| `GET /v1/analytics/retries` | Alias of failures (includes retry_rate) |
| `GET /v1/analytics/workers` | Per-worker throughput share |
| `GET /v1/analytics/queue` | Queue wait from QUEUED→STARTED |
| `GET /v1/analytics/scheduler` | Scheduler event counts |
| `GET /v1/analytics/capacity?target_per_worker=30` | Peak/min + recommended workers |
| `GET /v1/analytics/anomalies?window=15m` | Statistical threshold anomalies |

If ClickHouse is down, these return **503**; job scheduling continues.

## Grafana

```bash
make docker-analytics
# http://localhost:3000  admin / chronos
# Dashboard: Chronos / Chronos Overview
```

## Invariant

ClickHouse is never required for the operational plane.
