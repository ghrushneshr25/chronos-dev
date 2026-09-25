# ADR-012 Materialized Views

## Decision

Use ClickHouse MVs to populate:

- `job_minute_stats` (AggregatingMergeTree + TDigest states)
- `worker_minute_stats` / `failure_minute_stats` (SummingMergeTree)
- `job_hourly_stats` / `job_daily_stats` for longer retention

## Rationale

Dashboard and API hot paths should not repeatedly scan 30 days of raw events. Raw table remains source for ad-hoc / replay queries.

## Note

MVs only see rows inserted **after** MV creation. Historical backfill can use `event-replay` or `INSERT SELECT`.
