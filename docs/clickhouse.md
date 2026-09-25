# ClickHouse

## Role

OLAP only. Never the source of truth for current job state.

## Schema

See `deploy/clickhouse/init.sql`.

- Engine: `ReplacingMergeTree(ingested_at)`
- Dedup key: `event_id`
- Query deduped rows: `SELECT ... FROM chronos_events FINAL`

## Consumer

`cmd/analytics-consumer` — Kafka consumer group `chronos-analytics`.

If ClickHouse is unavailable the consumer retries inserts and does not commit offsets. Job execution is unaffected.

## Local

```bash
docker compose --profile analytics up -d
# credentials (compose): chronos / chronos
clickhouse-client --user chronos --password chronos \
  -q "SELECT count(), event_type FROM chronos.chronos_events GROUP BY event_type"
```
