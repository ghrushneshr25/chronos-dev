# ADR-009 ClickHouse Schema

## Decision

Primary table: `chronos_events` using **ReplacingMergeTree(ingested_at)** ordered by `(event_id, event_type, event_time)`.

## Rationale

- High-volume append-only event log
- Dedup on `event_id` after merges / `FINAL`
- Monthly partitions + 30-day TTL on raw events
- Operational plane never queries ClickHouse

## Invariant

If ClickHouse is down, Kafka buffers; Chronos scheduling continues.
