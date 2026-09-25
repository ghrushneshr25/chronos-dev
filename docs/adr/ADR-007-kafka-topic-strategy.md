# ADR-007 Kafka Topic Strategy

## Topics

| Topic | Purpose | Key |
|-------|---------|-----|
| `chronos.jobs` | Work queue (pluggable backend default) | `job_id` |
| `chronos.job-events` | Job lifecycle domain events | `job_id` / aggregate_id |
| `chronos.worker-events` | Worker lifecycle | `worker_id` |
| `chronos.scheduler-events` | Scheduler control plane | `scheduler_id` |

## Partitioning

Entity events are keyed by entity id so a single partition preserves per-entity order. Global order is not required.

## Retention

Kafka retains independently of ClickHouse so analytics can be replayed (`cmd/event-replay`).
