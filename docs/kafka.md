# Kafka

## Topology

```text
chronos.jobs              # work delivery (queue backend)
chronos.job-events        # JOB_* domain events
chronos.worker-events     # WORKER_* 
chronos.scheduler-events  # SCHEDULER_*
```

## Publication path

```text
Postgres job state + outbox row
        ↓
outbox-publisher
        ↓
Kafka (key = aggregate id)
```

## Replay

```bash
go run ./cmd/event-replay -topic chronos.job-events -to stdout -max 50
go run ./cmd/event-replay -topic chronos.job-events -to clickhouse
```

Uses a non-group reader from the earliest offset. Safe to re-run; ClickHouse dedupes on `event_id`.
