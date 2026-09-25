# Architecture

## Planes

**Operational plane** — API, Scheduler, Queue, Workers, PostgreSQL.  
**Analytical plane** — Outbox → Kafka → ClickHouse → Analytics API.

If ClickHouse or the analytics consumer dies, jobs keep running. Kafka retains events.

## Control flow

```text
Client
  │  POST /v1/jobs  (API key)
  ▼
API ──persist──► PostgreSQL (jobs + outbox)
  │
  │ enqueue (if due)
  ▼
Queue backend (kafka | memory | …)
  │
  ▼
Scheduler consumer
  │  pick healthy worker (least active jobs)
  │  gRPC ExecuteJob push
  ▼
Worker
  │  builtin | webhook
  │  gRPC ReportResult
  ▼
Scheduler ──update state + outbox──► PostgreSQL
```

## Event reliability

```text
DB write (job state + outbox row)  →  outbox-publisher  →  Kafka topics
```

Topics: `chronos.job-events`, `chronos.worker-events`, `chronos.scheduler-events`, `chronos.jobs` (work queue).

## Delivery semantics

At-least-once execution. Clients should use idempotency keys and idempotent side effects.
