# Chronos

**Distributed job scheduling & analytics platform** built in Go.

[![Go](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Kafka](https://img.shields.io/badge/Kafka-event%20bus-231F20?logo=apachekafka&logoColor=white)](docs/kafka.md)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-OLAP-FFCC01?logo=clickhouse&logoColor=black)](docs/clickhouse.md)

Chronos is a production-oriented control plane for asynchronous work: submit jobs, schedule them (immediate / delayed / one-time / recurring), execute across a worker fleet, recover from failures, and stream every lifecycle event into a Kafka → ClickHouse analytics plane.

```text
                         CHRONOS
  ┌─────────────────────────────────────────────────────────┐
  │                   OPERATIONAL PLANE                      │
  │  API ──► Scheduler ──► Queue ──► Workers (gRPC push)     │
  │              │                                           │
  │         PostgreSQL (source of truth)                     │
  └──────────────┬──────────────────────────────────────────┘
                 │ transactional outbox
                 ▼
  ┌─────────────────────────────────────────────────────────┐
  │                   ANALYTICAL PLANE                       │
  │  Kafka ──► analytics-consumer ──► ClickHouse ──► API/UI  │
  └─────────────────────────────────────────────────────────┘
```

> **Core invariant:** ClickHouse is **never** required for scheduling or execution. If analytics is down, jobs keep running; Kafka buffers events until the pipeline recovers.

---

## Why Chronos exists

A naive “background goroutine” approach loses work on restart, has no central schedule, weak retries, and no historical visibility. Chronos demonstrates a miniature infrastructure platform that combines:

| Concern | How Chronos addresses it |
|--------|---------------------------|
| Durable job state | PostgreSQL as operational source of truth |
| Decoupled execution | Pluggable queue (`kafka` default, `memory` for tests) |
| Distributed workers | Registration, heartbeats, capacity, gRPC **push** assign |
| Reliability | Retries, timeouts, cancel, worker-loss recovery, idempotency keys |
| Event reliability | Transactional **outbox** → Kafka (no silent event loss) |
| Analytics | ClickHouse OLAP: MVs, projections, percentiles, capacity & anomalies |
| Isolation | Ops plane independent of ClickHouse availability |

This is a **portfolio / learning-grade production system**: real failure modes, explicit delivery semantics (**at-least-once**), and documented architecture decisions—not a CRUD demo.

---

## Features

### Scheduling & execution
- **Job types:** `builtin` (`echo`, `sleep`, `fail`, `cpu_burn`) and `webhook` (HTTP callbacks)
- **Schedules:** immediate, delayed, one-time (`run_at`), recurring (**cron**)
- **Priorities:** 1–10 with best-effort preferential dispatch
- **Retries:** fixed / exponential backoff, jitter, max attempts
- **Timeouts:** worker context + scheduler watchdog → `TIMEOUT` / retry
- **Cancellation:** `CANCEL_REQUESTED` → worker cancel (gRPC + heartbeat) → `CANCELLED`
- **Idempotency:** optional `idempotency_key` on create
- **Worker recovery:** stale heartbeats → requeue or fail open executions

### Events & analytics
- Domain events via **outbox** to Kafka topics (`job` / `worker` / `scheduler`)
- **Replay** tool: `cmd/event-replay`
- ClickHouse: raw events + minute/hourly/daily aggregates, projections, bloom indexes, TTL tiers
- Analytics API: throughput, latency (P50–P99), failures, workers, queue wait, capacity, anomalies
- Grafana overview dashboard (Compose analytics profile)

### Engineering
- Go monorepo, gRPC + HTTP (chi), DI via [nexus](https://pkg.go.dev/github.com/ghrushneshr25/nexus)
- Docker Compose (ops by default; scale + analytics profiles)
- Unit + in-process e2e tests; optional Docker smoke tests

---

## Quick start

### Prerequisites
- Go 1.23+
- Docker / Docker Compose
- `make`, `protoc` (optional; generated stubs are committed)

### One-command local stack

```bash
git clone https://github.com/ghrushneshr25/chronos-dev.git
cd chronos-dev
make setup    # go mod tidy + proto
make start    # docker compose up -d --build
make smoke    # submit a sample job
```

| Service | URL / port |
|---------|------------|
| HTTP API | http://localhost:8080 |
| Scheduler gRPC | `:9091` |
| PostgreSQL | `localhost:5432` (`chronos` / `chronos`) |
| Kafka | `localhost:9092` |

**Auth (dev default):**

```http
X-API-Key: dev-api-key-change-me
```

### Submit a job

```bash
curl -sS -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key-change-me' \
  -d '{
    "name": "demo-sleep",
    "type": "builtin",
    "priority": 8,
    "timeout_seconds": 10,
    "payload": { "handler": "sleep", "args": { "duration_ms": 500 } }
  }'
```

**Webhook example:**

```bash
curl -sS -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-api-key-change-me' \
  -d '{
    "name": "notify",
    "type": "webhook",
    "payload": {
      "url": "https://httpbin.org/post",
      "method": "POST",
      "body": { "hello": "chronos" }
    }
  }'
```

**Recurring (cron):**

```json
{
  "name": "hourly-report",
  "type": "builtin",
  "schedule": { "type": "recurring", "cron_expr": "0 * * * *" },
  "payload": { "handler": "echo", "args": { "message": "tick" } }
}
```

### Analytics profile (optional)

```bash
make docker-analytics
# ClickHouse :8123 / :9000  (user/pass: chronos/chronos)
# Grafana    http://localhost:3000  (admin / chronos)
# Prometheus http://localhost:9090
```

```bash
curl -sS 'http://localhost:8080/v1/analytics/throughput' \
  -H 'X-API-Key: dev-api-key-change-me'

curl -sS 'http://localhost:8080/v1/analytics/latency' \
  -H 'X-API-Key: dev-api-key-change-me'

curl -sS 'http://localhost:8080/v1/analytics/capacity' \
  -H 'X-API-Key: dev-api-key-change-me'
```

---

## Architecture

### Operational plane

```text
Client
  │  POST /v1/jobs
  ▼
API  ── persist job + outbox ──► PostgreSQL
  │
  │ enqueue (if due)
  ▼
Queue backend (CHRONOS_QUEUE_BACKEND=kafka|memory)
  │
  ▼
Scheduler (single node)
  │  pick healthy worker (least loaded)
  │  gRPC ExecuteJob  ──────────────────────────► Worker
  │  ◄────────────── ReportResult / Heartbeat ───┘
  ▼
Update job state + outbox events
```

### Analytical plane

```text
PostgreSQL outbox
       │
       ▼
outbox-publisher ──► Kafka (chronos.job-events, …)
       │
       ▼
analytics-consumer ──► ClickHouse (chronos_events + MVs)
       │
       ▼
GET /v1/analytics/*   ·   Grafana
```

### Delivery semantics

Chronos targets **at-least-once** execution. After worker loss, timeout retry, or queue redelivery, a job may run more than once. Use `idempotency_key` and design side effects to be idempotent. Analytics dedupes on `event_id` (ReplacingMergeTree).

---

## HTTP API (summary)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Liveness |
| `GET` | `/readyz` | Readiness (Postgres when configured) |
| `POST` | `/v1/jobs` | Create job |
| `GET` | `/v1/jobs` | List jobs (`?state=&limit=`) |
| `GET` | `/v1/jobs/{id}` | Get job |
| `POST` | `/v1/jobs/{id}/cancel` | Cancel job |
| `GET` | `/v1/workers` | Registered workers |
| `GET` | `/v1/queue/backends` | Active + registered queue backends |
| `GET` | `/v1/analytics/throughput` | Completions / rates |
| `GET` | `/v1/analytics/latency` | P50–P99 execution latency |
| `GET` | `/v1/analytics/failures` | Failure / timeout / retry rates |
| `GET` | `/v1/analytics/workers` | Per-worker throughput |
| `GET` | `/v1/analytics/queue` | Queue wait estimates |
| `GET` | `/v1/analytics/capacity` | Peak load + recommended workers |
| `GET` | `/v1/analytics/anomalies` | Baseline vs current deviations |
| `GET` | `/v1/analytics/scheduler` | Scheduler event counts |

---

## Job lifecycle

```text
CREATED → SCHEDULED → QUEUED → ASSIGNED → RUNNING
                              ↘ COMPLETED | FAILED | TIMEOUT | CANCELLED
FAILED / TIMEOUT → RETRYING → QUEUED (if attempts remain)
```

Cancellation while running: `CANCEL_REQUESTED` → confirmed `CANCELLED`.

Details: [docs/job-lifecycle.md](docs/job-lifecycle.md)

---

## Configuration (selected)

| Variable | Default | Meaning |
|----------|---------|---------|
| `CHRONOS_API_KEYS` | `dev-api-key-change-me` | Comma-separated API keys |
| `CHRONOS_WORKER_TOKENS` | `dev-worker-token-change-me` | Worker auth tokens |
| `CHRONOS_POSTGRES_DSN` | local Postgres DSN | Operational DB |
| `CHRONOS_KAFKA_BROKERS` | `localhost:9092` | Kafka brokers |
| `CHRONOS_QUEUE_BACKEND` | `kafka` | `kafka` \| `memory` |
| `CHRONOS_CLICKHOUSE_DSN` | `clickhouse://chronos:chronos@localhost:9000/chronos` | Analytics DB |
| `CHRONOS_ANALYTICS_DISABLED` | unset | Set `1` to skip CH on API |

See `internal/config` and `docker-compose.yml` for the full set.

---

## Repository layout

```text
cmd/
  api/                  HTTP control plane + analytics routes
  scheduler/            Due-job promotion, assign, recovery, watchdog
  worker/               gRPC WorkerService + builtin/webhook executors
  outbox-publisher/     Postgres outbox → Kafka
  analytics-consumer/   Kafka → ClickHouse
  event-replay/         Replay Kafka topics to stdout or ClickHouse
internal/
  domain/               Jobs, workers, events, schedules
  api/ scheduler/ worker/ queue/ store/ events/ analytics/ app/
proto/chronos/v1/       gRPC contracts
migrations/             PostgreSQL schema
deploy/                 ClickHouse, Prometheus, Grafana
docs/                   Architecture, Kafka, ClickHouse, analytics, ADRs
test/e2e/               In-process + optional Docker e2e
```

---

## Development

```bash
make setup          # deps + proto
make build          # all binaries → bin/
make test           # unit + e2e
make test-unit
make test-e2e
make test-docker-e2e   # CHRONOS_E2E_DOCKER=1, stack must be up
make lint
make logs
make docker-analytics
```

### Pluggable queue

```bash
CHRONOS_QUEUE_BACKEND=kafka    # default
CHRONOS_QUEUE_BACKEND=memory   # tests / no Kafka
```

Named backends are wired with **nexus** (`DeclareNamed`). Extend via `queue.Register("redis", factory)`.

### Event replay

```bash
go run ./cmd/event-replay -topic chronos.job-events -to stdout -max 50
go run ./cmd/event-replay -topic chronos.job-events -to clickhouse
```

---

## Milestone status

| # | Milestone | Status |
|---|-----------|--------|
| 1 | Basic scheduler (API → Scheduler → Worker) | ✅ |
| 2 | Persistent scheduler (PostgreSQL) | ✅ |
| 3 | Distributed workers (register / heartbeat / push) | ✅ |
| 4 | Reliability (retries, timeouts, cancel, recovery, cron) | ✅ |
| 5 | Kafka event platform (outbox, topics, replay) | ✅ |
| 6 | ClickHouse foundation (ingest, dedupe) | ✅ |
| 7 | Advanced ClickHouse (MVs, projections, indexes, TTL) | ✅ |
| 8 | Analytics API + Grafana | ✅ |
| 9 | Distributed ClickHouse | 🔜 |
| 10 | Load / chaos / benchmark report | 🔜 |

---

## Documentation

| Doc | Description |
|-----|-------------|
| [Architecture](docs/architecture.md) | Planes, control flow, invariants |
| [Job lifecycle](docs/job-lifecycle.md) | State machine |
| [Kafka](docs/kafka.md) | Topics, outbox, replay |
| [ClickHouse](docs/clickhouse.md) | Schema role, local access |
| [Analytics](docs/analytics.md) | MVs, API surface, Grafana |
| [ADRs](docs/adr/) | Architecture decision records |

---

## Design principles (portfolio lens)

1. **Separate planes** — operational decisions ≠ historical analysis  
2. **Explicit failure** — define behavior when Kafka / CH / workers die  
3. **At-least-once honesty** — no fake exactly-once for external side effects  
4. **Outbox over hope** — DB commit and event publish stay consistent  
5. **Measure what matters** — latency percentiles, capacity, anomalies  

> Chronos is a distributed job execution platform whose operational state is managed through a transactional control plane and whose high-volume execution history is transformed into a real-time analytical platform using Kafka and ClickHouse.

---

## License

MIT — see [LICENSE](LICENSE) if present; otherwise all rights reserved by the author pending LICENSE addition.

---

## Author

**Ghrushnesh Rathod** · [@ghrushneshr25](https://github.com/ghrushneshr25)

Related: [nexus](https://github.com/ghrushneshr25/nexus) — lightweight Go DI used for Chronos wiring.
