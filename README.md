# Chronos

**Module:** [`github.com/ghrushneshr25/chronos-dev`](https://github.com/ghrushneshr25/chronos-dev)

Distributed job scheduling & analytics platform (Go + Kafka + PostgreSQL + ClickHouse).

```text
API → Scheduler → Queue (pluggable) → Workers (gRPC push)
                ↘ Outbox → Kafka → ClickHouse (analytics plane)
```

**Invariant:** ClickHouse is never required for scheduling or execution.

## Quick start

```bash
make setup          # go mod tidy + proto
make start          # docker compose up -d --build
make smoke          # submit a sample job
```

API: `http://localhost:8080`  
Auth header: `X-API-Key: dev-api-key-change-me`

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
    "payload": {"handler": "sleep", "args": {"duration_ms": 500}}
  }'
```

Builtin handlers: `echo`, `sleep`, `fail`, `cpu_burn`  
Webhook: `"type":"webhook","payload":{"url":"http://...","method":"POST","body":{}}`

### Scale / analytics profiles

```bash
docker compose --profile scale up -d          # + worker-2
docker compose --profile analytics up -d      # ClickHouse, consumer, Prometheus, Grafana
```

## Architecture (decisions)

| Concern | Choice |
|--------|--------|
| Ops DB | PostgreSQL |
| Job queue | **Pluggable** — `CHRONOS_QUEUE_BACKEND=kafka\|memory` (extensible) |
| Worker dispatch | gRPC **push** from scheduler |
| Events | Transactional outbox → Kafka |
| DI | [nexus](https://pkg.go.dev/github.com/ghrushneshr25/nexus) |
| Auth | API keys / worker tokens |
| Scheduler HA | Single scheduler (M1–M4) |

See `docs/adr/` for ADRs.

## Layout

```text
cmd/           api, scheduler, worker, outbox-publisher, analytics-consumer
internal/      domain, api, scheduler, worker, queue, store, events, app (nexus wire)
proto/         gRPC WorkerService + SchedulerService
migrations/    PostgreSQL schema
deploy/        ClickHouse, Prometheus, Grafana
docs/          architecture + ADRs
```

## Pluggable queue

```go
// Named backends registered via nexus; switch at runtime:
CHRONOS_QUEUE_BACKEND=kafka   # default
CHRONOS_QUEUE_BACKEND=memory  # tests / no Kafka
```

Register future backends with `queue.Register("redis", factory)` + nexus `DeclareNamed`.

## Milestones

1. Basic scheduler ✅  
2. Persistent scheduler ✅ Postgres  
3. Distributed workers ✅ gRPC register/heartbeat/push  
4. Reliability ✅ retries, timeouts, cancel-running, recurring cron, recovery  
5. Kafka events ✅ outbox → Kafka, replay (`cmd/event-replay`)  
6. ClickHouse foundation ✅ analytics-consumer + ReplacingMergeTree dedupe  
7. Advanced ClickHouse ✅ MVs, aggregates, projections, bloom indexes, TTL tiers  
8. Analytics platform ✅ `/v1/analytics/*` API + Grafana overview dashboard  
9–10. Distributed CH, chaos/benchmarks — next  

### Analytics

```bash
make docker-analytics
# API (when CH up): GET /v1/analytics/throughput|latency|failures|workers|queue|capacity|anomalies
# Grafana: http://localhost:3000  admin / chronos
```


## Docs

- [Architecture](docs/architecture.md)
- [Job lifecycle](docs/job-lifecycle.md)
- [ADRs](docs/adr/)
