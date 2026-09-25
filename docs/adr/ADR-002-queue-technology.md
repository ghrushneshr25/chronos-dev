# ADR-002 Queue Technology

## Decision

Pluggable `domain.Queue` interface. Default backend: **Kafka** (`chronos.jobs`).  
Also shipped: **memory** (tests/dev). Future: postgres, redis via `queue.Register` + nexus named services.

## Selection

`CHRONOS_QUEUE_BACKEND=kafka|memory`

## Rationale

Kafka already required for the event plane; using it for work delivery reduces moving parts while keeping a clean swap path for backends with stronger priority/visibility semantics.
