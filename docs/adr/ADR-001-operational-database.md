# ADR-001 Operational Database

## Decision

PostgreSQL is the source of truth for jobs, executions, workers, and the transactional outbox.

## Rationale

Strong consistency, `FOR UPDATE SKIP LOCKED`, JSONB, mature ops. ClickHouse must not hold current state.
