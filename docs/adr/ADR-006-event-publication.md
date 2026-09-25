# ADR-006 Event Publication Strategy

## Decision

Transactional outbox in PostgreSQL. `outbox-publisher` drains to Kafka.

## Rationale

Avoids “DB committed, event lost”. Analytics may lag; ops never blocks on Kafka publish path for the critical write (outbox insert is local).
