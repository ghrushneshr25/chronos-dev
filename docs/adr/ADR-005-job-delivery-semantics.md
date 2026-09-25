# ADR-005 Job Delivery Semantics

## Decision

**At-least-once** execution. Workers may re-run a job after worker loss, timeout retry, or Kafka redelivery.

## Consequences

- Clients should use `idempotency_key` and design side effects to be idempotent.
- Analytics must tolerate duplicate domain events (`event_id` dedupe in ClickHouse).
- We do **not** claim exactly-once for external side effects.
