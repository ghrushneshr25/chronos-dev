# ADR-003 Scheduler Architecture

## Decision

Single scheduler process for M1–M4. State recovered from PostgreSQL on restart.

## Future

Leader election (Postgres advisory locks or external consensus) without changing the worker protocol.
