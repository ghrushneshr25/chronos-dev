-- Chronos operational schema (PostgreSQL)
-- Source of truth for current job/worker state. ClickHouse is analytics-only.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS jobs (
    id               UUID PRIMARY KEY,
    name             TEXT NOT NULL,
    type             TEXT NOT NULL CHECK (type IN ('builtin', 'webhook')),
    payload          JSONB NOT NULL DEFAULT '{}',
    priority         INT NOT NULL DEFAULT 5,
    state            TEXT NOT NULL,
    schedule         JSONB NOT NULL DEFAULT '{}',
    retry_policy     JSONB NOT NULL DEFAULT '{}',
    timeout_ms       BIGINT NOT NULL DEFAULT 30000,
    idempotency_key  TEXT,
    attempt          INT NOT NULL DEFAULT 0,
    max_attempts     INT NOT NULL DEFAULT 3,
    worker_id        TEXT,
    execution_id     UUID,
    last_error       TEXT,
    scheduled_at     TIMESTAMPTZ,
    queued_at        TIMESTAMPTZ,
    started_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    next_retry_at    TIMESTAMPTZ,
    metadata         JSONB NOT NULL DEFAULT '{}',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_uidx
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

CREATE INDEX IF NOT EXISTS jobs_state_priority_idx ON jobs (state, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS jobs_scheduled_due_idx ON jobs (state, scheduled_at)
    WHERE state IN ('SCHEDULED', 'RETRYING');
CREATE INDEX IF NOT EXISTS jobs_worker_idx ON jobs (worker_id) WHERE worker_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS job_executions (
    id           UUID PRIMARY KEY,
    job_id       UUID NOT NULL REFERENCES jobs(id),
    worker_id    TEXT NOT NULL,
    attempt      INT NOT NULL,
    state        TEXT NOT NULL,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    duration_ms  BIGINT,
    error        TEXT,
    output       JSONB,
    retryable    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS job_executions_job_idx ON job_executions (job_id, attempt);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    address         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'HEALTHY',
    capacity        INT NOT NULL DEFAULT 1,
    active_jobs     INT NOT NULL DEFAULT 0,
    capabilities    JSONB NOT NULL DEFAULT '{}',
    labels          JSONB NOT NULL DEFAULT '{}',
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata        JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS workers_status_idx ON workers (status, last_heartbeat);

-- Transactional outbox for reliable event publication to Kafka
CREATE TABLE IF NOT EXISTS outbox (
    id            UUID PRIMARY KEY,
    event_type    TEXT NOT NULL,
    aggregate_id  TEXT,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (created_at)
    WHERE published_at IS NULL;
