CREATE DATABASE IF NOT EXISTS chronos;

-- =============================================================================
-- M6/M7: Raw events — ReplacingMergeTree + projection + skipping indexes + TTL
-- =============================================================================
CREATE TABLE IF NOT EXISTS chronos.chronos_events
(
    event_id UUID,
    event_type LowCardinality(String),
    event_version UInt16,
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    job_id String DEFAULT '',
    execution_id String DEFAULT '',
    worker_id String DEFAULT '',
    scheduler_id String DEFAULT '',
    attempt UInt32 DEFAULT 0,
    trace_id String DEFAULT '',
    payload String DEFAULT '{}',

    INDEX idx_job_id job_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_execution_id execution_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_event_type event_type TYPE set(0) GRANULARITY 4,
    INDEX idx_worker_id worker_id TYPE bloom_filter(0.01) GRANULARITY 1,

    PROJECTION proj_worker_time
    (
        SELECT
            event_id, event_type, event_version, event_time, ingested_at,
            job_id, execution_id, worker_id, scheduler_id, attempt, trace_id, payload
        ORDER BY (worker_id, event_time, event_id)
    ),
    PROJECTION proj_type_time
    (
        SELECT
            event_id, event_type, event_version, event_time, ingested_at,
            job_id, execution_id, worker_id, scheduler_id, attempt, trace_id, payload
        ORDER BY (event_type, event_time, event_id)
    )
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_type, event_time, event_id)
TTL toDateTime(event_time) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;

CREATE VIEW IF NOT EXISTS chronos.chronos_events_final AS
SELECT * FROM chronos.chronos_events FINAL;

-- =============================================================================
-- M7: Minute aggregates (hot path) — 90 day TTL
-- =============================================================================
CREATE TABLE IF NOT EXISTS chronos.job_minute_stats
(
    minute DateTime('UTC'),
    event_type LowCardinality(String),
    cnt UInt64,
    uniq_jobs AggregateFunction(uniq, String),
    duration_sum UInt64,
    duration_count UInt64,
    duration_p50 AggregateFunction(quantileTDigest(0.5), Float64),
    duration_p90 AggregateFunction(quantileTDigest(0.9), Float64),
    duration_p95 AggregateFunction(quantileTDigest(0.95), Float64),
    duration_p99 AggregateFunction(quantileTDigest(0.99), Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, event_type)
TTL minute + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS chronos.job_minute_stats_mv
TO chronos.job_minute_stats
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    event_type,
    count() AS cnt,
    uniqState(job_id) AS uniq_jobs,
    sumIf(JSONExtractUInt(payload, 'duration_ms'), event_type = 'JOB_COMPLETED') AS duration_sum,
    countIf(event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0) AS duration_count,
    quantileTDigestStateIf(0.5)(toFloat64(JSONExtractUInt(payload, 'duration_ms')), event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0) AS duration_p50,
    quantileTDigestStateIf(0.9)(toFloat64(JSONExtractUInt(payload, 'duration_ms')), event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0) AS duration_p90,
    quantileTDigestStateIf(0.95)(toFloat64(JSONExtractUInt(payload, 'duration_ms')), event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0) AS duration_p95,
    quantileTDigestStateIf(0.99)(toFloat64(JSONExtractUInt(payload, 'duration_ms')), event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0) AS duration_p99
FROM chronos.chronos_events
GROUP BY minute, event_type;

CREATE TABLE IF NOT EXISTS chronos.worker_minute_stats
(
    minute DateTime('UTC'),
    worker_id String,
    event_type LowCardinality(String),
    cnt UInt64,
    duration_sum UInt64,
    duration_count UInt64
)
ENGINE = SummingMergeTree((cnt, duration_sum, duration_count))
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, worker_id, event_type)
TTL minute + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS chronos.worker_minute_stats_mv
TO chronos.worker_minute_stats
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    worker_id,
    event_type,
    count() AS cnt,
    sum(if(event_type = 'JOB_COMPLETED', JSONExtractUInt(payload, 'duration_ms'), 0)) AS duration_sum,
    sum(if(event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0, 1, 0)) AS duration_count
FROM chronos.chronos_events
WHERE worker_id != ''
GROUP BY minute, worker_id, event_type;

CREATE TABLE IF NOT EXISTS chronos.failure_minute_stats
(
    minute DateTime('UTC'),
    event_type LowCardinality(String),
    cnt UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, event_type)
TTL minute + INTERVAL 90 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS chronos.failure_minute_stats_mv
TO chronos.failure_minute_stats
AS
SELECT
    toStartOfMinute(event_time) AS minute,
    event_type,
    count() AS cnt
FROM chronos.chronos_events
WHERE event_type IN ('JOB_FAILED', 'JOB_TIMEOUT', 'JOB_CANCELLED', 'JOB_RETRY_SCHEDULED')
GROUP BY minute, event_type;

-- =============================================================================
-- M7: Hourly rollups — 1 year TTL
-- =============================================================================
CREATE TABLE IF NOT EXISTS chronos.job_hourly_stats
(
    hour DateTime('UTC'),
    event_type LowCardinality(String),
    cnt UInt64,
    duration_sum UInt64,
    duration_count UInt64
)
ENGINE = SummingMergeTree((cnt, duration_sum, duration_count))
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, event_type)
TTL hour + INTERVAL 365 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS chronos.job_hourly_stats_mv
TO chronos.job_hourly_stats
AS
SELECT
    toStartOfHour(event_time) AS hour,
    event_type,
    count() AS cnt,
    sum(if(event_type = 'JOB_COMPLETED', JSONExtractUInt(payload, 'duration_ms'), 0)) AS duration_sum,
    sum(if(event_type = 'JOB_COMPLETED' AND JSONExtractUInt(payload, 'duration_ms') > 0, 1, 0)) AS duration_count
FROM chronos.chronos_events
GROUP BY hour, event_type;

-- Daily long-term (no aggressive TTL)
CREATE TABLE IF NOT EXISTS chronos.job_daily_stats
(
    day Date,
    event_type LowCardinality(String),
    cnt UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (day, event_type);

CREATE MATERIALIZED VIEW IF NOT EXISTS chronos.job_daily_stats_mv
TO chronos.job_daily_stats
AS
SELECT
    toDate(event_time) AS day,
    event_type,
    count() AS cnt
FROM chronos.chronos_events
GROUP BY day, event_type;
