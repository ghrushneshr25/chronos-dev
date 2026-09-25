# ADR-011 ClickHouse ORDER BY

## Decision

Primary `ORDER BY (event_type, event_time, event_id)` on `chronos_events`.

Secondary access via **projections**:

- worker + time
- event_type + time (redundant with primary but keeps explicit secondary plan)

## Rationale

Most analytics filter by event type and time range first; worker drill-downs use projection.
