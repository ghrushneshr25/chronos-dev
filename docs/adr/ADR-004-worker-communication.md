# ADR-004 Worker Communication

## Decision

Workers expose `WorkerService` (gRPC). Scheduler **pushes** `ExecuteJob`.  
Workers call `SchedulerService` for Register / Heartbeat / ReportResult / Deregister.

## Rationale

Push enables immediate assignment with capacity awareness; gRPC is typed and efficient.
