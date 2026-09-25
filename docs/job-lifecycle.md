# Job lifecycle

```text
CREATED → SCHEDULED → QUEUED → ASSIGNED → RUNNING
                              ↘ COMPLETED | FAILED | TIMEOUT | CANCELLED
FAILED/TIMEOUT → RETRYING → QUEUED (if attempts remain)
```

Cancellation: `CANCEL_REQUESTED` while running, then `CANCELLED` when confirmed.
