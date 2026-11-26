# Issue #72 — durable audit with the #70 workload

#70 traded write-through audit for latency. #72 restores durability for a
healthy store: the handler enqueues a record, the writer batches queued records
into one conditional append, and every waiter is acknowledged before the response
is released. A saturated queue falls back to a direct CAS append; persistence
errors stay fail-open but increment visible counters.

Image: `verdigris-bench:issue72`
`sha256:e97e30c1f7fcae3eaeeb79fb00057c5f68ec4f9626dd972373794650c82f5194`

## #70 trace workload, audit enabled

Three fresh deployments, 1M rows, 10 warmups + 100 measured, 4 CPU/3 GiB vdg +
4 CPU/3 GiB MinIO:

| Run | Trace p50 | Trace p95 | Audit wait p50 | Search p50 | Aggregate p50 |
|---|---:|---:|---:|---:|---:|
| 1 | 17.69 | 20.07 | 4.0 ms | 23.73 | 25.65 |
| 2 | 17.77 | 19.97 | 4.0 ms | 24.29 | 25.61 |
| 3 | 17.95 | 20.64 | 4.0 ms | 24.32 | 25.88 |

Raw JSON, image metadata, machine info and logs are in `audit-1..3/`.

## Tradeoff

The #70 target was ≤15 ms p50 / ≤20 ms p95 on the rows-only path. With durable
audit enabled, p50 is **17.7–18.0 ms** and p95 is **20.0–20.6 ms**. The
regression is the 4 ms median audit acknowledgement, measured separately in
`trace_phase_ms.audit`; engine work is unchanged. p95 is at the target boundary.
This is the cost of guaranteeing that a completed query is immediately visible
from a second replica and after restart.

## Behavior

- Healthy store: record is persisted before response (batched conditional append).
- Full queue: bounded wait, then direct CAS append; `audit.saturatedTotal`.
- Worker/store failure: fail-open response, `audit.failedTotal`.
- Handler acknowledgement deadline: `audit.timedOutTotal`; record may still land.
- `/v1/metrics.audit` exposes queue depth, saturation, failures, timeouts,
  acknowledgements and last wait.
- SIGINT/SIGTERM stop the HTTP server and drain the queue for up to 5 s.
