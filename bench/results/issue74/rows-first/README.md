# Issue #74 · Rows-first API and browser measurement

One settled 1M-row corpus ran on Verdigris and disk-backed MinIO at
2 CPU / 3 GiB each. The shipping SPA used Arrow IPC. The API cohorts used
JSON, alternated request order and measured 100 requests per path after
10 warmups. Read counts came from a separate profile cohort with the same
schedule. [Workflow](https://github.com/explise/verdigris/actions/runs/45993822170)

| Path | API p50 / p95 (ms) | Executions | Range batches | Logical ranges | Bytes returned |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rows-only | 16.40 / 19.64 | 1 | 4 | 32 | 1,083,754 |
| Combined, legacy default | 31.74 / 39.64 | 2 | 11 | 46 | 3,314,315 |
| Activity only | 15.90 / 19.93 | 1 | 7 | 14 | 2,230,561 |

The rows-only endpoint met the 30 ms API p95 gate. JSON returned the
same 200 rows in order on both query paths. The activity route returned
only the aggregation and matched the legacy histogram, including a
selected-file activity count above the row page's 200. Read counters
cover the query execution's object-store calls. They do not include
manifest and audit I/O. Range batches and logical ranges are reported
separately from whole-object GETs in the raw samples. All 440 expected
warm API audit acknowledgements arrived, with zero failures, saturation
or timeouts.

Ten Chromium navigations measured a separate **60.15 ms p50 / 73.40 ms
p95** from submitting the query to the first expected Arrow row being
visible. The browser also requested activity independently. Each run
waited for the first expected row and two animation frames. This is a
local hosted-runner measurement with the browser, API and object store
on one machine.

Held, failed, stale and cross-workspace activity and cold confirmation
are covered by the integration browser fixture.
[Web CI](https://github.com/explise/verdigris/actions/runs/45992883735) ·
[Rust CI](https://github.com/explise/verdigris/actions/runs/45992883659)

[Raw samples](samples.json.gz) · [Runner and images](machine.txt)
