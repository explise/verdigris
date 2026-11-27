# Issue #86 · Newest-window search and sparse fallback

The dense search found all 200 rows within a fixed two-minute window
at both sizes, meeting the 25/75 ms p95 gates in this screen. The sparse
case forced a second request to the full scope. That increased p95 by
76% at 1M and 10% at 20M. Keep the unconditional two-request strategy
out of serving. The next candidate needs a way to admit the fast path
on evidence of recent matches, or an execution plan that continues
from the first work without paying for a full second request.
[Workflow and source](https://github.com/explise/verdigris/actions/runs/45985680418)

| Case | 1M p50/p95, ms | 20M p50/p95, ms |
| --- | ---: | ---: |
| Dense, full scope | 16.97/19.49 | 92.05/105.63 |
| Dense, newest window | 12.51/13.32 | 21.81/22.76 |
| Sparse, full scope | 19.03/19.53 | 167.35/178.37 |
| Sparse, newest window plus full fallback | 31.65/34.41 | 185.72/196.75 |

Each case has 100 measured requests per arm and diagnostics mode
with the same disk-backed 4 CPU and 6 GiB full deployment budget.
The dense window is anchored at the latest seeded timestamp. It is
independent of the timestamps of the matching rows. The synthetic
sparse case has zero results within the window and 200 in full scope.
The returned rows, order and file/byte quote validated at every stage.
The fallback includes both HTTP requests and both audit acknowledgements.
The four cases recorded 1,100 expected audit acknowledgements per size
with zero failures, timeouts or saturation.

At 20M, the dense search's returned Parquet bytes and ranges fell from
5.32 MB/253 to 1.13 MB/38. The sparse window read an additional
1.30 MB/26 ranges before its full-scope request. Read measurements use
the separate diagnostics-on cohort.

This screens a fixed layout with distinct qualifying timestamps. A
serving candidate needs stable tie behavior, the same approved file
scope and cost contract, one pinned manifest, late-write and overlap
coverage, and repeatability across fixtures with different densities.

[1M raw](one-million.json.gz) · [20M raw](twenty-million.json.gz)
