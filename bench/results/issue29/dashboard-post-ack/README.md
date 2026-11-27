# Issue #29 · Post-acknowledgement dashboard replay

**Decision: freshness verified on both arms. Keep the reuse profile in draft.**
The reuse arm met the 120 ms mutating trace target in this run and
recorded 17 query audit acknowledgement timeouts. The run exercises
the same 20M disk-backed fixture, four query permits with two reserved,
250 ms general wait, 1,000 ms scheduled-alert wait and 60-second
retirement grace as the earlier replay. The reuse-enabled arm ran
first. Each arm has three 60-second steady trials, one 60-second
saturated trial with five acknowledged writes, then an exact
freshness check with one concurrent trace request per dashboard sample.
[Workflow and source](https://github.com/explise/verdigris/actions/runs/45984047332)

| Metric | Reuse off | Reuse on |
| --- | ---: | ---: |
| Steady complete dashboards | 19/90 | 90/90 |
| Steady traces completed | 1,980/1,980 | 1,980/1,980 |
| Steady trace p95 across trials | 69.10–71.23 ms | 66.83–68.64 ms |
| Mean steady heavy goodput | 7.74 req/s | 8.06 req/s |
| Steady scheduled alert evaluations | 33/33 | 33/33 |
| Saturated mutating complete dashboards | 7/30 | 10/30 |
| Complete dashboards begun after first ACK | 6 | 4 |
| Saturated mutating traces completed | 660/660 | 660/660 |
| Saturated mutating trace p95 | 72.47 ms | 115.59 ms |
| Saturated mutating heavy goodput | 7.52 req/s | 7.62 req/s |
| Saturated mutating audit acknowledgement timeouts | 0 | 17 |

Both arms resolved every acknowledged write by exact trace lookup.
Every complete saturated dashboard response matched the row bounds
for the writes visible during its request. After the saturated trial,
an additional acknowledged write changed the exact dashboard count
from 20,000,005 to 20,000,006 in both arms. Both returned the new count
after compaction merged two more files. In the reuse arm the
post-acknowledgement request invalidated the warm snapshot and warmed
a new one. Compaction invalidated that snapshot again. There were
zero sampled OOMs, restarts, audit saturation events or persistence
failures, and all uncompleted dashboards shed with HTTP 429.

The 17 timeouts occurred in two bursts in the saturated mutating
window. They are handler acknowledgement deadlines. The counters by
themselves do not establish lost records or identify the cause of
the stall. The earlier reader-grace replay had a 153.96 ms treatment
trace tail with zero audit timeouts. Both conditions must pass on
repeat before recommending the profile for write-heavy deployments.
[Earlier reader-grace replay](../dashboard-grace/README.md)

[Raw reuse-off samples](cache-false.json.gz) ·
[Raw reuse-on samples](cache-true.json.gz)
