# Issue #29 · Dashboard reuse with a 60-second retirement grace

Both arms used a disk-backed 20M-row fixture, 2 GiB for the Verdigris
node, four total query permits with two reserved, a 250 ms general wait,
and a 1,000 ms scheduled-alert wait. Each arm ran three 60-second steady
trials and one 60-second trial with five acknowledged single-row writes
at ten-second intervals and background compaction. The arms differed in
dashboard snapshot reuse. [Source and workflow](https://github.com/explise/verdigris/actions/runs/45979622792).

| Metric | Reuse off | Reuse on |
| --- | ---: | ---: |
| Steady complete dashboards | 25/90 | 90/90 |
| Steady completed-dashboard p95 across trials | 1,114.53–1,124.32 ms | 12.59–14.77 ms |
| Steady traces completed | 1,980/1,980 | 1,980/1,980 |
| Steady trace p95 across trials | 73.14–74.77 ms | 71.60–73.13 ms |
| Steady heavy goodput | 6.51 req/s | 6.98 req/s |
| Steady scheduled alert evaluations completed | 33/33 | 33/33 |
| Mutating complete dashboards | 1/30 | 6/30 |
| Mutating complete dashboard requests started after first write acknowledgement | 0 | 0 |
| Mutating traces completed | 660/660 | 660/660 |
| Mutating trace p95 | 75.89 ms | 153.96 ms |
| Mutating heavy goodput | 6.47 req/s | 6.55 req/s |
| Mutating scheduled alert evaluations completed | 12/12 | 12/12 |
| Mutating compaction passes | 5 | 5 |

All uncompleted dashboard requests returned 429. Both arms recovered all
five probe rows by exact trace query immediately after the load interval.
There were zero OOMs, restarts, audit failures, audit timeouts or audit
saturation events. Dashboard count checks covered only requests started
before the first probe acknowledgement. The mutating replay therefore
left post-acknowledgement dashboard freshness unverified and the reuse
arm exceeded the 120 ms trace p95 gate. Keep #91 opt-in and in draft.
One mutating trial per arm gives a no-go for promotion on this evidence
and warrants a controlled repeat before assigning the tail increase to reuse.

For the next check, verify every acknowledged write with an exact
dashboard request under bounded load, record successful observations
after compaction changes active objects, and keep those observations
separate from the fixed-rate saturation test. Require the full mutating
trace gate on repeated fresh trials before recommending the profile for
write-heavy deployments.

[Raw reuse-off samples](cache-false.json.gz) ·
[Raw reuse-on samples](cache-true.json.gz).
