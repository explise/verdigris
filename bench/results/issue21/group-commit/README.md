# Issue #21 · Sustained ingest group-commit control

**Decision: keep individual manifest commits in the release path.**
The grouped arm cut ingest manifest CAS observations by 71% and
completed fewer rows within the measured ten minutes. This paired
screen used the same 220k offered rows/s, 10k rows per request, seed,
corpus timestamp and single four-CPU runner. Verdigris had 3 CPU /
4 GiB. Disk-backed MinIO had 1 CPU / 2 GiB. The group cap changed
from 1 to 8 with no fill delay. [Workflow and source](https://github.com/explise/verdigris/actions/runs/45982861055)

| Metric | Individual | Group cap 8 |
| --- | ---: | ---: |
| Acknowledged rows/s during 600 s | 33,983 | 31,883 |
| Rows acknowledged during 600 s | 20,390,000 | 19,130,000 |
| Rows acknowledged after driver drain | 20,550,000 | 19,290,000 |
| Acknowledged batches reconciled before and after restart | 2,055/2,055 | 1,929/1,929 |
| Driver inflight sheds, out of 13,200 planned batches | 11,145 | 11,271 |
| Observed visibility probes, out of 600 scheduled | 48 | 43 |
| Observed probes visible within 1 s | 12 | 30 |
| Ingest manifest CAS observations | 2,055 | 599 |
| Trigram merge time, measured | 234.09 s | 40.43 s |
| Mean ingest lock wait, first / second half | 3.02 / 7.51 s | 2.76 / 8.95 s |
| Measured compaction passes | 97 | 159 |
| Measured elapsed time across compaction passes | 320.50 s | 552.77 s |
| Small files at end of 600 s | 4,094 | 3,834 |

Both arms reconciled the exact ID and count of each acknowledged batch
before and after the deliberate Verdigris restart. There were zero
acknowledged mismatches, unseen or unexplained batches, and OOM kills.
The visibility probes were censored and partially shed at the driver.
Neither arm has a population p95 or satisfies the ≤1 s visibility
gate. The second arm's 30 observed sub-second probes are a diagnostic
count.

The group-cap arm spent less time on each ingest metadata stage and
recorded more time in compaction while holding the shared ingest
commit lock. Mean data PUT time rose from 24.28 to 37.41 ms, and
second-half ingest lock wait rose despite fewer CAS writes.
This is one pair of trials. It supports isolating compaction scheduling
and write pressure before a change to the publication protocol.
Measure rows acknowledged within the timed window and during drain,
compaction work and backlog, exact restart recovery and probe
coverage. Keep the normal Four-CPU diagnostic and 8+4 CPU final
acceptance budgets distinct.

[Raw individual-commit samples](group-1.json.gz) ·
[Raw grouped-commit samples](group-8.json.gz)
