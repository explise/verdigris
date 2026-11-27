# Issue #29 · Dashboard admission wait

Three fixed-rate 60-second trials per arm used a fresh 20M-row
disk-backed MinIO fixture, four total query permits, two reserved for
cheap queries, a 1,000 ms scheduled-alert wait, and a 50 ms interactive
alert-validation wait. Only dashboard admission wait changed. Both arms ran
on the same four-CPU runner, with independent fixture seeding.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45970679743)

| Metric | 250 ms control | 1,000 ms treatment |
| --- | ---: | ---: |
| Complete dashboard refreshes | 9/90 | 90/90 |
| Dashboard refresh p95 by trial | 957–1,032 ms | 1,154–1,177 ms |
| Trace success | 100% | 100% |
| Trace p95 by trial | 769.11 / 148.71 / 94.99 ms | 303.74 / 121.26 / 307.13 ms |
| Trials passing the trace gate | 1/3 | 0/3 |
| Mean heavy goodput | 7.11 req/s | 5.97 req/s |
| Scheduled evaluations completed | 33/33 | 33/33 |
| Interactive alert validations completed | 3/18 | 3/18 |
| Audit acknowledgement timeouts | 16 | 29 |

There were zero OOM kills, restarts, audit write failures, or audit queue
saturation. Even the control missed two trace latency gates and had audit
timeouts, and the control shows variability across runs. The treatment
completed all dashboard requests at the cost of trace tail latency,
16% less heavy goodput, and additional audit timeouts.

Treatment trace execution p95 was 42–44 ms across trials. Admission p95
ranged from 31 to 216 ms and audit wait p95 from 39 to 80 ms. Control
execution p95 was 41–42 ms, admission p95 7–702 ms, and audit wait p95
34–146 ms. These are separate percentiles. The largest trial-to-trial change
was in admission. Keep the existing dashboard wait and test a
reduction in repeated dashboard scans. The result will need verification while the manifest
changes as well as on the settled fixture.

[Control raw results](wait-250.json.gz) · [Treatment raw results](wait-1000.json.gz)
