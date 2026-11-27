# Issue #29 · Disk-backed admission replay

Three 60-second fixed-rate trials ran on one settled 20M-row MinIO volume.
The fixture used the same four Verdigris permits and per-container CPU and
memory limits as the RAM-backed trial. The AMD EPYC runner had four logical
CPUs available to the deployment. The RAM-backed laptop used a different
host and storage medium, so the two runs cannot isolate the effect of disk.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45960535098).

## Results

| Trial | Class | Offered/s | OK | 429 | Other | OK/s | p50 ms | p95 ms | p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Heavy aggregate | 28.0 | 479 | 1201 | 0 | 7.98 | 588.01 | 685.62 | 808.53 |
| 1 | Recent search | 11.0 | 464 | 196 | 0 | 7.73 | 1046.00 | 1106.54 | 1266.96 |
| 1 | Trace lookup | 11.0 | 408 | 252 | 0 | 6.80 | 1016.73 | 1093.62 | 1168.93 |
| 1 | Expensive LIMIT | 4.0 | 51 | 189 | 0 | 0.85 | 257.72 | 396.98 | 487.42 |
| 1 | Dashboard | 0.5 | 9 | 21 | 0 | 0.15 | 1147.55 | 1221.34 | 1221.34 |
| 1 | Alert creation | 0.1 | 1 | 5 | 0 | 0.02 | 71.50 | 71.50 | 71.50 |
| 2 | Heavy aggregate | 28.0 | 469 | 1211 | 0 | 7.82 | 589.51 | 714.46 | 793.73 |
| 2 | Recent search | 11.0 | 477 | 183 | 0 | 7.95 | 1045.99 | 1113.12 | 1226.14 |
| 2 | Trace lookup | 11.0 | 389 | 271 | 0 | 6.48 | 1015.42 | 1079.51 | 1197.97 |
| 2 | Expensive LIMIT | 4.0 | 48 | 192 | 0 | 0.80 | 261.85 | 350.23 | 478.88 |
| 2 | Dashboard | 0.5 | 9 | 21 | 0 | 0.15 | 1154.14 | 1266.62 | 1266.62 |
| 2 | Alert creation | 0.1 | 1 | 5 | 0 | 0.02 | 91.84 | 91.84 | 91.84 |
| 3 | Heavy aggregate | 28.0 | 489 | 1191 | 0 | 8.15 | 592.06 | 638.41 | 678.32 |
| 3 | Recent search | 11.0 | 534 | 126 | 0 | 8.90 | 1047.75 | 1090.98 | 1109.03 |
| 3 | Trace lookup | 11.0 | 428 | 232 | 0 | 7.13 | 1011.34 | 1064.84 | 1091.15 |
| 3 | Expensive LIMIT | 4.0 | 48 | 192 | 0 | 0.80 | 263.03 | 285.07 | 293.51 |
| 3 | Dashboard | 0.5 | 8 | 22 | 0 | 0.13 | 1204.28 | 1260.93 | 1260.93 |
| 3 | Alert creation | 0.1 | 1 | 5 | 0 | 0.02 | 78.88 | 78.88 | 78.88 |

| Trial | Trace success | Admission p95 ms | Execution p95 ms | Audit wait p95 ms | Scheduled alerts completed / shed |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 61.8% | 998.60 | 51.38 | 79.31 | 0 / 12 |
| 2 | 58.9% | 998.54 | 52.34 | 74.90 | 0 / 12 |
| 3 | 64.8% | 998.06 | 48.62 | 23.22 | 0 / 12 |

The trace gate failed all three trials. Heavy work continued at **7.82–8.15/s**
against 28/s offered. Scheduled alerts completed **0 of 36** evaluations and
all 36 hit the admission limit. Alert creation completed separately in 3 of 18
requests. Dashboards completed 26 of 90 HTTP requests. The raw artifact shows
zero client drops and unexpected statuses. Neither container was OOM-killed or
restarted. Sampled container memory peaked at 486.6 MiB for Verdigris and
485.2 MiB for MinIO.

Trace admission p95 was **998.06–998.60 ms** against a one-second reserved-lane
queue timeout. Execution p95 was **48.62–52.34 ms**. Investigate wait as well as
permit occupancy. The handler holds the query permit through the durable audit
acknowledgement. Moving that wait outside the permit is a candidate only if
concurrent pending responses retain their own bounded memory accounting and
response durability is preserved. Compare any scheduling change against these
same arrival rates and report heavy work and scheduled alert progress.

The reproducible command is `VDG_IMAGE=<release-image> OUT=<new-dir>
bench/admission/run-fixed.sh`. See [machine.txt](machine.txt) for the exact
source, images, and container IDs. Raw [trial 1](trial-1.json.gz),
[trial 2](trial-2.json.gz), and [trial 3](trial-3.json.gz) include every arrival.
