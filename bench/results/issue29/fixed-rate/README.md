# Issue #29 · Fixed-rate mixed load

The trials use a fresh 20,000,000-row synthetic corpus and a release binary
built from `b43938b`. Verdigris has 4 CPUs and 2 GiB, with four query permits
and durable audit enabled. MinIO has 2 CPUs and 2 GiB. The host disk was below
MinIO's write threshold, so the successful run used a 4 GiB tmpfs for the
MinIO volume, charged to MinIO's memory cgroup. Compare these trials with each
other. The older closed-loop disk-backed results use a different workload and
storage fixture.

Each trial schedules the same 60 seconds of arrivals, independent of response
speed. Latency runs from the scheduled arrival through the full HTTP response;
p95 and p99 use nearest rank on successful requests. The generator caps local
in-flight requests at 64 and records a dropped arrival as an error. Trace
responses are profiled and must contain exactly the requested trace. Recent
search uses `last 20m`, so its window advances throughout the shared run. The
six classes include the real dashboard `/v1/metrics` request and real alert
creation plus SQL evaluation. Created benchmark rules are deleted after each
trial.

| Class | Offered/s | Attempts per trial |
| --- | ---: | ---: |
| Heavy aggregate | 28 | 1,680 |
| Recent search | 11 | 660 |
| Trace lookup | 11 | 660 |
| Expensive full-scan `LIMIT 10` | 4 | 240 |
| Dashboard | 0.5 | 30 |
| Alert creation | 0.1 | 6 |

## Results

| Trial | Class | OK | 429 | Other | OK/s | p50 ms | p95 ms | p99 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | Heavy aggregate | 776 | 904 | 0 | 12.93 | 452.37 | 491.64 | 522.06 |
| 1 | Recent search | 660 | 0 | 0 | 11.00 | 77.67 | 116.03 | 150.72 |
| 1 | Trace lookup | 660 | 0 | 0 | 11.00 | 59.48 | 109.72 | 136.04 |
| 1 | Expensive LIMIT | 76 | 164 | 0 | 1.27 | 250.60 | 271.37 | 278.92 |
| 1 | Dashboard | 13 | 17 | 0 | 0.22 | 835.29 | 889.99 | 889.99 |
| 1 | Alert creation | 1 | 5 | 0 | 0.02 | 74.36 | 74.36 | 74.36 |
| 2 | Heavy aggregate | 803 | 877 | 0 | 13.38 | 449.93 | 487.66 | 514.94 |
| 2 | Recent search | 660 | 0 | 0 | 11.00 | 77.36 | 121.07 | 151.62 |
| 2 | Trace lookup | 660 | 0 | 0 | 11.00 | 58.13 | 112.52 | 147.42 |
| 2 | Expensive LIMIT | 77 | 163 | 0 | 1.28 | 243.07 | 267.03 | 277.82 |
| 2 | Dashboard | 7 | 23 | 0 | 0.12 | 853.32 | 871.35 | 871.35 |
| 2 | Alert creation | 1 | 5 | 0 | 0.02 | 54.89 | 54.89 | 54.89 |
| 3 | Heavy aggregate | 796 | 884 | 0 | 13.27 | 450.63 | 497.23 | 522.04 |
| 3 | Recent search | 660 | 0 | 0 | 11.00 | 75.63 | 122.76 | 156.57 |
| 3 | Trace lookup | 660 | 0 | 0 | 11.00 | 58.62 | 109.99 | 151.09 |
| 3 | Expensive LIMIT | 65 | 175 | 0 | 1.08 | 247.56 | 267.52 | 275.49 |
| 3 | Dashboard | 7 | 23 | 0 | 0.12 | 818.10 | 851.45 | 851.45 |
| 3 | Alert creation | 1 | 5 | 0 | 0.02 | 83.15 | 83.15 | 83.15 |

| Trial | Trace admission p50 / p95 (ms) | Trace execution p50 / p95 (ms) | Audit wait p50 / p95 (ms) | VDG sampled memory (MiB) | MinIO sampled memory (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.37 / 69.54 | 21.51 / 35.01 | 4.74 / 10.67 | 471.8 | 1913.9 |
| 2 | 20.19 / 73.47 | 21.33 / 34.49 | 4.80 / 10.48 | 471.7 | 2048.0 |
| 3 | 16.62 / 69.81 | 21.65 / 34.84 | 4.75 / 10.58 | 471.6 | 2048.0 |

Trace achieved **100% success and 109.72–112.52 ms p95** against the local
≥99% / ≤120 ms target. Heavy aggregates completed 12.93–13.38 requests/s at
28 offered/s. There were no driver drops, unexpected HTTP statuses, OOM kills
or container restarts. Memory here is sampled Docker container usage. MinIO
reached its 2 GiB limit and recorded reclaim pressure on tmpfs. Verdigris
stayed below 472 MiB sampled. Memory-event snapshots are from late in the third
trial; container exit state was checked at teardown.

Queueing for admission was the largest measured phase for the slowest 5% of
traces. Median admission in that subset was 80–95 ms across trials. Dashboard
and alert traffic was charged to the shared budget and shed 63 of 90 dashboard
requests and 15 of 18 alert creations. The built-in periodic alert evaluator
also logged admission rejections. Report those classes separately before
changing scheduling. The trace gate passed on this fixture. Keep #29 open for
a disk-backed repeat and alert/dashboard progress under the same offered load.
Keep #85 parked pending [evidence of removable read wait](../../issue83/README.md).

## Reproduction

Build `b43938b` using `bench/Dockerfile.vdg`. Start the pinned MinIO image in
[machine.txt](machine.txt) on a fresh volume using network alias `minio`, create
bucket `verdigris`, and start `vdg serve --port 8080 --table bench --role all`
with [config.toml](config.toml) and `TOKIO_WORKER_THREADS=4`. For the recorded
fixture mount `/data` as `--tmpfs /data:rw,size=4g,uid=65532,gid=65532`.
Set the same CPU and memory caps as above. For disk-backed validation mount a
fresh volume in its place. Use enough free backing space for MinIO to accept
writes.

```sh
# Set VDG_PORT, VDG_CONTAINER and MINIO_CONTAINER to the new fixture's values.
export MIXED_BASE_TS_MS=$(python3 -c 'import time; print((int(time.time()*1000)-200*60_000)//60_000*60_000)')
python3 bench/admission/mixed.py --port "$VDG_PORT" --rows 20000000 \
  --batches 400 --threads 4 --seed-only --out /tmp/mixed-seed.json
# Wait for /v1/metrics to report compaction.pendingFiles == 0.
for trial in 1 2 3; do
  python3 bench/admission/mixed.py --port "$VDG_PORT" --rows 20000000 \
    --reuse --duration 60 --fixed-rates --trial "$trial" \
    --stats-container "$VDG_CONTAINER" --stats-container "$MINIO_CONTAINER" \
    --out "trial-$trial.json"
done
```

The corpus took 132.5 s to ingest and verified exactly 20,000,000 visible rows.
Raw samples are available for [trial 1](trial-1.json.gz), [trial 2](trial-2.json.gz)
and [trial 3](trial-3.json.gz).
