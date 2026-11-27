# Issue #29 · Query admission under sustained mixed load

The [disk-backed replay](disk-backed/README.md) missed the trace target and
measured scheduled alert starvation.

See the [fixed-rate follow-up](fixed-rate/README.md) for the evaluated arrival
mix, queue timings, and dashboard and alert outcomes. The trials below are the
original closed-loop baseline.

Image `verdigris-bench:perf81-cfe5146`
(`sha256:861a0f0974ea6fab517059789880a038186be777099fb2b3c68bc380575e4ce2`).
Budget: **vdg 4 CPU / 2 GiB + MinIO 2 CPU / 2 GiB**. Corpus: 20,000,000 rows
ingested once into a fresh MinIO volume; one corpus timestamp is reused for all
three trials.

Offered concurrency: 10 heavy full-scan aggregates, 2 genuinely pruned
window searches, 2 selective trace lookups and 1 expensive full-scan `LIMIT 10`
(which must not use the reserved lane). Each trial is **60 seconds continuous**.
Peak RSS is sampled once per second from `docker stats`.

| Trial | Class | Attempts | 200 | 429 | Offered/s | p50 | p95 | p99 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | Heavy aggregate | 1,711 | 737 | 970 | 28.3 | 480.4 | 509.6 | 523.3 |
| 1 | Pruned cheap search | 681 | 681 | 0 | 11.2 | 176.7 | 202.3 | 216.9 |
| 1 | Trace lookup | 682 | 681 | 0 | 11.3 | 175.6 | 203.3 | 212.0 |
| 1 | Expensive `LIMIT 10` | 233 | 43 | 190 | 3.85 | 266.7 | 274.6 | 277.4 |
| 2 | Heavy aggregate | 1,714 | 753 | 961 | 28.3 | 478.1 | 509.0 | 523.5 |
| 2 | Pruned cheap search | 686 | 686 | 0 | 11.3 | 174.8 | 196.8 | 209.6 |
| 2 | Trace lookup | 685 | 685 | 0 | 11.3 | 174.3 | 199.3 | 209.2 |
| 2 | Expensive `LIMIT 10` | 235 | 49 | 186 | 3.88 | 266.1 | 279.3 | 280.8 |
| 3 | Heavy aggregate | 1,708 | 774 | 934 | 28.2 | 471.8 | 500.9 | 513.2 |
| 3 | Pruned cheap search | 727 | 727 | 0 | 12.0 | 165.1 | 187.1 | 197.6 |
| 3 | Trace lookup | 726 | 726 | 0 | 12.0 | 165.0 | 189.8 | 199.8 |
| 3 | Expensive `LIMIT 10` | 234 | 46 | 188 | 3.87 | 261.1 | 280.8 | 283.6 |

Peak RSS: **483.0 / 447.2 / 447.3 MiB** of 2 GiB (vdg), and 529.2 / 546.9 /
537.8 MiB (MinIO). No OOM kill. Heavy goodput was 737/1711, 753/1714 and
774/1708; the rest were shed with 429 + `Retry-After`. The expensive `LIMIT 10`
was mostly shed (190/233, 186/235, 188/234), proving a small LIMIT alone does not
buy the cheap lane. Selective point lookups stayed at 99.7–100% success.

Trial 1 recorded five 400-class responses during the initial compaction window
(4 heavy, 1 trace); trials 2 and 3 recorded none. Those requests are
counted as failures, not silently dropped.

**Open blocker:** the accepted trace p95 target is ≤120 ms; this profile measured
**189.8–203.3 ms p95** under the heavy offered load. The admission layer preserved
trace success and prevented OOM, but service time under contention remains the
next performance problem. The read-wait evidence is in `bench/results/issue83/`.

The [reserved-permit experiment](permit-split/README.md) restored the trace
latency and success criteria with two slots. Scheduled alerts still starved
and dashboard progress regressed. The treatments remain experimental.

## Check a fixed-rate replay

```sh
python3 -B bench/admission/check-fixed.py --expect-backing disk <result-directory>
```

Use Python 3.11 or newer. The checker accepts raw or gzipped trials, verifies
arrivals and per-class goodput, and recomputes nearest-rank p95 and trace
admission wait from the samples. It reports the configured permit limit,
background alert counter deltas and container teardown state. The Rust
admission tests verify enforcement of the permit limit. Exit 0 means the
mixed-load trace, heavy progress and container health criteria passed. Exit 1
means the trial recorded a no-go. Exit 2 means the artifact could not support
a decision. Include host and backing-storage information when interpreting
its result.

## Reproduction

```sh
# start the 2 GiB profile (unique names, disposable volume), then:
export MIXED_BASE_TS_MS=$(python3 -c 'import time; print((int(time.time()*1000)-200*60_000)//60_000*60_000)')
python3 bench/admission/mixed.py --rows 20000000 --batches 500 --threads 4 \
  --heavy 10 --cheap 2 --trace 2 --expensive 1 --duration 60 \
  --trial trial-1 --stats-container <vdg-container> --stats-container <minio-container> \
  --out trial-1.json
python3 bench/admission/mixed.py --rows 20000000 --reuse ... --trial trial-2 ...
python3 bench/admission/mixed.py --rows 20000000 --reuse ... --trial trial-3 ...
```

The profile config uses `max_concurrent = 4`, `admission_wait_ms = 250`,
`cheap_scan_mib = 100`; the cheap class is a `last 20m` window over one compacted
file, not a full-scan LIMIT.
