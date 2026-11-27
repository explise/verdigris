# Issue #21 · Four-CPU disk-backed ingest diagnostic

Ten minutes on a four-vCPU host with a shared release driver, Verdigris at
3 CPU / 4 GiB and MinIO at 1 CPU / 2 GiB. The driver offered 220,000 distinct
rows per second in 10,000-row batches with a 16-request in-flight cap. The
reference lane uses 8 CPU for Verdigris and 4 CPU for MinIO. This run measures
a smaller deployment. [Completed workflow](https://github.com/explise/verdigris/actions/runs/45968362441)

| Metric | Result |
| --- | ---: |
| Scheduled rows / batches | 132,000,000 / 13,200 |
| Acknowledged within 600 seconds | 21,330,000 rows |
| Commit goodput | 35,550 rows/s |
| Total acknowledged after drain | 21,490,000 rows |
| HTTP 200 / driver in-flight cap | 2,149 / 11,051 batches |
| Acknowledgement p50 / p95 | 4.55 / 8.32 seconds |
| Sampled batches sent / scheduled | 101 / 600 |
| Visibility observed / timed out among sent probes | 49 / 52 |
| Visibility p95 over all scheduled samples | Unavailable within the probe deadline |
| Acknowledged batch counts matching before and after crash | 2,149 / 2,149 |

The first minute committed 110,333 rows/s and the last minute 18,667 rows/s.
Compaction advanced through 102 generations and published 4,282 active small
files at the end of the sampling window. The raw ledger reports no generator
deadline misses, HTTP rejections or ambiguous acknowledgements. The requests
that never reached Verdigris had no free slot in the client. This shows the
observed concurrency was occupied by long acknowledgements. It does not
separate manifest lock wait, object writes, or compaction interference.
Neither container was OOM-killed. The only forced process kill was the
intentional recovery check.

The 200,000 rows/s and 1-second visibility gates remain open. First measure
parse and Parquet write, manifest lock wait, manifest commit, and compaction
work under this same diagnostic load. Use those phases to identify the cause
of the declining rate before tuning concurrency. The recovery evidence verifies
counts for each acknowledged batch at both snapshots.

[Raw arrival ledger, storage samples, recovery data, host and config](diagnostic.json.gz)
