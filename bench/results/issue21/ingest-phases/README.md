# Issue #21 · Ten-minute write-path timing

The four-CPU hosted runner allocated 3 CPU and 4 GiB to Verdigris, and
1 CPU and 2 GiB to disk-backed MinIO. The driver offered 220,000 rows/s
for 600 seconds and shared the host. Phase timing used the injected
monotonic clock. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45973184792)

| Metric | Observed |
| --- | ---: |
| Rows acknowledged within 600 seconds | 17,920,000 |
| Mean acknowledged rate | 29,867 rows/s |
| First / last minute acknowledged rate | 93,000 / 15,333 rows/s |
| Rows acknowledged after drain | 18,080,000 |
| Acknowledged batches matching counts before and after kill/restart | 1,808/1,808 |
| Scheduled batches missed at the driver's in-flight cap | 11,392/13,200 |
| Visibility probes observed / issued | 34/77 |
| Visibility p95 | unavailable |
| Active small files at end | 3,592 |

| Phase | First half mean | Second half mean | Observations |
| --- | ---: | ---: | ---: |
| Parse | 13.80 ms | 14.58 ms | 1,808 |
| Parquet encode and hash | 5.01 ms | 5.14 ms | 5,424 |
| Parquet object PUT | 32.92 ms | 74.90 ms | 5,424 |
| Ingest lock wait | 3.43 s | 8.38 s | 1,808 |
| Trigram merge | 103.90 ms | 226.23 ms | 1,808 |
| Manifest read | 6.94 ms | 13.51 ms | 1,808 |
| Manifest CAS | 10.73 ms | 16.64 ms | 1,808 |
| Compaction lock wait | 2.40 s | 4.10 s | 88 |
| Compaction pass | 2.71 s | 4.81 s | 87 |

Mean times come from phase counter deltas split at 300 seconds. The
second window includes the final drain. The file, request and compaction
phases have different observation counts. Concurrent request waits and
object writes can overlap. Completed compaction passes accounted for
about **309 seconds** under the shared ingest lock. Ingest trigram
merges accounted for **255 seconds** under the same lock.

A separately hosted uninstrumented diagnostic at the same nominal budget
acknowledged 35,550 rows/s. This comparison combines instrumentation and
host variation. Use the within-run growth to prioritize the next
experiment. Keep compaction enabled and group up to eight prepared writes
into a single publication under the existing lock. Success requires
higher sustained goodput and earlier query visibility with exact
acknowledged record IDs after restart, bounded backlog, and the existing
CAS and durability behavior. Repeat a winning change on the reference
hardware before applying the 200,000 rows/s and 1-second gates.

[Raw counters, arrival ledger and batch-count recovery](phase-timing.json.gz)
