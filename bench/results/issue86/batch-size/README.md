# Issue #86 · DataFusion execution batch size

The paired run compared 8,192 and 1,024 rows per execution batch on
settled disk-backed 1M and 20M fixtures. Verdigris and MinIO each used
2 CPU and 3 GiB. Both arms retained the same Parquet layout, source
partitions, dynamic TopK filter and full SQL result. Each arm ran 100
warm samples per workload and diagnostic mode with durable audit.
[GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45978872745)

| Workload | 1M 8,192 p95 | 1M 1,024 p95 | 20M 8,192 p95 | 20M 1,024 p95 |
| --- | ---: | ---: | ---: | ---: |
| Trace hit | 20.50 ms | 19.56 ms | 17.75 ms | 21.95 ms |
| Trace miss | 4.31 ms | 4.77 ms | 4.93 ms | 4.58 ms |
| Cross-tier trace | 27.10 ms | 21.25 ms | 30.49 ms | 29.45 ms |
| Recent search | 24.93 ms | 21.01 ms | 107.36 ms | 105.09 ms |
| Minute aggregation | 36.58 ms | 36.30 ms | 365.18 ms | 400.82 ms |

At 20M, the recent-search scan still emitted a median of 12,789 rows
for the 200-row page, read 253 logical ranges and returned 5.37 MB
in both arms. At 1M it emitted 5,883 rows and read 32 ranges and
1.10 MB. Row-group and page pruning counts stayed the same.

All arms returned exact rows and buckets. The recent-search quote,
scanned files and bytes, and combined and separate activity responses
matched. Warm trace misses read zero Parquet bytes. Both arms recorded
zero audit failures, timeouts, saturation, or OOMs.

Keep the default execution batch size. The smaller batch missed the
20M recent-search gate and increased 20M aggregation p95. The next
read-work control isolates how much of the wide projection can be
avoided by knowing the timestamp cutoff, while counting the work of
finding that cutoff.

[1M raw samples](one-million.json.gz) · [20M raw samples](twenty-million.json.gz)
