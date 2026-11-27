# Issue #86 · Staged recent-search timestamp cutoff

**Decision: no-go on the two-request staging strategy.** Discovering
the exact cutoff with a timestamp-only query still cost 90.77 ms p95
at 20M. The known-cutoff control shows how much work the full-width
scan can skip when it already has a useful time bound.

| Warm recent search | 1M p50/p95, ms | 20M p50/p95, ms |
| --- | ---: | ---: |
| Baseline, entire fixture | 19.08/20.34 | 95.62/110.27 |
| Two requests, narrow then full-width | 25.88/27.77 | 93.74/106.21 |
| Full-width with the cutoff supplied by the fixture | 14.21/15.43 | 15.83/20.01 |

The staged latency is measured across both HTTP requests, including
both audit acknowledgements and extraction of the cutoff. With
diagnostics disabled, its narrow and bounded stages individually reached
12.66 and 15.12 ms p95 at 1M and 90.77 and 16.65 ms at 20M. The table
uses p95 of the actual paired duration. The known-cutoff arm receives
the exact 200th timestamp from the fixture and measures one request.

On 20M, median returned Parquet bytes and ranges were 5.36 MB/253 for
the baseline, 3.53 MB/216 for the narrow stage, and 0.48 MB/17 for
the second stage. All requests used the same file and byte scope in
their quotes, and execution matched it. The output retained the
full 200 rows in order. All measurements used the same timestamp
schedule and ordinary audit path; the sample has distinct qualifying
timestamps. The warm counters recorded 896 acknowledgements and zero
audit failures, timeouts or saturation in each fixture.

The disk-backed Verdigris and MinIO deployment used two CPUs and
3 GiB per container. This is one fresh deployment per size with
100 measured requests per arm and diagnostics mode. The raw download
includes the physical plans and stage-level samples.

- [Workflow and source](https://github.com/explise/verdigris/actions/runs/45982317423)
- [1M raw comparison](one-million.json.gz) · [20M raw comparison](twenty-million.json.gz)
