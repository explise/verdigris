# Issue #24 · Exact minute-summary feasibility

**Decision: go to a serving design and write-path pilot.** The
feature-gated benchmark reached 4.47 ms p95 at 20M with the same 50
ordered minute totals and error counts as raw SQL. It used the normal
HTTP admission, response format, raw-scope estimate and durable audit
path. The bench built a single bounded bundle offline from the active
immutable files. The test used disk-backed MinIO and 4 CPU / 6 GiB
for each complete deployment. [Workflow and source](https://github.com/explise/verdigris/actions/runs/45986707905)

| Metric | 1M rows | 20M rows |
| --- | ---: | ---: |
| Raw warm p50 / p95 | 44.67 / 56.47 ms | 374.39 / 388.12 ms |
| Summary warm p50 / p95 | 5.09 / 18.33 ms | 4.02 / 4.47 ms |
| Active Parquet | 19.42 MB | 366.50 MB |
| Summary bundle | 9,325 bytes | 10,434 bytes |
| Summary bytes / active Parquet | 0.0480% | 0.00285% |
| Summary object GETs per hit | 1 | 1 |
| Offline fixture build, wall time | 0.254 s | 0.572 s |

Each arm used 100 measured warm requests with diagnostics off and
100 with diagnostics on, on one fresh deployment per size. The
diagnostic raw scan read about 3.04 MB at 1M and 38.19 MB at 20M.
Summary hits read one bundle and zero Parquet data bytes.
All 441 expected warm-window audit acknowledgements landed per size.
The requested file set and raw quote remained identical between arms.

The read path rejected missing and corrupt bundles, coverage from an
old snapshot after a late write, and an unsupported descending-order
query. Those requests returned exact raw SQL results. After the late
write, the exact affected minute gained one row and one error.
The rebuilt bundle covered the compacted current snapshot. Restart
verified that the bundle remained readable. Those probes also kept
healthy audit counters.

The benchmark builds a settled snapshot offline. A serving proposal
still needs to measure summary generation and publication during
ingest and compaction. It must use one estimate and execution contract
that accounts for bundle lookups and the raw fallback. Preserve
all-or-nothing coverage, immutable identities, reader retirement
grace, exact results and audit. Repeat on three fresh deployments
before changing the default query path.

[1M raw](one-million.json.gz) · [20M raw](twenty-million.json.gz)
