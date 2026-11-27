# Issue #86 · Search file-pruning control

Two equivalent recent-search requests ran against one settled 20M-row
fixture. Both requested all tiers, the full fixed corpus, seven fields, and
the newest 200 checkout errors. The driver alternated order and excluded result
validation from timing. All 20 measured results per form and all warmups
matched the fixture in order. Both requests matched their own preflight cost
quote in planned files and bytes. Durable audit and profiling were enabled.

| Input | Files | Planned bytes | Logical ranges | Returned read bytes | Execution p50 (ms) | Client p50 / p95 (ms) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Explicit SQL | 7 | 366,912,171 | 253 | 5,393,583 | 105.12 | 111.05 / 115.80 |
| Search syntax | 1 | 22,609,235 | 364 | 10,956,853 | 70.36 | 76.49 / 79.76 |

The search planner excluded six files with existing manifest predicates. The
physical plans retained the same `TopK` and filter. The single-file plan split
its work into four Parquet groups and read more bytes, so fewer selected files
alone does not predict I/O. File pruning is worth evaluating for equivalent
SQL queries. This run still exceeded the 75 ms p95 target with diagnostics
on, so in a subsequent experiment measure the remaining reader and `TopK`
work before changing layout.

This was a RAM-backed MinIO test on an AMD Ryzen 7 6800H, with 4 CPU and
3 GiB per container. The release image ID was
`sha256:4a2db38a6384cd267555f0f262cd5e7adc5ad09565b1ea16d7f21ed92977a4e0`.
The driver was `6256ffb`. The result describes this fixture and has one
independent deployment. The disk-backed sizing replay is separate.

Set `ROWS=20000000 BATCH_ROWS=50000 SEARCH_CONTROL=1` and run
`bench/compare/logs/run-profile.sh` to include the two-form control. The base
corpus timestamp is recorded in the [seed report](seed.json.gz). Estimates,
physical plan excerpts and every sample are in the [raw comparison](search-comparison.json.gz).
