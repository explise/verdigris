# Issue #29 — query admission under mixed load

Node budget: **vdg 4 CPU / 2 GiB** + **MinIO 2 CPU / 2 GiB**. Image
`verdigris-bench:issue29b`
(`sha256:767ff9a3820136f5ce40db1894373400364acf5ea85797c16ac229582a0d9`).

`bench/admission/mixed.py` ingests a deterministic corpus, then fires 10
concurrent full-scan aggregates plus 2 cheap `SELECT * ... LIMIT 10` queries in
one round.

## 20M rows

| Class | Attempts | 200 | 429 | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|---:|
| Heavy aggregate | 10 | 6 | 4 | 348.4 ms | 450.6 ms | 457.9 ms |
| Cheap LIMIT | 2 | 2 | 0 | 32.9 ms | 24.9 ms | 40.9 ms |

Peak observed container memory during the run: **432 MiB / 2 GiB** (vdg) and
373 MiB / 2 GiB (MinIO). No OOM kill; every heavy request either completed or
was shed with 429, and both cheap queries completed in under 41 ms.

## 1M rows (no shedding)

| Class | Attempts | 200 | 429 | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| Heavy aggregate | 30 | 30 | 0 | 101.3 ms | 144.9 ms |
| Cheap LIMIT | 6 | 6 | 0 | 81.5 ms | 120.4 ms |

Raw JSON is in `mixed-1m.json` and `mixed-20m.json`. 429 responses carry
`Retry-After: 1` and `queueDepth` in the body (pinned by unit test).

The admission gate is charged before opening data files; a plan over
`query.max_scan_gib` is refused with 413 and the estimate attached.
