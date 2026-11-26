# Issue #29 — query admission under mixed load

Node budget: **vdg 4 CPU / 2 GiB** + **MinIO 2 CPU / 2 GiB**. Image
`verdigris-bench:issue29b`
(`sha256:767ff9a3820136f5ce40db1894373400364acf5ea85797c16ac229582a0d9`).

`bench/admission/mixed.py` ingests a deterministic corpus, then fires 10
concurrent full-scan aggregates plus 2 cheap `SELECT * ... LIMIT 10` queries in
one round.

## 20M rows, sustained (20 rounds / 9.5 s)

| Class | Attempts | 200 | 429 | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|---:|
| Heavy aggregate | 200 | 96 | 104 | 324.5 ms | 435.1 ms | 1610.9 ms |
| Cheap LIMIT | 40 | 40 | 0 | 32.4 ms | 54.1 ms | 67.4 ms |
| Trace lookup | 40 | 40 | 0 | 64.3 ms | 99.4 ms | 161.0 ms |

Sustained traffic keeps cheap and trace queries progressing while heavy work
sheds with 429. Peak observed container memory after the run: **488 MiB / 2 GiB**
(vdg) and 526 MiB / 2 GiB (MinIO); no OOM kill.

## 20M rows, one burst (earlier pre-fix run)

| Class | Attempts | 200 | 429 | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| Heavy aggregate | 10 | 6 | 4 | 348.4 ms | 450.6 ms |
| Cheap LIMIT | 2 | 2 | 0 | 32.9 ms | 24.9 ms |

## 1M rows (no shedding)

| Class | Attempts | 200 | 429 | p50 | p95 |
|---|---:|---:|---:|---:|---:|
| Heavy aggregate | 30 | 30 | 0 | 101.3 ms | 144.9 ms |
| Cheap LIMIT | 6 | 6 | 0 | 81.5 ms | 120.4 ms |

Raw JSON is in `mixed-1m.json` and `mixed-20m.json`. 429 responses carry
`Retry-After: 1` and `queueDepth` in the body (pinned by unit test).

The admission gate is charged before opening data files; a plan over
`query.max_scan_gib` is refused with 413 and the estimate attached.
