# Ingest load test — results

Run date: 2026-07-19. Commit: `72d531b` + the `BatchPolicy` config change.
Host: 16 vCPU / 11 GiB, WSL2 (kernel 6.18.33.2), Docker 29.6.2, overlayfs.
Stack: MinIO (4 CPU limit) + one `vdg` node (`--role all`, 8 CPU limit,
`TOKIO_WORKER_THREADS=8`), both containerized. Corpus seed 42.

Raw artifacts: `c2-*.json` (concurrency sweep), `b2-*.json` (body-size sweep),
`loss-*.json` (durability matrix), `sustained.json`, `resources.csv`.

---

## The headline

**Ingest saturates at ~43 MiB/s (~205k records/s), and the limit is
serialization, not any hardware resource.** The node used **0.84 of the 8 cores
it was given**; MinIO used 0.39 of 4. No 429s, no 413s, no errors.

The named first bottleneck is **the process-wide `ingest_lock`
(`crates/vdg/src/serve.rs:1667`)**, which is held across the entire
encode → compress → PUT → manifest-commit sequence rather than only the manifest
read-modify-write that actually requires mutual exclusion.

## Evidence

### 1. Throughput is flat across a 32x concurrency range

4 MiB bodies, 12s steps, offered rate far above capacity:

| concurrency | MiB/s | records/s | req/s | p50 ms | p99 ms | 429 |
|---|---|---|---|---|---|---|
| 1 | 37.9 | 181,267 | 9.3 | 107 | 122 | 0 |
| 2 | 43.2 | 206,270 | 10.6 | 189 | 213 | 0 |
| 4 | 43.1 | 205,772 | 10.6 | 380 | 420 | 0 |
| 8 | 42.3 | 202,362 | 10.4 | 763 | 842 | 0 |
| 16 | 41.9 | 200,382 | 10.3 | 1518 | 1696 | 0 |
| 32 | 41.7 | 199,272 | 10.2 | 3023 | 3212 | 0 |

Throughput is constant at ~10.3 req/s while p50 latency scales linearly with
concurrency — 107ms x N, almost exactly. That is Little's Law for a server
processing one request at a time: added concurrency buys queueing delay and
nothing else. A CPU- or IO-bound server would show rising throughput and a knee;
this shows a flat line from N=1.

Corroborating: `resources.csv` peaks at **84.2% CPU for the node** — 0.84 of 8
cores. Seven of eight cores were idle at saturation.

### 2. Cost decomposes cleanly into fixed + per-byte

Concurrency 1, varying body size:

| body MiB | MiB/s | req/s | service ms | files/req |
|---|---|---|---|---|
| 0.25 | 4.6 | 18.0 | 56.7 | 3 |
| 1.00 | 16.1 | 15.8 | 64.9 | 3 |
| 4.00 | 40.3 | 9.9 | 101.9 | 3 |
| 8.00 | 51.8 | 6.3 | 156.8 | 3 |
| 12.00 | 57.0 | 4.7 | 212.5 | 3 |

Least squares: **service_ms = 51.4 + 13.30 x MiB** (r² > 0.999).

- **51.4 ms fixed per request** — 3 PUTs (one per tier; the generator spans all
  three) plus the manifest commit's GET+PUT, so ~5 object-store round trips at
  ~10ms each against local MinIO on overlayfs.
- **13.30 ms/MiB** — Arrow build + ZSTD-3 + bloom filters. Inverted, that is a
  **75 MiB/s single-core encode ceiling**, which is the asymptote the ramp was
  pressing against.

Because the lock serializes this, total throughput is
`B / (51.4 + 13.3B)` MiB/ms — rising with batch size toward 75 MiB/s and never
past it, regardless of cores.

### 3. Sustained rate does not decay

Fixed 100 MiB/s offered, 8 x 20s steps, ~5,600 files accumulated, compaction's
300s interval never fired:

47.4, 47.5, 47.9, 48.1, 47.4, 47.4, 46.8, 47.0 MiB/s

Flat across 160s. **Hypothesis 3 (manifest CAS contention) is not confirmed at
this scale.** The manifest is rewritten per commit and grows O(N), but at ~5,600
entries (5.0 KiB) that cost is invisible next to the ~100ms service time. It
remains a real scale concern for M1.1 — this run simply does not reach the point
where it bites, and says nothing about 10^6 files.

### 4. Durability holds

Every acked record is queryable, across the matrix (1 MiB bodies, 12s):

| case | compaction | conc | acked | stored | loss |
|---|---|---|---|---|---|
| no-compaction-serial | off | 1 | 809,250 | 809,250 | 0.00% |
| compaction-serial | on | 1 | 814,125 | 814,125 | 0.00% |
| no-compaction-conc8 | off | 8 | 906,750 | 906,750 | 0.00% |
| compaction-conc8 | on | 8 | 872,625 | 872,625 | 0.00% |

Reproduce with `bench/loss-matrix.sh`.

### 5. Compaction does contend (hypothesis 5, partially confirmed)

The scan-corpus load (8 MiB bodies, concurrency 2, 40s, compaction on) fell to
16.7 MiB/s with **p99 4,669ms against p50 802ms**. Compaction takes the same
`ingest_lock`, so a pass stalls ingest outright; the bounded-pass design
(`max_merge_files_per_pass`) limits how long, but the tail is visible. This is a
second-order effect behind the lock scope itself.

---

## Hypotheses, settled

| # | Hypothesis | Verdict |
|---|---|---|
| 1 | PUT-per-request dominates at small bodies | **Confirmed** — 51.4ms fixed cost is ~5 round trips; at 256 KiB bodies it is 91% of service time |
| 2 | ZSTD-3 encode is CPU-bound and the real per-core ceiling | **Confirmed** — 13.3 ms/MiB, a 75 MiB/s per-core ceiling |
| 3 | Manifest CAS contention degrades under load | **Not confirmed at this scale** — flat over 160s / 5,600 files |
| 4 | Bloom filters on `message` cost real write CPU | **Not isolated** — folded into the 13.3 ms/MiB; needs a build with filters off to separate |
| 5 | Compaction competes with ingest | **Confirmed** — p99 5.8x p50 while compacting |

Plus one the brief did not list, which outranks all of them: **the lock scope**.

## The fix, in priority order

1. **Narrow `ingest_lock` to the manifest commit only.** Encode and PUT are
   pure per-request work — content-addressed paths mean concurrent writers cannot
   collide (`crates/ingest/src/lib.rs:286-289`). Only `append_files` needs mutual
   exclusion, and it already has optimistic CAS retry for cross-process safety.
   Expected: 8 cores x 75 MiB/s ≈ 600 MiB/s of encode, ~14x today.
2. **Amortize the 51.4ms fixed cost** with larger server-side batches, now that
   `ingest.max_batch_rows` / `max_batch_bytes` are configurable. Fewer, bigger
   files also reduce compaction pressure.
3. **Revisit ZSTD-3** only after (1). It is the ceiling *per core*, so it matters
   once encode is parallel; before that it is not the binding constraint.

## Extrapolation to 1 GB/s

1 GB/s = 1024 MiB/s of logical logs. At the measured 13.3 ms/MiB of encode, that
is **13.6 core-seconds of encode per wall-second — ~14 cores saturated on
compression alone**, plus PUT overhead and headroom, so ~16-20 cores of a modern
x86 for a single node. That is reachable on one large instance *only after* the
lock is narrowed; with today's code the answer is "no hardware reaches 1 GB/s",
because the ceiling is 75 MiB/s on any core count.

**The component that must change first is the lock scope, not the hardware.**

## Calibration output

`query.modeled_mibps_per_core` was a placeholder at 250.0. Measured against the
same stack (3.24M rows, 58 MiB stored, 169 files, 8 cores, median of 5):

| query shape | MiB/s | MiB/s/core |
|---|---|---|
| `COUNT(*)` (metadata-only — reads no column data) | 2676 | 334 |
| group by service (1 column) | 1102 | 138 |
| wide scan (4 columns) | 892 | 111 |
| full-projection aggregate | 635 | 79 |

Set to **100.0** — the round number in the 79-111 realistic band. Only the
metadata-only query, which never touches column data, reached the old 250.

Note this is the **scan** constant, and everything else on this page measures the
**ingest encode** path. They are different operations; the brief's instruction to
feed ingest MiB/s-per-core into `modeled_mibps_per_core` would have put a write
number into a read model. The scan measurement above is the correct input.

## What these numbers are not

- **Not S3.** MinIO on localhost has ~10ms round trips where real S3 has 20-50ms.
  The 51.4ms fixed cost would grow substantially; the 13.3 ms/MiB encode cost
  would not. Conclusions about *CPU* cost transfer; conclusions about the
  fixed/per-byte *balance* do not.
- **Not multi-node.** One node, one manifest writer, per ADR-0003.
- **Not cold-cache.** Scan numbers read data MinIO had just written.
- **Shared host.** MinIO, node, and generator on 16 cores. Peak combined CPU was
  ~1.3 cores, so contention was not a factor here — but `resources.csv` is the
  check, not an assumption.

## A methodology bug worth recording

The first version of this harness cycled 8 pre-built bodies and had each worker
resend the same one. `Ingestor::write_file` names objects by content hash, so
byte-identical batches deduplicate and `append_files` returns before committing
the manifest. Symptoms: an apparent 96-99% "data loss" (only distinct bodies were
ever stored — conc=1 stored exactly 1 body, conc=8 exactly 8), and throughput
that omitted the manifest-commit cost entirely. Fixing it moved the fixed cost
from 42ms to 51.4ms; the ~9ms delta *is* the commit.

The generator now streams zero-copy windows out of one large corpus and reports
`replayed_requests` per step, so a run that starts replaying content can no
longer look clean. **Any step with `replayed_requests > 0` has overstated
throughput** — grow `--corpus-mib`.
