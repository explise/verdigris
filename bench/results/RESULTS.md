# Ingest load test — results

Run date: 2026-07-19. Commit: `72d531b` + the `BatchPolicy` config change. Every
artifact here predates `f5685ad`, the lock-scope fix, so the curves describe the
old process-wide lock — which is what this report was written to measure. A subset
predates the generator's dedup fix as well; see **Corrections**.
Host: 16 vCPU / 11 GiB, WSL2 (kernel 6.18.33.2), Docker 29.6.2, overlayfs.
Stack: MinIO (4 CPU limit) + one `vdg` node (`--role all`, 8 CPU limit,
`TOKIO_WORKER_THREADS=8`), both containerized. Corpus seed 42.

Raw artifacts: `c2-*.json` (concurrency sweep), `b2-*.json` (body-size sweep),
`scan-corpus.json`, `sustained-100k.json`, `latest.json`, `loss-*.json` (durability
matrix) — all post-fix. `sustained.json`, `run1-archive/*`, and the
`body-*`/`conc-*` sweeps predate the generator fix. `resources.csv` carries no run
metadata at all; **Corrections** says what each of these is still good for.

---

## Corrections

The load generator had a content-hash dedup bug (recorded in "A methodology bug
worth recording" below). It was fixed at 19:32 on the run date. `sustained.json`
(19:18), `run1-archive/*` (19:08), and the `body-*`/`conc-*` sweeps were written
before that and are not evidence of ingest throughput. Everything post-fix carries
a `replayed_requests` field that the fix added; those artifacts do not, which
identifies them without trusting a file timestamp.

Withdrawn or qualified by this revision:

- **The CPU figures.** "The node used 0.84 of its 8 cores, MinIO used 0.39 of 4" is
  a real reading — of `run1-archive/resources.csv`, whose peaks are 84.15% and
  39.26%. That is the first run of the day, written before the dedup fix, and it
  is not used as evidence anywhere below. The `resources.csv` at the top of this
  directory is a different, longer run and peaks at **335.43%** and **264.31%** —
  3.35 of 8 cgroup cores (~42%) and 2.64 of 4 (~66%). "Seven of eight cores were
  idle" is withdrawn. The dedup bug skipped the manifest commit, not the encode
  (`append_files` drops duplicate paths *after* `write_file` has encoded and PUT
  the file), so a CPU series still describes the real encode path; but neither CSV
  carries run metadata, so treat these as indicative and re-record them on the
  next run.
- **§3 and hypothesis 3.** The flat 160s plateau came from `sustained.json`. A
  deduped run never grows its manifest, and flat is exactly what that produces. H3
  goes back to **untested**, and the "~5,600 files accumulated" figure with it.
- **The least-squares line.** `b2-12582912.json` carries `replayed_requests: 2`,
  which this report's own rule excludes. Refit on the four clean points:
  **52.1 + 12.95 ms/MiB** (r² > 0.998), single-core ceiling **77 MiB/s**.
- **Hypothesis 2.** 12.95 ms/MiB is the whole encode path — Arrow build + ZSTD-3 +
  bloom filters. The ZSTD share was never separated (that is hypothesis 4), so H2
  is **partially confirmed**, not confirmed.
- **The per-core projection.** 77 MiB/s was measured at concurrency 1 with the
  lock held. `8 × 77 ≈ 620 MiB/s` and the 16-20 core estimate for 1 GB/s both
  assume that per-core rate survives 8-way parallelism; memory bandwidth and
  allocator contention erode it. Both are **upper bounds**.
- **§5 and hypothesis 5.** The cited run never compacted (40s against a 300s check
  interval), and the mechanism described was wrong: the scheduler takes the ingest
  lock per *bounded* pass, not across a whole pass. H5 is **untested**.

---

## The headline

**Ingest saturates at ~43 MiB/s (~205k records/s), and the limit is serialization,
not any hardware resource.** The node peaked at **3.35 of the 8 cores** it was
given (~42%) and MinIO at **2.64 of 4** (~66%); neither was pinned at its limit.
No 429s, no 413s, no errors.

The named first bottleneck is **the process-wide `ingest_lock`**, which is held
across the entire encode → compress → PUT → manifest-commit sequence rather than
only the manifest read-modify-write that actually requires mutual exclusion. That
has since been fixed in `f5685ad` (the lock now wraps only `commit_files`); no ramp
against the narrowed lock is recorded here.

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

Corroborating CPU: the peaks in `resources.csv` are 3.35 of the node's 8 cores and
2.64 of MinIO's 4. Both are comfortably below their cgroup limits, so the flat
line is not the node or the object store running out of CPU.

### 2. Cost decomposes cleanly into fixed + per-byte

Concurrency 1, varying body size:

| body MiB | MiB/s | req/s | service ms | files/req | replayed |
|---|---|---|---|---|---|
| 0.25 | 4.6 | 18.0 | 56.7 | 3 | 0 |
| 1.00 | 16.1 | 15.8 | 64.9 | 3 | 0 |
| 4.00 | 40.3 | 9.9 | 101.9 | 3 | 0 |
| 8.00 | 51.8 | 6.3 | 156.8 | 3 | 0 |
| 12.00 | 57.0 | 4.7 | 212.5 | 3 | 2 — excluded |

Least squares over the four clean points: **service_ms = 52.1 + 12.95 x MiB**
(r² > 0.998). The 12 MiB step is excluded because `replayed_requests: 2` means two
of its 56 requests resent content already on the server: its throughput is
overstated by an amount this harness does not quantify. All five points give
51.4 + 13.30 (ceiling 75 MiB/s), the line an earlier version of this report
published.

- **52.1 ms fixed per request** — 3 PUTs (one per tier; the generator spans all
  three) plus the manifest commit's GET+PUT, so ~5 object-store round trips at
  ~10ms each against local MinIO on overlayfs.
- **12.95 ms/MiB** — Arrow build + ZSTD-3 + bloom filters. Inverted, that is a
  **77 MiB/s single-core encode ceiling**, which is the asymptote the ramp was
  pressing against. How much of the 12.95 is ZSTD-3 is not separated; see
  hypothesis 4.

Because the lock serializes this, total throughput is
`B / (52.1 + 12.95B)` MiB/ms — rising with batch size toward 77 MiB/s and never
past it, regardless of cores.

### 3. Sustained rate: not measured

The previous version of this section reported `sustained.json` — 8 x 20s steps at a
fixed 100 MiB/s offered, flat at 46.8-48.1 MiB/s, ~5,600 files accumulated — as
proof that the sustained rate does not decay, and concluded that hypothesis 3
(manifest CAS contention) was not confirmed at this scale.

That run predates the dedup fix, and flatness is precisely what the bug produces:
duplicate batches return from `append_files` before committing, so the manifest
stops growing and no run ever pays the cost of a growing manifest. The 5,600-file
figure is suspect for the same reason. The run also lacks `replayed_requests`,
which every post-fix artifact carries.

So the section is withdrawn rather than corrected: **at this scale, sustained
throughput has not been measured.** H3 is untested. It remains a real concern for
M1.1, and the one clean comparison in the artifacts points at it — see below.

#### An unresolved throughput gap

`c2-16.json` reports 41.9 MiB/s at concurrency 16, same body size as
`latest.json`'s 100 MiB/s step, which reached **68.6 MiB/s** at the same
concurrency. The likely difference is stack state: the `c2`/`b2` sweeps ran
against a stack that accumulated files as they went, while `latest.json` started
from a wiped volume. Within that same run the next step fell to 57.6 MiB/s, so
whatever the fresh bucket bought had partly eroded within 40s.

If manifest growth explains the 41.9 vs 68.6 gap, that is a direct H3 signal. This
report does not establish it: the two runs differ in more than file count, and no
run sweeps file count as a variable. Unresolved.

### 4. Durability holds

Every acked record is queryable, across the matrix (1 MiB bodies, 12s):

| case | compaction | conc | acked | stored | loss |
|---|---|---|---|---|---|
| no-compaction-serial | off | 1 | 809,250 | 809,250 | 0.00% |
| compaction-serial | on | 1 | 814,125 | 814,125 | 0.00% |
| no-compaction-conc8 | off | 8 | 906,750 | 906,750 | 0.00% |
| compaction-conc8 | on | 8 | 872,625 | 872,625 | 0.00% |

Reproduce with `bench/loss-matrix.sh`.

What the matrix does *not* show is anything about compaction. Its arms ran 12s
against a 300s check interval, so neither "compaction" arm ever compacted: each
on/off pair is the same experiment run twice, and their spread (14.10 vs 14.00
MiB/s serial, 15.01 vs 14.51 MiB/s at concurrency 8) is run-to-run noise, not a
compaction cost. The zero-loss result stands — ack-then-query is a real check —
but it is a check of *uncompacted* ingest.

The script now sets a 4s interval for its arms and asks the node whether a pass
actually ran (`compaction.runsTotal` from `/v1/metrics`), refusing the
`compaction-*` label otherwise. No re-run has been done, so this report contains
no measurement of a compacting run.

### 5. Compaction contention: untested

The previous version read the `scan-corpus` run (8 MiB bodies, concurrency 2, 40s,
compaction on) as evidence that compaction contends: 16.7 MiB/s with p99 4,669 ms
against p50 802 ms, because "a pass stalls ingest outright".

Two things are wrong with that. The run was 40s against a 300s interval, so
compaction never ran — `sustained-100k-machine.txt` records the same for a sibling
run ("compaction interval_secs=300 so no pass fires in 70s"). And the mechanism
was misdescribed: `maybe_compact` takes `ingest_lock` per *bounded* pass
(`max_merge_files_per_pass`), drops it, and yields between passes, so a pass
cannot stall ingest for its whole duration by construction.

The 4,669 ms tail is real — it is in `scan-corpus.json` — but this report has no
established cause for it. A 40s run at concurrency 2, on a stack that had already
accumulated files, is simply a different experiment. **H5 is untested**, and after
#48 it is clear no run in this directory ever tested it.

---

## Hypotheses

| # | Hypothesis | Verdict |
|---|---|---|
| 1 | PUT-per-request dominates at small bodies | **Confirmed** — 52.1ms fixed cost is ~5 round trips; at 256 KiB bodies it is 92% of service time |
| 2 | ZSTD-3 encode is CPU-bound and the real per-core ceiling | **Partially confirmed** — the encode *path* is CPU-bound at 12.95 ms/MiB (~77 MiB/s single-core); the ZSTD-3 share is not separated |
| 3 | Manifest CAS contention degrades under load | **Untested** — the only sustained run predates the dedup fix (§3) |
| 4 | Bloom filters on `message` cost real write CPU | **Not isolated** — folded into the 12.95 ms/MiB; needs a build with filters off to separate |
| 5 | Compaction competes with ingest | **Untested** — no load-test run has reached a compaction pass (#48) |

Plus one the brief did not list, which outranks all of them: **the lock scope**.

## The fix, in priority order

1. **Narrow `ingest_lock` to the manifest commit only.** Encode and PUT are pure
   per-request work — content-addressed paths mean concurrent writers cannot
   collide (`crates/ingest/src/lib.rs:286-289`). Only `append_files` needs mutual
   exclusion, and it already has optimistic CAS retry for cross-process safety.
   Expected: 8 cores x 77 MiB/s ≈ 620 MiB/s of encode, ~14x — an upper bound, since
   77 MiB/s was measured at concurrency 1 and per-core throughput does not have to
   survive 8-way parallelism. This has since landed (`f5685ad`); it is unmeasured
   here.
2. **Amortize the 52.1ms fixed cost** with larger server-side batches, now that
   `ingest.max_batch_rows` / `max_batch_bytes` are configurable. Fewer, bigger
   files also reduce compaction pressure.
3. **Revisit ZSTD-3** only after (1). It is the ceiling *per core*, so it matters
   once encode is parallel; before that it is not the binding constraint.

## Extrapolation to 1 GB/s

1 GB/s = 1024 MiB/s of logical logs. At the measured 12.95 ms/MiB of encode, that
is **13.3 core-seconds of encode per wall-second — ~13 cores saturated on
compression alone**, plus PUT overhead and headroom, so ~16-20 cores of a modern
x86 for a single node. That count is optimistic for the same reason the 620 MiB/s
projection is: it assumes the concurrency-1 per-core rate survives 8-way
parallelism. It is reachable on one large instance *only after* the lock is
narrowed; with the old code the answer was "no hardware reaches 1 GB/s", because
the serialized ceiling was 77 MiB/s on any core count.

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
  The 52.1ms fixed cost would grow substantially; the 12.95 ms/MiB encode cost
  would not. Conclusions about *CPU* cost transfer; conclusions about the
  fixed/per-byte *balance* do not.
- **Not multi-node.** One node, one manifest writer, per ADR-0003.
- **Not cold-cache.** Scan numbers read data MinIO had just written.
- **Shared host, and not an idle one.** MinIO, node and generator share 16 cores.
  The node peaked at 3.35 of its 8 cgroup cores and MinIO at 2.64 of its 4, so
  neither was pinned at its limit and the ceiling is not a CPU-limit artifact —
  but the host was carrying ~6 cores of load when it happened, which is exactly
  when MinIO's share of the CPU matters. The CSV samples every ~4s, so
  instantaneous peaks are understated.
- **Not a compaction measurement.** Every run here took place with compaction
  idle (§4, §5).

## A methodology bug worth recording

The first version of this harness cycled 8 pre-built bodies and had each worker
resend the same one. `Ingestor::write_file` names objects by content hash, so
byte-identical batches deduplicate and `append_files` returns before committing
the manifest. Symptoms: an apparent 96-99% "data loss" (only distinct bodies were
ever stored — conc=1 stored exactly 1 body, conc=8 exactly 8), and throughput that
omitted the manifest-commit cost entirely. Fixing it moved the fixed cost from
~41ms (`body-*.json`) to 52.1ms; the ~11ms delta *is* the commit.

The generator now streams zero-copy windows out of one large corpus and reports
`replayed_requests` per step, so a run that starts replaying content can no
longer look clean. **Any step with `replayed_requests > 0` has overstated
throughput** — grow `--corpus-mib`.
