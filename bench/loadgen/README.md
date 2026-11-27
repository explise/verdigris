# Sustained ingest and recovery

Issue #21 uses a fresh, disposable S3-compatible fixture with one Verdigris
writer and compaction enabled. The driver offers 220,000 distinct rows/s for
600 seconds and counts rows acknowledged within that window. This leaves room
for the 200,000 committed rows/s target. Batches that miss their scheduled
arrival or the connection limit remain visible in the raw ledger.

```sh
docker build -f bench/Dockerfile.vdg -t verdigris-ingest-bench .
cargo build --locked --release --manifest-path bench/loadgen/Cargo.toml
VDG_IMAGE=verdigris-ingest-bench \
  LOADGEN="$PWD/bench/loadgen/target/release/verdigris-loadgen" \
  OUT=/tmp/verdigris-sustained bench/loadgen/run-sustained.sh
```

The default node budget is 8 CPU / 4 GiB for Verdigris and 4 CPU / 2 GiB for
MinIO. Record smaller host budgets as separate diagnostics. The driver runs
on the same host. Set `VDG_CPUS` and `MINIO_CPUS` to reproduce a specific CPU
allocation. The runner starts and tears down its own network, containers and
volume, and leaves the output directory with images, machine and config details.

The run samples one batch per offered second for visibility. Its clock starts
at client send and stops when the query returns the expected row, including
network and probe cost. Probes start after acknowledgement and use a 50 ms
poll interval. Missing visibility samples count above measured latencies when
calculating nearest-rank p95. Storage samples record small, compacted and
retired files and the compaction generation without a data scan.

The recovery check verifies contiguous, unique synthetic row IDs in
bounded batches and compares the count of every acknowledged batch before and
after an intentional process kill and restart. MinIO stays running.
Ambiguous transport or acknowledgement batches are reported separately. Report the 600-second
committed rate, ≤1 second visibility threshold, recovery outcome, and storage
backlog together. The original pre-generated ramp continues to use the same
CLI.
