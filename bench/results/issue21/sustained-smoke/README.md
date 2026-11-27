# Issue #21 · Sustained-driver recovery smoke

A 12-second synthetic workload offered 2,000 rows/s against a disposable
RAM-backed MinIO fixture. The debug generator sent 24 distinct batches of
1,000 rows each. Verdigris acknowledged all 24,000 rows within the window.
Twelve visibility probes completed at **39.29 ms p95** from client send.
The batch counts matched all acknowledgements before and after killing and
restarting the Verdigris process. MinIO stayed up through the restart.

This checks the driver, sampled visibility, and batch-level reconciliation.
The 600-second throughput gate requires the release binaries and disk-backed
fixture. See the [driver documentation](../../../loadgen/README.md) and
[raw arrivals](run.json), [before counts](before.json),
[after counts](after.json), and [recovery check](recovery.json).
