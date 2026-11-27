# Issue #63 · Four-CPU equivalent log-query baseline

Three fresh 1M-row deployments per engine ran sequentially on the same
four-vCPU Ubuntu runner. Each complete deployment had 4 CPU and 6 GiB;
Verdigris and MinIO each had 2 CPU and 3 GiB. All engines used the same
corpus timestamp, request schedule and response fields. Every warmup,
measurement and first query after restart passed full result validation.
Each shape has 100 measured warm requests after 10 warmups per trial.
Restart measurements use a fresh engine process and retain the disk and
filesystem cache. Verdigris acknowledges durable audit before returning
a healthy query response. Engine storage and acknowledgement semantics
differ. [GitHub Actions run](https://github.com/explise/verdigris/actions/runs/45963225959).

| Workload | Verdigris + MinIO | ClickHouse | VictoriaLogs |
| --- | --- | --- | --- |
| Exact trace hit | 12.97/18.00, 13.01/17.27, 13.23/18.65 | 2.49/5.84, 2.50/5.89, 2.49/6.00 | 1.52/3.21, 1.47/2.55, 0.96/1.83 |
| Exact trace miss | 4.14/4.44, 4.13/4.36, 4.02/4.27 | 1.51/1.61, 1.51/1.64, 1.51/1.65 | 0.53/0.64, 0.55/0.63, 0.58/0.66 |
| Cross-tier trace | 18.48/24.48, 18.41/25.60, 18.68/26.26 | 3.63/3.83, 3.64/3.88, 3.60/3.81 | 2.85/3.13, 2.60/2.78, 1.16/1.25 |
| Recent search | 16.88/17.89, 16.60/17.96, 17.15/18.67 | 3.53/3.85, 3.52/3.84, 3.57/3.78 | 13.66/15.97, 13.58/16.01, 5.58/7.56 |
| Minute aggregation | 31.19/38.10, 28.36/37.96, 30.32/37.84 | 6.68/8.62, 6.69/8.55, 6.75/8.81 | 9.49/15.45, 9.48/15.26, 8.76/10.63 |

Warm p50/p95 in milliseconds for each trial in run order.

| Engine | Trace hit | Trace miss | Cross-tier trace | Search | Aggregation |
| --- | ---: | ---: | ---: | ---: | ---: |
| vdg | 25.39–35.74 | 12.50–12.71 | 29.24–31.84 | 31.80–32.79 | 41.00–51.03 |
| clickhouse | 9.87–11.23 | 10.28–13.71 | 11.33–12.33 | 7.59–8.50 | 10.25–10.48 |
| victorialogs | 6.96–9.16 | 2.17–2.38 | 8.12–9.71 | 39.86–43.89 | 13.09–18.51 |

First-query latency ranges in milliseconds across the three independent restarts per shape.

| Engine | Ack rows/s | Visibility wait (s) | Retained filesystem bytes |
| --- | ---: | ---: | ---: |
| vdg | 383,981–391,833 | 0.062–0.064 | 165,849,870–166,475,953 |
| clickhouse | 711,649–764,445 | 0.002–0.002 | 130,769,651–130,780,002 |
| victorialogs | 610,464–617,269 | 1.002–1.003 | 20,831,516–20,945,724 |

Acknowledgement throughput and visibility are separate short-run measures.
Verdigris retained **19.68–19.69 MB** in the MinIO bucket. The runtime
volume reported roughly **166 MB**, of which about **147 MB** was under
`.minio.sys`. The job did not sample that directory by subpath. Keep this
internal footprint separate from bucket bytes and from the active file
count. ClickHouse and VictoriaLogs figures describe their data directory
with the test server running.

Raw values, machine/config data and all restart samples are in
[trial 1](trial-1.json.gz), [trial 2](trial-2.json.gz), and [trial 3](trial-3.json.gz).
Run `TOTAL_CPUS=4 WARMUP=10 ITERATIONS=100 RESTART=1 bench/compare/logs/run.sh`
once per fresh trial, using one `CORPUS_TS_MS` across all three trials.
