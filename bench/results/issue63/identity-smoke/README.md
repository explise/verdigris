# Issue #63 · Exact row identity smoke

Verdigris, ClickHouse and VictoriaLogs each returned all **100,000**
generated row identities with exact multiplicity. The check compares
the unique index carried by each generated log message. It therefore
distinguishes the three rows sharing a cross-tier trace ID. The scan
runs after the latency samples and outside the audit and cache
measurement windows. A separate negative control rejected one missing
identity replaced by a duplicate, despite unchanged total count.

```sh
TOTAL_CPUS=4 ROWS=100000 VERIFY_ALL_ROW_IDS=1 ITERATIONS=5 bench/compare/logs/run.sh
```

[Workflow](https://github.com/explise/verdigris/actions/runs/45994503427) ·
[Engine reports, images and runner](results.json.gz)
