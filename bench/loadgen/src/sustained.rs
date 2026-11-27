//! Fixed-rate ingest against one fresh benchmark table.
use super::{ndjson_line, Args};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{sleep, sleep_until, timeout, Instant};
use verdigris_ingest::generate::generate_iter;

const VISIBILITY_POLL: Duration = Duration::from_millis(50);
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(5);
const VISIBILITY_QUEUE: usize = 8;

struct Batch {
    id: usize,
    first_row: usize,
    rows: usize,
    body: String,
}

struct Probe {
    id: usize,
    sent: Instant,
    ack: Instant,
}

#[derive(Serialize)]
struct Visibility {
    id: usize,
    visible_ms: Option<f64>,
    ack_to_visible_ms: Option<f64>,
}

#[derive(Serialize)]
struct Outcome {
    id: usize,
    rows: usize,
    bytes: usize,
    status: Option<u16>,
    acked_rows: Option<u64>,
    start_ms: Option<f64>,
    ack_ms: Option<f64>,
    error: Option<&'static str>,
}

impl Outcome {
    fn new(batch: &Batch) -> Self {
        Self {
            id: batch.id,
            rows: batch.rows,
            bytes: batch.body.len(),
            status: None,
            acked_rows: None,
            start_ms: None,
            ack_ms: None,
            error: None,
        }
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

fn marker(id: usize) -> String {
    format!("sustained-{id:012}")
}

fn produce(
    args: &Args,
    rate: usize,
    rows: usize,
    epoch: i64,
    namespace: &str,
    tx: mpsc::Sender<Batch>,
) -> Result<()> {
    let mut records = generate_iter(rows, args.seed, 0);
    for (id, first_row) in (0..rows).step_by(args.batch_rows).enumerate() {
        let batch_rows = args.batch_rows.min(rows - first_row);
        let trace = format!("{namespace}-{id:012}");
        let mut body = String::with_capacity(args.body_bytes);
        for (offset, mut record) in records.by_ref().take(batch_rows).enumerate() {
            record.trace_id = Some(format!("{trace}-{offset:06}"));
            let offset_ms = ((first_row + offset) as u128 * 1_000 / rate as u128) as i64;
            record.ts_millis = epoch + offset_ms;
            let line = ndjson_line(&record);
            ensure!(
                body.len() + line.len() + 1 <= args.body_bytes,
                "batch {id} exceeds --body-bytes"
            );
            body.push_str(&line);
            body.push('\n');
        }
        if tx
            .blocking_send(Batch {
                id,
                first_row,
                rows: batch_rows,
                body,
            })
            .is_err()
        {
            break;
        }
    }
    Ok(())
}

async fn query(client: &reqwest::Client, url: &str, sql: String) -> Result<Vec<Value>> {
    let response = client
        .post(url)
        .json(&json!({"sql": sql, "histogram": false}))
        .send()
        .await?;
    let mut payload: Value = response.error_for_status()?.json().await?;
    match payload["rows"].take() {
        Value::Array(rows) => Ok(rows),
        _ => anyhow::bail!("query response has no rows field"),
    }
}

async fn visibility_worker(
    client: reqwest::Client,
    url: String,
    table: String,
    mut rx: mpsc::Receiver<Probe>,
) -> Vec<Visibility> {
    let mut measurements = Vec::new();
    while let Some(probe) = rx.recv().await {
        let id = format!("{}-000000", marker(probe.id));
        let sql = format!("SELECT trace_id FROM {table} WHERE trace_id = '{id}' LIMIT 1");
        let deadline = probe.sent + VISIBILITY_TIMEOUT;
        let visible = async {
            loop {
                if let Ok(rows) = query(&client, &url, sql.clone()).await {
                    if rows.iter().any(|row| row["trace_id"] == id) {
                        return Instant::now();
                    }
                }
                sleep(VISIBILITY_POLL).await;
            }
        };
        let observed = if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            timeout(remaining, visible).await.ok()
        } else {
            None
        };
        measurements.push(Visibility {
            id: probe.id,
            visible_ms: observed.map(|at| ms(at.duration_since(probe.sent))),
            ack_to_visible_ms: observed.map(|at| ms(at.duration_since(probe.ack))),
        });
    }
    measurements
}

async fn sample_storage(
    client: reqwest::Client,
    url: String,
    start: Instant,
    mut stop: oneshot::Receiver<()>,
) -> Vec<Value> {
    let mut measurements = Vec::new();
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    loop {
        tokio::select! {
            biased;
            _ = &mut stop => break,
            _ = interval.tick() => {
                let elapsed_ms = ms(start.elapsed());
                let sample = async {
                    let payload: Value = client.get(&url).send().await?.error_for_status()?.json().await?;
                    Ok::<Value, reqwest::Error>(payload["compaction"].clone())
                };
                let compaction = timeout(Duration::from_secs(2), sample).await.ok().and_then(Result::ok);
                measurements.push(json!({"elapsed_ms": elapsed_ms, "compaction": compaction}));
            }
        }
    }
    measurements
}

async fn submit(
    client: reqwest::Client,
    endpoint: String,
    batch: Batch,
    start: Instant,
    sample: bool,
    visibility: mpsc::Sender<Probe>,
) -> Outcome {
    let mut result = Outcome::new(&batch);
    let sent = Instant::now();
    result.start_ms = Some(ms(sent.duration_since(start)));
    match client
        .post(endpoint)
        .header("content-type", "application/x-ndjson")
        .body(batch.body)
        .send()
        .await
    {
        Err(_) => result.error = Some("transport"),
        Ok(response) => {
            result.status = Some(response.status().as_u16());
            if response.status().is_success() {
                match response.json::<Value>().await {
                    Ok(payload) => result.acked_rows = payload["ingested"].as_u64(),
                    Err(_) => result.error = Some("invalid_ack"),
                }
                if result.acked_rows != Some(batch.rows as u64) {
                    result.error = Some("invalid_ack");
                    result.acked_rows = None;
                } else {
                    let ack = Instant::now();
                    result.ack_ms = Some(ms(ack.duration_since(start)));
                    if sample
                        && visibility
                            .try_send(Probe {
                                id: batch.id,
                                sent,
                                ack,
                            })
                            .is_err()
                    {
                        result.error = Some("visibility_queue_full");
                    }
                }
            }
        }
    }
    result
}

pub(super) async fn run(args: &Args, rate: usize) -> Result<()> {
    ensure!(
        rate > 0 && args.duration_secs > 0 && args.batch_rows > 0 && args.concurrency > 0,
        "rate, duration, batch size and concurrency must be positive"
    );
    ensure!(
        args.batch_rows <= 1_000_000 && args.body_bytes > 0,
        "sustained batches must fit six-digit offsets and a nonzero byte cap"
    );
    ensure!(
        args.path == "/v1/ingest",
        "sustained mode expects NDJSON at /v1/ingest"
    );
    ensure!(
        !args.table.is_empty()
            && args.table.as_bytes()[0].is_ascii_alphabetic()
            && args
                .table
                .bytes()
                .all(|c| c == b'_' || c.is_ascii_alphanumeric()),
        "invalid table name"
    );
    let duration = Duration::from_secs(args.duration_secs);
    let rows = usize::try_from(args.duration_secs)?
        .checked_mul(rate)
        .context("corpus size overflow")?;
    let epoch = args
        .corpus_start_ms
        .unwrap_or(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64);
    epoch
        .checked_add(i64::try_from(duration.as_millis())?)
        .context("corpus timestamp overflow")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let url = args.url.trim_end_matches('/');
    let query_url = format!("{url}/v1/query");
    let existing = query(
        &client,
        &query_url,
        format!(
            "SELECT count(*) AS n FROM {} WHERE {}",
            args.table,
            if std::env::var("VDG_BENCH_ALLOW_PRELOAD").as_deref() == Ok("1") {
                "coalesce(trace_id NOT LIKE 'preload-%', true)"
            } else {
                "true"
            }
        ),
    )
    .await?;
    ensure!(
        existing.is_empty() || (existing.len() == 1 && existing[0]["n"] == 0),
        "sustained mode requires a fresh dedicated benchmark table"
    );

    if let Some(preload_rows) = std::env::var_os("VDG_BENCH_PRELOAD_ROWS") {
        ensure!(
            std::env::var("VDG_BENCH_ALLOW_PRELOAD").as_deref() == Ok("1"),
            "preloading requires VDG_BENCH_ALLOW_PRELOAD=1"
        );
        let preload_rows: usize = preload_rows
            .to_str()
            .context("non-UTF-8 preload row count")?
            .parse()?;
        ensure!(preload_rows > 0, "preload row count must be positive");
        let preload_ms = i64::try_from((preload_rows as u128 * 1_000).div_ceil(rate as u128))?;
        let preload_epoch = epoch
            .checked_sub(preload_ms)
            .context("preload timestamp overflow")?;
        let (tx, mut rx) = mpsc::channel(1);
        let generator_args = args.clone();
        let producer = tokio::task::spawn_blocking(move || {
            produce(
                &generator_args,
                rate,
                preload_rows,
                preload_epoch,
                "preload",
                tx,
            )
        });
        let (probe_tx, _probe_rx) = mpsc::channel(1);
        let endpoint = format!("{url}{}", args.path);
        let start = Instant::now();
        let mut acknowledged = 0;
        while let Some(batch) = rx.recv().await {
            let expected = u64::try_from(batch.rows)?;
            let outcome = submit(
                client.clone(),
                endpoint.clone(),
                batch,
                start,
                false,
                probe_tx.clone(),
            )
            .await;
            ensure!(
                outcome.acked_rows == Some(expected),
                "preload batch {} failed: {:?}",
                outcome.id,
                outcome.error
            );
            acknowledged += expected;
        }
        producer.await??;
        ensure!(
            acknowledged == u64::try_from(preload_rows)?,
            "incomplete preload"
        );
        eprintln!("preloaded {acknowledged} rows");
    }

    let (tx, mut rx) = mpsc::channel(args.concurrency);
    let generator_args = args.clone();
    let producer = tokio::task::spawn_blocking(move || {
        produce(&generator_args, rate, rows, epoch, "sustained", tx)
    });
    let (probe_tx, probe_rx) = mpsc::channel(VISIBILITY_QUEUE);
    let probes = tokio::spawn(visibility_worker(
        client.clone(),
        query_url,
        args.table.clone(),
        probe_rx,
    ));
    let endpoint = format!("{url}{}", args.path);
    let mut inflight = JoinSet::new();
    let mut outcomes = Vec::new();
    let mut next_sample_second = 0;
    let mut sampled_batches = Vec::new();
    let mut next = rx.recv().await;
    let mut next_id = 0;
    let start = Instant::now();
    let (stop_storage, storage_stop) = oneshot::channel();
    let storage = tokio::spawn(sample_storage(
        client.clone(),
        format!("{url}/v1/storage/tiers"),
        start,
        storage_stop,
    ));
    while let Some(batch) = next {
        next_id = batch.id + 1;
        let offset = Duration::from_secs_f64(batch.first_row as f64 / rate as f64);
        sleep_until(start + offset).await;
        while let Some(ready) = inflight.try_join_next() {
            outcomes.push(ready?);
        }
        let sample = offset.as_secs() >= next_sample_second;
        if sample {
            next_sample_second = offset.as_secs() + 1;
            sampled_batches.push(batch.id);
        }
        let lag_limit = Duration::from_secs_f64(args.batch_rows as f64 / rate as f64);
        let elapsed = start.elapsed();
        if elapsed >= duration
            || elapsed.saturating_sub(offset) > lag_limit
            || inflight.len() >= args.concurrency
        {
            let mut result = Outcome::new(&batch);
            result.error = Some(
                if elapsed >= duration || elapsed.saturating_sub(offset) > lag_limit {
                    "missed_schedule"
                } else {
                    "inflight_limit"
                },
            );
            outcomes.push(result);
        } else {
            inflight.spawn(submit(
                client.clone(),
                endpoint.clone(),
                batch,
                start,
                sample,
                probe_tx.clone(),
            ));
        }
        next = tokio::time::timeout_at(start + duration, rx.recv())
            .await
            .unwrap_or(None);
    }
    drop(rx);
    let scheduled_batches = rows.div_ceil(args.batch_rows);
    for id in next_id..scheduled_batches {
        let first_row = id * args.batch_rows;
        let offset = Duration::from_secs_f64(first_row as f64 / rate as f64);
        if offset.as_secs() >= next_sample_second {
            next_sample_second = offset.as_secs() + 1;
            sampled_batches.push(id);
        }
        outcomes.push(Outcome {
            id,
            rows: args.batch_rows.min(rows - first_row),
            bytes: 0,
            status: None,
            acked_rows: None,
            start_ms: None,
            ack_ms: None,
            error: Some("generator_deadline"),
        });
    }
    let generation = producer.await?;
    while let Some(result) = inflight.join_next().await {
        outcomes.push(result?);
    }
    drop(probe_tx);
    let visibility = probes.await?;
    let _ = stop_storage.send(());
    let storage = storage.await?;
    outcomes.sort_unstable_by_key(|sample| sample.id);
    let acked: u64 = outcomes.iter().filter_map(|sample| sample.acked_rows).sum();
    let acked_in_window: u64 = outcomes
        .iter()
        .filter(|sample| sample.ack_ms.is_some_and(|t| t < ms(duration)))
        .filter_map(|sample| sample.acked_rows)
        .sum();
    let mut visible: Vec<_> = visibility
        .iter()
        .filter_map(|sample| sample.visible_ms)
        .collect();
    visible.sort_by(f64::total_cmp);
    // Missing or timed-out visibility samples sort above all observed values.
    let p95_position = (sampled_batches.len() * 95).div_ceil(100);
    let p95 = p95_position
        .checked_sub(1)
        .and_then(|p| visible.get(p))
        .copied();
    let report = json!({"table": args.table, "seed": args.seed, "corpus_start_ms": epoch,
        "duration_s": args.duration_secs, "offered_rows_per_s": rate, "planned_rows": rows,
        "planned_batches": scheduled_batches, "outcomes": outcomes, "acked_rows": acked,
        "acked_in_window": acked_in_window, "acked_rows_per_s": acked_in_window as f64 / duration.as_secs_f64(),
        "visibility_poll_ms": VISIBILITY_POLL.as_millis(), "visibility_deadline_ms": VISIBILITY_TIMEOUT.as_millis(),
        "sampled_batches": sampled_batches, "visibility": visibility, "visibility_p95_ms": p95,
        "concurrency": args.concurrency, "buffered_batches": args.concurrency,
        "storage_samples": storage,
        "body_bytes_limit": args.body_bytes, "generation_error": generation.as_ref().err().map(ToString::to_string)});
    if let Some(parent) = Path::new(&args.out)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.out, serde_json::to_vec_pretty(&report)?)?;
    generation?;
    ensure!(
        outcomes.len() == scheduled_batches,
        "incomplete arrival ledger"
    );
    println!(
        "{} acknowledged rows, {:.0} rows/s committed in window",
        acked,
        acked_in_window as f64 / duration.as_secs_f64()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn batches_have_unique_row_ids_in_the_requested_time_range() {
        let args = Args::parse_from(["loadgen", "--batch-rows", "2", "--body-bytes", "4096"]);
        let (tx, mut rx) = mpsc::channel(2);
        produce(&args, 2, 3, 1000, tx).unwrap();
        let first = rx.try_recv().unwrap();
        let last = rx.try_recv().unwrap();
        assert_eq!((first.rows, last.rows), (2, 1));
        let rows: Vec<Value> = first
            .body
            .lines()
            .chain(last.body.lines())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[0]["trace_id"], "sustained-000000000000-000000");
        assert_eq!(rows[1]["trace_id"], "sustained-000000000000-000001");
        assert_eq!(rows[2]["trace_id"], "sustained-000000000001-000000");
        assert_eq!(rows[0]["ts_millis"], 1000);
        assert_eq!(rows[2]["ts_millis"], 2000);
    }
}
