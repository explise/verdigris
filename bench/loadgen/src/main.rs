//! Ingest load generator: ramp offered load until the server saturates, and
//! record *what* saturated.
//!
//! The deliverable is a throughput curve plus a named bottleneck, not a pass/fail
//! against a target rate. A fixed target tells you whether one number was hit; a
//! ramp tells you which component is the ceiling and what to fix first.
//!
//! Deterministic by construction: the corpus comes from
//! `verdigris_ingest::generate` with a fixed seed and is serialized to NDJSON
//! once, up front. Per-request work is a memcpy of pre-built bytes, so the
//! numbers describe the server rather than this process's serde cost.

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser, Debug)]
#[command(about = "Ramp load against a Verdigris ingest endpoint until it saturates")]
struct Args {
    /// Base URL of the ingest node.
    #[arg(long, default_value = "http://localhost:8080")]
    url: String,

    /// First step's offered rate (MiB/s).
    #[arg(long, default_value_t = 50.0)]
    start_mibps: f64,

    /// Increment per step (MiB/s).
    #[arg(long, default_value_t = 50.0)]
    step_mibps: f64,

    /// Stop after this many steps.
    #[arg(long, default_value_t = 12)]
    steps: usize,

    /// Seconds of steady load per step.
    #[arg(long, default_value_t = 20)]
    step_secs: u64,

    /// Seconds to idle between steps, letting compaction and the manifest settle
    /// so a step measures steady state rather than the previous step's backlog.
    #[arg(long, default_value_t = 5)]
    settle_secs: u64,

    /// Uncompressed NDJSON bytes per POST. Must stay under the server's
    /// `ingest.max_body_bytes` (default 16 MiB) or every request 413s.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    body_bytes: usize,

    /// Concurrent in-flight requests. Above the server's `ingest.max_inflight`
    /// (default 32) the excess is shed as 429 rather than queued.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Corpus seed. Fixed by default so runs are comparable.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Size of the pre-generated NDJSON corpus (MiB). Must exceed the bytes a
    /// single step will send, or requests start replaying content the server has
    /// already stored — which it deduplicates by content hash, skipping the
    /// manifest commit and inflating throughput. `replayed_requests` in the
    /// results reports whether this happened.
    #[arg(long, default_value_t = 1536)]
    corpus_mib: usize,

    /// Stop early once a step's accepted rate fails to improve on the best seen
    /// by this fraction — saturation, so further steps only add queueing.
    #[arg(long, default_value_t = 0.02)]
    saturation_epsilon: f64,

    /// Where to write the JSON results artifact.
    #[arg(long, default_value = "bench/results/latest.json")]
    out: String,

    /// Ingest path. `/v1/ingest` is NDJSON; the OTLP receiver has a different
    /// body shape and is not what this generator emits.
    #[arg(long, default_value = "/v1/ingest")]
    path: String,
}

/// One large contiguous NDJSON buffer that requests take distinct windows of.
///
/// **Every request must carry distinct content.** `Ingestor::write_file` names
/// objects by the content hash of the encoded Parquet, so two requests with
/// byte-identical records produce the same path and `append_files` correctly
/// deduplicates the second — it returns before committing the manifest. A
/// generator that recycles a handful of bodies therefore measures a server that
/// has stopped doing most of its work: no manifest commit, and ZSTD compressing
/// data it has already seen. An earlier version of this file cycled 8 bodies and
/// produced exactly that artifact.
///
/// One buffer + zero-copy `Bytes::slice` windows keeps that correct without
/// paying a memcpy per request: slicing is a refcount bump, and consecutive
/// windows are genuinely different records.
struct Corpus {
    /// The whole NDJSON corpus, one allocation.
    buf: bytes::Bytes,
    /// Byte offset of each line start, plus a final sentinel at `buf.len()`.
    line_offsets: Vec<usize>,
    /// How many lines make up one request-sized window.
    lines_per_body: usize,
    mean_line_bytes: f64,
}

impl Corpus {
    /// The `n`-th request window, as a zero-copy slice. Wraps around when the
    /// corpus is exhausted; `wrapped()` reports whether that happened so a run
    /// that silently started replaying content is visible in the results.
    fn window(&self, n: usize) -> (bytes::Bytes, bool) {
        let windows = (self.line_offsets.len() - 1) / self.lines_per_body;
        let idx = n % windows.max(1);
        let start = self.line_offsets[idx * self.lines_per_body];
        let end = self.line_offsets[((idx + 1) * self.lines_per_body).min(self.line_offsets.len() - 1)];
        (self.buf.slice(start..end), n >= windows)
    }

    fn windows(&self) -> usize {
        ((self.line_offsets.len() - 1) / self.lines_per_body).max(1)
    }
}

/// Build the corpus once, sized so a whole run can be served without replaying
/// content (see the `Corpus` doc comment for why replay corrupts the numbers).
fn build_corpus(seed: u64, body_bytes: usize, corpus_bytes: usize) -> Result<Corpus> {
    // Sample to learn the mean serialized line size, then size everything by it.
    let sample = verdigris_ingest::generate::generate(1_000, seed, 0);
    let sample_bytes: usize = sample.iter().map(|r| ndjson_line(r).len()).sum();
    let mean_line_bytes = sample_bytes as f64 / sample.len() as f64;
    let lines_per_body = ((body_bytes as f64 / mean_line_bytes).floor() as usize).max(1);

    let total_lines = ((corpus_bytes as f64 / mean_line_bytes).ceil() as usize)
        .max(lines_per_body)
        // Whole windows only, so the last request is the same size as the rest.
        .next_multiple_of(lines_per_body);

    let records = verdigris_ingest::generate::generate(total_lines, seed, 0);
    let mut buf = String::with_capacity(corpus_bytes + 1024);
    let mut line_offsets = Vec::with_capacity(total_lines + 1);
    for r in &records {
        line_offsets.push(buf.len());
        buf.push_str(&ndjson_line(r));
        buf.push('\n');
    }
    line_offsets.push(buf.len());

    Ok(Corpus {
        buf: bytes::Bytes::from(buf),
        line_offsets,
        lines_per_body,
        mean_line_bytes,
    })
}

/// Serialize one record in the `crates/ingest/src/wire.rs::JsonLog` shape.
fn ndjson_line(r: &verdigris_core::batch::LogRecord) -> String {
    let attrs: BTreeMap<&str, &str> = r
        .attrs
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    serde_json::json!({
        "ts_millis": r.ts_millis,
        "level": r.level.as_str(),
        "service": r.service,
        "status": r.status,
        "message": r.message,
        "trace_id": r.trace_id,
        "attrs": attrs,
    })
    .to_string()
}

#[derive(Default)]
struct StepCounters {
    ok: AtomicU64,
    too_many: AtomicU64,
    too_large: AtomicU64,
    other_err: AtomicU64,
    transport_err: AtomicU64,
    bytes_offered: AtomicU64,
    bytes_accepted: AtomicU64,
    records_accepted: AtomicU64,
    files_written: AtomicU64,
    parquet_bytes: AtomicU64,
    /// Client-observed latencies in microseconds, for percentiles.
    latencies_us: std::sync::Mutex<Vec<u64>>,
}

/// The `/metrics` counters this harness reads. Scraped rather than recomputed —
/// the server already exposes them and a parallel counter stack would drift.
#[derive(Debug, Clone, Default, serde::Serialize)]
struct ServerMetrics {
    ingest_records_total: f64,
    http_requests_total: f64,
    latency_sum: f64,
    latency_count: f64,
}

fn parse_metrics(text: &str) -> ServerMetrics {
    let mut m = ServerMetrics::default();
    for line in text.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some((name_and_labels, value)) = line.rsplit_once(char::is_whitespace) else {
            continue;
        };
        let Ok(v) = value.trim().parse::<f64>() else {
            continue;
        };
        let name = name_and_labels
            .split('{')
            .next()
            .unwrap_or(name_and_labels)
            .trim();
        match name {
            "verdigris_ingest_records_total" => m.ingest_records_total += v,
            "verdigris_http_requests_total" => m.http_requests_total += v,
            "verdigris_http_request_duration_seconds_sum" => m.latency_sum += v,
            "verdigris_http_request_duration_seconds_count" => m.latency_count += v,
            _ => {}
        }
    }
    m
}

async fn scrape(client: &reqwest::Client, url: &str) -> ServerMetrics {
    match client.get(format!("{url}/metrics")).send().await {
        Ok(r) => match r.text().await {
            Ok(t) => parse_metrics(&t),
            Err(_) => ServerMetrics::default(),
        },
        Err(_) => ServerMetrics::default(),
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

#[derive(Debug, serde::Serialize)]
struct StepResult {
    target_mibps: f64,
    offered_mibps: f64,
    accepted_mibps: f64,
    accepted_records_per_sec: f64,
    /// Parquet bytes actually landed in the object store, per second. Lower than
    /// `accepted_mibps` by the compression ratio; this is the real S3 write rate.
    parquet_mibps: f64,
    compression_ratio: f64,
    ok: u64,
    too_many_429: u64,
    too_large_413: u64,
    other_err: u64,
    transport_err: u64,
    files_written: u64,
    puts_per_sec: f64,
    mean_file_bytes: f64,
    p50_ms: f64,
    p99_ms: f64,
    elapsed_secs: f64,
    server_records_delta: f64,
    server_mean_latency_ms: f64,
    /// Requests that reused corpus content already sent. Any value above zero
    /// means the server deduplicated those writes by content hash and skipped
    /// the manifest commit, so throughput for this step is overstated. Grow
    /// `--corpus-mib` until this is 0.
    replayed_requests: u64,
}

#[allow(clippy::too_many_arguments)]
async fn run_step(
    client: &reqwest::Client,
    args: &Args,
    corpus: &Arc<Corpus>,
    target_mibps: f64,
) -> Result<StepResult> {
    let endpoint = format!("{}{}", args.url, args.path);
    let counters = Arc::new(StepCounters::default());

    let target_bps = target_mibps * 1024.0 * 1024.0;
    let body_len = corpus.window(0).0.len() as f64;
    // Absolute deadlines rather than sleep(interval): sleeping a fixed interval
    // accumulates the service time into the gap, so the achieved rate silently
    // undershoots the target and the curve reads as saturation that isn't there.
    let interval = Duration::from_secs_f64(body_len / target_bps);

    let before = scrape(client, &args.url).await;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.step_secs);

    // One shared cursor, so every request across every worker gets a distinct
    // window. Per-worker counters would hand each worker the same sequence.
    let cursor = Arc::new(AtomicU64::new(0));
    let replayed = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::with_capacity(args.concurrency);
    for w in 0..args.concurrency {
        let client = client.clone();
        let endpoint = endpoint.clone();
        let corpus = Arc::clone(corpus);
        let counters = Arc::clone(&counters);
        let cursor = Arc::clone(&cursor);
        let replayed = Arc::clone(&replayed);
        // Stagger workers across one interval so they do not all fire together.
        let worker_interval = interval * args.concurrency as u32;
        let mut next = start + interval * w as u32;

        workers.push(tokio::spawn(async move {
            loop {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                if next > now {
                    tokio::time::sleep(next - now).await;
                }
                next += worker_interval;

                let n = cursor.fetch_add(1, Ordering::Relaxed) as usize;
                let (body, is_replay) = corpus.window(n);
                if is_replay {
                    replayed.fetch_add(1, Ordering::Relaxed);
                }
                let body_len = body.len() as u64;
                counters.bytes_offered.fetch_add(body_len, Ordering::Relaxed);

                let sent = Instant::now();
                let resp = client
                    .post(&endpoint)
                    .header("content-type", "application/x-ndjson")
                    .body(body)
                    .send()
                    .await;
                let elapsed_us = sent.elapsed().as_micros() as u64;

                match resp {
                    Ok(r) => {
                        let status = r.status().as_u16();
                        counters
                            .latencies_us
                            .lock()
                            .expect("latency mutex")
                            .push(elapsed_us);
                        match status {
                            200..=299 => {
                                counters.ok.fetch_add(1, Ordering::Relaxed);
                                counters.bytes_accepted.fetch_add(body_len, Ordering::Relaxed);
                                if let Ok(v) = r.json::<serde_json::Value>().await {
                                    let n = |k: &str| v.get(k).and_then(|x| x.as_u64()).unwrap_or(0);
                                    counters
                                        .records_accepted
                                        .fetch_add(n("ingested"), Ordering::Relaxed);
                                    counters
                                        .files_written
                                        .fetch_add(n("filesWritten"), Ordering::Relaxed);
                                    counters
                                        .parquet_bytes
                                        .fetch_add(n("bytesWritten"), Ordering::Relaxed);
                                }
                            }
                            429 => {
                                counters.too_many.fetch_add(1, Ordering::Relaxed);
                            }
                            413 => {
                                counters.too_large.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {
                                counters.other_err.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(_) => {
                        counters.transport_err.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for w in workers {
        let _ = w.await;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let after = scrape(client, &args.url).await;

    let mut lat = counters
        .latencies_us
        .lock()
        .expect("latency mutex")
        .clone();
    lat.sort_unstable();

    let accepted_bytes = counters.bytes_accepted.load(Ordering::Relaxed) as f64;
    let parquet_bytes = counters.parquet_bytes.load(Ordering::Relaxed) as f64;
    let files = counters.files_written.load(Ordering::Relaxed);
    let mib = 1024.0 * 1024.0;

    let lat_count = after.latency_count - before.latency_count;
    let lat_sum = after.latency_sum - before.latency_sum;

    Ok(StepResult {
        target_mibps,
        offered_mibps: counters.bytes_offered.load(Ordering::Relaxed) as f64 / mib / elapsed,
        accepted_mibps: accepted_bytes / mib / elapsed,
        accepted_records_per_sec: counters.records_accepted.load(Ordering::Relaxed) as f64 / elapsed,
        parquet_mibps: parquet_bytes / mib / elapsed,
        compression_ratio: if parquet_bytes > 0.0 {
            accepted_bytes / parquet_bytes
        } else {
            0.0
        },
        ok: counters.ok.load(Ordering::Relaxed),
        too_many_429: counters.too_many.load(Ordering::Relaxed),
        too_large_413: counters.too_large.load(Ordering::Relaxed),
        other_err: counters.other_err.load(Ordering::Relaxed),
        transport_err: counters.transport_err.load(Ordering::Relaxed),
        files_written: files,
        puts_per_sec: files as f64 / elapsed,
        mean_file_bytes: if files > 0 {
            parquet_bytes / files as f64
        } else {
            0.0
        },
        p50_ms: percentile(&lat, 0.50) as f64 / 1000.0,
        p99_ms: percentile(&lat, 0.99) as f64 / 1000.0,
        elapsed_secs: elapsed,
        server_records_delta: after.ingest_records_total - before.ingest_records_total,
        server_mean_latency_ms: if lat_count > 0.0 {
            lat_sum / lat_count * 1000.0
        } else {
            0.0
        },
        replayed_requests: replayed.load(Ordering::Relaxed),
    })
}

#[derive(serde::Serialize)]
struct Report {
    config: ReportConfig,
    steps: Vec<StepResult>,
    peak_accepted_mibps: f64,
    peak_accepted_records_per_sec: f64,
    saturated: bool,
}

#[derive(serde::Serialize)]
struct ReportConfig {
    url: String,
    path: String,
    seed: u64,
    body_bytes: usize,
    lines_per_body: usize,
    mean_line_bytes: f64,
    concurrency: usize,
    step_secs: u64,
    settle_secs: u64,
    cpus: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let corpus = Arc::new(build_corpus(
        args.seed,
        args.body_bytes,
        args.corpus_mib * 1024 * 1024,
    )?);
    eprintln!(
        "corpus: {:.0} MiB, {} distinct windows x {} lines (~{:.0} B/line, {:.2} MiB/window)",
        corpus.buf.len() as f64 / 1024.0 / 1024.0,
        corpus.windows(),
        corpus.lines_per_body,
        corpus.mean_line_bytes,
        corpus.window(0).0.len() as f64 / 1024.0 / 1024.0,
    );

    let client = reqwest::Client::builder()
        // Generous: at saturation a request can legitimately take many seconds,
        // and a client-side timeout would be indistinguishable from server
        // saturation in the results.
        .timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(args.concurrency * 2)
        .build()
        .context("build http client")?;

    // Preflight: fail loudly now rather than reporting a curve of transport errors.
    client
        .get(format!("{}/healthz", args.url))
        .send()
        .await
        .with_context(|| format!("{} is not reachable — is `vdg serve` up?", args.url))?;

    println!(
        "{:>8} {:>10} {:>10} {:>12} {:>8} {:>7} {:>8} {:>9} {:>9}",
        "target", "offered", "accepted", "records/s", "PUT/s", "429", "p50 ms", "p99 ms", "ratio"
    );

    let mut steps = Vec::new();
    let mut peak = 0.0f64;
    let mut saturated = false;

    for i in 0..args.steps {
        let target = args.start_mibps + args.step_mibps * i as f64;
        let r = run_step(&client, &args, &corpus, target).await?;

        println!(
            "{:>8.0} {:>10.1} {:>10.1} {:>12.0} {:>8.1} {:>7} {:>8.1} {:>9.1} {:>8.1}x",
            r.target_mibps,
            r.offered_mibps,
            r.accepted_mibps,
            r.accepted_records_per_sec,
            r.puts_per_sec,
            r.too_many_429,
            r.p50_ms,
            r.p99_ms,
            r.compression_ratio,
        );

        let improved = r.accepted_mibps > peak * (1.0 + args.saturation_epsilon);
        if r.accepted_mibps > peak {
            peak = r.accepted_mibps;
        }
        steps.push(r);

        // Two consecutive non-improving steps: one can be noise, two is a ceiling.
        if !improved && steps.len() >= 2 {
            let prev = &steps[steps.len() - 2];
            if prev.accepted_mibps <= peak * (1.0 + args.saturation_epsilon) {
                saturated = true;
                println!("\nsaturated — accepted rate stopped tracking offered load; stopping ramp");
                break;
            }
        }

        if args.settle_secs > 0 && i + 1 < args.steps {
            tokio::time::sleep(Duration::from_secs(args.settle_secs)).await;
        }
    }

    let peak_records = steps
        .iter()
        .map(|s| s.accepted_records_per_sec)
        .fold(0.0f64, f64::max);

    let report = Report {
        config: ReportConfig {
            url: args.url.clone(),
            path: args.path.clone(),
            seed: args.seed,
            body_bytes: args.body_bytes,
            lines_per_body: corpus.lines_per_body,
            mean_line_bytes: corpus.mean_line_bytes,
            concurrency: args.concurrency,
            step_secs: args.step_secs,
            settle_secs: args.settle_secs,
            cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
        },
        steps,
        peak_accepted_mibps: peak,
        peak_accepted_records_per_sec: peak_records,
        saturated,
    };

    if let Some(parent) = std::path::Path::new(&args.out).parent() {
        std::fs::create_dir_all(parent).ok();
    }
    std::fs::write(&args.out, serde_json::to_string_pretty(&report)?)
        .with_context(|| format!("write {}", args.out))?;

    println!(
        "\npeak accepted: {:.1} MiB/s ({:.0} records/s) — written to {}",
        report.peak_accepted_mibps, report.peak_accepted_records_per_sec, args.out
    );
    Ok(())
}
