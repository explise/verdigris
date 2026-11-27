#!/usr/bin/env python3
"""Mixed-load admission check: heavy aggregates, cheap searches and point lookups."""
import argparse, concurrent.futures, heapq, http.client, json, math, os, statistics, subprocess, threading, time

SERVICES = ["gateway", "checkout", "auth", "billing", "search", "catalog", "ingest", "query", "alerts", "settings"]
BASE = int(os.environ.get("MIXED_BASE_TS_MS", str((int(time.time() * 1000) - 200 * 60_000) // 60_000 * 60_000)))
WINDOW_MS = 200 * 60_000
# Keep this schedule shared with the result validator.
FIXED_RATES = {"heavy": 28.0, "cheap": 11.0, "trace": 11.0, "expensive": 4.0,
               "dashboard": 0.5, "alert": 0.1}


def record(i, rows):
    svc = SERVICES[i % len(SERVICES)]
    lvl = "ERROR" if i % 17 == 0 else ("WARN" if i % 7 == 0 else "INFO")
    ts = BASE + (i * WINDOW_MS) // max(1, rows)
    return {"ts_millis": ts, "level": lvl, "service": svc, "status": 500 if lvl == "ERROR" else 200,
            "message": f"{lvl.lower()} request {i} route=/{svc}", "trace_id": f"{i:016x}",
            "attrs": {"region": "us-east-1", "host": f"node-{i%50:02d}"}}


def client(port, timeout=60):
    return http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)


def request(port, method, path, body=None, headers=None):
    c = client(port)
    try:
        c.request(method, path, body=body, headers=headers or {"content-type": "application/json"})
        r = c.getresponse()
        return r.status, r.read(), dict((k.lower(), v) for k, v in r.getheaders())
    finally:
        c.close()


def post(port, path, body, headers=None):
    status, data, _ = request(port, "POST", path, body, headers)
    return status, data


def alert_counters(port):
    status, data, _ = request(port, "GET", "/metrics")
    if status != 200:
        raise RuntimeError(f"could not read benchmark counters: HTTP {status}")
    counters = {}
    for line in data.decode().splitlines():
        if line.startswith(("verdigris_alert_evaluations_total{", "verdigris_alert_evaluation_pass_failures_total ")):
            name, value = line.split()
            counters[name] = int(value)
    return counters


def fixed_mix(executor, run, overload, rates, duration, max_inflight):
    """Queue arrivals against a monotonic schedule, independent of replies."""
    start = time.monotonic()
    pending = set()
    schedule = [(0.0, kind, 0) for kind in rates]
    heapq.heapify(schedule)
    while schedule:
        offset, kind, seq = heapq.heappop(schedule)
        if offset >= duration:
            break
        arrival = start + offset
        remaining = arrival - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        done = {future for future in pending if future.done()}
        for future in done:
            future.result()
        pending.difference_update(done)
        if len(pending) == max_inflight:
            overload(kind, arrival)
        else:
            pending.add(executor.submit(run, kind, arrival, seq))
        heapq.heappush(schedule, ((seq + 1) / rates[kind], kind, seq + 1))
    for future in pending:
        future.result()


def rss_mib(text):
    text = text.strip().split()[0]
    factor = 1024 if text.endswith(("GiB", "GB")) else 1
    return float(text.rstrip("MiBGB")) * factor


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--batches", type=int, default=100)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--heavy", type=int, default=10)
    ap.add_argument("--cheap", type=int, default=2)
    ap.add_argument("--trace", type=int, default=2)
    ap.add_argument("--expensive", type=int, default=0)
    ap.add_argument("--rounds", type=int, default=20)
    ap.add_argument("--duration", type=float, default=0, help="continuous seconds (overrides rounds)")
    ap.add_argument("--fixed-rates", action="store_true", help="use absolute arrivals with dashboard and alert traffic")
    ap.add_argument("--max-inflight", type=int, default=64, help="driver concurrency cap for fixed arrivals")
    ap.add_argument("--seed-only", action="store_true", help="ingest and verify visibility, then exit")
    ap.add_argument("--stats-container", action="append", default=[], help="container to sample for peak RSS")
    ap.add_argument("--reuse", action="store_true", help="reuse an already-ingested corpus")
    ap.add_argument("--ingest-probe-every", type=int, default=0,
                    help="append one identified row at this interval during a fixed-rate run")
    ap.add_argument("--out", required=True)
    ap.add_argument("--trial", default="")
    args = ap.parse_args()
    if args.fixed_rates and (args.duration <= 0 or args.max_inflight < 1):
        ap.error("--fixed-rates needs positive --duration and --max-inflight")
    if args.ingest_probe_every and (not args.fixed_rates or args.ingest_probe_every < 1):
        ap.error("--ingest-probe-every needs a positive interval and --fixed-rates")
    per = max(1, math.ceil(args.rows / args.batches))

    def send(b):
        start = b * per
        end = min(args.rows, start + per)
        body = ("\n".join(json.dumps(record(i, args.rows), separators=(",", ":")) for i in range(start, end)) + "\n").encode()
        return post(args.port, "/v1/ingest", body, {"content-type": "application/x-ndjson"})[0]

    if not args.reuse:
        t0 = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as ex:
            codes = list(ex.map(send, range(args.batches)))
        if any(c != 200 for c in codes):
            raise SystemExit(f"ingest codes {set(codes)}")
        print(f"ingested {args.rows} rows in {time.perf_counter()-t0:.1f}s", flush=True)

    def count():
        body = json.dumps({"sql": "SELECT count(*) AS n FROM bench", "histogram": False}).encode()
        s, d = post(args.port, "/v1/query", body)
        return json.loads(d)["rows"][0]["n"] if s == 200 else -1

    deadline = time.time() + 60
    while time.time() < deadline and count() != args.rows:
        time.sleep(1)
    visible = count()
    print("visible", visible, flush=True)
    if visible != args.rows:
        raise SystemExit(f"expected {args.rows} visible rows, got {visible}")
    if args.seed_only:
        return

    heavy_sql = "SELECT service, count(*) AS c FROM bench GROUP BY service"
    expensive_sql = "SELECT * FROM bench LIMIT 10"
    # A small window over one compacted file is genuinely cheap; a full-scan
    # LIMIT 10 is covered by the expensive class and must not use this lane.
    cheap_sql = "service:checkout level:ERROR | last 20m"
    lock = threading.Lock()
    results = {kind: [] for kind in ("heavy", "cheap", "trace", "expensive", "dashboard", "alert")}
    trace_cursor = 0
    created_alerts = []
    dashboard_observations = []
    dashboard_bucket_seconds = max(1, ((args.rows - 1) * WINDOW_MS // args.rows) // 60_000)
    rates = FIXED_RATES

    def run(kind, arrival=None, seq=None):
        nonlocal trace_cursor
        start = time.monotonic()
        headers = {"content-type": "application/json"}
        if kind == "dashboard":
            method, path, body = "GET", "/v1/metrics", None
        elif kind == "alert":
            method, path = "POST", "/v1/alerts"
            body = json.dumps({
                "name": f"admission benchmark {args.trial}/{seq}",
                "sql": "SELECT count(*) AS v FROM bench WHERE level = 'ERROR'",
                "threshold": 1000000000,
            }).encode()
        else:
            method, path = "POST", "/v1/query"
            if kind == "heavy":
                sql = heavy_sql
            elif kind == "expensive":
                sql = expensive_sql
            elif kind == "cheap":
                sql = cheap_sql
            else:
                if seq is None:
                    with lock:
                        seq = trace_cursor
                        trace_cursor += 1
                i = (seq * 7919) % args.rows
                # Exercise the product query syntax used by the trace benchmark.
                sql = f"trace_id:{i:016x}"
                if args.fixed_rates:
                    headers["x-vdg-query-profile"] = "1"
            body = json.dumps({"sql": sql, "histogram": False}).encode()

        phases = {}
        detail = None
        alert_id = None
        dashboard_sample = None
        try:
            status, data, reply_headers = request(args.port, method, path, body, headers)
            elapsed = time.monotonic() - start
            if kind == "trace" and args.fixed_rates and status == 200:
                payload = json.loads(data)
                if len(payload.get("rows", [])) != 1 or payload["rows"][0]["trace_id"] != f"{i:016x}":
                    raise RuntimeError("trace returned a different result")
                phases = json.loads(reply_headers["x-vdg-query-profile"])["phases_us"]
            if kind == "alert" and status == 201:
                alert_id = json.loads(data)["id"]
            if kind == "dashboard" and status == 200:
                payload = json.loads(data)
                if (len(payload.get("volumeByService", [])) != len(SERVICES)
                    or not payload.get("ingestRate")
                    or len(payload["ingestRate"]) != len(payload.get("errorRate", []))):
                    status, detail = 0, "dashboard_incomplete"
                elif args.ingest_probe_every:
                    chart_rows = round(math.fsum(payload["ingestRate"]) * dashboard_bucket_seconds)
                    dashboard_sample = {"start_s": start, "end_s": start + elapsed, "rows": chart_rows}
            if detail is None and status not in (200, 201, 429):
                detail = data[:200].decode(errors="replace")
        except (OSError, http.client.HTTPException) as exc:
            status, detail = 0, type(exc).__name__
            elapsed = time.monotonic() - start
        with lock:
            if dashboard_sample is not None:
                dashboard_observations.append(dashboard_sample)
            if alert_id:
                created_alerts.append(alert_id)
            results[kind].append((status, (elapsed + start - (arrival or start)) * 1000,
                                  elapsed * 1000, phases, detail))

    def overloaded(kind, arrival):
        with lock:
            results[kind].append((0, (time.monotonic() - arrival) * 1000, None, {},
                                  "driver_inflight_limit"))

    peak = {}
    stop = threading.Event()

    def sample_rss():
        while not stop.wait(1):
            for c in args.stats_container:
                try:
                    out = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", c],
                        capture_output=True, text=True, timeout=5,
                    )
                    if out.returncode == 0 and out.stdout.strip():
                        peak[c] = max(peak.get(c, 0.0), rss_mib(out.stdout))
                except Exception:
                    pass

    sampler = threading.Thread(target=sample_rss, daemon=True)
    alert_start = alert_counters(args.port) if args.fixed_rates else {}
    alert_end = {}
    probe_rows = []

    def ingest_probes():
        started = time.monotonic()
        for offset in range(args.ingest_probe_every, int(args.duration), args.ingest_probe_every):
            time.sleep(max(0, started + offset - time.monotonic()))
            row = record(offset, args.rows)
            row["message"] = f"mixed-ingest freshness probe {offset}"
            row["trace_id"] = f"{(1 << 64) - offset:016x}"
            row["ts_millis"] = BASE + WINDOW_MS // 2
            sent = time.monotonic()
            status, data = post(args.port, "/v1/ingest", (json.dumps(row) + "\n").encode(),
                                {"content-type": "application/x-ndjson"})
            if status != 200 or json.loads(data).get("ingested") != 1:
                raise RuntimeError(f"ingest probe failed at {offset}s: HTTP {status}")
            probe_rows.append({"offset": offset, "trace_id": row["trace_id"],
                               "sent_s": sent, "acknowledged_s": time.monotonic()})

    t0 = time.perf_counter()
    if args.fixed_rates:
        sampler.start()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_inflight) as ex:
                if args.ingest_probe_every:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as writer:
                        probes = writer.submit(ingest_probes)
                        fixed_mix(ex, run, overloaded, rates, args.duration, args.max_inflight)
                        probes.result()
                else:
                    fixed_mix(ex, run, overloaded, rates, args.duration, args.max_inflight)
            alert_end = alert_counters(args.port)
        finally:
            stop.set()
            sampler.join(timeout=2)
            for alert_id in created_alerts:
                status, _, _ = request(args.port, "DELETE", f"/v1/alerts/{alert_id}")
                if status != 200:
                    raise RuntimeError(f"could not clean up benchmark alert {alert_id}: {status}")
    elif args.duration > 0:
        deadline = time.monotonic() + args.duration

        def loop(kind):
            while time.monotonic() < deadline:
                run(kind)

        sampler.start()
        workers = args.heavy + args.cheap + args.trace + args.expensive
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = (
                [ex.submit(loop, "heavy") for _ in range(args.heavy)]
                + [ex.submit(loop, "cheap") for _ in range(args.cheap)]
                + [ex.submit(loop, "trace") for _ in range(args.trace)]
                + [ex.submit(loop, "expensive") for _ in range(args.expensive)]
            )
            for f in futs:
                f.result()
        stop.set()
        sampler.join(timeout=2)
    else:
        for _ in range(args.rounds):
            workers = args.heavy + args.cheap + args.trace + args.expensive
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
                futs = (
                    [ex.submit(run, "heavy") for _ in range(args.heavy)]
                    + [ex.submit(run, "cheap") for _ in range(args.cheap)]
                    + [ex.submit(run, "trace") for _ in range(args.trace)]
                    + [ex.submit(run, "expensive") for _ in range(args.expensive)]
                )
                for f in futs:
                    f.result()
    elapsed = time.perf_counter() - t0

    def percentile(lat, q):
        return lat[math.ceil(q * len(lat)) - 1] if lat else None

    observed_seconds = args.duration if args.fixed_rates else elapsed

    def summarize(kind):
        vals = results[kind]
        expected = 201 if kind == "alert" else 200
        ok = sorted(d for s, d, _, _, _ in vals if s == expected)
        shed = sum(s == 429 for s, _, _, _, _ in vals)
        phases = {}
        for phase in ("admission", "execute", "audit_wait"):
            times = sorted(p[phase] / 1000 for s, _, _, p, _ in vals if s == expected and phase in p)
            if times:
                phases[phase] = {"p50_ms": statistics.median(times), "p95_ms": percentile(times, .95)}
        return {"attempts": len(vals), "ok": len(ok), "shed_429": shed,
                "other": len(vals) - len(ok) - shed, "offered_per_s": len(vals) / observed_seconds,
                "ok_per_s": len(ok) / observed_seconds, "p50_ms": statistics.median(ok) if ok else None,
                "p95_ms": percentile(ok, .95), "p99_ms": percentile(ok, .99),
                "max_ms": ok[-1] if ok else None, "phases": phases}

    if alert_start.keys() != alert_end.keys() or any(alert_end[k] < v for k, v in alert_start.items()):
        raise RuntimeError("background alert counters reset during the trial")
    alert_deltas = {name: alert_end[name] - before for name, before in alert_start.items()}
    kinds = tuple(results) if args.fixed_rates else ("heavy", "cheap", "trace", "expensive")
    out = {"rows": args.rows, "base_ts_ms": BASE, "elapsed_s": elapsed,
           "duration_s": args.duration if args.duration > 0 else None,
           "rounds": None if args.duration > 0 else args.rounds, "trial": args.trial,
           "fixed_rates": rates if args.fixed_rates else None,
           "background_alert_counters": alert_deltas or None,
           **{kind: summarize(kind) for kind in kinds}, "peak_rss_mib": peak,
           "raw": {kind: [{"status": s, "scheduled_ms": round(d, 3), "service_ms": svc,
                            "phases_us": p, "error": error}
                           for s, d, svc, p, error in results[kind]] for kind in kinds}}
    if args.ingest_probe_every:
        for probe in probe_rows:
            body = json.dumps({"sql": f"trace_id:{probe['trace_id']}", "histogram": False}).encode()
            status, data = post(args.port, "/v1/query", body)
            if status != 200 or [r["trace_id"] for r in json.loads(data)["rows"]] != [probe["trace_id"]]:
                raise RuntimeError(f"acknowledged probe {probe['trace_id']} was not visible")
        for sample in dashboard_observations:
            before = sum(p["acknowledged_s"] <= sample["start_s"] for p in probe_rows)
            during = sum(p["sent_s"] <= sample["end_s"] for p in probe_rows)
            sample["expected_min"] = args.rows + before
            sample["expected_max"] = args.rows + during
            if not sample["expected_min"] <= sample["rows"] <= sample["expected_max"]:
                raise RuntimeError(f"dashboard returned a stale or mixed snapshot: {sample}")
        out["committed_probe_rows"] = probe_rows
        out["dashboard_snapshot_checks"] = dashboard_observations
    print(json.dumps(out, indent=2), flush=True)
    open(args.out, "w").write(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
