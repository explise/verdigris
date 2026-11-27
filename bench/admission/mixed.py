#!/usr/bin/env python3
"""Mixed-load admission check: heavy aggregates, cheap searches and point lookups."""
import argparse, concurrent.futures, http.client, json, os, statistics, subprocess, threading, time

SERVICES = ["gateway", "checkout", "auth", "billing", "search", "catalog", "ingest", "query", "alerts", "settings"]
BASE = int(os.environ.get("MIXED_BASE_TS_MS", str((int(time.time() * 1000) - 200 * 60_000) // 60_000 * 60_000)))
WINDOW_MS = 200 * 60_000


def record(i, rows):
    svc = SERVICES[i % len(SERVICES)]
    lvl = "ERROR" if i % 17 == 0 else ("WARN" if i % 7 == 0 else "INFO")
    ts = BASE + (i * WINDOW_MS) // max(1, rows)
    return {"ts_millis": ts, "level": lvl, "service": svc, "status": 500 if lvl == "ERROR" else 200,
            "message": f"{lvl.lower()} request {i} route=/{svc}", "trace_id": f"{i:016x}",
            "attrs": {"region": "us-east-1", "host": f"node-{i%50:02d}"}}


def client(port, timeout=60):
    return http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)


def post(port, path, body, headers=None):
    c = client(port)
    c.request("POST", path, body=body, headers=headers or {"content-type": "application/json"})
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, data


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
    ap.add_argument("--stats-container", action="append", default=[], help="container to sample for peak RSS")
    ap.add_argument("--reuse", action="store_true", help="reuse an already-ingested corpus")
    ap.add_argument("--out", required=True)
    ap.add_argument("--trial", default="")
    args = ap.parse_args()
    per = max(1, args.rows // args.batches)

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
    print("visible", count(), flush=True)

    heavy_sql = "SELECT service, count(*) AS c FROM bench GROUP BY service"
    expensive_sql = "SELECT * FROM bench LIMIT 10"
    # A small window over one compacted file is genuinely cheap; a full-scan
    # LIMIT 10 is covered by the expensive class and must not use this lane.
    cheap_sql = "service:checkout level:ERROR | last 20m"
    lock = threading.Lock()
    results = {"heavy": [], "cheap": [], "trace": [], "expensive": []}
    trace_cursor = [0]

    def run(kind):
        if kind == "heavy":
            sql = heavy_sql
        elif kind == "expensive":
            sql = expensive_sql
        elif kind == "cheap":
            sql = cheap_sql
        else:
            i = (trace_cursor[0] * 7919) % args.rows
            trace_cursor[0] += 1
            sql = f"SELECT * FROM bench WHERE trace_id = '{i:016x}' LIMIT 10"
        body = json.dumps({"sql": sql, "histogram": False}).encode()
        t = time.perf_counter()
        status, data = post(args.port, "/v1/query", body)
        dt = (time.perf_counter() - t) * 1000
        with lock:
            results[kind].append((status, dt, data[:200]))

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
    t0 = time.perf_counter()
    if args.duration > 0:
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
        return lat[max(0, int(q * len(lat)) - 1)] if lat else None

    def summarize(kind):
        vals = results[kind]
        ok = [d for s, d, _ in vals if s == 200]
        shed = [s for s, _, _ in vals if s == 429]
        lat = sorted(ok)
        return {"attempts": len(vals), "ok": len(ok), "shed_429": len(shed), "other": len(vals) - len(ok) - len(shed),
                "offered_per_s": len(vals) / elapsed if elapsed else None,
                "ok_per_s": len(ok) / elapsed if elapsed else None,
                "p50_ms": statistics.median(lat) if lat else None,
                "p95_ms": percentile(lat, .95), "p99_ms": percentile(lat, .99),
                "max_ms": lat[-1] if lat else None}

    out = {"rows": args.rows, "base_ts_ms": BASE, "elapsed_s": elapsed,
           "duration_s": args.duration if args.duration > 0 else None,
           "rounds": None if args.duration > 0 else args.rounds, "trial": args.trial,
           "offered": {k: len(v) / elapsed if elapsed else 0 for k, v in results.items()},
           "heavy": summarize("heavy"), "cheap": summarize("cheap"),
           "trace": summarize("trace"), "expensive": summarize("expensive"),
           "peak_rss_mib": peak,
           "raw": {k: [(s, round(d, 1)) for s, d, _ in v] for k, v in results.items()}}
    print(json.dumps(out, indent=2), flush=True)
    open(args.out, "w").write(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
