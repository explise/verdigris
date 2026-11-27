"""Paired HTTP benchmark for the feature-gated exact minute summary."""
import argparse
import json
from pathlib import Path
import time

from compare import BASE_TS, MINUTE_AGGREGATE_SQL, Vdg, bucket_ms, record
from profile import percentiles


def run(args):
    backend = Vdg(argparse.Namespace(host="127.0.0.1", port=args.port))
    content = {"content-type": "application/json"}
    sql = MINUTE_AGGREGATE_SQL + (" DESC" if args.case == "unsupported" else "")

    def request(method, path, payload=None, headers=None):
        status, response_headers, body = backend.client.request(method, path, payload, headers)
        if status != 200:
            raise RuntimeError(f"{method} {path}: HTTP {status} {body[:350]!r}")
        return response_headers, json.loads(body)

    body = json.dumps({"sql": sql, "histogram": False, "tiers": []}).encode()
    quote = request("POST", "/v1/query/estimate", body, content)[1]
    audit_before = request("GET", "/v1/metrics")[1]["audit"]
    requests = 0

    def query(use_summary, profiled=False, expect_summary=False):
        nonlocal requests
        headers = dict(content)
        if use_summary:
            headers["x-vdg-bench-minute-summary"] = "1"
        if profiled:
            headers["x-vdg-query-profile"] = "1"
        start = time.perf_counter_ns()
        response_headers, response = request("POST", "/v1/query", body, headers)
        elapsed_ms = (time.perf_counter_ns() - start) / 1e6
        requests += 1
        stats = response["stats"]
        summary_bytes = stats.get("benchmarkSummaryBytes")
        summary_gets = stats.get("benchmarkSummaryGets", 0)
        if (summary_bytes is not None) != expect_summary or summary_gets != int(expect_summary):
            raise RuntimeError(f"{args.case} selected the wrong execution path")
        if expect_summary and not 0 < summary_bytes <= min(64 * 1024, quote["scanBytes"] // 100):
            raise RuntimeError("summary exceeded its auxiliary byte guardrail")
        if stats["files"] != quote["filesTouched"] or stats["scannedBytes"] != quote["scanBytes"]:
            raise RuntimeError(f"{args.case} did not match its approved raw scan scope")
        if stats.get("auditMs") is None or response["histogram"] != []:
            raise RuntimeError(f"{args.case} changed audit or histogram behavior")
        sample = {"elapsed_ms": elapsed_ms, "summary_bytes": summary_bytes,
                  "summary_gets": summary_gets, "audit_ms": stats["auditMs"]}
        if profiled:
            value = next(v for k, v in response_headers if k.lower() == "x-vdg-query-profile")
            sample["profile"] = json.loads(value)
        return response["rows"], sample

    reloaded = args.case in ("restart", "rebuild")
    if reloaded:
        rows, result = query(True, profiled=True, expect_summary=True)
    expected, _ = query(False)
    if args.case == "late":
        row = record(0, 1, "vdg")
        ts = BASE_TS + 20 * 60_000 + 1
        row.update(ts_millis=ts, level="ERROR", status=500, trace_id="fffffffffffffffe",
                   message="benchmark minute summary late record")
        ack = request("POST", "/v1/ingest", (json.dumps(row) + "\n").encode(),
                      {"content-type": "application/x-ndjson"})[1]
        if ack.get("ingested") != 1:
            raise RuntimeError(f"late row was not acknowledged: {ack}")
        quote = request("POST", "/v1/query/estimate", body, content)[1]
        minute = ts // 60_000 * 60_000
        matching = [entry for entry in expected if bucket_ms(entry["bucket"]) == minute]
        if len(matching) != 1:
            raise RuntimeError("late fixture did not select exactly one existing bucket")
        matching[0]["total"] += 1
        matching[0]["errors"] += 1

    samples = {arm: {mode: [] for mode in ("off", "on")} for arm in ("raw", "summary")}
    if args.case == "warm":
        for i in range(args.iterations + args.warmup):
            for mode in ("off", "on") if i % 2 == 0 else ("on", "off"):
                for arm in ("raw", "summary") if i % 2 == 0 else ("summary", "raw"):
                    use_summary = arm == "summary"
                    rows, sample = query(use_summary, profiled=(mode == "on"), expect_summary=use_summary)
                    if rows != expected:
                        raise RuntimeError(f"{arm}/{mode} returned different ordered JSON rows")
                    if i >= args.warmup:
                        samples[arm][mode].append(sample)
        result = {
            arm: {mode: {"elapsed_ms": percentiles([s["elapsed_ms"] for s in cohort]), "samples": cohort}
                  for mode, cohort in modes.items()}
            for arm, modes in samples.items()
        }
    else:
        if not reloaded:
            rows, result = query(True, profiled=True)
        if rows != expected:
            raise RuntimeError(f"{args.case} returned different ordered JSON rows")

    audit = request("GET", "/v1/metrics")[1]["audit"]
    if (audit["ackedTotal"] - audit_before["ackedTotal"] != requests
        or any(audit[k] for k in ("failedTotal", "saturatedTotal", "timedOutTotal"))):
        raise RuntimeError(f"summary benchmark audit unhealthy: {audit_before=} {audit=}, {requests=}")
    backend.close()
    return {"case": args.case, "quote": quote, "rows": expected, "results": result,
            "audit_before": audit_before, "audit_after": audit}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--case", choices=("missing", "warm", "restart", "unsupported", "late", "rebuild", "corrupt"),
                   required=True)
    p.add_argument("--iterations", type=int, default=100)
    p.add_argument("--warmup", type=int, default=10)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    if args.iterations < 1 or args.warmup < 0:
        p.error("iterations must be positive and warmup cannot be negative")
    args.out.write_text(json.dumps(run(args), indent=2) + "\n")


if __name__ == "__main__":
    main()
