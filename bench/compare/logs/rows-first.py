#!/usr/bin/env python3
"""Measure first-page API, scan I/O and browser rendering on one seeded corpus."""

import argparse
import json
import time
from pathlib import Path

from compare import BASE_TS, RECENT_SEARCH_SQL, HttpClient, expected_search, validate_search
from profile import percentiles

TIERS = ["hot", "warm"]
READ_KEYS = ("gets", "heads", "range_batches", "ranges", "result_bytes")
AUDIT_ERRORS = ("failedTotal", "saturatedTotal", "timedOutTotal")


def request(client, path, payload=None, profile=False):
    headers = {"content-type": "application/json"}
    if profile:
        headers["x-vdg-query-profile"] = "1"
    started = time.perf_counter_ns()
    status, response_headers, body = client.request(
        "GET" if payload is None else "POST", path,
        None if payload is None else json.dumps(payload).encode(), headers,
    )
    elapsed = (time.perf_counter_ns() - started) / 1e6
    if status != 200:
        raise RuntimeError(f"{path} returned {status}: {body[:300]!r}")
    trace = next((v for k, v in response_headers if k.lower() == "x-vdg-query-profile"), None)
    if profile and trace is None:
        raise RuntimeError(f"{path} did not return the requested profile")
    return json.loads(body), json.loads(trace) if trace else None, elapsed


def api_samples(client, expected, iterations, warmup, profiled):
    samples = {"rows": [], "combined": [], "activity": []}
    query = {"sql": RECENT_SEARCH_SQL, "tiers": TIERS}
    cases = {
        "rows": ("/v1/query", dict(query, histogram=False)),
        "combined": ("/v1/query", query),  # Exercise the legacy default.
        "activity": ("/v1/query/activity", query),
    }
    for i in range(iterations + warmup):
        payloads = {}
        order = tuple(cases) if i % 2 == 0 else tuple(reversed(cases))
        for name in order:
            path, body = cases[name]
            payload, profile, elapsed = request(client, path, body, profiled)
            payloads[name] = payload
            if profile is not None:
                if profile["executions"] != 1 or ("histogram" in profile) != (name == "combined"):
                    raise RuntimeError(f"{name}: unexpected execution count or histogram profile")
                if name == "combined" and profile["histogram"]["executions"] != 1:
                    raise RuntimeError("combined: histogram did not execute exactly once")
            if i >= warmup:
                samples[name].append({"ms": elapsed, "profile": profile})
        rows, combined, activity = (payloads[k] for k in cases)
        validate_search(rows["rows"], expected)
        validate_search(combined["rows"], expected)
        if rows["histogram"] or rows["stats"]["eventsExact"] or rows["stats"]["events"] != len(expected):
            raise RuntimeError("rows-only response includes an unexpected histogram or row count")
        if "rows" in activity or activity["histogram"] != combined["histogram"]:
            raise RuntimeError("activity response differs from the legacy histogram")
        total = sum(bucket["total"] for bucket in activity["histogram"])
        if (not combined["stats"]["eventsExact"] or combined["stats"]["events"] != total
                or activity["stats"]["events"] != total or total <= len(expected)):
            raise RuntimeError("fixture does not distinguish page rows from selected-file activity")
    summary = {}
    for name, values in samples.items():
        summary[name] = {"latency_ms": percentiles([s["ms"] for s in values]), "samples": values}
        if not profiled:
            continue
        profiles = [s["profile"] for s in values]
        reads = {key: [p["object_reads"][key] for p in profiles] for key in READ_KEYS}
        summary[name]["object_reads"] = {key: percentiles(v) for key, v in reads.items()}
        if name == "combined":
            histogram = {key: [p["histogram"]["object_reads"][key] for p in profiles]
                         for key in READ_KEYS}
            summary[name]["histogram_object_reads"] = {
                key: percentiles(v) for key, v in histogram.items()
            }
            summary[name]["combined_object_reads"] = {
                key: percentiles([a + b for a, b in zip(reads[key], histogram[key])])
                for key in READ_KEYS
            }
    return summary


def browser_samples(port, expected, iterations=10):
    from playwright.sync_api import sync_playwright

    results = []
    base = f"http://127.0.0.1:{port}"
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for _ in range(iterations):
            page = browser.new_page()
            try:
                page.goto(base + "/local/bench/logs", wait_until="networkidle")
                page.locator(".query-input input").fill(RECENT_SEARCH_SQL)
                with page.expect_response(lambda r: r.url == base + "/v1/query"
                                          and r.request.post_data_json.get("sql") == RECENT_SEARCH_SQL) as response:
                    elapsed = page.evaluate("""marker => new Promise((resolve, reject) => {
                        const started = performance.now();
                        const timeout = setTimeout(() => {
                            observer.disconnect();
                            reject(new Error("Expected first result did not become visible"));
                        }, 10000);
                        const observer = new MutationObserver(() => {
                            const first = document.querySelector(".vrow .c-msg");
                            if (first?.textContent === marker && first.getBoundingClientRect().height > 0) {
                                observer.disconnect();
                                clearTimeout(timeout);
                                requestAnimationFrame(() => requestAnimationFrame(() => {
                                    resolve(performance.now() - started);
                                }));
                            }
                        });
                        observer.observe(document.body, {subtree: true, childList: true, characterData: true});
                        document.querySelector(".querybar button").click();
                    })""", expected[0][4])
                res = response.value
                if (res.status != 200 or "application/vnd.apache.arrow.stream" not in
                        res.headers.get("content-type", "")):
                    raise RuntimeError("Browser failed to render the production Arrow wire")
                if res.request.post_data_json.get("histogram") is not False:
                    raise RuntimeError("Browser did not request the rows-only path")
                results.append(elapsed)
            finally:
                page.close()
        browser.close()
    return {"run_to_visible_ms": percentiles(results), "samples_ms": results, "wire": "arrow"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--seed-report", type=Path, required=True)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    corpus = json.loads(args.seed_report.read_text())["corpus"]
    if corpus["rows"] != 1000000 or corpus["base_ts_ms"] != BASE_TS or args.iterations < 100:
        parser.error("Expected the fixed 1M-row fixture and at least 100 warm requests per arm")
    expected = expected_search(corpus["rows"])
    client = HttpClient("127.0.0.1", args.port)
    before = request(client, "/v1/metrics")[0]["audit"]
    normal = api_samples(client, expected, args.iterations, args.warmup, False)
    profile = api_samples(client, expected, args.iterations, args.warmup, True)
    after = request(client, "/v1/metrics")[0]["audit"]
    expected_acks = 4 * (args.iterations + args.warmup)
    healthy = (after["ackedTotal"] - before["ackedTotal"] == expected_acks
               and all(after[k] == before[k] for k in AUDIT_ERRORS))
    out = {"corpus": corpus, "api": normal, "profile": profile,
           "audit": {"before": before, "after": after, "expected_acks": expected_acks},
           "api_gate_passed": healthy and normal["rows"]["latency_ms"]["p95"] <= 30}
    args.out.write_text(json.dumps(out, indent=2) + "\n")
    out["browser"] = browser_samples(args.port, expected)
    args.out.write_text(json.dumps(out, indent=2) + "\n")
    if not out["api_gate_passed"]:
        raise SystemExit("Rows-only p95 or durable audit missed the #74 gate")


if __name__ == "__main__":
    main()
