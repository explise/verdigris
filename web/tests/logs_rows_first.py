#!/usr/bin/env python3
"""Browser regression fixture for the Logs rows-first UI.

Covers the four states that matter for #74/#77:
  * rows render while the activity request is held open;
  * rows stay rendered when activity fails;
  * a late activity response for an older query is discarded;
  * cold-tier confirmation gates both the rows and activity requests.

The app is the production build (`web/dist`); API calls are served by a local
threaded fake so the test can hold or fail only the activity request.
"""
import json
import os
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1] / "dist"
DEFAULT_SQL = "service:auth status>=500 | last 1h"
LOCK = threading.Lock()
STATE = {
    "fail_activity": False,
    "hold": {},          # sql -> threading.Event
    "requests": [],      # (kind, sql)
}
TOTALS = {DEFAULT_SQL: 3, "STALE_A": 3, "STALE_B": 7, "COLD_QUERY": 5}

CONFIG = {
    "mode": "onprem",
    "apiBaseUrl": "",
    "useMocks": False,
    "wire": "json",
    "auth": {"kind": "none"},
    "orgs": [{"id": "acme", "name": "Acme"}],
    "environments": [
        {"id": "prod", "label": "prod", "region": "local", "bucket": "s3://logs"}
    ],
    "features": {},
}


def reset(fail_activity=False):
    with LOCK:
        STATE["fail_activity"] = fail_activity
        STATE["hold"] = {}
        STATE["requests"] = []


def record(kind, sql):
    with LOCK:
        STATE["requests"].append((kind, sql))


def request_count(kind, sql=None):
    with LOCK:
        return sum(
            1
            for k, s in STATE["requests"]
            if k == kind and (sql is None or s == sql)
        )


def wait_for(predicate, timeout=5.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, *_):
        pass

    def _json(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/config.json":
            return self._json(200, CONFIG)
        if self.path == "/" or not (ROOT / self.path.lstrip("/")).exists():
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            body = {}
        sql = str(body.get("sql", DEFAULT_SQL))

        if self.path.endswith("/v1/query/activity"):
            record("activity", sql)
            with LOCK:
                fail = STATE["fail_activity"]
                held = STATE["hold"].get(sql)
            if held is not None:
                # A held response is deliberately released by the caller.
                held.wait(timeout=30)
            if fail:
                return self._json(500, {"error": "activity unavailable"})
            total = TOTALS.get(sql, 3)
            return self._json(
                200,
                {
                    "histogram": [{"bucket": 0, "total": total, "errors": 1}],
                    "stats": {
                        "events": total,
                        "files": 1,
                        "elapsedMs": 1,
                        "engine": "datafusion",
                    },
                },
            )
        if self.path.endswith("/v1/query/estimate"):
            return self._json(
                200, {"scanGB": 0.5, "costUsd": 0.2, "coldRestore": True}
            )
        if self.path.endswith("/v1/query"):
            record("rows", sql)
            rows = [
                {
                    "ts": "2026-11-27T00:00:01.000Z",
                    "level": "ERROR",
                    "service": "checkout",
                    "status": 500,
                    "message": f"{sql} failed 1",
                    "trace_id": "a1",
                    "attrs_json": '{"region":"us-east-1","host":"node-01"}',
                },
                {
                    "ts": "2026-11-27T00:00:02.000Z",
                    "level": "ERROR",
                    "service": "checkout",
                    "status": 500,
                    "message": f"{sql} failed 2",
                    "trace_id": "a2",
                    "attrs_json": '{"region":"us-east-1","host":"node-02"}',
                },
            ]
            return self._json(
                200,
                {
                    "rows": rows,
                    "stats": {
                        "events": 2,
                        "scannedBytes": 10,
                        "elapsedMs": 1,
                        "engine": "datafusion",
                        "files": 1,
                        "eventsExact": False,
                    },
                    "histogram": [],
                },
            )
        return self._json(404, {"error": "not found"})


def open_app(browser, port):
    page = browser.new_page()
    page.goto(f"http://127.0.0.1:{port}/acme/prod/logs", wait_until="domcontentloaded")
    page.wait_for_selector(".vrow", timeout=10000)
    return page


def wait_rows(page, marker):
    page.wait_for_function(
        "marker => Array.from(document.querySelectorAll('.vrow'))"
        ".some(e => e.innerText.includes(marker))",
        arg=marker,
        timeout=10000,
    )


def chart_text(page):
    return page.locator(".histo").inner_text()


def test_held(browser, port):
    reset()
    with LOCK:
        ev = threading.Event()
        STATE["hold"][DEFAULT_SQL] = ev
    page = open_app(browser, port)
    try:
        assert "2 rows" in page.locator(".logs-foot").inner_text()
        wait_for(lambda: "loading activity" in chart_text(page), what="chart loading")
        ev.set()
        page.wait_for_function(
            "() => document.querySelector('.histo')?.innerText.includes('3 events in chart range')",
            timeout=10000,
        )
    finally:
        ev.set()
        page.close()


def test_failure(browser, port):
    reset(fail_activity=True)
    page = open_app(browser, port)
    try:
        assert "2 rows" in page.locator(".logs-foot").inner_text()
        page.wait_for_function(
            "() => document.querySelector('.histo')?.innerText.includes('activity unavailable')",
            timeout=10000,
        )
        assert page.locator(".vrow").count() >= 2
    finally:
        page.close()


def test_stale(browser, port):
    reset()
    with LOCK:
        ev = threading.Event()
        STATE["hold"]["STALE_A"] = ev
    page = open_app(browser, port)
    try:
        page.fill(".query-input input", "STALE_A")
        page.locator(".querybar button.primary").click()
        wait_rows(page, "STALE_A")
        wait_for(
            lambda: request_count("activity", "STALE_A") == 1,
            what="held activity request A",
        )

        page.fill(".query-input input", "STALE_B")
        page.locator(".querybar button.primary").click()
        wait_rows(page, "STALE_B")
        page.wait_for_function(
            "() => document.querySelector('.histo')?.innerText.includes('7 events in chart range')",
            timeout=10000,
        )

        ev.set()  # late response for A must not replace B's chart
        time.sleep(0.3)
        assert "7 events in chart range" in chart_text(page)
        assert "STALE_B failed" in page.locator(".vrow").first.inner_text()
    finally:
        ev.set()
        page.close()


def test_cold_gate(browser, port):
    reset()
    page = open_app(browser, port)
    try:
        page.locator("[data-tier=cold]").click()
        page.fill(".query-input input", "COLD_QUERY")
        page.locator(".querybar button.primary").click()
        page.wait_for_selector(".modal", timeout=10000)
        assert "This query scans cold storage" in page.locator(".modal").inner_text()
        assert request_count("rows", "COLD_QUERY") == 0
        assert request_count("activity", "COLD_QUERY") == 0

        page.get_by_role("button", name="Restore & run").click()
        wait_rows(page, "COLD_QUERY")
        page.wait_for_function(
            "() => document.querySelector('.histo')?.innerText.includes('5 events in chart range')",
            timeout=10000,
        )
        assert request_count("rows", "COLD_QUERY") == 1
        assert request_count("activity", "COLD_QUERY") == 1
    finally:
        page.close()


def main():
    if not (ROOT / "index.html").exists():
        raise SystemExit("build the web app first: npm --prefix web run build")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    executable = os.environ.get("CHROMIUM")
    try:
        with sync_playwright() as p:
            launch = {"headless": True}
            if executable:
                launch["executable_path"] = executable
            browser = p.chromium.launch(**launch)
            try:
                test_held(browser, port)
                test_failure(browser, port)
                test_stale(browser, port)
                test_cold_gate(browser, port)
            finally:
                browser.close()
    finally:
        server.shutdown()
    print("logs rows-first browser fixture: held/failed/stale/cold OK")


if __name__ == "__main__":
    main()
