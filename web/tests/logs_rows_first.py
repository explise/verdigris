#!/usr/bin/env python3
"""Browser fixture for #77: rows render while the activity request is held."""
import json, os, shutil, threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1] / "dist"
activity_release = threading.Event()

ROWS = [
    {"ts": "2026-11-27T00:00:01.000Z", "level": "ERROR", "service": "checkout",
     "status": 500, "message": "checkout failed 1", "trace_id": "a1",
     "attrs_json": '{"region":"us-east-1","host":"node-01"}'},
    {"ts": "2026-11-27T00:00:02.000Z", "level": "ERROR", "service": "checkout",
     "status": 500, "message": "checkout failed 2", "trace_id": "a2",
     "attrs_json": '{"region":"us-east-1","host":"node-02"}'},
]

CONFIG = {
    "mode": "onprem", "apiBaseUrl": "", "useMocks": False, "wire": "json",
    "auth": {"kind": "none"},
    "orgs": [{"id": "acme", "name": "Acme"}],
    "environments": [{"id": "prod", "label": "prod", "region": "local", "bucket": "s3://logs"}],
    "features": {},
}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=str(ROOT), **kw)

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
        if not (ROOT / self.path.lstrip("/")).exists() or self.path == "/":
            self.path = "/index.html"
        return super().do_GET()

    def do_POST(self):
        length = int(self.headers.get("content-length", 0))
        body = self.rfile.read(length)
        if self.path.endswith("/v1/query/activity"):
            # Hold the chart open until the test has asserted rows are visible.
            activity_release.wait(timeout=30)
            return self._json(200, {"histogram": [{"total": 2, "errors": 2}],
                                    "stats": {"events": 2, "files": 1, "elapsedMs": 1,
                                              "engine": "datafusion"}})
        if self.path.endswith("/v1/query/estimate"):
            return self._json(200, {"scanGB": 0.001, "costUsd": 0, "coldRestore": False})
        if self.path.endswith("/v1/query"):
            return self._json(200, {
                "rows": ROWS,
                "stats": {"events": 2, "scannedBytes": 10, "elapsedMs": 1,
                          "engine": "datafusion", "files": 1, "eventsExact": False},
                "histogram": [],
            })
        return self._json(404, {"error": "not found"})


def main():
    if not (ROOT / "index.html").exists():
        raise SystemExit("build the web app first: npm --prefix web run build")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    port = server.server_address[1]
    executable = os.environ.get("CHROMIUM") or shutil.which("chromium")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, executable_path=executable)
        page = browser.new_page()
        page.goto(f"http://127.0.0.1:{port}/acme/prod/logs", wait_until="domcontentloaded")
        page.wait_for_selector(".vrow", timeout=10000)
        footer = page.locator(".logs-foot").inner_text()
        assert "2 rows" in footer, footer
        assert page.locator(".vrow").count() >= 2
        # The chart is still held; rows are already usable.
        assert "loading activity" in page.locator(".histo").inner_text()
        activity_release.set()
        page.wait_for_function(
            "() => document.querySelector('.histo')?.innerText.includes('events in chart range')",
            timeout=10000,
        )
        browser.close()
    server.shutdown()
    print("rows rendered while activity request was held: OK")


if __name__ == "__main__":
    main()
