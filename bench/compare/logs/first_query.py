#!/usr/bin/env python3
"""First query after a process restart: one trace, one search, one aggregation."""
import argparse, base64, http.client, json, time, urllib.parse

def post(port, path, body, headers):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    t = time.perf_counter()
    c.request("POST", path, body=body, headers=headers)
    r = c.getresponse(); data = r.read(); c.close()
    return r.status, (time.perf_counter() - t) * 1000, data

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True, choices=["vdg", "clickhouse", "victorialogs"])
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    trace = f"{args.rows // 2:016x}"
    out = {}
    if args.system == "vdg":
        def q(sql):
            return post(args.port, "/v1/query",
                        json.dumps({"sql": sql, "histogram": False}).encode(),
                        {"content-type": "application/json"})
        for name, sql in {
            "trace": f"SELECT * FROM bench WHERE trace_id = '{trace}' LIMIT 10",
            "search": "SELECT * FROM bench WHERE service = 'checkout' AND level = 'ERROR' ORDER BY ts DESC LIMIT 200",
            "aggregate": "SELECT date_bin(INTERVAL '60 seconds', ts, TIMESTAMP '1970-01-01T00:00:00') AS bucket, count(*) AS total FROM bench GROUP BY bucket ORDER BY bucket",
        }.items():
            status, ms, data = q(sql)
            out[name] = {"status": status, "ms": ms, "rows": len(json.loads(data).get("rows", [])) if status == 200 else 0}
    elif args.system == "clickhouse":
        auth = {"Authorization": "Basic " + base64.b64encode(b"default:benchpass").decode()}
        for name, sql in {
            "trace": f"SELECT * FROM bench WHERE trace_id = '{trace}' LIMIT 10",
            "search": "SELECT * FROM bench WHERE service = 'checkout' AND level = 'ERROR' ORDER BY ts DESC LIMIT 200",
            "aggregate": "SELECT toStartOfInterval(ts, INTERVAL 60 SECOND) AS bucket, count() AS total FROM bench GROUP BY bucket ORDER BY bucket",
        }.items():
            status, ms, data = post(args.port, "/", sql + " FORMAT JSON", auth)
            out[name] = {"status": status, "ms": ms, "rows": len(json.loads(data)["data"]) if status == 200 else 0}
    else:
        for name, query in {
            "trace": f'trace_id:="{trace}" | fields _time, level, service, _msg, trace_id, attrs_json | limit 10',
            "search": "service:checkout level:ERROR | fields _time, level, service, _msg, trace_id, attrs_json | sort by (_time) desc | limit 200",
            "aggregate": "* | stats by (_time:1m) count() total",
        }.items():
            body = urllib.parse.urlencode({"query": query}).encode()
            status, ms, data = post(args.port, "/select/logsql/query", body,
                                    {"Content-Type": "application/x-www-form-urlencoded"})
            out[name] = {"status": status, "ms": ms, "rows": len([x for x in data.splitlines() if x.strip()]) if status == 200 else 0}
    open(args.out, "w").write(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))

if __name__ == "__main__": main()
