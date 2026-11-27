#!/usr/bin/env python3
"""First query after a process restart, one measured shape at a time."""
import argparse, base64, http.client, json, time, urllib.parse

SHAPES = ("trace", "search", "aggregate")


def post(port, path, body, headers):
    c = http.client.HTTPConnection("127.0.0.1", port, timeout=120)
    t = time.perf_counter()
    c.request("POST", path, body=body, headers=headers)
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, (time.perf_counter() - t) * 1000, data


def run_shape(system, port, rows, shape):
    trace = f"{rows // 2:016x}"
    if system == "vdg":
        sql = {
            "trace": f"SELECT * FROM bench WHERE trace_id = '{trace}' LIMIT 10",
            "search": "SELECT * FROM bench WHERE service = 'checkout' AND level = 'ERROR' ORDER BY ts DESC LIMIT 200",
            "aggregate": "SELECT date_bin(INTERVAL '60 seconds', ts, TIMESTAMP '1970-01-01T00:00:00') AS bucket, count(*) AS total FROM bench GROUP BY bucket ORDER BY bucket",
        }[shape]
        status, ms, data = post(
            port, "/v1/query", json.dumps({"sql": sql, "histogram": False}).encode(),
            {"content-type": "application/json"},
        )
        rows_out = len(json.loads(data).get("rows", [])) if status == 200 else 0
    elif system == "clickhouse":
        sql = {
            "trace": f"SELECT * FROM bench WHERE trace_id = '{trace}' LIMIT 10",
            "search": "SELECT * FROM bench WHERE service = 'checkout' AND level = 'ERROR' ORDER BY ts DESC LIMIT 200",
            "aggregate": "SELECT toStartOfInterval(ts, INTERVAL 60 SECOND) AS bucket, count() AS total FROM bench GROUP BY bucket ORDER BY bucket",
        }[shape]
        status, ms, data = post(
            port, "/", sql + " FORMAT JSON",
            {"Authorization": "Basic " + base64.b64encode(b"default:benchpass").decode()},
        )
        rows_out = len(json.loads(data)["data"]) if status == 200 else 0
    else:
        query = {
            "trace": f'trace_id:="{trace}" | fields _time, level, service, _msg, trace_id, attrs_json | limit 10',
            "search": "service:checkout level:ERROR | fields _time, level, service, _msg, trace_id, attrs_json | sort by (_time) desc | limit 200",
            "aggregate": "* | stats by (_time:1m) count() total",
        }[shape]
        status, ms, data = post(
            port, "/select/logsql/query", urllib.parse.urlencode({"query": query}).encode(),
            {"Content-Type": "application/x-www-form-urlencoded"},
        )
        rows_out = len([x for x in data.splitlines() if x.strip()]) if status == 200 else 0
    return {"status": status, "ms": ms, "rows": rows_out}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True, choices=["vdg", "clickhouse", "victorialogs"])
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--shape", choices=SHAPES, help="one shape; omit for all")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    shapes = (args.shape,) if args.shape else SHAPES
    out = {shape: run_shape(args.system, args.port, args.rows, shape) for shape in shapes}
    open(args.out, "w").write(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
