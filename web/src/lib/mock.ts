/* ═══════════════════════════════════════════════════════════════════
   Mock implementation of the data contract. Used when config.useMocks is
   true (dev / demo / air-gapped). Mirrors web/src/lib/types.ts exactly so
   pages render identically whether data is mocked or live.
   ═══════════════════════════════════════════════════════════════════ */
import type {
  QueryResult, EstimateResult, Metrics, Alert, NewAlert, Storage, Cost, Pipelines, Settings, LogRow, Level, TierId,
} from "./types";
import { TIER_ECON } from "./types";

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
let _seed = 1337;
const rnd = () => { _seed = (_seed * 1103515245 + 12345) & 0x7fffffff; return _seed / 0x7fffffff; };
const ser = (n: number, base: number, amp: number, jit: number) =>
  Array.from({ length: n }, (_, i) => Math.max(0, base + Math.sin(i / (n / 6)) * amp + (rnd() - 0.5) * jit));

const SERVICES = ["auth", "checkout", "session-store", "gateway", "billing", "search", "notifier"];
const MSGS: Record<Level, string[]> = {
  ERROR: [
    "token validation failed: signature mismatch kid=v2 (expired 4m ago)",
    "upstream 503 from session-store: connection refused after 3 retries",
    "jwks refresh failed: 504 from idp.internal after 2000ms",
    "payment intent declined: card_declined (issuer 51) order=ord_8821",
  ],
  WARN: [
    "connection pool at 92% capacity (46/50) — shedding low-priority reads",
    "latency p99 1840ms exceeds 1500ms SLO over trailing 60s",
    "retry budget for session-store exhausted; failing open for /healthz only",
  ],
  INFO: [
    "rotated signing key kid=v3 (sha 9f2a…); v2 grace window 5m",
    "circuit breaker session-store → half-open, probing 1 req/s",
    "checkout completed order=ord_8822 amount=142.00 USD",
  ],
  DEBUG: ["cache hit ratio 0.94 over 10s window", "gc pause 12ms heap=512MB/1GB"],
};
const LEVELS: Level[] = ["ERROR", "ERROR", "WARN", "INFO", "ERROR", "INFO", "WARN", "DEBUG", "ERROR", "INFO"];

export function genLogs(n: number, svc?: string): LogRow[] {
  const out: LogRow[] = [];
  let ms = 14 * 3600e3 + 22 * 60e3 + 9e3 + 412;
  for (let i = 0; i < n; i++) {
    const level = LEVELS[Math.floor(rnd() * LEVELS.length)];
    const service = svc || SERVICES[Math.floor(rnd() * SERVICES.length)];
    ms -= Math.floor(rnd() * 380) + 40;
    const t = new Date(ms);
    const ts = `${String(t.getUTCHours()).padStart(2, "0")}:${String(t.getUTCMinutes()).padStart(2, "0")}:${String(t.getUTCSeconds()).padStart(2, "0")}.${String(t.getUTCMilliseconds()).padStart(3, "0")}`;
    out.push({
      ts, level, service, message: MSGS[level][Math.floor(rnd() * MSGS[level].length)],
      trace_id: "4ac9" + Math.floor(rnd() * 1e6).toString(16) + "d21",
      attrs: { pod: `${service}-${Math.floor(rnd() * 9)}f${Math.floor(rnd() * 90)}`, region: "us-east-1", status: level === "ERROR" ? 503 : 200 },
    });
  }
  return out;
}

const TIER_SCAN_GB: Record<TierId, number> = { hot: 0.21, warm: 4.2, cold: 38 };

export const mock = {
  async queryLogs(svc?: string): Promise<QueryResult> {
    await wait(140);
    return {
      rows: genLogs(2000, svc),  // virtualized table handles this many trivially
      stats: { events: 2481, scannedBytes: 210e6, elapsedMs: 412, engine: "datafusion", files: 38, wire: "arrow" },
      histogram: ser(60, 22, 14, 18).map((v) => ({ total: Math.round(v), errors: Math.round(v * (0.2 + rnd() * 0.4)) })),
    };
  },
  async estimate(tiers: TierId[]): Promise<EstimateResult> {
    let gb = 0, cost = 0;
    tiers.forEach((t) => { gb += TIER_SCAN_GB[t]; cost += TIER_SCAN_GB[t] * TIER_ECON[t].perGB; });
    return { scanGB: gb, costUsd: cost, coldRestore: tiers.includes("cold") };
  },
  async metrics(): Promise<Metrics> {
    await wait(100);
    return {
      ingestRate: ser(120, 1180, 320, 160), errorRate: ser(120, 6.1, 3.2, 1.6), p99: ser(120, 1240, 480, 220),
      volumeByService: SERVICES.map((s) => ({ name: s, gb: +(2 + rnd() * 9).toFixed(1) })),
      tiles: {
        ingest: { value: "1.21k", unit: "ev/s", delta: 6.4 }, errors: { value: "6.4", unit: "%", delta: 1.8 },
        p99: { value: "1.24", unit: "s", delta: -4.2 }, stored: { value: "412", unit: "GB", delta: 2.1 },
      },
    };
  },
  async createAlert(_body: NewAlert): Promise<{ id: string }> {
    await wait(80);
    return { id: "mock-" + Date.now() };
  },
  async deleteAlert(_id: string): Promise<{ removed: boolean }> {
    await wait(60);
    return { removed: true };
  },
  async alerts(): Promise<Alert[]> {
    await wait(80);
    return [
      { id: "a1", name: "auth error rate spike", state: "firing", severity: "critical", cond: "count(level='ERROR' AND service='auth') > 100 over 5m", value: "148", since: "4m ago", channel: "#oncall-auth" },
      { id: "a2", name: "p99 latency SLO breach", state: "firing", severity: "warning", cond: "p99(latency_ms) > 1500 over 10m", value: "1840ms", since: "11m ago", channel: "#sre" },
      { id: "a3", name: "session-store 5xx", state: "ok", severity: "critical", cond: "count(status>=500 AND service='session-store') > 50 over 5m", value: "12", since: "—", channel: "#oncall-platform" },
      { id: "a4", name: "ingest pipeline lag", state: "ok", severity: "warning", cond: "ingest_lag_seconds > 30", value: "3s", since: "—", channel: "#data-eng" },
      { id: "a5", name: "cold-tier query budget", state: "ok", severity: "info", cond: "sum(glacier_retrieval_usd) per day > $50", value: "$8.40", since: "—", channel: "#finops" },
      { id: "a6", name: "compaction backlog", state: "ok", severity: "warning", cond: "uncompacted_small_files > 500k", value: "182k", since: "—", channel: "#data-eng" },
    ];
  },
  async storage(): Promise<Storage> {
    await wait(100);
    return {
      tiers: [
        { id: "hot", name: "Hot", class: "S3 Standard", bytesGB: 412, objects: "1.1M", perMonth: 9.48, age: "0–3 days", pct: 18 },
        { id: "warm", name: "Warm", class: "Glacier Instant", bytesGB: 1840, objects: "640k", perMonth: 7.36, age: "3–30 days", pct: 34 },
        { id: "cold", name: "Cold", class: "Glacier Flexible", bytesGB: 9120, objects: "210k", perMonth: 32.8, age: "30+ days", pct: 48 },
      ],
      lifecycle: [
        { at: "after 3 days", action: "transition Hot → Warm (Glacier Instant)" },
        { at: "after 30 days", action: "transition Warm → Cold (Glacier Flexible)" },
        { at: "after 400 days", action: "expire (delete) — configurable" },
      ],
      compaction: { smallFiles: "182k", compacted: "38.2k", targetSize: "256 MB", lastRun: "2m ago", reclaimedGB: 41, status: "running" },
      totalGB: 11372, totalPerMonth: 49.64,
    };
  },
  async cost(): Promise<Cost> {
    await wait(90);
    return {
      monthToDate: 61.7, projected: 92.4, lastMonth: 88.1,
      breakdown: [
        { label: "Hot storage (S3 Standard)", usd: 9.48, color: "var(--hot)" },
        { label: "Warm storage (Glacier IR)", usd: 7.36, color: "var(--warm)" },
        { label: "Cold storage (Glacier Flex)", usd: 32.8, color: "var(--cold)" },
        { label: "Query compute (EKS)", usd: 8.4, color: "var(--copper)" },
        { label: "Glacier retrieval", usd: 3.66, color: "var(--error)" },
      ],
      spendSeries: ser(30, 2.0, 0.7, 0.5),
      vsDatadog: { ours: 92, datadog: 4180 },
      expensiveQueries: [
        { q: "service:* level:ERROR | last 30d", tier: "cold", scanGB: 38, usd: 1.14, user: "ana@acme", when: "1h ago" },
        { q: "trace_id:4ac9* | last 14d", tier: "cold", scanGB: 22, usd: 0.66, user: "deploy-bot", when: "3h ago" },
        { q: "billing reconcile audit | last 90d", tier: "cold", scanGB: 61, usd: 1.83, user: "finops", when: "yesterday" },
      ],
    };
  },
  async pipelines(): Promise<Pipelines> {
    await wait(80);
    return {
      sources: [
        { name: "Vector DaemonSet", kind: "k8s/stdout", nodes: 42, rate: "1.18k ev/s", status: "healthy" },
        { name: "OTLP gRPC", kind: "otel-collector", nodes: 3, rate: "210 ev/s", status: "healthy" },
      ],
      transforms: [
        { name: "Tarnish drop rules", kind: "filter", dropped: "31%", note: "health-check + debug noise" },
        { name: "Severity routing", kind: "route", note: "error→hot · warn/info→warm · debug→cold" },
        { name: "Parquet batcher", kind: "sink", note: "128MB rolls · zstd · Iceberg commit" },
      ],
      throughput: ser(80, 1390, 280, 140), dropRate: 31, ingestLag: "3s", parquetRolls: "1 / 47s",
    };
  },
  async settings(): Promise<Settings> {
    await wait(50);
    return {
      bucket: "s3://acme-logs-prod", region: "us-east-1", iamRole: "arn:aws:iam::8821:role/verdigris-query",
      retentionDays: 400, queryCompute: 4, confirmColdScans: true,
      routing: [
        { match: "level = 'ERROR'", tier: "hot" },
        { match: "level IN ('WARN','INFO')", tier: "warm" },
        { match: "level = 'DEBUG'", tier: "cold" },
      ],
    };
  },
  // live tail: emits an event roughly every `every` ms; returns a stop handle.
  tail(onMsg: (e: LogRow) => void, every = 700) {
    let on = true;
    const tick = () => { if (!on) return; onMsg(genLogs(1)[0]); setTimeout(tick, every * (0.4 + rnd())); };
    setTimeout(tick, 120);
    return { stop() { on = false; } };
  },
};
