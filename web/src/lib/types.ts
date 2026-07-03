/* ═══════════════════════════════════════════════════════════════════
   Data contract — the shapes the backend MUST return.

   These are deliberately columnar-friendly. Tabular results (logs, tail)
   are designed to arrive as Apache Arrow IPC and be decoded to these row
   shapes (see arrow.ts). Aggregates (charts, tiers, cost) are small JSON.

   Rule for scale: tabular endpoints are Arrow; the engine pre-aggregates
   every chart server-side (GROUP BY time_bucket) so the UI never receives
   raw event cardinality. See web/AGENTS.md.
   ═══════════════════════════════════════════════════════════════════ */

export type Level = "ERROR" | "WARN" | "INFO" | "DEBUG";
export type TierId = "hot" | "warm" | "cold";

/** One log row. In Arrow this is a record batch row; fields map to columns. */
export interface LogRow {
  ts: string;
  level: Level;
  service: string;
  message: string;
  trace_id: string;
  attrs: Record<string, unknown>;
}

export interface QueryStats {
  events: number;
  scannedBytes: number;
  elapsedMs: number;
  engine: string;       // "datafusion"
  files: number;
  wire: "json" | "arrow";
}

export interface HistogramBucket { total: number; errors: number; }

export interface QueryResult {
  rows: LogRow[];
  stats: QueryStats;
  histogram: HistogramBucket[];
}

export interface EstimateResult {
  scanGB: number;
  costUsd: number;
  coldRestore: boolean;
}

export interface MetricTile { value: string; unit: string; delta: number; }
export interface NamedValue { name: string; gb: number; }
export interface Metrics {
  ingestRate: number[];
  errorRate: number[];
  p99: number[];
  volumeByService: NamedValue[];
  tiles: { ingest: MetricTile; errors: MetricTile; p99: MetricTile; stored: MetricTile };
}

export interface Alert {
  id: string; name: string; state: "firing" | "ok";
  severity: "critical" | "warning" | "info";
  cond: string; value: string; since: string; channel: string;
}

export interface Tier {
  id: TierId; name: string; class: string;
  bytesGB: number; objects: string; perMonth: number; age: string; pct: number;
}
export interface Storage {
  tiers: Tier[];
  lifecycle: { at: string; action: string }[];
  compaction: { smallFiles: string; compacted: string; targetSize: string; lastRun: string; reclaimedGB: number; status: string };
  totalGB: number; totalPerMonth: number;
}

export interface CostBreakdownItem { label: string; usd: number; color: string; }
export interface ExpensiveQuery { q: string; tier: TierId; scanGB: number; usd: number; user: string; when: string; }
export interface Cost {
  monthToDate: number; projected: number; lastMonth: number;
  breakdown: CostBreakdownItem[];
  spendSeries: number[];
  vsDatadog: { ours: number; datadog: number };
  expensiveQueries: ExpensiveQuery[];
}

export interface Pipelines {
  sources: { name: string; kind: string; nodes: number; rate: string; status: string }[];
  transforms: { name: string; kind: string; dropped?: string; note: string }[];
  throughput: number[];
  dropRate: number; ingestLag: string; parquetRolls: string;
}

export interface Settings {
  bucket: string; region: string; iamRole: string;
  retentionDays: number; queryCompute: number; confirmColdScans: boolean;
  routing: { match: string; tier: TierId }[];
}

/** Tier economics shared by the cost estimator (mirrors the backend sim model). */
export const TIER_ECON: Record<TierId, { perGB: number; latency: string; storage: number; label: string }> = {
  hot:  { perGB: 0,    latency: "0.4s", storage: 0.023,  label: "S3 Standard" },
  warm: { perGB: 0.01, latency: "~6s",  storage: 0.004,  label: "Glacier Instant" },
  cold: { perGB: 0.03, latency: "3–5h", storage: 0.0036, label: "Glacier Flexible" },
};
