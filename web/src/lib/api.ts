/* ═══════════════════════════════════════════════════════════════════
   Typed API — the data seam pages depend on.

   createApi(scope) returns methods bound to one org+env. Each method is
   either mock or HTTP depending on runtime config. The HTTP paths use the
   transport (Arrow for tabular, JSON for aggregates). The return TYPES are
   the contract — backend matches these and nothing in the pages changes.

   To make a live backend: set config.useMocks=false and ensure the HTTP
   endpoints return these shapes. That is the entire integration surface.
   ═══════════════════════════════════════════════════════════════════ */
import { getConfig } from "@/config/runtime";
import type { Scope } from "./transport";
import { json, send, queryTable, eventSourceUrl } from "./transport";
import { mock } from "./mock";
import type {
  QueryResult, QueryStats, HistogramBucket, EstimateResult, Metrics, Alert, NewAlert, Storage, Cost, Pipelines, Settings, LogRow, TierId,
} from "./types";

/** Backend rows carry attributes as a JSON string (`attrs_json`, the schema-
    evolution escape hatch). The UI renders a parsed object. */
function parseAttrs(raw: string | undefined): Record<string, unknown> {
  if (!raw) return {};
  try { return JSON.parse(raw) as Record<string, unknown>; } catch { return {}; }
}

/** `ts` differs by wire: the Arrow column is a Timestamp (decodes to epoch ms),
    the JSON wire sends an ISO string. Normalize to the ISO string the UI formats
    so both wires render identically. */
function normalizeTs(ts: unknown): string {
  if (typeof ts === "string") return ts;
  if (typeof ts === "number") return new Date(ts).toISOString();
  if (typeof ts === "bigint") return new Date(Number(ts)).toISOString();
  if (ts instanceof Date) return ts.toISOString();
  return String(ts ?? "");
}

export interface Api {
  queryLogs(opts: { sql: string; service?: string; tiers: TierId[] }): Promise<QueryResult>;
  estimate(tiers: TierId[]): Promise<EstimateResult>;
  tail(onMsg: (e: LogRow) => void, every?: number): { stop(): void };
  metrics(): Promise<Metrics>;
  alerts(): Promise<Alert[]>;
  createAlert(body: NewAlert): Promise<{ id: string }>;
  deleteAlert(id: string): Promise<{ removed: boolean }>;
  storage(): Promise<Storage>;
  cost(): Promise<Cost>;
  pipelines(): Promise<Pipelines>;
  settings(): Promise<Settings>;
}

export function createApi(scope: Scope): Api {
  const live = !getConfig().useMocks;

  return {
    async queryLogs({ sql, service, tiers }) {
      if (!live) return mock.queryLogs(service);
      // One /v1/query round-trip returns { rows, stats, histogram } across either
      // wire — Arrow (columnar rows + stats/histogram in headers) or a JSON
      // envelope; the transport negotiates and normalizes that. Adapt rows here:
      // parse attrs_json → attrs, and normalize ts (Arrow: epoch ms; JSON: ISO).
      const { rows: raw, stats, histogram, wire } = await queryTable<LogRow & { attrs_json?: string }>(
        "/query", scope, { sql, tiers },
      );
      const rows: LogRow[] = raw.map((r) => ({
        ...r,
        ts: normalizeTs(r.ts),
        attrs: r.attrs ?? parseAttrs(r.attrs_json),
      }));
      return {
        rows,
        stats: { ...(stats as Omit<QueryStats, "wire">), wire },
        histogram: histogram as HistogramBucket[],
      };
    },
    estimate(tiers) {
      return live ? json<EstimateResult>("/query/estimate", scope, { tiers }) : mock.estimate(tiers);
    },
    tail(onMsg, every) {
      // Mock mode: the in-memory ticker. Live mode: a real SSE stream from
      // GET /v1/tail (text/event-stream). Each `data:` frame is one log row as
      // JSON, same shape family as query rows — so we reuse parseAttrs to turn
      // the `attrs_json` string into the parsed `attrs` object the UI renders.
      if (!live) return mock.tail(onMsg, every);
      const es = new EventSource(eventSourceUrl("/tail", scope));
      es.onmessage = (ev) => {
        try {
          const r = JSON.parse(ev.data) as LogRow & { attrs_json?: string };
          onMsg({ ...r, attrs: r.attrs ?? parseAttrs(r.attrs_json) });
        } catch {
          /* skip malformed frames rather than tearing down the stream */
        }
      };
      // EventSource auto-reconnects on transient errors; only stop() closes it.
      return { stop() { es.close(); } };
    },
    metrics() { return live ? json<Metrics>("/metrics", scope) : mock.metrics(); },
    alerts() { return live ? json<Alert[]>("/alerts", scope) : mock.alerts(); },
    createAlert(body) { return live ? send<{ id: string }>("POST", "/alerts", scope, body) : mock.createAlert(body); },
    deleteAlert(id) { return live ? send<{ removed: boolean }>("DELETE", `/alerts/${encodeURIComponent(id)}`, scope) : mock.deleteAlert(id); },
    storage() { return live ? json<Storage>("/storage/tiers", scope) : mock.storage(); },
    cost() { return live ? json<Cost>("/cost", scope) : mock.cost(); },
    pipelines() { return live ? json<Pipelines>("/pipelines", scope) : mock.pipelines(); },
    settings() { return live ? json<Settings>("/settings", scope) : mock.settings(); },
  };
}
