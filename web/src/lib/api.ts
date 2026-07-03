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
import { json, table } from "./transport";
import { mock } from "./mock";
import type {
  QueryResult, EstimateResult, Metrics, Alert, Storage, Cost, Pipelines, Settings, LogRow, TierId,
} from "./types";

export interface Api {
  queryLogs(opts: { sql: string; service?: string; tiers: TierId[] }): Promise<QueryResult>;
  estimate(tiers: TierId[]): Promise<EstimateResult>;
  tail(onMsg: (e: LogRow) => void, every?: number): { stop(): void };
  metrics(): Promise<Metrics>;
  alerts(): Promise<Alert[]>;
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
      // tabular → Arrow; histogram+stats come back in JSON headers/body envelope.
      const { rows, wire } = await table<LogRow>("/query", scope, { sql, tiers });
      const meta = await json<Omit<QueryResult, "rows">>("/query/meta", scope, { sql, tiers });
      return { rows, stats: { ...meta.stats, wire }, histogram: meta.histogram };
    },
    estimate(tiers) {
      return live ? json<EstimateResult>("/query/estimate", scope, { tiers }) : mock.estimate(tiers);
    },
    tail(onMsg, every) {
      // Live impl: SSE/WebSocket. Scaffold uses the mock ticker either way.
      return mock.tail(onMsg, every);
    },
    metrics() { return live ? json<Metrics>("/metrics", scope) : mock.metrics(); },
    alerts() { return live ? json<Alert[]>("/alerts", scope) : mock.alerts(); },
    storage() { return live ? json<Storage>("/storage/tiers", scope) : mock.storage(); },
    cost() { return live ? json<Cost>("/cost", scope) : mock.cost(); },
    pipelines() { return live ? json<Pipelines>("/pipelines", scope) : mock.pipelines(); },
    settings() { return live ? json<Settings>("/settings", scope) : mock.settings(); },
  };
}
