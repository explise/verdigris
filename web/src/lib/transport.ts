/* ═══════════════════════════════════════════════════════════════════
   Transport — HTTP seam. All backend traffic goes through here.

   - Scopes every request by org + env (multi-tenant / org-wide).
   - Content-negotiates Arrow for tabular endpoints, JSON for aggregates.
   - Auth header injection lives here (one place for OIDC/token/none).

   Pages never call fetch directly; they call the typed api (api.ts), which
   calls this. Swapping JSON→Arrow or adding auth is a change here only.
   ═══════════════════════════════════════════════════════════════════ */
import { getConfig } from "@/config/runtime";
import { ARROW_MIME, decodeArrowRows } from "./arrow";

export interface Scope {
  org: string;
  env: string;
}

function authHeader(): Record<string, string> {
  const { auth } = getConfig();
  // Real impl: pull token from the auth provider. Stubbed for the scaffold.
  if (auth.kind === "token") return { Authorization: "Bearer <token>" };
  if (auth.kind === "oidc") return { Authorization: "Bearer <oidc-access-token>" };
  return {};
}

function url(path: string, scope: Scope): string {
  const { apiBaseUrl, mode } = getConfig();
  // On-prem / single-tenant the backend serves a FLAT surface (/v1/<resource>) —
  // there are no tenancy path segments to route by. Cloud (multi-org) scopes
  // every request by org+env in the path. This is the one place that differs.
  if (mode === "onprem" || mode === "airgap") {
    return `${apiBaseUrl}/v1${path}`;
  }
  return `${apiBaseUrl}/v1/org/${encodeURIComponent(scope.org)}/env/${encodeURIComponent(scope.env)}${path}`;
}

/** Absolute URL for an EventSource (SSE) stream. Mirrors url()'s flat-vs-tenant
    routing so streaming endpoints respect apiBaseUrl and the deployment mode.
    NOTE: EventSource cannot send an Authorization header. If/when this app grows
    auth, the usual workaround is a short-lived token via query param
    (e.g. `?access_token=…`) that the backend accepts for the stream — NOT
    implemented here (auth is currently `none`). */
export function eventSourceUrl(path: string, scope: Scope): string {
  return url(path, scope);
}

/** JSON GET/POST for aggregate endpoints (small payloads). */
export async function json<T>(path: string, scope: Scope, body?: unknown): Promise<T> {
  const res = await fetch(url(path, scope), {
    method: body ? "POST" : "GET",
    headers: { "content-type": "application/json", accept: "application/json", ...authHeader() },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res.json() as Promise<T>;
}

/** Explicit-method JSON request (POST/DELETE/…). Surfaces the backend's
    `{ error }` message on a non-2xx so callers can show why a mutation failed
    (e.g. a rejected alert query). */
export async function send<T>(method: string, path: string, scope: Scope, body?: unknown): Promise<T> {
  const res = await fetch(url(path, scope), {
    method,
    headers: { "content-type": "application/json", accept: "application/json", ...authHeader() },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data && (data as { error?: string }).error) || `${path} → ${res.status}`);
  return data as T;
}

/** The `/v1/query` envelope, wire-agnostic. In Arrow mode the rows arrive as an
    Arrow-IPC body and stats/histogram ride in response headers (still one round
    trip); in JSON mode all three are in the body. Falls back to JSON if the
    backend doesn't speak Arrow. */
export async function queryTable<T>(
  path: string,
  scope: Scope,
  body: unknown,
): Promise<{ rows: T[]; stats: Record<string, unknown>; histogram: unknown[]; wire: "arrow" | "json" }> {
  const wantArrow = getConfig().wire === "arrow";
  const res = await fetch(url(path, scope), {
    method: "POST",
    headers: { "content-type": "application/json", accept: wantArrow ? `${ARROW_MIME}, application/json` : "application/json", ...authHeader() },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    // The backend returns { error } with a 4xx on a bad query; surface it.
    let msg = `${path} → ${res.status}`;
    try { const e = await res.json(); if (e?.error) msg = e.error; } catch { /* non-JSON body */ }
    throw new Error(msg);
  }
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("arrow")) {
    const buf = await res.arrayBuffer();
    // apache-arrow is dynamically imported (lazy chunk); empty body = zero rows.
    const rows = buf.byteLength ? await decodeArrowRows<T>(buf) : [];
    const stats = JSON.parse(res.headers.get("x-verdigris-stats") ?? "{}");
    const histogram = JSON.parse(res.headers.get("x-verdigris-histogram") ?? "[]");
    return { rows, stats, histogram, wire: "arrow" };
  }
  const env = (await res.json()) as { rows?: T[]; stats?: Record<string, unknown>; histogram?: unknown[] };
  return { rows: env.rows ?? [], stats: env.stats ?? {}, histogram: env.histogram ?? [], wire: "json" };
}

/** Tabular fetch: asks for Arrow, transparently decodes to rows; falls back to
    JSON if the backend can't (or won't) speak Arrow. Returns rows + wire used. */
export async function table<T>(path: string, scope: Scope, body?: unknown): Promise<{ rows: T[]; wire: "arrow" | "json" }> {
  const wantArrow = getConfig().wire === "arrow";
  const res = await fetch(url(path, scope), {
    method: body ? "POST" : "GET",
    headers: { "content-type": "application/json", accept: wantArrow ? `${ARROW_MIME}, application/json` : "application/json", ...authHeader() },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("arrow")) {
    const buf = await res.arrayBuffer();
    // apache-arrow is dynamically imported here (lazy chunk) — see arrow.ts.
    return { rows: await decodeArrowRows<T>(buf), wire: "arrow" };
  }
  return { rows: (await res.json()) as T[], wire: "json" };
}
