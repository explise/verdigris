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
import { getToken, authRequired } from "./session";

export interface Scope {
  org: string;
  env: string;
}

function authHeader(): Record<string, string> {
  const { auth } = getConfig();
  if (auth.kind === "token") {
    // The user's token, collected by the TokenGate and kept in localStorage.
    const t = getToken();
    return t ? { Authorization: `Bearer ${t}` } : {};
  }
  // OIDC federation is a later milestone (ROADMAP M2.1 deferral).
  return {};
}

/** A 401 means the token is missing/invalid/revoked: raise the token gate and
    fail the call. (403 is NOT a gate — the token is valid, the role is just
    insufficient — so it surfaces as a normal error.) */
function check401(res: Response, path: string): void {
  if (res.status === 401) {
    authRequired();
    throw new Error(`${path} → 401 (authentication required)`);
  }
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
    EventSource cannot send an Authorization header, so when the deployment
    requires a token it rides as `?access_token=…` — the backend's stream
    endpoints accept exactly that (`require_auth` in serve.rs). */
export function eventSourceUrl(path: string, scope: Scope): string {
  const base = url(path, scope);
  if (getConfig().auth.kind !== "token") return base;
  const t = getToken();
  if (!t) return base;
  return `${base}${base.includes("?") ? "&" : "?"}access_token=${encodeURIComponent(t)}`;
}

/** JSON GET/POST for aggregate endpoints (small payloads). */
export async function json<T>(path: string, scope: Scope, body?: unknown): Promise<T> {
  const res = await fetch(url(path, scope), {
    method: body ? "POST" : "GET",
    headers: { "content-type": "application/json", accept: "application/json", ...authHeader() },
    body: body ? JSON.stringify(body) : undefined,
  });
  check401(res, path);
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
  check401(res, path);
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
  check401(res, path);
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
  check401(res, path);
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  const ct = res.headers.get("content-type") ?? "";
  if (ct.includes("arrow")) {
    const buf = await res.arrayBuffer();
    // apache-arrow is dynamically imported here (lazy chunk) — see arrow.ts.
    return { rows: await decodeArrowRows<T>(buf), wire: "arrow" };
  }
  return { rows: (await res.json()) as T[], wire: "json" };
}
