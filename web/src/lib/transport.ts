/* ═══════════════════════════════════════════════════════════════════
   Transport — HTTP seam. All backend traffic goes through here.

   - Scopes every request by org + env (multi-tenant / org-wide).
   - Content-negotiates Arrow for tabular endpoints, JSON for aggregates.
   - Auth header injection lives here (one place for OIDC/token/none).

   Pages never call fetch directly; they call the typed api (api.ts), which
   calls this. Swapping JSON→Arrow or adding auth is a change here only.
   ═══════════════════════════════════════════════════════════════════ */
import { getConfig } from "@/config/runtime";
import { ARROW_MIME, decodeArrow, tableToRows } from "./arrow";

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
  const { apiBaseUrl } = getConfig();
  // Tenancy is in the path: /v1/org/:org/env/:env/<resource>
  return `${apiBaseUrl}/v1/org/${encodeURIComponent(scope.org)}/env/${encodeURIComponent(scope.env)}${path}`;
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
    return { rows: tableToRows<T>(decodeArrow(buf)), wire: "arrow" };
  }
  return { rows: (await res.json()) as T[], wire: "json" };
}
