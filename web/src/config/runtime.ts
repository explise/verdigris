/* ═══════════════════════════════════════════════════════════════════
   Runtime deployment config — THE tenancy / deployment decoupling seam.

   The build is identical everywhere. WHERE it runs (Verdigris Cloud,
   single-tenant on-prem, an air-gapped install) is decided at runtime by a
   `/config.json` the host serves. Nothing about deployment is baked into the
   bundle, so the same artifact ships to:
     - cloud  : multi-org, OIDC auth, many environments
     - onprem : single org, token/none auth, the customer's own buckets
     - airgap : no external calls, mock or local engine

   Add an `org` → org-wide UI. Add a `feature` flag → progressive rollout.
   None of it touches page code.
   ═══════════════════════════════════════════════════════════════════ */

export type DeploymentMode = "cloud" | "onprem" | "airgap";
export type WireFormat = "json" | "arrow";

export interface AuthConfig {
  kind: "none" | "token" | "oidc";
  issuer?: string;
  clientId?: string;
}

export interface OrgRef {
  id: string;
  name: string;
}

export interface EnvironmentRef {
  id: string;          // url-safe, used in routes: /:org/:env/...
  label: string;
  region: string;
  bucket: string;      // customer's own S3 bucket — source of truth
}

export interface RuntimeConfig {
  mode: DeploymentMode;
  /** Base URL of the Verdigris query API. "" = same origin (embedded binary). */
  apiBaseUrl: string;
  /** When true, the app serves entirely from in-memory mocks (no backend). */
  useMocks: boolean;
  /** Preferred wire format. Backend may downgrade arrow→json via content-negotiation. */
  wire: WireFormat;
  auth: AuthConfig;
  /** Orgs visible to this user. Single-element for on-prem. Enables org-wide UI. */
  orgs: OrgRef[];
  /** Environments available within an org. */
  environments: EnvironmentRef[];
  /** Feature flags — progressive delivery without rebuilds. */
  features: Record<string, boolean>;
}

/* Sensible default: dev / demo runs fully mocked, multi-org, so every surface
   is exercisable without a backend. The host overrides this with /config.json. */
export const DEFAULT_CONFIG: RuntimeConfig = {
  mode: "cloud",
  apiBaseUrl: "",
  useMocks: true,
  wire: "arrow",
  auth: { kind: "none" },
  orgs: [
    { id: "acme", name: "Acme Corp" },
    { id: "globex", name: "Globex" },
  ],
  environments: [
    { id: "prod-us-east-1", label: "prod-us-east-1", region: "N. Virginia", bucket: "s3://acme-logs-prod" },
    { id: "prod-eu-west-1", label: "prod-eu-west-1", region: "Ireland", bucket: "s3://acme-logs-eu" },
    { id: "staging-us-east-1", label: "staging-us-east-1", region: "N. Virginia", bucket: "s3://acme-logs-stg" },
  ],
  features: {
    liveTail: true,
    duckdbWasm: false,   // phase-2: client-side re-aggregation over Arrow
    grafanaDatasource: false,
    orgWideOverview: true,
  },
};

let _config: RuntimeConfig | null = null;

/** Loaded once at boot (main.tsx). Reads /config.json, falls back to default. */
export async function loadRuntimeConfig(): Promise<RuntimeConfig> {
  if (_config) return _config;
  try {
    const res = await fetch("/config.json", { cache: "no-store" });
    if (res.ok) {
      const partial = (await res.json()) as Partial<RuntimeConfig>;
      _config = { ...DEFAULT_CONFIG, ...partial, features: { ...DEFAULT_CONFIG.features, ...(partial.features ?? {}) } };
      return _config;
    }
  } catch {
    /* no host config (dev / file serve) → fall through to default */
  }
  _config = DEFAULT_CONFIG;
  return _config;
}

export function getConfig(): RuntimeConfig {
  return _config ?? DEFAULT_CONFIG;
}
