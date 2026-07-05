# Verdigris — Demo-Parity Roadmap

> Everything between today's codebase and a demo where Verdigris can stand in for
> a **hosted SaaS log platform** — not sit beside one as a toy. Based on what
> those platforms ship and what platform teams look for when picking one; each
> "have" and each gap references the relevant source files.
>
> Companion to `ROADMAP.md` (engineering milestones, M-numbers). This doc grades
> work by what shows up in a first demo. Where an item already exists as an
> M-milestone, it's cross-referenced rather than duplicated.

---

## The bar

A first "replace your log platform" demo is judged against the incumbents'
core loop. Evaluations walk the same seven stations:

1. **Collect** — agents/collectors on every node, syslog, HTTP, cloud sources,
   OpenTelemetry; "does it ingest what we already emit, today?"
2. **Pipeline** — parse/extract fields at ingest, filter & drop noise (cost
   control), redact sensitive data (PII/secrets), enrich.
3. **Store** — tiered retention, archives that are *still queryable*, budget
   and quota controls. (Their weak point: cold data must be re-indexed /
   "rehydrated" before search — our headline differentiator.)
4. **Explore** — fast free-text search ("grep the stack trace"), dynamic
   fields/facets from log attributes, group-by analytics with timeseries /
   top-list visualizations, saved views, live tail, pattern clustering.
5. **Alert** — threshold monitors on any query, multiple notification
   channels, mute/snooze; anomaly detection is the expected roadmap answer.
6. **Dashboard** — user-built, shareable dashboards fed by log queries.
7. **Govern** — RBAC, SSO (OIDC/SAML), audit trail, retention policy,
   usage visibility. Data residency & egress control — our structural win:
   the data never leaves the customer's account, so an entire class of
   compliance questions ("subprocessor?", "data residency?", "egress fees?")
   collapses to "it's your bucket."

The two things the incumbents structurally cannot fix (their pricing *is* the
product) stay our wedge and must anchor the demo narrative: **no per-GB
ingestion margin** (bytes go straight to the customer's bucket) and **no
rehydration step** (cold data is queried in place, cost-gated, never
re-indexed).

---

## Scoring

- **DP0 — replacement-claim blocker.** Without it, the demo audience concludes
  "nice prototype." Must be *shown working*.
- **DP1 — first-meeting question.** Will be asked in the room; needs either a
  working answer or a *specific, dated* roadmap answer. Shipping it makes the
  demo materially stronger.
- **DP2 — evaluation/POC blocker, not demo blocker.** Fine as a roadmap slide;
  needed before a paid pilot converts.

Effort: S ≈ days · M ≈ 1–2 weeks · L ≈ weeks-plus.

---

## Pillar 1 — Collect

**Have:** Vector DaemonSet shipping node logs (`deploy/helm/**`),
HTTP NDJSON/array ingest (`POST /v1/ingest`), OTLP/HTTP **JSON** logs
(`POST /v1/otlp/logs`), bounded-memory backpressure (413/429), synthetic
generator for demos.

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| C-1 | **OTLP protobuf (+ gRPC) ingest.** Today only OTLP/JSON is accepted (`crates/ingest/src/otlp.rs`); default OpenTelemetry Collector/SDK exporters send protobuf, so "we're OTel-native" fails the first real collector pointed at us. | OTel-native is the table-stakes claim every modern competitor makes. | M | **DP0** |
| C-2 | **Fluent Bit & Fluentd config recipes** (docs + tested config snippets against `/v1/ingest`). Vector is wired; the other two dominant shippers are unproven. | "Does it ingest what we already run?" — most K8s estates run one of these three. | S | DP1 |
| C-3 | **Syslog intake path** (documented via Vector/Fluent Bit syslog source → Verdigris sink; not a native listener). | Network gear / legacy hosts come up in every enterprise conversation. | S | DP1 |
| C-4 | **AWS-source recipes:** CloudWatch Logs → Firehose/Lambda → `/v1/ingest`, S3-drop ingestion. | The pitch is EKS+S3-native; buyers will ask for their Lambda and ALB logs in the same breath. | M | DP2 |

## Pillar 2 — Pipeline (parse · drop · redact)

**Have:** severity→tier routing at write time (`config.rs`
`RoutingConfig`). **Nothing else** — no parsing, no drop rules, no redaction
(the "noise filter" in UI copy is mock-only; no such code path exists).

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| X-1 | **Drop/keep filter rules at ingest** (match on service/level/attr → drop or sample %, counters surfaced on the Pipelines page — making the existing `dropRate` UI real). | Cost control via "don't store noise" is the #1 pipeline use of the incumbents; it's also *our* story ("pay object-storage prices for what you keep"). | M | **DP0** |
| X-2 | **Ingest-time field extraction** (JSON auto-flatten; regex/grok for plaintext lines). Today a non-JSON line lands as one opaque `message`. | Field-extraction rules are the incumbents' bread and butter; structured-only ingest reads as a prototype constraint. Minimum demoable slice: JSON auto-flatten into queryable attrs. | L (M for the JSON-flatten slice) | DP1 (JSON slice **DP0** if the demo dataset isn't already structured) |
| X-3 | **Sensitive-data redaction** (regex rule set applied at ingest: mask card/API-key/email patterns before bytes land; per-rule counters). | PII scanning is now a checklist item in security review; "logs are forever in S3" makes it sharper for us, not softer. | M | DP1 |
| X-4 | **Logs→metrics rules** (count/rate of matching logs persisted as a cheap series feeding dashboards/alerts without rescanning). | The incumbents' cost-saving pattern buyers actually use; pairs with our alert engine. | M | DP2 |

## Pillar 3 — Store & lifecycle

**Have:** hot/warm/cold severity routing; S3 lifecycle policy
generation + real apply; compaction with CAS commits; cost estimator with
cold-scan confirm gate; tier+window+value-stat file pruning shared by
estimate and scan (`ROADMAP.md` M4.1/M4.2 ✅). This pillar is our strength —
the demo should *lead* with it.

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| S-1 | **Retention actually enforced & shown** — `expire_days` renders into the lifecycle policy; surface it as a first-class Settings control with per-tier ages, and show the policy that was applied to the bucket. | "What's your retention story?" must be answered from the product UI, not a TOML file. | S | **DP0** |
| S-2 | **Usage & budget visibility** — per-service ingest volume trend, monthly scan-spend vs a configurable budget, projected bill (extends the existing `/v1/cost`). | Their "budget control" pitch is index quotas; ours is *actual dollars*. Make it visibly better. | M | DP1 |
| S-3 | **Schema-evolution story** — document the fixed 7-column core + attrs model and its limits (`crates/ingest/src/schema.rs`); Iceberg swap (`M1.1`) is the scale/evolution answer. | "What happens when my log fields change?" needs a crisp answer in the room. | S (docs) — L (real, = M1.1) | DP1 answer / DP2 implementation |

## Pillar 4 — Explore (the daily-driver bar)

**Have:** SQL + search DSL, severity histogram, virtualized table,
live tail (SSE), Arrow wire, bloom-filter equality speedups (M1.2 ✅),
cold-scan gate. **The gaps here are the widest in the product.**

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| Q-1 | **Fast free-text search** — ✅ **file-level slice shipped (2026-07-05):** per-file character-trigram sets prune `ILIKE '%…%'` scans before Parquet is opened, shared by estimate + scan (`crates/core/src/text.rs`; trigrams, not word tokens, so in-word substrings stay correct). Remaining: within-file row scans on surviving files (inverted index / row-group trigram structure) and `attrs_json` matches. | "Grep this stack trace across last month" is *the* log-platform moment. A visible full-scan on stage kills the replacement claim. | L (slice: done) | **DP0** (slice ✅) |
| Q-2 | **Facets from attributes** — auto-surface attr keys/top-values as clickable filters. Today `attrs_json` is an opaque string matched via `LIKE` (`search.rs`); competitors auto-facet every field. | Facet-click exploration is how people *actually* use these tools; typing SQL on stage is a power feature, not a substitute. | M | **DP0** |
| Q-3 | **Group-by analytics + visualizations** — `GROUP BY` any field/time-bucket rendered as timeseries/top-list *in the product UI* (engine already does SQL; this is a UI/endpoint slice). | "Top 10 services by error count, graphed" is a first-demo request, verbatim. | M | **DP0** |
| Q-4 | **Saved views** (persisted query+columns+range, shareable per team) — `ROADMAP.md` M4.3. | Teams live in saved views; absence reads as "stateless viewer." | M | DP1 |
| Q-5 | **Pattern clustering** ("group 100k errors into 12 shapes"). | The incumbents' signature analytics moment; the expected "do you have this?" question. | L | DP2 (roadmap answer) |

## Pillar 5 — Alert

**Have:** real alert engine — SQL rule + threshold + state machine,
15s scheduler, webhook on transitions, CRUD API + UI (`ROADMAP.md` M3.1 ✅).

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| L-1 | **Named notification channels** — Slack/Teams/PagerDuty-shaped payloads + email (SMTP), instead of one generic webhook (`crates/core/src/alert.rs` has `webhook: Option<String>` only). | "Where does the page land?" is asked within a minute of showing alerts. Slack/PagerDuty are webhook dialects — cheap win. | S–M | **DP0** (Slack-compatible + email) |
| L-2 | **Mute/snooze & alert audit** (silence a rule for N hours; who changed what). | Ops hygiene expected of anything that pages humans. | S | DP1 |
| L-3 | **Anomaly/outlier monitors** (baseline + deviation). | Expected as a roadmap answer only; the incumbents lean on ML marketing here. Do not build for the demo. | L | DP2 |

## Pillar 6 — Dashboards

**Have:** fixed product pages (metrics/storage/cost/pipelines) and
a Grafana **datasource** (`deploy/grafana/datasource.yaml`). No user-built
dashboards.

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| D-1 | **First-class Grafana path** — ship the datasource + a provisioned starter dashboard in the Helm chart; demo it live. Positioning: "your dashboards live in the tool you already standardize on; we won't clone a dashboard editor." | Credible and demoable *now*; buyers largely have Grafana already. | S | **DP0** |
| D-2 | **In-product saved dashboards** (grid of saved-view panels) — only after Q-3/Q-4 exist. | Closes the "single pane" objection for teams without Grafana. | L | DP2 |

## Pillar 7 — Govern

**Have:** per-user revocable tokens + RBAC (3 roles) with hashed
storage (M2.1 ✅); query audit history + admin endpoint (M2.3 ✅, in-memory);
Prometheus `/metrics` (M3.2 ✅); data residency by construction.

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| G-1 | **UI login/auth integration** — ✅ **shipped (2026-07-05):** `/config.json` advertises token auth, the transport sends the user's real token (token-gate overlay collects/stores it, 401 re-raises it), SSE rides `?access_token=`. OIDC remains G-3. | Demoing an enterprise tool with security disabled contradicts the pitch on stage. | S | **DP0** ✅ |
| G-2 | **Durable audit trail** — ✅ **shipped (2026-07-05):** query history persists to `_audit/query-history.json` under CAS, loads on boot; the audit endpoint reads the persisted doc. | "Does the audit log survive a restart?" — yes, verified. | S | **DP0** ✅ |
| G-3 | **OIDC SSO** (any OIDC IdP → role mapping; SAML/SCIM explicitly later). | SSO is the first governance question in every enterprise screen; tokens+RBAC carry the demo, OIDC carries the POC. | L | DP1 (roadmap answer in-demo; needed for POC) |
| G-4 | **TLS/encryption posture doc** — ingress-terminated TLS in Helm values; S3 SSE + bucket-policy guidance. | Checklist item; a documented answer suffices. | S | DP1 |

## Pillar 8 — Run it (reliability & packaging)

**Have:** split-role serve (1 writer / N readers), CAS commits,
backpressure, Helm chart + Dockerfile, DST harness (4 scenarios), 78 tests
green across the feature matrix. Open M-items: Iceberg (M1.1), full DST
(M1.3), tenant isolation (M2.2), publishing (M5.1), releases (M5.2),
benchmarks (M5.3).

| # | Item | Why | Effort | Grade |
|---|------|-----|--------|-------|
| R-1 | **Published, pullable artifacts** — image on a public registry + chart repo + semver tag (M5.1/M5.2 demo slice). | "One `helm install`" is the promise; a local-only image breaks it in the first five minutes. | M | **DP0** |
| R-2 | **Validated S3 + Kubernetes run** — end-to-end on kind/k3d + MinIO (S3 API); optionally a real EKS+S3 dry run before the actual meeting (costs real money — owner's call). | Everything so far is verified on local/in-memory stores; the demo claim is S3. | M | **DP0** |
| R-3 | **Demo corpus + rehearsed script** — multi-service, multi-day, backdated dataset populating all three tiers (non-trivial cost numbers); scripted hot-query → cold-gate → confirm arc; 30-min soak; fallback recording. | The flagship cost-gate moment shows $0.00 on an empty tier; Glacier's real 3–5h restore is unstageable unrehearsed. | S–M | **DP0** |
| R-4 | **Numbers page** — a one-slide benchmark: ingest rate sustained, p50/p95 query latency hot tier, storage $/GB/mo vs list-price SaaS at the same volume (M5.3 slice). | "Faster/cheaper" needs at least one reproducible number; buyers discount claims without them. | M | DP1 |
| R-5 | **HA story doc** — reader scale-out is real; single-writer ingest + failover behavior documented plainly (what breaks, what recovers, RPO). | Platform teams ask; a plain answer beats a hand-wavy one. | S | DP1 |

---

## What we deliberately do NOT build for this demo

- **In-product dashboard editor** — Grafana path instead (D-1); revisit post-POC (D-2).
- **Anomaly detection / ML analytics** — roadmap slide only (L-3, Q-5).
- **SAML + SCIM** — OIDC is the POC answer (G-3); SAML/SCIM at GA.
- **Metrics & traces signals** — logs-first wedge, OTel-compatible; do not dilute the demo.
- **Multi-tenant SaaS isolation** (`M2.2`) — the pitch *is* single-tenant sovereignty.
- **Compliance certifications** — self-hosted: the customer's controls, our hardening guide (G-4). No cert theater.

## Sequencing (three phases to the meeting)

**Phase A — close the daily-driver gap (the long pole):**
Q-1 token-bloom text search · Q-2 facets · Q-3 group-by analytics · C-1 OTLP
protobuf · X-1 drop rules. *(Pillars 1/2/4 are where "prototype" becomes
"product." Start Q-1 and C-1 first; they're the deepest.)*

**Phase B — close the trust gap:**
G-1 UI auth · G-2 durable audit · L-1 notification channels · S-1 retention
UI · X-2 JSON-flatten slice (if dataset needs it) · G-4 + R-5 + S-3 docs.

**Phase C — close the stage gap:**
R-1 published artifacts · R-2 kind+MinIO validation (optional real-EKS dry
run) · R-3 corpus + rehearsal · D-1 Grafana dashboard · R-4 numbers · Q-4
saved views if time allows.

**Demo-ready test:** a platform engineer in the audience can (1) `helm
install` from public artifacts, (2) point their existing OTel collector at
it, (3) grep a stack-trace fragment across 30 days spanning all three tiers,
(4) get cost-gated on the cold part and approve it, (5) click a facet, graph
top error services, save the view, (6) wire a Slack alert — all under RBAC
with an audit trail, with every byte in their own bucket. Anything on the DP0
list missing breaks one of those six steps.

---

*Grades reflect a first replacement demo, not GA. Cross-references: `ROADMAP.md`
(M-milestones), `docs/dst-architecture.md` (testing), `BACKEND_TODO.md`
(UI-contract punch list). Update this file as items land, citing the source for
each "have."*
