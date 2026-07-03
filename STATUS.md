# Verdigris Frontend — Status & Roadmap

Master status doc for the UI work. Last updated 2026-06-29.

There are **two frontends** in this repo, on purpose:

| | `frontend/` | `web/` |
|---|---|---|
| **What** | Original design prototype | Production rebuild |
| **Stack** | Vanilla JS + CSS + inline SVG, no build | Vite + SolidJS + TS + uPlot, Arrow-ready |
| **Runs** | Open `index.html` (or served by `vdg` at `:8080`) | `npm run dev` → `:5173`; `npm run build` → `dist/` |
| **Data** | Wired to the **live backend** (`USE_MOCKS=false` in `api.js`) | **Mock mode** (`useMocks:true`); live swap is one flag |
| **Purpose** | Fast iteration, visual + contract reference | The scale architecture we ship: virtualization, canvas charts, multi-tenant, on-prem |

The prototype is the reference; `web/` is where the real product goes. Both render the same 8 pages and share the same design language and data contract.

---

## ✅ Done

### Prototype (`frontend/`)
- 8 pages: Logs, Live tail, Dashboards, Alerts, Storage tiers, Cost, Pipelines, Settings.
- Hash router, sidebar nav, env dropdown switcher.
- Logs: query bar, severity histogram (scale-normalized), tier pills with **live cost estimate**, expandable JSON rows, **cold-scan confirm gate**, dynamic footer (events/files/elapsed/engine from `stats`).
- Live tail: streaming with pause/resume + level filter that applies **retroactively** to on-screen lines.
- Wired to the live `vdg` backend; `api.js` adapts backend shapes (parses `attrs_json`, etc.).
- Design system in `styles.css` (oxidized-copper green tokens in `:root`).
- Headless render check: `node frontend/_verify.js`.
- Docs: `frontend/AGENTS.md` (full contract), `frontend/CLAUDE.md` (pointer).

### Web app (`web/`) — the scale rebuild
- **Builds green** (`npm run build`, ~100 KB gzip) and **typechecks clean** (`npm run typecheck`).
- Fully wired: `main.tsx` (loads runtime config → mounts) + `App.tsx` (router) + all 8 pages.
- **Decoupling seams in place:**
  - Data — `lib/api.ts` (+ `transport.ts`, `mock.ts`): the single backend swap point. Mock-vs-HTTP per config.
  - Deployment/tenancy — `config/runtime.ts`: cloud / on-prem / airgap, orgs, envs, feature flags via `/config.json`.
  - Charts — `charts/index.ts`: SVG (low-density) + uPlot canvas (time-series). Swap per chart here.
  - Design tokens — `:root` in `ui/app.css`.
- **Scale primitives working:** virtualized log table (`@tanstack/solid-virtual`, ~30 of N rows in DOM), uPlot canvas charts on Dashboards/Pipelines.
- **Multi-tenant routing:** `/:org/:env/:page`; org+env switcher in the sidebar. On-prem single-org works (config-driven).
- **Arrow path scaffolded:** `lib/arrow.ts` + content-negotiation in `transport.ts` (asks for Arrow, decodes to rows, falls back to JSON).
- Contract is typed in `lib/types.ts` — the authoritative shape spec.

### Cross-cutting
- **Backend integration probed** end-to-end; results + punch list in `BACKEND_TODO.md` (backend agent has resolved most; see that file).
- Engine label aligned to **`datafusion`** (ADR-001) across both UIs and mocks.
- Pricing/contract docs: product spec in `CLAUDE.md`; frontend pointer section added there.

---

## 🔧 To do later

### Frontend — `web/` (the path to production)
1. **Point `web/` at the live backend.** Set `useMocks:false` + `apiBaseUrl` in runtime config and verify each page against `vdg`. Today it's mock-only.
2. **Wire `web/` transport to the real tenancy paths.** `transport.ts` uses `/v1/org/:org/env/:env/...`; the current backend serves flat `/v1/...`. Reconcile (either backend adds tenancy path segments, or `transport.ts` maps to flat paths until then).
3. **Real Arrow round-trip.** Backend emits Arrow IPC for `/v1/query`; confirm `arrow.ts` decode + feed columnar data into the virtualized table (and eventually uPlot). Today the decode path is built but untested against real Arrow bytes.
4. **`web/` interactivity parity with the prototype:** Run button should re-execute the query (currently only opens the cold gate); query-box edits should refetch; histogram/footer already reactive.
5. **uPlot over Arrow columns** — feed `x[]`/`y[]` straight from Arrow columns instead of materialized arrays (the real scale win).
7. **DuckDB-Wasm (phase 2)** — `duckdbWasm` feature flag exists; wire client-side re-aggregation/zoom over Arrow result sets (Grafana-beating interaction, on-brand with the engine).
8. **Code-split `apache-arrow`** — it dominates the 100 KB gzip bundle; lazy-load it on the Logs route.

### Frontend — `frontend/` prototype
9. **Offline fallback.** With `USE_MOCKS=false`, opening `index.html` standalone shows fetch errors on every page but Live tail. Add a "fall back to mocks if no backend responds" path so the prototype still demos offline. (Open decision — see below.)
10. **`_verify.js` can't run offline anymore** (pages now fetch). Either add a `fetch` stub/mock toggle to the harness, or run it against a live `vdg`.

### Cross-cutting / product
11. ~~**Pricing rates swapped in client `TIER_ECON`**~~ — **FIXED.** `warm:0.03` (Glacier Instant per-GET), `cold:0.01` (Glacier Flexible standard) per `CLAUDE.md`, in both `frontend/api.js` and `web/src/lib/types.ts`.
12. **Logo is a placeholder glyph** — needs a real verdigris/patina mark.
13. **Real-engine footer copy** — UI now reads `stats.engine`; confirm product wants "datafusion" surfaced to users vs a friendlier label.

### Depends on backend (tracked in `BACKEND_TODO.md`)
14. **`/v1/tail` streaming (SSE/WebSocket)** — not built; live tail stays mock-only until it exists.
15. **`scanGB` time-range pruning** in the cost estimator (backend note: still tier-total, doesn't prune by the query's time window).
16. **`expensiveQueries`** populated once query-history tracking exists; **`p99`** is modeled until logs carry a latency field.
17. **`attrs` as an object** instead of `attrs_json` string (currently parsed client-side).

---

## Decisions & known issues
- **Two-frontend strategy is intentional**, not duplication: prototype = reference/disposable, `web/` = production. Don't delete the prototype.
- **Engine is DataFusion** (ADR-001), even though early product copy said DuckDB. UIs read `stats.engine`.
- **Severity never prices anything** — routing is placement only (core product principle, reflected in Settings copy). Keep it.
- **Open question:** should the prototype keep a mock fallback for offline demos, or stay strictly pointed at the live backend? (#9)

## Where things live
- Product spec & principles → `CLAUDE.md`
- Prototype contract / how to extend → `frontend/AGENTS.md`
- Web app contract / seams → this doc + `web/RESUME.md` + inline headers in `web/src/lib/*`
- Backend punch list → `BACKEND_TODO.md`
- Data contract (authoritative shapes) → `web/src/lib/types.ts`
