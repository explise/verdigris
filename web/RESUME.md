# web/ — resume notes

> **For overall status & roadmap, see [`../STATUS.md`](../STATUS.md) (master doc).**
> This file is now just the per-directory cheat sheet.

Scale-oriented rebuild of the UI: **Vite + SolidJS + uPlot + Arrow-ready**,
multi-tenant (org/env in the route), deployment-decoupled (cloud / on-prem /
airgap via runtime config). The prototype in `../frontend/` stays as the visual
+ contract reference.

## Status: scaffold COMPLETE — builds green, typechecks clean, runs on :5173

`App.tsx` + `main.tsx` are wired; all 8 pages render in mock mode. Remaining work
(point at live backend, real Arrow round-trip, tenancy paths, etc.) is tracked in
`../STATUS.md`. Original scaffold notes below for reference.

### (historical) scaffold checklist — all done

### Done
- Project config: `package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`
- Decoupling core:
  - `src/config/runtime.ts` — deployment config (cloud/on-prem/airgap, orgs, envs, feature flags)
  - `src/lib/types.ts` — the data contract (Arrow-ready shapes) ← backend targets this
  - `src/lib/transport.ts` — HTTP seam, Arrow/JSON content-negotiation, org/env scoping, auth
  - `src/lib/arrow.ts` — Arrow IPC decode
  - `src/lib/api.ts` — typed api, mock-vs-http per config (the single swap point)
  - `src/lib/mock.ts` — mock data (mirrors types)
  - `src/charts/` — seam: `svg.tsx` (low-density) + `uplot.tsx` (canvas, time-series) + `index.ts`
- Shell: `src/shell/Sidebar.tsx`, `src/shell/Switcher.tsx` (org+env)
- UI: `src/ui/app.css` (ported tokens), `src/ui/extra.css`, `src/ui/primitives.tsx`, `src/store.tsx`
- All 8 pages: `src/pages/{Logs,Dashboards,LiveTail,Alerts,Storage,Cost,Pipelines,Settings}.tsx`
  - Logs uses `@tanstack/solid-virtual` (virtualized table = scale primitive)
  - Dashboards/Pipelines use the uPlot canvas seam

### TODO to make it run (next session)
1. **`src/App.tsx`** — Router with routes `/:org/:env/:page` → page components, a
   layout root rendering `<Sidebar/>` + `<Outlet/>`, and `/` → redirect to
   `/<first org>/<first env>/logs`. Import `app.css` + `extra.css`.
2. **`src/main.tsx`** — `await loadRuntimeConfig()` then render `<Router>` inside
   `<AppProvider>` into `#root`.
3. `cd web && npm install`
4. `npm run typecheck` and `npm run build` — fix any TS/JSX errors (watch:
   solid-virtual `createVirtualizer` reactivity on `count`; uPlot CSS import path).
5. `npm run dev` → open, click through all 8 pages + org/env switcher, check console clean.

### Verify checklist
- Virtualized log table scrolls smoothly over 2,000 mock rows (only ~30 in DOM).
- uPlot charts render on Dashboards/Pipelines.
- Cold tier → cost gate modal fires.
- Org/env switcher changes the URL and re-scopes data.

### Decoupling seams (don't break these)
- Data: `src/lib/api.ts` (+ `transport.ts`) — backend wiring is here only.
- Deployment/tenancy: `src/config/runtime.ts` (`/config.json` at runtime).
- Charts: `src/charts/index.ts` — swap SVG↔canvas↔WebGL per chart here.
- Design tokens: `:root` in `src/ui/app.css`.

Backend rule for scale: tabular endpoints emit **Arrow**; every chart series is
**pre-aggregated server-side** (GROUP BY time_bucket) so the UI never sees raw
event cardinality.
