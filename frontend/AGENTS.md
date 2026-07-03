# Verdigris Frontend — Agent Guide

> Read this first. It's the contract for working in `frontend/`. Any agent
> (Claude Code, Cursor, etc.) should be productive after this file alone —
> no need to reverse-engineer the code.

The frontend is a **dependency-free, no-build single-page app**. Vanilla JS +
CSS + hand-rolled SVG charts. It runs by opening `index.html` in a browser
(`file://` works — classic `<script>`/`<link>`, no modules, no fetch-to-load,
no bundler). This is deliberate: it matches Verdigris's "one binary, plug and
play" promise and keeps the UI hackable by an agent without a toolchain.

---

## Run / verify

```bash
open frontend/index.html              # macOS — just open it, no server needed
# or: python3 -m http.server -d frontend 8080  (only if you want a real origin)
```

**Verification harness** (catches render/logic errors without a browser):

```bash
node frontend/_verify.js              # runs every page's render() against a DOM stub,
                                      # flags undefined / NaN / [object Object] leaks
```

Run `_verify.js` after any change to `pages.js`, `api.js`, or `charts.js`.
For interactive changes (mount handlers, the live tail, the cold-scan gate),
open the page in a browser and check the devtools console is clean.

---

## File map — what lives where

| File | Responsibility | Touch it when… |
|------|----------------|----------------|
| `index.html` | Static shell: sidebar brand, `#nav` mount, `#view` router target, `#scrim` modal mount. Loads the 4 scripts in order. | Adding a top-level shell region. Rarely. |
| `styles.css` | **The entire design system.** Tokens in `:root`, then components. No styles live anywhere else. | Any visual change. Add a token, never a hardcoded hex. |
| `api.js` (`window.VApi`) | **The only backend swap point.** Promise-returning methods, one per page. Mock data now; flip `USE_MOCKS=false` to go live. Shapes returned here ARE the API contract. | Wiring real endpoints, or changing a data shape. |
| `charts.js` (`window.VCharts`) | Pure functions returning SVG strings: `area`, `spark`, `hbars`, `donut`, `vbars`. No DOM, no state. | Adding a chart type. |
| `pages.js` (`window.VPages`) | The 8 page modules. Each is `{ title, sub, badge?, render(view), mount?(view) }`. | Adding/editing a page's content or interactivity. |
| `app.js` | Router + sidebar nav build + env dropdown. Hash-based (`#cost`), handles page cleanup (e.g. live-tail interval). | Adding a route, changing nav, shell-level behavior. |

Load order (set in `index.html`, do not reorder): `api.js` → `charts.js` →
`pages.js` → `app.js`. Each attaches one global; later files depend on earlier.

---

## The page module contract

Every page in `pages.js` is an object on `window.VPages`:

```js
const MyPage = {
  title: "My Page",            // shown in the view header
  sub: "one-line subtitle",    // optional
  badge: "live",               // optional — renders a live pill in the sidebar
  async render(view) {         // returns an HTML string. `view` is the #view element.
    const data = await api.myData();   // ALWAYS get data via window.VApi
    return `${head("My Page", "subtitle", actionsHtml)}
      <div class="view-body"> ...cards/charts/tables... </div>`;
  },
  mount(view) {                // optional — wire events AFTER html is in the DOM
    view.querySelector("#thing").addEventListener("click", ...);
    // For anything with a timer/stream, set view._cleanup = () => stop();
    // The router calls _cleanup before navigating away.
  },
};
```

Rules:
- `render` returns a **string**; the router sets `view.innerHTML` then calls `mount`.
- Data comes **only** from `window.VApi` — never inline fixtures in a page. The
  whole point is that the backend agent swaps `api.js` and every page goes live.
- Use the `head(title, sub, actions)` helper for the page header so headers are
  consistent. Wrap scrollable content in `<div class="view-body">`.
- Long-lived resources (intervals, sockets) **must** register `view._cleanup`.

### Adding a new page (checklist)

1. Add a `render` (and optional `mount`) object in `pages.js`, export it on `window.VPages` under a key, e.g. `traces`.
2. Add a data method to `api.js` (mock now, documented shape) — e.g. `async traces() {...}`.
3. Add a nav entry + an SVG icon in `app.js` (`NAV` array + `ICONS` map). Key must match the `VPages` key.
4. `node frontend/_verify.js` → confirm it renders clean. Open in browser → check console + scroll.

---

## Backend integration — the swap

`api.js` is the **single file the backend agent touches.** Every method returns
a Promise of plain JSON; the returned shapes are the contract the UI renders
against. To go live:

1. Set `USE_MOCKS = false` and `BASE = "https://…"` at the top of `api.js`.
2. Each method already has the `fetch()` call stubbed next to its mock. Fill them in.
3. Keep the **return shapes identical** to the mocks (the UI depends on field names
   like `rows[].level`, `stats.elapsedMs`, `tiers[].bytesGB`, `breakdown[].usd`).
   If the backend's shape differs, adapt **inside `api.js`** — never in pages.
4. `tail()` is the only streaming method: today it's a `setInterval`; swap it for
   SSE or a WebSocket that calls `onMsg(event)` and return a `{ stop() }` handle.

Suggested REST surface is documented at the top of `api.js` and mirrors the methods.

---

## Design system conventions (the "best practice" rules)

These keep the UI coherent as it grows. Follow them; don't freelance styles.

- **Tokens, never literals.** All color/spacing/radius live in `:root` in
  `styles.css`. Use `var(--copper)`, `var(--ink-dim)`, etc. If you need a value
  that doesn't exist, add a token — do not paste a raw hex into a component.
- **Brand = oxidized-copper green/teal.** The whole log-tooling category is blue/
  purple; green is the wedge. Accent is `--copper` / `--copper-bright`. Don't
  introduce blue/purple accents.
- **Warm-dark, not sterile-dark.** Surfaces carry a faint green cast
  (`--bg`, `--panel`, `--panel-2/3`). Borders: `--line` family.
- **Severity colors are fixed:** error `--error` (red), warn `--warn` (amber),
  info/debug muted. **Tiers** map to fixed colors: hot=`--hot`, warm=`--warm`,
  cold=`--cold`. Reuse these everywhere (badges, rails, charts, legends).
- **Data is monospace.** Numbers, timestamps, bytes, dollars, IDs, SQL → `--mono`.
  Prose/labels → `--sans`.
- **Charts are inline SVG** via `VCharts` — no chart library, keeps it offline.
  Pass colors from `VCharts.C` (which mirror the CSS tokens).
- **Icons are inline SVG** (`stroke="currentColor"`, 1.8 stroke). No icon font/CDN.
- **Components already exist** — reuse before inventing: `.card`, `.stat`,
  `.badge` (`ok/warn/error/muted/hot/warm/cold`), `.btn` (`primary/danger/ghost/sm`),
  `.tbl`, `.seg`, `.grid.cols-2/3/4`, `.bar-track`, `.toggle`, `.form-row`, `.modal`.
- **Escape user/log strings** with the `esc()` helper in `pages.js` before
  interpolating into HTML.
- **Flex scroll gotcha:** a scrollable flex child needs `min-height: 0` or tall
  content overflows the viewport instead of scrolling. `.view-body`, `.logwrap`,
  `.tail-wrap` already set it — copy that pattern for any new scroll region.

---

## Product framing the UI must keep selling

Verdigris's three differentiators are baked into the UI copy on purpose. Preserve
them when editing:

1. **Data sovereignty** — "data never leaves your account", the `s3://…` bucket in
   the sidebar footer, cost framed as *your own AWS bill*.
2. **No rehydration** — "queried in place · no rehydration" in the Logs footer.
3. **Cost is legible** — the live scan/$ estimate on the query bar and the
   cold-scan confirm gate. Never let a cold-tier query run without the estimate.

See the root `CLAUDE.md` for the full product rationale.

---

## Current pages (all live with mock data)

`logs` · `tail` · `dashboards` · `alerts` · `storage` · `cost` · `pipelines` ·
`settings`. Keys match `window.VPages`, the `NAV` array in `app.js`, and the URL
hash (`index.html#cost`).
