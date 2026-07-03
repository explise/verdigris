# Verdigris Frontend

**Read [`AGENTS.md`](./AGENTS.md) before working here** — it is the full contract
for this directory (architecture, file map, page-module contract, the single
backend swap point, design-system conventions, and how to add a page).

Quick facts:
- No build step. Open `index.html` (works over `file://`). Vanilla JS + CSS + SVG.
- Backend wires in **one file**: `api.js` (`USE_MOCKS=false`). Return shapes are the contract.
- Verify after edits: `node frontend/_verify.js`.
- Brand is **oxidized-copper green**; tokens live in `:root` in `styles.css` — never hardcode hex.
