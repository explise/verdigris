/* ═══════════════════════════════════════════════════════════════════
   Verdigris frontend — headless render check (no browser, no deps)
   Runs every page's render() against a minimal DOM stub and flags
   undefined / NaN / [object Object] leaks in the output.

   Usage:  node frontend/_verify.js
   Run this after editing pages.js / api.js / charts.js.
   For interactive behavior (mount/tail/modal) verify in a real browser.
   ═══════════════════════════════════════════════════════════════════ */
const fs = require("fs"), vm = require("vm"), path = require("path");
const dir = __dirname;

const stubEl = () => ({
  addEventListener() {}, querySelector() { return null; }, querySelectorAll() { return []; },
  classList: { toggle() {}, add() {}, remove() {}, contains() { return false; } },
  innerHTML: "", style: { setProperty() {} }, appendChild() {}, removeChild() {},
  insertAdjacentElement() {}, firstChild: null, childNodes: [], parentElement: null,
});
const document = {
  getElementById: stubEl, createElement: () => ({ set innerHTML(v) { this._c = v; }, get content() { return { firstElementChild: stubEl() }; } }),
  querySelector: () => null, querySelectorAll: () => [], addEventListener() {},
};
const sandbox = { window: {}, document, setTimeout, clearTimeout, console, Math, Date, JSON, location: { hash: "" } };
sandbox.window.document = document;
vm.createContext(sandbox);

for (const f of ["api.js", "charts.js", "pages.js"]) {
  vm.runInContext(fs.readFileSync(path.join(dir, f), "utf8"), sandbox, { filename: f });
}

(async () => {
  const P = sandbox.window.VPages;
  let fail = 0;
  for (const [k, page] of Object.entries(P)) {
    try {
      const html = await page.render({});
      if (typeof html !== "string" || html.length < 50) throw new Error(`render returned ${typeof html} len ${(html || "").length}`);
      const bad = html.match(/.{0,30}(undefined|NaN|\[object Object\]).{0,30}/);
      if (bad) { console.log(`⚠  ${k}: suspicious output → …${bad[0]}…`); fail++; }
      else console.log(`✓  ${k} (${html.length} chars)`);
    } catch (e) { console.log(`✗  ${k}: ${e.message}`); fail++; }
  }
  console.log(fail ? `\n${fail} page(s) need attention` : "\nAll pages render clean.");
  process.exit(fail ? 1 : 0);
})();
