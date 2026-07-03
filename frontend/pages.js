/* ═══════════════════════════════════════════════════════════════════
   Verdigris — pages
   Each page exports { title, sub, badge?, render(view), mount?(view) }.
   render() returns HTML; mount() wires events after it's in the DOM.
   ═══════════════════════════════════════════════════════════════════ */
(function () {
  const api = window.VApi, ch = window.VCharts;
  const el = (html) => { const t = document.createElement("template"); t.innerHTML = html.trim(); return t.content.firstElementChild; };
  const esc = (s) => String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
  const fmtBytes = (gb) => gb < 1 ? `${Math.round(gb * 1000)} MB` : gb >= 1000 ? `${(gb / 1000).toFixed(1)} TB` : `${gb % 1 === 0 ? gb : gb.toFixed(1)} GB`;
  const usd = (n) => "$" + n.toFixed(2);

  function head(title, sub, actions) {
    return `<div class="view-head"><div class="titles"><h1>${title}</h1>${sub ? `<div class="sub">${sub}</div>` : ""}</div><div class="actions">${actions || ""}</div></div>`;
  }

  const ICON = {
    refresh: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-3-6.7L21 8"/><path d="M21 3v5h-5"/></svg>',
    download: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12m0 0 4-4m-4 4-4-4M5 21h14"/></svg>',
    plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg>',
  };

  /* ════════════════════ LOGS ════════════════════ */
  const Logs = {
    title: "Logs", sub: "Query Parquet in place — no rehydration",
    async render(view) {
      const data = await api.queryLogs({ sql: "service:auth status>=500 | last 1h" });
      this._data = data;
      const histo = renderHisto(data.histogram);
      const rows = data.rows.map((r, i) => rowHtml(r, i)).join("");
      return `<div class="main" style="height:100%">
        <div class="query-shell">
          <div class="querybar">
            <div class="query-input">
              <span class="qicon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.2-3.2"/></svg></span>
              <input id="q" value="service:auth status>=500 | last 1h" spellcheck="false"/>
              <span class="lang-toggle" id="lang">SQL</span>
            </div>
            <button class="btn primary" id="run" style="height:50px;padding:0 26px">Run</button>
          </div>
          <div class="meta-row">
            <span class="tier-label">tier</span>
            <div class="pills">
              <span class="pill sel" data-tier="hot"><span class="dot"></span>hot <span class="t-time">· 0.4s</span></span>
              <span class="pill sel" data-tier="warm"><span class="dot"></span>warm <span class="t-time">· ~6s</span></span>
              <span class="pill" data-tier="cold"><span class="dot"></span>cold <span class="t-time">· restore</span></span>
            </div>
            <div class="cost">
              <span class="chk on" id="chk" title="Require confirmation before cold-tier scans"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5L20 7"/></svg></span>
              this query scans <b id="scan">~210 MB</b> · <span class="dollar free" id="dollar">~$0.00</span>
            </div>
          </div>
          <div class="histo">${histo}</div>
        </div>
        <div class="logwrap">
          <table class="log-table">
            <thead><tr><th>Timestamp</th><th>Level</th><th>Service</th><th>Message</th></tr></thead>
            <tbody id="rows">${rows}</tbody>
          </table>
        </div>
        <div class="logs-foot">
          <span class="count"><b>${(data.stats && data.stats.events != null ? data.stats.events : data.rows.length).toLocaleString()}</b> events</span><span class="sep">·</span>
          <span>queried in place · no rehydration</span>
          <span class="sep">·</span><span>scanned <b id="files" class="mono" style="color:var(--ink)">${data.stats ? data.stats.files : 0}</b> files in <b id="elapsed" class="mono" style="color:var(--ink)">${data.stats ? data.stats.elapsedMs : 0}ms</b></span>
          <span class="right"><span class="glyph">▦</span> parquet on s3 · <span id="engine">${(data.stats && data.stats.engine) || "datafusion"}</span></span>
        </div>
      </div>`;
    },
    mount(view) {
      const $ = (s) => view.querySelector(s);
      const pills = [...view.querySelectorAll(".pill")];
      const scanEl = $("#scan"), dollarEl = $("#dollar"), chk = $("#chk");
      let gate = true;
      let lastEstimate = null; // most recent real estimate, fed to the cold gate
      const selected = () => pills.filter((p) => p.classList.contains("sel")).map((p) => p.dataset.tier);
      async function recompute() {
        const qbox = view.querySelector("#q");
        // The real estimate prunes by the query's time window, so pass the query.
        const est = await api.estimate(selected(), qbox ? qbox.value : "");
        lastEstimate = est;
        const scanGB = est.scanGB || 0, costUsd = est.costUsd || 0;
        scanEl.textContent = "~" + fmtBytes(scanGB);
        dollarEl.textContent = costUsd === 0 ? "~$0.00" : "~" + usd(costUsd);
        dollarEl.className = "dollar " + (costUsd === 0 ? "free" : costUsd < 0.5 ? "warn" : "danger");
      }
      pills.forEach((p) => p.addEventListener("click", () => {
        p.classList.toggle("sel");
        if (selected().length === 0) p.classList.add("sel");
        recompute();
      }));
      chk.addEventListener("click", () => { gate = !gate; chk.classList.toggle("on", gate); });
      recompute();

      // expandable rows
      $("#rows").addEventListener("click", (e) => {
        const tr = e.target.closest("tr[data-i]"); if (!tr) return;
        const i = +tr.dataset.i;
        const next = tr.nextElementSibling;
        if (next && next.classList.contains("detail")) { next.remove(); tr.classList.remove("open"); return; }
        view.querySelectorAll("tr.detail").forEach((d) => d.remove());
        view.querySelectorAll("tr.open").forEach((d) => d.classList.remove("open"));
        tr.classList.add("open");
        tr.insertAdjacentElement("afterend", el(detailHtml(this._data.rows[i])));
      });

      const run = $("#run");
      const qInput = $("#q");
      const rowsEl = $("#rows");
      const histoEl = view.querySelector(".histo");
      const cntEl = view.querySelector(".count b");
      // Actually run the query in the box (DSL or SQL) and re-render results.
      const runQuery = async () => {
        const sql = qInput.value.trim();
        if (!sql) return;
        run.textContent = "Running…";
        try {
          const data = await api.queryLogs({ sql });
          this._data = data;
          rowsEl.innerHTML = data.rows.length
            ? data.rows.map((r, i) => rowHtml(r, i)).join("")
            : `<tr><td colspan="4" style="padding:18px;opacity:.6">no matches</td></tr>`;
          histoEl.innerHTML = renderHisto(data.histogram);
          if (cntEl) cntEl.textContent = ((data.stats && data.stats.events != null) ? data.stats.events : data.rows.length).toLocaleString();
          if (data.stats) {
            const set = (id, v) => { const el = view.querySelector(id); if (el) el.textContent = v; };
            set("#files", data.stats.files);
            set("#elapsed", data.stats.elapsedMs + "ms");
            if (data.stats.engine) set("#engine", data.stats.engine);
          }
        } catch (err) {
          rowsEl.innerHTML = `<tr><td colspan="4" style="padding:18px;color:var(--error)">${esc((err && err.message) || err)}</td></tr>`;
        } finally {
          run.textContent = "Run";
        }
      };
      run.addEventListener("click", async () => {
        await recompute(); // refresh the real estimate for the current query before gating
        if (gate && selected().includes("cold")) return showColdGate(runQuery, lastEstimate);
        runQuery();
      });
      qInput.addEventListener("keydown", (e) => { if (e.key === "Enter") run.click(); });
      $("#lang").addEventListener("click", (ev) => { ev.target.textContent = ev.target.textContent === "SQL" ? "DSL" : "SQL"; });
    },
  };
  // Histogram bars are normalized against the tallest bucket, so they render
  // correctly at any scale (mock totals ~20 or real backend totals in the 100s).
  function renderHisto(h) {
    const arr = h || [];
    const maxTotal = Math.max(1, ...arr.map((b) => b.total || 0));
    return arr.map((b) => {
      const ht = Math.max(3, Math.min(100, (b.total / maxTotal) * 100));
      const e = b.total ? (b.errors / b.total) * 100 : 0;
      return `<div class="bar" style="height:${ht}%" title="${b.total} events · ${b.errors} errors"><div class="err" style="height:${e}%"></div></div>`;
    }).join("");
  }
  // Backend sends ISO timestamps; show just the time-of-day (mock sent HH:MM:SS).
  function fmtTs(ts) {
    const m = /[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)/.exec(String(ts || ""));
    return m ? m[1] : String(ts || "");
  }
  function rowHtml(r, i) {
    return `<tr data-lvl="${r.level}" data-i="${i}"><td class="ts">${fmtTs(r.ts)}</td><td class="lvl">${r.level}</td><td class="svc">${r.service}</td><td class="msg">${esc(r.message)}</td></tr>`;
  }
  function detailHtml(r) {
    const j = JSON.stringify({ timestamp: r.ts, level: r.level, service: r.service, trace_id: r.trace_id, message: r.message, attrs: r.attrs }, null, 2)
      .replace(/"(\w+)":/g, '<span class="k">"$1"</span>:').replace(/: "(.*?)"/g, ': <span class="s">"$1"</span>');
    return `<tr class="detail"><td colspan="4"><div class="detail-inner">${j}</div></td></tr>`;
  }
  function fmtDuration(ms) {
    if (!ms) return "instant";
    const h = ms / 3600000; if (h >= 1) return (h < 10 ? h.toFixed(1) : Math.round(h)) + " h";
    const m = ms / 60000; if (m >= 1) return Math.round(m) + " min";
    return Math.round(ms / 1000) + " s";
  }
  function showColdGate(onGo, est) {
    const scrim = document.getElementById("scrim");
    const scan = est ? "~" + fmtBytes(est.scanGB || 0) : "—";
    const cost = est ? "~" + usd(est.costUsd || 0) : "—";
    const restore = est ? fmtDuration(est.restoreMs || 0) : "—";
    scrim.innerHTML = `<div class="modal">
      <h3><span class="warnico">⚠</span> This query scans cold storage</h3>
      <p>Selecting the <b>cold</b> tier restores from Glacier Flexible before it can be scanned. Cold logs are always queryable — but retrieval is billed by scanned-GB and isn't instant.</p>
      <div class="modal-grid">
        <div class="mg"><div class="mk">Scans</div><div class="mv figure danger">${scan}</div></div>
        <div class="mg"><div class="mk">Retrieval cost</div><div class="mv figure danger">${cost}</div></div>
        <div class="mg"><div class="mk">Restore wait</div><div class="mv figure">${restore}</div></div>
        <div class="mg"><div class="mk">Mode</div><div class="mv figure">Standard</div></div>
      </div>
      <div class="modal-actions">
        <button class="btn" id="m-cancel">Cancel</button>
        <button class="btn primary" id="m-go" style="background:linear-gradient(160deg,var(--warn),#b8801f)">Restore &amp; run</button>
      </div></div>`;
    scrim.classList.add("open");
    const close = () => { scrim.classList.remove("open"); scrim.innerHTML = ""; };
    scrim.querySelector("#m-cancel").onclick = close;
    scrim.querySelector("#m-go").onclick = () => { close(); onGo(); };
    scrim.onclick = (e) => { if (e.target === scrim) close(); };
  }

  /* ════════════════════ LIVE TAIL ════════════════════ */
  const LiveTail = {
    title: "Live tail", sub: "Streaming from the hot tier", badge: "live",
    async render() {
      return `${head("Live tail", "Streaming from the hot tier · last 200 lines",
        `<div class="seg" id="lvlfilter"><button class="on" data-l="all">all</button><button data-l="ERROR">error</button><button data-l="WARN">warn</button><button data-l="INFO">info</button></div>
         <button class="btn" id="pause"><span class="pulse-dot live"></span> pause</button>`)}
        <div class="tail-wrap" id="tail"></div>`;
    },
    mount(view) {
      const wrap = view.querySelector("#tail");
      let filter = "all", paused = false, follow = true;
      const buffer = []; // keep recent events of ALL levels so the filter is retroactive
      const matches = (e) => filter === "all" || e.level === filter;
      const lineHtml = (e) => `<div class="tail-line" data-lvl="${e.level}"><span class="t">${e.ts}</span><span class="l">${e.level}</span><span class="s">${e.service}</span><span class="m">${esc(e.message)}</span></div>`;
      const stick = () => { if (follow) wrap.scrollTop = wrap.scrollHeight; };
      const renderAll = () => { wrap.innerHTML = buffer.filter(matches).map(lineHtml).join(""); stick(); };
      wrap.addEventListener("scroll", () => { follow = wrap.scrollTop + wrap.clientHeight >= wrap.scrollHeight - 40; });
      const handle = api.tail({ onMsg: (e) => {
        if (paused) return;
        buffer.push(e);
        while (buffer.length > 200) buffer.shift();
        if (!matches(e)) return;              // buffered, but not shown under the current filter
        wrap.appendChild(el(lineHtml(e)));
        while (wrap.children.length > 200) wrap.removeChild(wrap.firstChild);
        stick();
      }});
      view._cleanup = () => handle.stop();
      view.querySelector("#lvlfilter").addEventListener("click", (e) => {
        const b = e.target.closest("button"); if (!b) return;
        view.querySelectorAll("#lvlfilter button").forEach((x) => x.classList.remove("on"));
        b.classList.add("on"); filter = b.dataset.l;
        renderAll();                          // apply the new level to lines already on screen
      });
      const pause = view.querySelector("#pause");
      pause.addEventListener("click", () => {
        paused = !paused;
        pause.innerHTML = paused ? `<span class="pulse-dot" style="background:var(--ink-faint)"></span> resume` : `<span class="pulse-dot live"></span> pause`;
      });
    },
  };

  /* ════════════════════ DASHBOARDS ════════════════════ */
  const Dashboards = {
    title: "Dashboards", sub: "Last 1 hour",
    async render() {
      const m = await api.metrics();
      const tile = (label, t, color) => `<div class="card stat">
        <div class="label">${label}</div>
        <div class="value">${t.value}<small> ${t.unit}</small></div>
        <div class="delta ${t.delta > 0 ? (label === "p99 latency" ? "down" : "up") : "down"}">${t.delta > 0 ? "▲" : "▼"} ${Math.abs(t.delta)}% <span class="muted" style="font-weight:500">vs 1h ago</span></div>
      </div>`;
      return `${head("Dashboards", "Last 1 hour · auto-refresh 30s", `<div class="seg"><button>1h</button><button class="on">6h</button><button>24h</button><button>7d</button></div><button class="btn">${ICON.refresh} Refresh</button>`)}
        <div class="view-body">
          <div class="grid cols-4" style="margin-bottom:16px">
            ${tile("Ingest rate", m.tiles.ingest)}
            ${tile("Error rate", m.tiles.errors)}
            ${tile("p99 latency", m.tiles.p99)}
            ${tile("Stored (30d)", m.tiles.stored)}
          </div>
          <div class="grid cols-2" style="margin-bottom:16px">
            <div class="card"><div class="card-head"><h3>Ingest rate</h3><span class="hint">events / sec</span></div>${ch.area(m.ingestRate, { color: ch.C.copper, max: 1800 })}</div>
            <div class="card"><div class="card-head"><h3>Error rate</h3><span class="hint">% of events</span></div>${ch.area(m.errorRate, { color: ch.C.error, max: 12 })}</div>
          </div>
          <div class="grid cols-2">
            <div class="card"><div class="card-head"><h3>p99 latency</h3><span class="hint">ms</span></div>${ch.area(m.p99, { color: ch.C.warn, max: 2000 })}</div>
            <div class="card"><div class="card-head"><h3>Log volume by service</h3><span class="hint">GB / 24h</span></div>${ch.hbars(m.volumeByService.map((s) => ({ name: s.name, value: s.gb, label: s.gb + " GB" })))}</div>
          </div>
        </div>`;
    },
  };

  /* ════════════════════ ALERTS ════════════════════ */
  const Alerts = {
    title: "Alerts", sub: "Rules evaluated continuously over the hot tier",
    async render() {
      const list = await api.alerts();
      const firing = list.filter((a) => a.state === "firing");
      const row = (a) => `<div class="alert-row ${a.state}">
        <div class="ai">${a.state === "firing"
          ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 9v4m0 4h.01M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/></svg>'
          : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>'}</div>
        <div class="amain">
          <div class="atitle">${a.name} <span class="badge ${a.severity === "critical" ? "error" : a.severity === "warning" ? "warn" : "muted"}" style="margin-left:6px">${a.severity}</span></div>
          <div class="acond">${esc(a.cond)}</div>
        </div>
        <div class="ameta">
          <div class="badge ${a.state === "firing" ? "error" : "ok"}"><span class="dot" style="background:currentColor"></span>${a.state === "firing" ? "FIRING" : "OK"}</div>
          <div style="margin-top:6px">now <b class="mono" style="color:${a.state === "firing" ? "var(--error)" : "var(--ink)"}">${a.value}</b> ${a.since !== "—" ? "· " + a.since : ""}</div>
          <div class="muted mono" style="font-size:11px;margin-top:3px">${a.channel}</div>
        </div>
      </div>`;
      return `${head("Alerts", `${firing.length} firing · ${list.length} rules`, `<button class="btn primary">${ICON.plus} New alert</button>`)}
        <div class="view-body">
          ${firing.length ? `<div class="card pad-lg" style="border-color:rgba(227,106,106,.3);background:linear-gradient(90deg,var(--error-soft),transparent 50%);margin-bottom:18px">
            <div class="row"><span class="pulse-dot live"></span><b style="color:var(--error)">${firing.length} alerts firing</b><span class="muted">— paging ${[...new Set(firing.map((f) => f.channel))].join(", ")}</span></div></div>` : ""}
          ${list.map(row).join("")}
        </div>`;
    },
  };

  /* ════════════════════ STORAGE TIERS ════════════════════ */
  const Storage = {
    title: "Storage tiers", sub: "Lifecycle-managed hot → warm → cold",
    async render() {
      const s = await api.storage();
      const tcard = (t, arrow) => `<div class="tier-card ${t.id}"><div class="topline"></div>
        <div class="tname"><span class="badge ${t.id}"><span class="dot" style="background:currentColor"></span>${t.name}</span></div>
        <div class="tclass">${t.class}</div>
        <div class="tbig">${fmtBytes(t.bytesGB)}</div>
        <div class="muted" style="font-size:11.5px;margin-bottom:8px">${t.age}</div>
        <div class="trow"><span>Objects</span><span>${t.objects}</span></div>
        <div class="trow"><span>Storage / mo</span><span>${usd(t.perMonth)}</span></div>
      </div>`;
      const arrow = (lbl) => `<div class="flow-arrow"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14m-6-6 6 6-6 6"/></svg><div class="lbl">${lbl}</div></div>`;
      const c = s.compaction;
      return `${head("Storage tiers", `${fmtBytes(s.totalGB)} total · ${usd(s.totalPerMonth)}/mo storage`, `<button class="btn">${ICON.refresh} Refresh</button>`)}
        <div class="view-body">
          <div class="tier-flow" style="margin-bottom:22px">
            ${tcard(s.tiers[0])}${arrow("after 3d")}${tcard(s.tiers[1])}${arrow("after 30d")}${tcard(s.tiers[2])}
          </div>
          <div class="grid cols-2">
            <div class="card pad-lg">
              <div class="card-head"><h3>Data distribution</h3><span class="hint">share of total bytes</span></div>
              <div class="bar-track" style="height:14px;margin-bottom:6px">
                ${s.tiers.map((t) => `<div class="seg-fill ${t.id}" style="width:${t.pct}%"></div>`).join("")}
              </div>
              <div class="legend">${s.tiers.map((t) => `<div class="item"><span class="sw" style="background:var(--${t.id})"></span>${t.name} · ${t.pct}%</div>`).join("")}</div>
              <div style="margin-top:22px" class="card-head"><h3>Lifecycle policy</h3></div>
              ${s.lifecycle.map((l) => `<div class="row" style="padding:8px 0;border-top:1px solid var(--line-soft)"><span class="badge muted mono">${l.at}</span><span class="muted">${l.action}</span></div>`).join("")}
            </div>
            <div class="card pad-lg">
              <div class="card-head"><h3>Compaction</h3><span class="badge ok" style="margin-left:auto"><span class="dot" style="background:currentColor"></span>${c.status}</span></div>
              <p class="muted" style="font-size:12.5px;line-height:1.6;margin:0 0 16px">Merges the millions of tiny Parquet files streaming logs produce into ${c.targetSize} files — fixing scan speed and the Glacier 40KB-per-object metadata tax at once.</p>
              <div class="grid cols-2" style="gap:12px">
                <div class="card stat" style="padding:14px"><div class="label">Small files</div><div class="value" style="font-size:21px">${c.smallFiles}</div><div class="delta down">▼ merging</div></div>
                <div class="card stat" style="padding:14px"><div class="label">Compacted</div><div class="value" style="font-size:21px">${c.compacted}</div><div class="delta up">▲ target ${c.targetSize}</div></div>
                <div class="card stat" style="padding:14px"><div class="label">Reclaimed</div><div class="value" style="font-size:21px">${c.reclaimedGB} GB</div><div class="muted" style="font-size:11px">this week</div></div>
                <div class="card stat" style="padding:14px"><div class="label">Last run</div><div class="value" style="font-size:21px">${c.lastRun}</div><div class="muted" style="font-size:11px">background job</div></div>
              </div>
            </div>
          </div>
        </div>`;
    },
  };

  /* ════════════════════ COST ════════════════════ */
  const Cost = {
    title: "Cost", sub: "Live spend across storage + compute", badge: "live",
    async render() {
      const c = await api.cost();
      const items = c.breakdown.map((b) => ({ ...b, value: b.usd }));
      const savings = Math.round((1 - c.vsDatadog.ours / c.vsDatadog.datadog) * 100);
      return `${head("Cost", "Billing month to date · us-east-1", `<div class="seg"><button>7d</button><button class="on">30d</button><button>90d</button></div>`)}
        <div class="view-body">
          <div class="grid cols-4" style="margin-bottom:16px">
            <div class="card stat"><div class="label">Month to date</div><div class="value">${usd(c.monthToDate)}</div><div class="delta flat">across all tiers</div></div>
            <div class="card stat"><div class="label">Projected</div><div class="value">${usd(c.projected)}</div><div class="delta ${c.projected > c.lastMonth ? "down" : "up"}">${c.projected > c.lastMonth ? "▲" : "▼"} vs ${usd(c.lastMonth)} last mo</div></div>
            <div class="card stat"><div class="label">Glacier retrieval</div><div class="value">${usd(c.breakdown.find((b) => b.label.includes("retrieval")).usd)}</div><div class="delta flat">pay only when queried</div></div>
            <div class="card stat" style="border-color:rgba(70,201,171,.3)"><div class="label">vs Datadog (est)</div><div class="value" style="color:var(--copper-bright)">${savings}%</div><div class="delta up">cheaper · same volume</div></div>
          </div>
          <div class="grid cols-2" style="margin-bottom:16px">
            <div class="card pad-lg">
              <div class="card-head"><h3>Spend breakdown</h3><span class="hint">this month</span></div>
              <div class="row" style="gap:26px;align-items:center">
                ${ch.donut(items, { center: usd(c.monthToDate).replace(".00",""), centerSub: "month to date" })}
                <div style="flex:1">${ch.hbars(items.map((b) => ({ name: b.label, value: b.usd, label: usd(b.usd), color: b.color })))}</div>
              </div>
            </div>
            <div class="card pad-lg">
              <div class="card-head"><h3>Daily spend</h3><span class="hint">last 30 days · $/day</span></div>
              ${ch.area(c.spendSeries, { color: ch.C.copper, max: 4 })}
              <div class="card-head" style="margin-top:18px"><h3>Sovereignty</h3></div>
              <p class="muted" style="font-size:12.5px;line-height:1.6;margin:0">Every figure here is <b style="color:var(--ink)">your own AWS bill</b> — Verdigris charges no per-GB ingestion margin. Data never leaves <span class="mono" style="color:var(--copper)">s3://acme-logs-prod</span>.</p>
            </div>
          </div>
          <div class="card pad-lg">
            <div class="card-head"><h3>Most expensive queries</h3><span class="hint">cold-tier scans drive retrieval cost</span></div>
            <table class="tbl"><thead><tr><th>Query</th><th>Tier</th><th class="num">Scanned</th><th class="num">Cost</th><th>User</th><th>When</th></tr></thead>
            <tbody>${c.expensiveQueries.map((q) => `<tr>
              <td class="mono" style="color:var(--ink)">${esc(q.q)}</td>
              <td><span class="badge ${q.tier}">${q.tier}</span></td>
              <td class="num">${fmtBytes(q.scanGB)}</td>
              <td class="num" style="color:var(--warn)">${usd(q.usd)}</td>
              <td class="mono muted">${q.user}</td><td class="muted">${q.when}</td></tr>`).join("")}</tbody></table>
          </div>
        </div>`;
    },
  };

  /* ════════════════════ PIPELINES ════════════════════ */
  const Pipelines = {
    title: "Pipelines", sub: "Ingestion → transform → Parquet on S3",
    async render() {
      const p = await api.pipelines();
      const node = (title, sub, stat, badge) => `<div class="pipe-node">
        <div class="pn-name">${title}${badge ? ` <span class="badge ok" style="margin-left:auto">${badge}</span>` : ""}</div>
        <div class="pn-sub">${sub}</div>${stat ? `<div class="pn-stat" style="color:var(--copper-bright)">${stat}</div>` : ""}</div>`;
      const arrow = `<div class="flow-arrow"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14m-6-6 6 6-6 6"/></svg></div>`;
      return `${head("Pipelines", "Vector DaemonSet → Tarnish → Parquet batcher", `<button class="btn">${ICON.refresh} Refresh</button>`)}
        <div class="view-body">
          <div class="grid cols-4" style="margin-bottom:18px">
            <div class="card stat"><div class="label">Throughput</div><div class="value">1.39k<small> ev/s</small></div><div class="delta up">▲ healthy</div></div>
            <div class="card stat"><div class="label">Drop rate (Tarnish)</div><div class="value">${p.dropRate}<small>%</small></div><div class="delta flat">noise filtered</div></div>
            <div class="card stat"><div class="label">Ingest lag</div><div class="value">${p.ingestLag}</div><div class="delta up">▲ real-time</div></div>
            <div class="card stat"><div class="label">Parquet rolls</div><div class="value" style="font-size:20px">${p.parquetRolls}</div><div class="muted" style="font-size:11px">128MB · zstd</div></div>
          </div>
          <div class="card pad-lg" style="margin-bottom:16px">
            <div class="card-head"><h3>Flow</h3><span class="hint">live</span></div>
            <div class="pipe">
              ${node("Vector DaemonSet", "k8s stdout/stderr · 42 nodes", "1.18k ev/s", "healthy")}
              ${arrow}
              ${node("Tarnish", "drop rules · −31% noise", "filter")}
              ${arrow}
              ${node("Parquet batcher", "128MB rolls · Iceberg commit", "→ s3", "healthy")}
            </div>
          </div>
          <div class="grid cols-2">
            <div class="card"><div class="card-head"><h3>Ingest throughput</h3><span class="hint">events / sec</span></div>${ch.area(p.throughput, { color: ch.C.copper, max: 1800 })}</div>
            <div class="card pad-lg"><div class="card-head"><h3>Sources & transforms</h3></div>
              <table class="tbl"><thead><tr><th>Stage</th><th>Kind</th><th>Detail</th></tr></thead><tbody>
              ${p.sources.map((s) => `<tr><td><span class="badge ok">src</span> ${s.name}</td><td class="mono muted">${s.kind}</td><td class="mono">${s.rate} · ${s.nodes} nodes</td></tr>`).join("")}
              ${p.transforms.map((t) => `<tr><td><span class="badge muted">xform</span> ${t.name}</td><td class="mono muted">${t.kind}</td><td class="muted">${t.note}${t.dropped ? ` · <b style="color:var(--warn)">${t.dropped}</b>` : ""}</td></tr>`).join("")}
              </tbody></table>
            </div>
          </div>
        </div>`;
    },
  };

  /* ════════════════════ SETTINGS ════════════════════ */
  const Settings = {
    title: "Settings", sub: "Bucket, retention, routing & query compute",
    async render() {
      const s = await api.settings();
      const fr = (label, hint, ctrl) => `<div class="form-row"><div><div class="flabel">${label}</div><div class="fhint">${hint}</div></div><div class="form-ctrl">${ctrl}</div></div>`;
      return `${head("Settings", "Applies to environment prod-us-east-1", `<button class="btn primary">Save changes</button>`)}
        <div class="view-body" style="max-width:920px">
          <div class="card pad-lg" style="margin-bottom:16px">
            <div class="card-head"><h3>Storage</h3></div>
            ${fr("S3 bucket", "Source of truth. Verdigris never copies data out of it.", `<input class="input mono" value="${s.bucket}" style="width:340px"/>`)}
            ${fr("Region", "", `<input class="input mono" value="${s.region}" style="width:200px"/>`)}
            ${fr("IAM role", "Assumed for query + lifecycle operations.", `<input class="input mono" value="${s.iamRole}" style="width:420px"/>`)}
            ${fr("Retention", "After this, objects expire (delete) via S3 lifecycle.", `<div class="row"><input class="input mono" value="${s.retentionDays}" style="width:90px"/> <span class="muted">days</span></div>`)}
          </div>

          <div class="card pad-lg" style="margin-bottom:16px">
            <div class="card-head"><h3>Query compute</h3><span class="hint">storage and compute are decoupled — this dial only changes speed</span></div>
            ${fr("Provisioned compute", "More compute = faster queries from colder tiers. Storage cost is unaffected.",
              `<div class="dial" style="width:380px"><input type="range" id="dial" min="1" max="16" value="${s.queryCompute}" style="--p:${(s.queryCompute / 16) * 100}%"/><span class="dval" id="dialv">${s.queryCompute} vCPU · ~${s.queryCompute * 0.4}s hot</span></div>`)}
          </div>

          <div class="card pad-lg" style="margin-bottom:16px">
            <div class="card-head"><h3>Severity routing</h3><span class="hint">decides which prefix / storage class a log lands in at write time</span></div>
            ${s.routing.map((r) => `<div class="rule"><span class="mono">${esc(r.match)}</span><span class="arrow">→</span><span><span class="badge ${r.tier}">${r.tier}</span></span><button class="btn ghost sm">✕</button></div>`).join("")}
            <button class="btn sm" style="margin-top:6px">${ICON.plus} Add rule</button>
            <p class="fhint" style="margin-top:12px">Routing is a storage-placement hint, <b>not</b> a price lever — severity never changes what a GB costs. Customers can't game cost by relabeling <span class="mono">error</span> as <span class="mono">debug</span>.</p>
          </div>

          <div class="card pad-lg">
            <div class="card-head"><h3>Safety</h3></div>
            ${fr("Confirm cold-tier scans", "Show a cost + restore-time gate before any query that touches Glacier Flexible.",
              `<div class="toggle ${s.confirmColdScans ? "on" : ""}" id="cold-toggle"></div>`)}
          </div>
        </div>`;
    },
    mount(view) {
      const dial = view.querySelector("#dial"), dv = view.querySelector("#dialv");
      if (dial) dial.addEventListener("input", () => {
        const v = +dial.value;
        dial.style.setProperty("--p", (v / 16) * 100 + "%");
        dv.textContent = `${v} vCPU · ~${(v * 0.4).toFixed(1)}s hot`;
      });
      const t = view.querySelector("#cold-toggle");
      if (t) t.addEventListener("click", () => t.classList.toggle("on"));
    },
  };

  window.VPages = { logs: Logs, tail: LiveTail, dashboards: Dashboards, alerts: Alerts, storage: Storage, cost: Cost, pipelines: Pipelines, settings: Settings };
})();
