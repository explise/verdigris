/* ═══════════════════════════════════════════════════════════════════
   Verdigris — router / shell wiring
   ═══════════════════════════════════════════════════════════════════ */
(function () {
  const pages = window.VPages;
  const view = document.getElementById("view");
  const navEl = document.getElementById("nav");

  const ICONS = {
    logs: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 6h16M4 12h16M4 18h11"/></svg>',
    tail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M5 12a7 7 0 0 1 14 0M2.5 12a9.5 9.5 0 0 1 19 0"/></svg>',
    dashboards: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="9" rx="1"/><rect x="14" y="3" width="7" height="5" rx="1"/><rect x="14" y="12" width="7" height="9" rx="1"/><rect x="3" y="16" width="7" height="5" rx="1"/></svg>',
    alerts: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9M13.7 21a2 2 0 0 1-3.4 0"/></svg>',
    storage: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>',
    cost: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v10M9.5 9.2c0-1.1 1.1-1.8 2.5-1.8s2.5.7 2.5 1.8-1 1.5-2.5 1.8-2.5.9-2.5 1.9 1.1 1.8 2.5 1.8 2.5-.7 2.5-1.8"/></svg>',
    pipelines: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 7h6l2 3h10M3 7v10h18v-7"/></svg>',
    settings: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-2.7 1.1V21a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-2.7-1.1l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0-1.1-2.7H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.1-2.7l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 2.7-1.1V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 2.7 1.1l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0 1.5 1z"/></svg>',
  };

  const NAV = [
    { group: "Explore", items: [
      { id: "logs", label: "Logs" },
      { id: "tail", label: "Live tail" },
      { id: "dashboards", label: "Dashboards" },
      { id: "alerts", label: "Alerts" },
    ]},
    { group: "Operate", items: [
      { id: "storage", label: "Storage tiers" },
      { id: "cost", label: "Cost", live: true },
      { id: "pipelines", label: "Pipelines" },
      { id: "settings", label: "Settings" },
    ]},
  ];

  // build nav
  navEl.innerHTML = NAV.map((g) =>
    `<div class="nav-label">${g.group}</div>` + g.items.map((it) =>
      `<div class="nav-item" data-route="${it.id}">${ICONS[it.id]}<span>${it.label}</span>${
        it.live ? '<span class="pill-badge"><span class="dot"></span>live</span>' : it.count ? `<span class="count-badge">${it.count}</span>` : ""}</div>`
    ).join("")
  ).join("");

  let current = null;
  async function go(route) {
    const page = pages[route];
    if (!page) return;
    // cleanup previous (e.g. live tail interval)
    if (view._cleanup) { try { view._cleanup(); } catch (e) {} view._cleanup = null; }
    current = route;
    navEl.querySelectorAll(".nav-item").forEach((n) => n.classList.toggle("active", n.dataset.route === route));
    location.hash = route;
    view.innerHTML = `<div class="empty">loading…</div>`;
    const html = await page.render(view);
    if (current !== route) return; // navigated away while awaiting
    view.innerHTML = html;
    if (page.mount) page.mount(view);
  }

  navEl.addEventListener("click", (e) => {
    const n = e.target.closest(".nav-item");
    if (n) go(n.dataset.route);
  });
  window.addEventListener("hashchange", () => {
    const r = location.hash.slice(1);
    if (r && r !== current && pages[r]) go(r);
  });

  // env switcher dropdown
  (function () {
    const ENVS = [
      { id: "prod-us-east-1", region: "N. Virginia" },
      { id: "prod-eu-west-1", region: "Ireland" },
      { id: "staging-us-east-1", region: "N. Virginia" },
      { id: "dev-us-west-2", region: "Oregon" },
    ];
    const btn = document.getElementById("env-switch");
    const label = btn.childNodes[0];
    let cur = label.textContent.trim();

    const menu = document.createElement("div");
    menu.className = "env-menu";
    const render = () => {
      menu.innerHTML = `<div class="em-label">Environment</div>` + ENVS.map((e) =>
        `<div class="env-item ${e.id === cur ? "sel" : ""}" data-env="${e.id}">
           <span class="region-dot"></span>
           <span><div>${e.id}</div><div style="font-size:11px;color:var(--ink-faint)">${e.region}</div></span>
           <span class="check">✓</span>
         </div>`).join("");
    };
    render();
    btn.parentElement.appendChild(menu);

    const close = () => { menu.classList.remove("open"); btn.classList.remove("open"); };
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const willOpen = !menu.classList.contains("open");
      menu.classList.toggle("open", willOpen);
      btn.classList.toggle("open", willOpen);
    });
    menu.addEventListener("click", (e) => {
      const item = e.target.closest(".env-item");
      if (!item) return;
      cur = item.dataset.env;
      label.textContent = cur + " ";
      render();
      close();
    });
    document.addEventListener("click", (e) => { if (!menu.contains(e.target) && e.target !== btn) close(); });
  })();

  go(location.hash.slice(1) || "logs");
})();
