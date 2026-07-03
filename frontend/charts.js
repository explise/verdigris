/* ═══════════════════════════════════════════════════════════════════
   Verdigris — hand-rolled SVG charts (no dependency, works offline)
   Each helper returns an SVG string. Colors come from CSS vars.
   ═══════════════════════════════════════════════════════════════════ */
(function () {
  const C = {
    copper: "#46c9ab", copperDeep: "#0d3b32", warn: "#dca041", error: "#e36a6a",
    cold: "#6b8f9e", warm: "#dca041", hot: "#46c9ab", ink: "#61776e", grid: "#1e2823",
  };

  function path(points) {
    return points.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
  }

  // area / line chart
  function area(data, opts = {}) {
    const w = opts.w || 560, h = opts.h || 150, pad = 8;
    const color = opts.color || C.copper;
    const max = opts.max || Math.max(...data) * 1.15 || 1;
    const min = opts.min || 0;
    const n = data.length;
    const x = (i) => pad + (i / (n - 1)) * (w - pad * 2);
    const y = (v) => h - pad - ((v - min) / (max - min)) * (h - pad * 2);
    const pts = data.map((v, i) => [x(i), y(v)]);
    const id = "g" + Math.floor(Math.abs(Math.sin(n * (data[0] || 1)) * 1e6));
    const fill = `${path(pts)} L ${x(n - 1)} ${h - pad} L ${x(0)} ${h - pad} Z`;
    const grid = [0.25, 0.5, 0.75].map((g) => `<line x1="${pad}" x2="${w - pad}" y1="${pad + g * (h - pad * 2)}" y2="${pad + g * (h - pad * 2)}" stroke="${C.grid}" stroke-width="1"/>`).join("");
    return `<svg class="chart-wrap" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" style="height:${h}px">
      <defs><linearGradient id="${id}" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0" stop-color="${color}" stop-opacity="0.32"/><stop offset="1" stop-color="${color}" stop-opacity="0"/>
      </linearGradient></defs>
      ${grid}
      <path d="${fill}" fill="url(#${id})"/>
      <path d="${path(pts)}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
      <circle cx="${x(n - 1).toFixed(1)}" cy="${y(data[n - 1]).toFixed(1)}" r="3.5" fill="${color}"/>
    </svg>`;
  }

  // small inline sparkline
  function spark(data, opts = {}) {
    const w = opts.w || 120, h = opts.h || 32, color = opts.color || C.copper, pad = 3;
    const max = Math.max(...data) * 1.1 || 1, min = Math.min(...data) * 0.9;
    const n = data.length;
    const x = (i) => pad + (i / (n - 1)) * (w - pad * 2);
    const y = (v) => h - pad - ((v - min) / (max - min || 1)) * (h - pad * 2);
    const pts = data.map((v, i) => [x(i), y(v)]);
    return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}"><path d="${path(pts)}" fill="none" stroke="${color}" stroke-width="1.8" stroke-linecap="round"/></svg>`;
  }

  // horizontal bar list  [{name, value}]
  function hbars(items, opts = {}) {
    const max = Math.max(...items.map((i) => i.value)) || 1;
    const color = opts.color || C.copper;
    return `<div style="display:flex;flex-direction:column;gap:11px">` + items.map((it) => `
      <div>
        <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:5px">
          <span style="color:var(--ink-dim)">${it.name}</span>
          <span class="mono" style="color:var(--ink)">${it.label || it.value}</span>
        </div>
        <div class="bar-track"><div class="seg-fill" style="width:${(it.value / max) * 100}%;background:${it.color || color}"></div></div>
      </div>`).join("") + `</div>`;
  }

  // donut chart  [{label, usd/value, color}]
  function donut(items, opts = {}) {
    const size = opts.size || 168, sw = opts.stroke || 22, r = (size - sw) / 2, cx = size / 2, cy = size / 2;
    const key = opts.key || "value";
    const total = items.reduce((s, i) => s + i[key], 0) || 1;
    const circ = 2 * Math.PI * r;
    let off = 0;
    const segs = items.map((it) => {
      const frac = it[key] / total;
      const seg = `<circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${it.color}" stroke-width="${sw}"
        stroke-dasharray="${(frac * circ).toFixed(2)} ${circ.toFixed(2)}" stroke-dashoffset="${(-off * circ).toFixed(2)}"
        transform="rotate(-90 ${cx} ${cy})" stroke-linecap="butt"/>`;
      off += frac;
      return seg;
    }).join("");
    return `<svg viewBox="0 0 ${size} ${size}" width="${size}" height="${size}">
      <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="${C.grid}" stroke-width="${sw}"/>
      ${segs}
      ${opts.center ? `<text x="${cx}" y="${cy - 2}" text-anchor="middle" fill="var(--ink)" font-size="20" font-weight="650">${opts.center}</text>
        <text x="${cx}" y="${cy + 16}" text-anchor="middle" fill="var(--ink-faint)" font-size="10">${opts.centerSub || ""}</text>` : ""}
    </svg>`;
  }

  // grouped vertical bars for comparisons [{label,value,color}]
  function vbars(items, opts = {}) {
    const h = opts.h || 150, w = opts.w || 220, pad = 24, gap = 18;
    const max = Math.max(...items.map((i) => i.value)) || 1;
    const bw = (w - pad - gap * (items.length - 1)) / items.length;
    const bars = items.map((it, i) => {
      const bh = (it.value / max) * (h - pad - 20);
      const x = pad / 2 + i * (bw + gap);
      const y = h - 20 - bh;
      return `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" rx="4" fill="${it.color}"/>
        <text x="${x + bw / 2}" y="${h - 6}" text-anchor="middle" fill="var(--ink-faint)" font-size="10">${it.label}</text>
        <text x="${x + bw / 2}" y="${y - 6}" text-anchor="middle" fill="var(--ink)" font-size="11" font-weight="600">${it.top || ""}</text>`;
    }).join("");
    return `<svg viewBox="0 0 ${w} ${h}" width="100%" height="${h}">${bars}</svg>`;
  }

  window.VCharts = { area, spark, hbars, donut, vbars, C };
})();
