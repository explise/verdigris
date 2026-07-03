/* SVG renderers — for low-density charts (sparklines, bar lists, donuts).
   Crisp, CSS-styleable, accessible. Not for high-cardinality time-series. */
import { For } from "solid-js";

const C = { copper: "#46c9ab", grid: "#1e2823" };

export function AreaSVG(props: { data: number[]; color?: string; max?: number; h?: number }) {
  const h = props.h ?? 150, w = 560, pad = 8;
  const color = props.color ?? C.copper;
  const max = props.max ?? (Math.max(...props.data) * 1.15 || 1);
  const n = () => props.data.length;
  const x = (i: number) => pad + (i / (n() - 1)) * (w - pad * 2);
  const y = (v: number) => h - pad - (v / max) * (h - pad * 2);
  const line = () => props.data.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`).join(" ");
  const fill = () => `${line()} L ${x(n() - 1)} ${h - pad} L ${x(0)} ${h - pad} Z`;
  const id = `g${Math.round(max)}_${n()}`;
  return (
    <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ width: "100%", height: `${h}px` }}>
      <defs>
        <linearGradient id={id} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" stop-color={color} stop-opacity="0.32" />
          <stop offset="1" stop-color={color} stop-opacity="0" />
        </linearGradient>
      </defs>
      <For each={[0.25, 0.5, 0.75]}>{(g) => <line x1={pad} x2={w - pad} y1={pad + g * (h - pad * 2)} y2={pad + g * (h - pad * 2)} stroke={C.grid} />}</For>
      <path d={fill()} fill={`url(#${id})`} />
      <path d={line()} fill="none" stroke={color} stroke-width="2" stroke-linejoin="round" />
    </svg>
  );
}

export function HBars(props: { items: { name: string; value: number; label?: string; color?: string }[] }) {
  const max = () => Math.max(...props.items.map((i) => i.value)) || 1;
  return (
    <div style={{ display: "flex", "flex-direction": "column", gap: "11px" }}>
      <For each={props.items}>{(it) => (
        <div>
          <div style={{ display: "flex", "justify-content": "space-between", "font-size": "12px", "margin-bottom": "5px" }}>
            <span style={{ color: "var(--ink-dim)" }}>{it.name}</span>
            <span class="mono" style={{ color: "var(--ink)" }}>{it.label ?? it.value}</span>
          </div>
          <div class="bar-track"><div class="seg-fill" style={{ width: `${(it.value / max()) * 100}%`, background: it.color ?? "var(--copper)" }} /></div>
        </div>
      )}</For>
    </div>
  );
}

export function Donut(props: { items: { color: string; value: number }[]; center?: string; centerSub?: string; size?: number }) {
  const size = props.size ?? 168, sw = 22, r = (size - sw) / 2, cx = size / 2, cy = size / 2;
  const total = () => props.items.reduce((s, i) => s + i.value, 0) || 1;
  const circ = 2 * Math.PI * r;
  let off = 0;
  const segs = () => props.items.map((it) => {
    const frac = it.value / total();
    const seg = { color: it.color, dash: `${(frac * circ).toFixed(2)} ${circ.toFixed(2)}`, offset: (-off * circ).toFixed(2) };
    off += frac;
    return seg;
  });
  return (
    <svg viewBox={`0 0 ${size} ${size}`} width={size} height={size}>
      <circle cx={cx} cy={cy} r={r} fill="none" stroke={C.grid} stroke-width={sw} />
      <For each={segs()}>{(s) => (
        <circle cx={cx} cy={cy} r={r} fill="none" stroke={s.color} stroke-width={sw} stroke-dasharray={s.dash} stroke-dashoffset={s.offset} transform={`rotate(-90 ${cx} ${cy})`} />
      )}</For>
      {props.center && <text x={cx} y={cy - 2} text-anchor="middle" fill="var(--ink)" font-size="20" font-weight="650">{props.center}</text>}
      {props.centerSub && <text x={cx} y={cy + 16} text-anchor="middle" fill="var(--ink-faint)" font-size="10">{props.centerSub}</text>}
    </svg>
  );
}
