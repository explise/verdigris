import { For, Show } from "solid-js";
import { A, useParams, useLocation } from "@solidjs/router";
import { useConfig } from "@/store";
import { Switcher } from "./Switcher";

const ICON: Record<string, string> = {
  logs: "M4 6h16M4 12h16M4 18h11",
  tail: "",
  dashboards: "",
  alerts: "M18 8a6 6 0 0 0-12 0c0 7-3 9-3 9h18s-3-2-3-9M13.7 21a2 2 0 0 1-3.4 0",
  storage: "",
  cost: "",
  pipelines: "M3 7h6l2 3h10M3 7v10h18v-7",
  settings: "",
};

// inline svgs that need more than one path
function NavIcon(props: { id: string }) {
  const s = { fill: "none", stroke: "currentColor", "stroke-width": "1.8", "stroke-linecap": "round" as const, "stroke-linejoin": "round" as const };
  switch (props.id) {
    case "tail": return <svg viewBox="0 0 24 24" {...s}><circle cx="12" cy="12" r="3" /><path d="M5 12a7 7 0 0 1 14 0M2.5 12a9.5 9.5 0 0 1 19 0" /></svg>;
    case "dashboards": return <svg viewBox="0 0 24 24" {...s}><rect x="3" y="3" width="7" height="9" rx="1" /><rect x="14" y="3" width="7" height="5" rx="1" /><rect x="14" y="12" width="7" height="9" rx="1" /><rect x="3" y="16" width="7" height="5" rx="1" /></svg>;
    case "storage": return <svg viewBox="0 0 24 24" {...s}><ellipse cx="12" cy="5" rx="8" ry="3" /><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6" /></svg>;
    case "cost": return <svg viewBox="0 0 24 24" {...s}><circle cx="12" cy="12" r="9" /><path d="M12 7v10M9.5 9.2c0-1.1 1.1-1.8 2.5-1.8s2.5.7 2.5 1.8-1 1.5-2.5 1.8-2.5.9-2.5 1.9 1.1 1.8 2.5 1.8 2.5-.7 2.5-1.8" /></svg>;
    case "settings": return <svg viewBox="0 0 24 24" {...s}><circle cx="12" cy="12" r="3" /><path d="M19.4 15a1.6 1.6 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.6 1.6 0 0 0-2.7 1.1V21a2 2 0 1 1-4 0v-.1a1.6 1.6 0 0 0-2.7-1.1l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.6 1.6 0 0 0-1.1-2.7H3a2 2 0 1 1 0-4h.1a1.6 1.6 0 0 0 1.1-2.7l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.6 1.6 0 0 0 2.7-1.1V3a2 2 0 1 1 4 0v.1a1.6 1.6 0 0 0 2.7 1.1l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.6 1.6 0 0 0 1.5 1z" /></svg>;
    default: return <svg viewBox="0 0 24 24" {...s}><path d={ICON[props.id]} /></svg>;
  }
}

const NAV = [
  { group: "Explore", items: [
    { id: "logs", label: "Logs" }, { id: "tail", label: "Live tail", live: true },
    { id: "dashboards", label: "Dashboards" }, { id: "alerts", label: "Alerts", count: 2 },
  ] },
  { group: "Operate", items: [
    { id: "storage", label: "Storage tiers" }, { id: "cost", label: "Cost", live: true },
    { id: "pipelines", label: "Pipelines" }, { id: "settings", label: "Settings" },
  ] },
];

export function Sidebar() {
  const config = useConfig();
  const params = useParams();
  const loc = useLocation();
  const base = () => `/${params.org}/${params.env}`;
  const active = (id: string) => loc.pathname.endsWith(`/${id}`);
  const env = () => config.environments.find((e) => e.id === params.env) ?? config.environments[0];

  return (
    <aside class="sidebar">
      <div class="brand">
        <div class="logo" aria-hidden="true">
          <svg viewBox="0 0 24 24" fill="none" stroke="#04140f" stroke-width="2.1" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16M4 12h16M4 17h11" /></svg>
        </div>
        <div>
          <div class="name">Verdigris</div>
          <Switcher />
        </div>
      </div>

      <nav class="nav">
        <For each={NAV}>{(g) => <>
          <div class="nav-label">{g.group}</div>
          <For each={g.items}>{(it) => (
            <A href={`${base()}/${it.id}`} class={`nav-item ${active(it.id) ? "active" : ""}`}>
              <NavIcon id={it.id} />
              <span>{it.label}</span>
              <Show when={(it as any).live}><span class="pill-badge"><span class="dot" />live</span></Show>
              <Show when={(it as any).count}><span class="count-badge">{(it as any).count}</span></Show>
            </A>
          )}</For>
        </>}</For>
      </nav>

      <div class="sidebar-foot">
        <div class="row" style={{ "justify-content": "space-between", "margin-bottom": "8px" }}>
          <span class="deploy-chip">{config.mode}</span>
        </div>
        <div class="bucket"><span class="dot" />{env()?.bucket}</div>
        <div class="bucket-sub">data never leaves your account</div>
      </div>
    </aside>
  );
}
