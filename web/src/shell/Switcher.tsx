/* Org + Environment switcher. Both live in the URL (/:org/:env/...), so this
   is the entry point for multi-tenant / org-wide navigation. On-prem configs
   ship a single org; cloud configs ship many. */
import { createSignal, For, Show, onCleanup } from "solid-js";
import { useNavigate, useParams, useLocation } from "@solidjs/router";
import { useConfig } from "@/store";

export function Switcher() {
  const config = useConfig();
  const params = useParams();
  const loc = useLocation();
  const nav = useNavigate();
  const [open, setOpen] = createSignal(false);

  const page = () => loc.pathname.split("/")[3] ?? "logs";
  const curOrg = () => config.orgs.find((o) => o.id === params.org) ?? config.orgs[0];
  const curEnv = () => config.environments.find((e) => e.id === params.env) ?? config.environments[0];

  const goOrg = (id: string) => { nav(`/${id}/${params.env}/${page()}`); setOpen(false); };
  const goEnv = (id: string) => { nav(`/${params.org}/${id}/${page()}`); setOpen(false); };

  const onDoc = (e: MouseEvent) => { if (!(e.target as HTMLElement).closest(".switcher")) setOpen(false); };
  document.addEventListener("click", onDoc);
  onCleanup(() => document.removeEventListener("click", onDoc));

  return (
    <div class={`switcher ${open() ? "open" : ""}`}>
      <button class="env" onClick={(e) => { e.stopPropagation(); setOpen(!open()); }}>
        {curEnv()?.label} <span class="caret">▾</span>
      </button>
      <Show when={config.orgs.length > 1}><div class="org">{curOrg()?.name}</div></Show>
      <Show when={open()}>
        <div class="switch-menu" onClick={(e) => e.stopPropagation()}>
          <Show when={config.orgs.length > 1}>
            <div class="em-label">Organization</div>
            <For each={config.orgs}>{(o) => (
              <div class={`switch-item ${o.id === curOrg()?.id ? "sel" : ""}`} onClick={() => goOrg(o.id)}>
                <span class="region-dot" /><span>{o.name}</span><span class="check">✓</span>
              </div>
            )}</For>
          </Show>
          <div class="em-label">Environment</div>
          <For each={config.environments}>{(e) => (
            <div class={`switch-item ${e.id === curEnv()?.id ? "sel" : ""}`} onClick={() => goEnv(e.id)}>
              <span class="region-dot" />
              <span><div>{e.label}</div><div style={{ "font-size": "11px", color: "var(--ink-faint)" }}>{e.region}</div></span>
              <span class="check">✓</span>
            </div>
          )}</For>
        </div>
      </Show>
    </div>
  );
}
