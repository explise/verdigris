import { createResource, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead } from "@/ui/primitives";

export default function Alerts() {
  const api = useApi();
  const [list] = createResource(() => api(), (a) => a.alerts());
  const firing = () => (list() ?? []).filter((a) => a.state === "firing");

  return (
    <>
      <ViewHead title="Alerts" sub="Rules evaluated continuously over the hot tier"
        actions={<button class="btn primary">+ New alert</button>} />
      <Show when={list()} fallback={<div class="empty">loading…</div>}>
        <div class="view-body">
          <Show when={firing().length}>
            <div class="card pad-lg" style={{ "border-color": "rgba(227,106,106,.3)", background: "linear-gradient(90deg,var(--error-soft),transparent 50%)", "margin-bottom": "18px" }}>
              <div class="row"><span class="pulse-dot live" /><b style={{ color: "var(--error)" }}>{firing().length} alerts firing</b>
                <span class="muted">— paging {[...new Set(firing().map((f) => f.channel))].join(", ")}</span></div>
            </div>
          </Show>
          <For each={list()}>{(a) => (
            <div class={`alert-row ${a.state}`}>
              <div class="ai">{a.state === "firing"
                ? <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 9v4m0 4h.01M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" /></svg>
                : <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5" /></svg>}</div>
              <div class="amain">
                <div class="atitle">{a.name} <span class={`badge ${a.severity === "critical" ? "error" : a.severity === "warning" ? "warn" : "muted"}`} style={{ "margin-left": "6px" }}>{a.severity}</span></div>
                <div class="acond">{a.cond}</div>
              </div>
              <div class="ameta">
                <div class={`badge ${a.state === "firing" ? "error" : "ok"}`}><span class="dot" style={{ background: "currentColor" }} />{a.state === "firing" ? "FIRING" : "OK"}</div>
                <div style={{ "margin-top": "6px" }}>now <b class="mono" style={{ color: a.state === "firing" ? "var(--error)" : "var(--ink)" }}>{a.value}</b> {a.since !== "—" ? "· " + a.since : ""}</div>
                <div class="muted mono" style={{ "font-size": "11px", "margin-top": "3px" }}>{a.channel}</div>
              </div>
            </div>
          )}</For>
        </div>
      </Show>
    </>
  );
}
