import { createResource, createSignal, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead } from "@/ui/primitives";
import type { NewAlert } from "@/lib/types";

export default function Alerts() {
  const api = useApi();
  const [list, { refetch }] = createResource(() => api(), (a) => a.alerts());
  const firing = () => (list() ?? []).filter((a) => a.state === "firing");

  const [open, setOpen] = createSignal(false);
  const [name, setName] = createSignal("");
  const [sql, setSql] = createSignal("");
  const [cmp, setCmp] = createSignal<NewAlert["comparator"]>("gt");
  const [threshold, setThreshold] = createSignal("");
  const [severity, setSeverity] = createSignal<NewAlert["severity"]>("warning");
  const [webhook, setWebhook] = createSignal("");
  const [busy, setBusy] = createSignal(false);
  const [err, setErr] = createSignal("");

  const reset = () => {
    setName(""); setSql(""); setCmp("gt"); setThreshold("");
    setSeverity("warning"); setWebhook(""); setErr("");
  };

  const submit = async (e: Event) => {
    e.preventDefault();
    if (!name().trim() || !sql().trim()) return;
    setBusy(true); setErr("");
    const body: NewAlert = {
      name: name().trim(),
      sql: sql().trim(),
      comparator: cmp(),
      threshold: parseFloat(threshold()) || 0,
      severity: severity(),
      ...(webhook().trim() ? { webhook: webhook().trim() } : {}),
    };
    try {
      await api().createAlert(body);
      reset(); setOpen(false); refetch();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string) => {
    if (!confirm("Delete this alert rule?")) return;
    try { await api().deleteAlert(id); refetch(); }
    catch (e) { alert("Failed to delete: " + (e instanceof Error ? e.message : String(e))); }
  };

  return (
    <>
      <ViewHead title="Alerts" sub="Rules evaluated continuously over the hot tier"
        actions={<button class="btn primary" onClick={() => setOpen(!open())}>+ New alert</button>} />
      <Show when={list.loading}><div class="empty">loading…</div></Show>
      <Show when={list()}>
        <div class="view-body">
          <Show when={open()}>
            <form class="card pad-lg" style={{ "margin-bottom": "18px" }} onSubmit={submit}>
              <div style={{ display: "grid", "grid-template-columns": "1fr 2fr", gap: "10px", "margin-bottom": "10px" }}>
                <input class="input" placeholder="Rule name — e.g. High error volume"
                  value={name()} onInput={(e) => setName(e.currentTarget.value)} />
                <input class="input mono" placeholder="SELECT count(*) AS v FROM logs WHERE level = 'ERROR'"
                  value={sql()} onInput={(e) => setSql(e.currentTarget.value)} />
              </div>
              <div style={{ display: "flex", gap: "10px", "align-items": "center", "flex-wrap": "wrap" }}>
                <select class="input" style={{ width: "auto" }} value={cmp()}
                  onChange={(e) => setCmp(e.currentTarget.value as NewAlert["comparator"])}>
                  <option value="gt">&gt;</option><option value="ge">&ge;</option>
                  <option value="lt">&lt;</option><option value="le">&le;</option>
                </select>
                <input class="input mono" type="number" step="any" placeholder="threshold" style={{ width: "130px" }}
                  value={threshold()} onInput={(e) => setThreshold(e.currentTarget.value)} />
                <select class="input" style={{ width: "auto" }} value={severity()}
                  onChange={(e) => setSeverity(e.currentTarget.value as NewAlert["severity"])}>
                  <option value="critical">critical</option><option value="warning">warning</option><option value="info">info</option>
                </select>
                <input class="input mono" placeholder="webhook URL (optional)" style={{ flex: "1", "min-width": "180px" }}
                  value={webhook()} onInput={(e) => setWebhook(e.currentTarget.value)} />
                <button class="btn primary" type="submit" disabled={busy()}>{busy() ? "Creating…" : "Create rule"}</button>
              </div>
              <div class="muted" style={{ "font-size": "11.5px", "margin-top": "8px" }}>
                The query must return one number (a <code>v</code> column, or its first numeric column). Evaluated every 15s against the live table.
              </div>
              <Show when={err()}>
                <div style={{ color: "var(--error)", "font-size": "12.5px", "margin-top": "8px" }}>{err()}</div>
              </Show>
            </form>
          </Show>

          <Show when={firing().length}>
            <div class="card pad-lg" style={{ "border-color": "rgba(227,106,106,.3)", background: "linear-gradient(90deg,var(--error-soft),transparent 50%)", "margin-bottom": "18px" }}>
              <div class="row"><span class="pulse-dot live" /><b style={{ color: "var(--error)" }}>{firing().length} alert{firing().length > 1 ? "s" : ""} firing</b></div>
            </div>
          </Show>

          <Show when={list()!.length} fallback={<div class="empty" style={{ padding: "48px" }}>No alert rules yet. Click <b>New alert</b> to add one.</div>}>
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
                <button class="alert-del" title="Delete rule" aria-label="Delete rule" onClick={() => remove(a.id)}>×</button>
              </div>
            )}</For>
          </Show>
        </div>
      </Show>
    </>
  );
}
