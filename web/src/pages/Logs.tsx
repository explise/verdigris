/* Logs — flagship explore view.
   Demonstrates the scale primitives: a VIRTUALIZED table (renders ~30 of N
   rows), the live cost estimate, the cold-scan confirm gate, and a wire-format
   badge showing arrow vs json. Data flows through the tenant-scoped api seam. */
import { createResource, createSignal, createMemo, For, Show, createEffect, onMount } from "solid-js";
import { createVirtualizer } from "@tanstack/solid-virtual";
import { useApi } from "@/store";
import type { LogRow, TierId } from "@/lib/types";
import { fmtBytes, usd } from "@/lib/format";

// backend sends ISO timestamps; show just the time-of-day
const fmtTs = (ts: string) => (/[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)/.exec(ts || "") ?? [, ts])[1];

const ALL_TIERS: { id: TierId; t: string }[] = [
  { id: "hot", t: "0.4s" }, { id: "warm", t: "~6s" }, { id: "cold", t: "restore" },
];

export default function Logs() {
  const api = useApi();
  const [sql, setSql] = createSignal("service:auth status>=500 | last 1h");
  const [tiers, setTiers] = createSignal<TierId[]>(["hot", "warm"]);
  const [gate, setGate] = createSignal(true);
  const [confirm, setConfirm] = createSignal(false);
  const [lang, setLang] = createSignal<"SQL" | "DSL">("SQL");

  // `submitted` is the query that has actually been RUN — the resource keys on it,
  // so editing the box doesn't refetch until Run/Enter. A fresh object each time
  // means Run always re-executes, even with unchanged text.
  const [submitted, setSubmitted] = createSignal({ sql: sql(), tiers: tiers() });
  const [query] = createResource(
    () => ({ a: api(), q: submitted() }),
    ({ a, q }) => a.queryLogs({ sql: q.sql, tiers: q.tiers }),
  );
  const [est] = createResource(tiers, (t) => api().estimate(t));

  const execute = () => setSubmitted({ sql: sql(), tiers: tiers() });

  const toggleTier = (id: TierId) =>
    setTiers((cur) => (cur.includes(id) ? (cur.length > 1 ? cur.filter((x) => x !== id) : cur) : [...cur, id]));

  // Cold scans go through the confirm gate first; everything else runs immediately.
  const run = () => { if (gate() && tiers().includes("cold")) { setConfirm(true); return; } execute(); };

  // virtualized rows
  let scrollEl!: HTMLDivElement;
  const rows = createMemo<LogRow[]>(() => query()?.rows ?? []);
  const virt = createVirtualizer({
    get count() { return rows().length; },
    getScrollElement: () => scrollEl,
    estimateSize: () => 33,
    overscan: 14,
  });

  const [openRow, setOpenRow] = createSignal<number | null>(null);

  return (
    <>
      <div class="query-shell">
        <div class="querybar">
          <div class="query-input">
            <span class="qicon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="11" cy="11" r="7" /><path d="m20 20-3.2-3.2" /></svg></span>
            <input value={sql()} spellcheck={false} onInput={(e) => setSql(e.currentTarget.value)} onKeyDown={(e) => e.key === "Enter" && run()} />
            <span class="lang-toggle" onClick={() => setLang(lang() === "SQL" ? "DSL" : "SQL")}>{lang()}</span>
          </div>
          <button class="btn primary" style={{ height: "50px", padding: "0 26px" }} onClick={run}>Run</button>
        </div>

        <div class="meta-row">
          <span class="tier-label">tier</span>
          <div class="pills">
            <For each={ALL_TIERS}>{(p) => (
              <span class={`pill ${tiers().includes(p.id) ? "sel" : ""}`} data-tier={p.id} onClick={() => toggleTier(p.id)}>
                <span class="dot" />{p.id} <span class="t-time">· {p.t}</span>
              </span>
            )}</For>
          </div>
          <div class="cost">
            <span class={`chk ${gate() ? "on" : ""}`} title="Require confirmation before cold-tier scans" onClick={() => setGate(!gate())}>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5L20 7" /></svg>
            </span>
            this query scans <b>~{fmtBytes(est()?.scanGB ?? 0)}</b> ·{" "}
            <span class={`dollar ${!est()?.costUsd ? "free" : est()!.costUsd < 0.5 ? "warn" : "danger"}`}>
              {!est()?.costUsd ? "~$0.00" : "~" + usd(est()!.costUsd)}
            </span>
          </div>
        </div>

        <div class="histo">
          <For each={query()?.histogram ?? []}>{(b) => {
            // normalize against the tallest bucket so bars are correct at any data scale
            const max = Math.max(1, ...(query()?.histogram ?? []).map((x) => x.total || 0));
            return (
              <div class="bar" style={{ height: `${Math.max(3, Math.min(100, (b.total / max) * 100))}%` }} title={`${b.total} events · ${b.errors} errors`}>
                <div class="err" style={{ height: `${b.total ? (b.errors / b.total) * 100 : 0}%` }} />
              </div>
            );
          }}</For>
        </div>
      </div>

      <div class="vlog-head"><div>Timestamp</div><div>Level</div><div>Service</div><div>Message</div></div>
      <div class="vlog" ref={scrollEl}>
        <div style={{ height: `${virt.getTotalSize()}px`, position: "relative" }}>
          <For each={virt.getVirtualItems()}>{(vi) => {
            const r = () => rows()[vi.index];
            return (
              <div class="vrow" data-lvl={r().level} style={{ height: "33px", transform: `translateY(${vi.start}px)` }}
                onClick={() => setOpenRow(openRow() === vi.index ? null : vi.index)}>
                <div class="c-ts">{fmtTs(r().ts)}</div><div class="c-lvl">{r().level}</div>
                <div class="c-svc">{r().service}</div><div class="c-msg">{r().message}</div>
              </div>
            );
          }}</For>
        </div>
      </div>

      <div class="logs-foot">
        <span class="count"><b>{query()?.stats.events.toLocaleString() ?? "…"}</b> events</span>
        <span class="sep">·</span><span>queried in place · no rehydration</span>
        <span class="sep">·</span>
        <span>scanned {query()?.stats.files ?? "…"} files in <b class="mono" style={{ color: "var(--ink)" }}>{query()?.stats.elapsedMs ?? "…"}ms</b></span>
        <span class="right">
          <span class="wire-badge">wire: {query()?.stats.wire ?? "…"}</span>
          <span class="glyph" style={{ color: "var(--copper)" }}>▦</span> parquet on s3 · {query()?.stats.engine ?? "datafusion"}
        </span>
      </div>

      <Show when={confirm()}>
        <div class="scrim open" onClick={(e) => e.target === e.currentTarget && setConfirm(false)}>
          <div class="modal">
            <h3><span class="warnico">⚠</span> This query scans cold storage</h3>
            <p>The <b>cold</b> tier restores from Glacier Flexible before it can be scanned. Cold logs are always queryable — but retrieval is billed by scanned-GB and isn't instant.</p>
            <div class="modal-grid">
              <div class="mg"><div class="mk">Scans</div><div class="mv figure danger">~{fmtBytes(est()?.scanGB ?? 0)}</div></div>
              <div class="mg"><div class="mk">Retrieval cost</div><div class="mv figure danger">~{usd(est()?.costUsd ?? 0)}</div></div>
              <div class="mg"><div class="mk">Restore wait</div><div class="mv figure">3–5 h</div></div>
              <div class="mg"><div class="mk">Mode</div><div class="mv figure">Standard</div></div>
            </div>
            <div class="modal-actions">
              <button class="btn" onClick={() => setConfirm(false)}>Cancel</button>
              <button class="btn primary" style={{ background: "linear-gradient(160deg,var(--warn),#b8801f)" }} onClick={() => { setConfirm(false); execute(); }}>Restore &amp; run</button>
            </div>
          </div>
        </div>
      </Show>
    </>
  );
}
