import { createResource, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead, Card, CardHead } from "@/ui/primitives";
import { fmtBytes, usd } from "@/lib/format";
import type { Tier } from "@/lib/types";

function TierCard(props: { t: Tier }) {
  return (
    <div class={`tier-card ${props.t.id}`}>
      <div class="topline" />
      <div class="tname"><span class={`badge ${props.t.id}`}><span class="dot" style={{ background: "currentColor" }} />{props.t.name}</span></div>
      <div class="tclass">{props.t.class}</div>
      <div class="tbig">{fmtBytes(props.t.bytesGB)}</div>
      <div class="muted" style={{ "font-size": "11.5px", "margin-bottom": "8px" }}>{props.t.age}</div>
      <div class="trow"><span>Objects</span><span>{props.t.objects}</span></div>
      <div class="trow"><span>Storage / mo</span><span>{usd(props.t.perMonth)}</span></div>
    </div>
  );
}

const Arrow = (props: { lbl: string }) => (
  <div class="flow-arrow">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14m-6-6 6 6-6 6" /></svg>
    <div class="lbl">{props.lbl}</div>
  </div>
);

export default function Storage() {
  const api = useApi();
  const [s] = createResource(() => api(), (a) => a.storage());
  return (
    <>
      <ViewHead title="Storage tiers" sub="Lifecycle-managed hot → warm → cold"
        actions={<button class="btn">Refresh</button>} />
      <Show when={s()} fallback={<div class="empty">loading…</div>}>
        <div class="view-body">
          <div class="tier-flow" style={{ "margin-bottom": "22px" }}>
            <TierCard t={s()!.tiers[0]} /><Arrow lbl="after 3d" /><TierCard t={s()!.tiers[1]} /><Arrow lbl="after 30d" /><TierCard t={s()!.tiers[2]} />
          </div>
          <div class="grid cols-2">
            <Card class="pad-lg">
              <CardHead title="Data distribution" hint="share of total bytes" />
              <div class="bar-track" style={{ height: "14px", "margin-bottom": "6px" }}>
                <For each={s()!.tiers}>{(t) => <div class={`seg-fill ${t.id}`} style={{ width: `${t.pct}%` }} />}</For>
              </div>
              <div class="legend"><For each={s()!.tiers}>{(t) => <div class="item"><span class="sw" style={{ background: `var(--${t.id})` }} />{t.name} · {t.pct}%</div>}</For></div>
              <div style={{ "margin-top": "22px" }}><CardHead title="Lifecycle policy" /></div>
              <For each={s()!.lifecycle}>{(l) => <div class="row" style={{ padding: "8px 0", "border-top": "1px solid var(--line-soft)" }}><span class="badge muted mono">{l.at}</span><span class="muted">{l.action}</span></div>}</For>
            </Card>
            <Card class="pad-lg">
              <CardHead title="Compaction" right={<span class="badge ok"><span class="dot" style={{ background: "currentColor" }} />{s()!.compaction.status}</span>} />
              <p class="muted" style={{ "font-size": "12.5px", "line-height": 1.6, margin: "0 0 16px" }}>Merges the millions of tiny Parquet files streaming logs produce into {s()!.compaction.targetSize} files — fixing scan speed and the Glacier 40KB-per-object metadata tax at once.</p>
              <div class="grid cols-2" style={{ gap: "12px" }}>
                <div class="card stat" style={{ padding: "14px" }}><div class="label">Small files</div><div class="value" style={{ "font-size": "21px" }}>{s()!.compaction.smallFiles}</div><div class="delta down">▼ merging</div></div>
                <div class="card stat" style={{ padding: "14px" }}><div class="label">Compacted</div><div class="value" style={{ "font-size": "21px" }}>{s()!.compaction.compacted}</div><div class="delta up">▲ {s()!.compaction.targetSize}</div></div>
                <div class="card stat" style={{ padding: "14px" }}><div class="label">Reclaimed</div><div class="value" style={{ "font-size": "21px" }}>{s()!.compaction.reclaimedGB} GB</div><div class="muted" style={{ "font-size": "11px" }}>this week</div></div>
                <div class="card stat" style={{ padding: "14px" }}><div class="label">Last run</div><div class="value" style={{ "font-size": "21px" }}>{s()!.compaction.lastRun}</div><div class="muted" style={{ "font-size": "11px" }}>background</div></div>
              </div>
            </Card>
          </div>
        </div>
      </Show>
    </>
  );
}
