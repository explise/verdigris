import { createResource, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead, Card, CardHead, Stat } from "@/ui/primitives";
import { Donut, HBars, AreaSVG } from "@/charts";
import { fmtBytes, usd } from "@/lib/format";

export default function Cost() {
  const api = useApi();
  const [c] = createResource(() => api(), (a) => a.cost());
  const savings = () => Math.round((1 - c()!.vsDatadog.ours / c()!.vsDatadog.datadog) * 100);

  return (
    <>
      <ViewHead title="Cost" sub="Live spend across storage + compute"
        actions={<div class="seg"><button>7d</button><button class="on">30d</button><button>90d</button></div>} />
      <Show when={c()} fallback={<div class="empty">loading…</div>}>
        <div class="view-body">
          <div class="grid cols-4" style={{ "margin-bottom": "16px" }}>
            <Stat label="Month to date" value={usd(c()!.monthToDate)} delta={<span class="delta flat">across all tiers</span>} />
            <Stat label="Projected" value={usd(c()!.projected)} delta={<span class={`delta ${c()!.projected > c()!.lastMonth ? "down" : "up"}`}>{c()!.projected > c()!.lastMonth ? "▲" : "▼"} vs {usd(c()!.lastMonth)} last mo</span>} />
            <Stat label="Glacier retrieval" value={usd(c()!.breakdown.find((b) => b.label.includes("retrieval"))!.usd)} delta={<span class="delta flat">pay only when queried</span>} />
            <Stat label="vs Datadog (est)" class="" value={<span style={{ color: "var(--copper-bright)" }}>{savings()}%</span>} delta={<span class="delta up">cheaper · same volume</span>} />
          </div>
          <div class="grid cols-2" style={{ "margin-bottom": "16px" }}>
            <Card class="pad-lg">
              <CardHead title="Spend breakdown" hint="this month" />
              <div class="row" style={{ gap: "26px", "align-items": "center" }}>
                <Donut items={c()!.breakdown.map((b) => ({ color: b.color, value: b.usd }))} center={usd(c()!.monthToDate).replace(".00", "")} centerSub="month to date" />
                <div style={{ flex: 1 }}><HBars items={c()!.breakdown.map((b) => ({ name: b.label, value: b.usd, label: usd(b.usd), color: b.color }))} /></div>
              </div>
            </Card>
            <Card class="pad-lg">
              <CardHead title="Daily spend" hint="last 30 days · $/day" />
              <AreaSVG data={c()!.spendSeries} max={4} />
              <div style={{ "margin-top": "18px" }}><CardHead title="Sovereignty" /></div>
              <p class="muted" style={{ "font-size": "12.5px", "line-height": 1.6, margin: 0 }}>Every figure here is <b style={{ color: "var(--ink)" }}>your own AWS bill</b> — Verdigris charges no per-GB ingestion margin. Data never leaves your bucket.</p>
            </Card>
          </div>
          <Card class="pad-lg">
            <CardHead title="Most expensive queries" hint="cold-tier scans drive retrieval cost" />
            <table class="tbl">
              <thead><tr><th>Query</th><th>Tier</th><th class="num">Scanned</th><th class="num">Cost</th><th>User</th><th>When</th></tr></thead>
              <tbody>
                <For each={c()!.expensiveQueries}>{(q) => (
                  <tr>
                    <td class="mono" style={{ color: "var(--ink)" }}>{q.q}</td>
                    <td><span class={`badge ${q.tier}`}>{q.tier}</span></td>
                    <td class="num">{fmtBytes(q.scanGB)}</td>
                    <td class="num" style={{ color: "var(--warn)" }}>{usd(q.usd)}</td>
                    <td class="mono muted">{q.user}</td><td class="muted">{q.when}</td>
                  </tr>
                )}</For>
              </tbody>
            </table>
          </Card>
        </div>
      </Show>
    </>
  );
}
