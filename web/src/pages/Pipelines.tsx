import { createResource, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead, Card, CardHead, Stat } from "@/ui/primitives";
import { TimeSeries } from "@/charts";

const PNode = (props: { name: string; sub: string; stat?: string; badge?: string }) => (
  <div class="pipe-node">
    <div class="pn-name">{props.name}{props.badge && <span class="badge ok" style={{ "margin-left": "auto" }}>{props.badge}</span>}</div>
    <div class="pn-sub">{props.sub}</div>
    {props.stat && <div class="pn-stat" style={{ color: "var(--copper-bright)" }}>{props.stat}</div>}
  </div>
);
const Arrow = () => <div class="flow-arrow"><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14m-6-6 6 6-6 6" /></svg></div>;

export default function Pipelines() {
  const api = useApi();
  const [p] = createResource(() => api(), (a) => a.pipelines());
  return (
    <>
      <ViewHead title="Pipelines" sub="Vector DaemonSet → Tarnish → Parquet batcher" actions={<button class="btn">Refresh</button>} />
      <Show when={p()} fallback={<div class="empty">loading…</div>}>
        <div class="view-body">
          <div class="grid cols-4" style={{ "margin-bottom": "18px" }}>
            <Stat label="Throughput" value="1.39k" unit="ev/s" delta={<span class="delta up">▲ healthy</span>} />
            <Stat label="Drop rate (Tarnish)" value={String(p()!.dropRate ?? 0)} unit="%" delta={<span class="delta flat">noise filtered</span>} />
            <Stat label="Ingest lag" value={p()!.ingestLag || "—"} delta={<span class="delta up">▲ real-time</span>} />
            <Stat label="Parquet rolls" value={<span style={{ "font-size": "20px" }}>{p()!.parquetRolls || "—"}</span>} delta={<span class="muted" style={{ "font-size": "11px" }}>128MB · zstd</span>} />
          </div>
          <Card class="pad-lg" >
            <CardHead title="Flow" hint="live" />
            <div class="pipe">
              <PNode name="Vector DaemonSet" sub="k8s stdout/stderr · 42 nodes" stat="1.18k ev/s" badge="healthy" />
              <Arrow />
              <PNode name="Tarnish" sub="drop rules · −31% noise" stat="filter" />
              <Arrow />
              <PNode name="Parquet batcher" sub="128MB rolls · Iceberg commit" stat="→ s3" badge="healthy" />
            </div>
          </Card>
          <div class="grid cols-2" style={{ "margin-top": "16px" }}>
            <Card><CardHead title="Ingest throughput" hint="events / sec" />
              <Show when={(p()!.throughput ?? []).length > 1} fallback={<div class="empty" style={{ padding: "44px 0" }}>no throughput data yet</div>}>
                <TimeSeries data={p()!.throughput} color="#46c9ab" max={1800} />
              </Show>
            </Card>
            <Card class="pad-lg">
              <CardHead title="Sources & transforms" />
              <Show when={(p()!.sources ?? []).length || (p()!.transforms ?? []).length} fallback={<div class="empty">no sources or transforms configured</div>}>
                <table class="tbl"><thead><tr><th>Stage</th><th>Kind</th><th>Detail</th></tr></thead>
                  <tbody>
                    <For each={p()!.sources ?? []}>{(s) => <tr><td><span class="badge ok">src</span> {s.name}</td><td class="mono muted">{s.kind}</td><td class="mono">{s.rate} · {s.nodes} nodes</td></tr>}</For>
                    <For each={p()!.transforms ?? []}>{(t) => <tr><td><span class="badge muted">xform</span> {t.name}</td><td class="mono muted">{t.kind}</td><td class="muted">{t.note}{t.dropped && <> · <b style={{ color: "var(--warn)" }}>{t.dropped}</b></>}</td></tr>}</For>
                  </tbody>
                </table>
              </Show>
            </Card>
          </div>
        </div>
      </Show>
    </>
  );
}
