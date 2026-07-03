/* Dashboards — time-series via the uPlot (canvas) seam = the scale path.
   Charts get pre-aggregated series from the api; uPlot draws them on canvas. */
import { createResource, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead, Card, CardHead, Stat } from "@/ui/primitives";
import { TimeSeries, HBars } from "@/charts";

export default function Dashboards() {
  const api = useApi();
  const [m] = createResource(() => api(), (a) => a.metrics());

  const delta = (d: number, invert = false) => {
    const good = invert ? d < 0 : d > 0;
    return <span class={`delta ${good ? "up" : "down"}`}>{d > 0 ? "▲" : "▼"} {Math.abs(d)}% <span class="muted" style={{ "font-weight": 500 }}>vs 1h ago</span></span>;
  };

  return (
    <>
      <ViewHead title="Dashboards" sub="Last 1 hour · auto-refresh 30s"
        actions={<div class="seg"><button>1h</button><button class="on">6h</button><button>24h</button><button>7d</button></div>} />
      <Show when={m()} fallback={<div class="empty">loading…</div>}>
        <div class="view-body">
          <div class="grid cols-4" style={{ "margin-bottom": "16px" }}>
            <Stat label="Ingest rate" value={m()!.tiles.ingest.value} unit={m()!.tiles.ingest.unit} delta={delta(m()!.tiles.ingest.delta)} />
            <Stat label="Error rate" value={m()!.tiles.errors.value} unit={m()!.tiles.errors.unit} delta={delta(m()!.tiles.errors.delta)} />
            <Stat label="p99 latency" value={m()!.tiles.p99.value} unit={m()!.tiles.p99.unit} delta={delta(m()!.tiles.p99.delta, true)} />
            <Stat label="Stored (30d)" value={m()!.tiles.stored.value} unit={m()!.tiles.stored.unit} delta={delta(m()!.tiles.stored.delta)} />
          </div>
          <div class="grid cols-2" style={{ "margin-bottom": "16px" }}>
            <Card><CardHead title="Ingest rate" hint="events / sec · canvas" /><TimeSeries data={m()!.ingestRate} color="#46c9ab" max={1800} /></Card>
            <Card><CardHead title="Error rate" hint="% of events" /><TimeSeries data={m()!.errorRate} color="#e36a6a" fill="rgba(227,106,106,0.12)" max={12} /></Card>
          </div>
          <div class="grid cols-2">
            <Card><CardHead title="p99 latency" hint="ms" /><TimeSeries data={m()!.p99} color="#dca041" fill="rgba(220,160,65,0.12)" max={2000} /></Card>
            <Card><CardHead title="Log volume by service" hint="GB / 24h" /><HBars items={m()!.volumeByService.map((s) => ({ name: s.name, value: s.gb, label: s.gb + " GB" }))} /></Card>
          </div>
        </div>
      </Show>
    </>
  );
}
