/* ═══════════════════════════════════════════════════════════════════
   uPlot canvas renderer — the SCALE path for time-series.

   Canvas, ~40KB, built for real-time metric/log dashboards with thousands–
   millions of points. One <canvas>, no per-point DOM. Hand it columnar data
   (x[], y[]) — ideally straight from an Arrow column — and it draws fast.

   This component owns the uPlot lifecycle (create / resize / update / destroy)
   and exposes a plain props API so pages stay renderer-agnostic.
   ═══════════════════════════════════════════════════════════════════ */
import { onCleanup, onMount, createEffect } from "solid-js";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";

export function UPlotChart(props: {
  /** y-series values. x is synthesized as 0..n unless `x` is provided. */
  data: number[];
  x?: number[];
  color?: string;
  fill?: string;
  height?: number;
  max?: number;
}) {
  let host!: HTMLDivElement;
  let plot: uPlot | null = null;

  const build = () => {
    const n = props.data.length;
    const xs = props.x ?? Array.from({ length: n }, (_, i) => i);
    const color = props.color ?? "#46c9ab";
    const opts: uPlot.Options = {
      width: host.clientWidth || 560,
      height: props.height ?? 150,
      cursor: { y: false, points: { size: 6 } },
      legend: { show: false },
      scales: { x: { time: false }, y: props.max ? { range: [0, props.max] } : {} },
      axes: [
        { stroke: "#61776e", grid: { stroke: "#1e2823", width: 1 }, ticks: { stroke: "#1e2823" }, font: "11px ui-monospace, monospace" },
        { stroke: "#61776e", grid: { stroke: "#1e2823", width: 1 }, ticks: { stroke: "#1e2823" }, size: 44, font: "11px ui-monospace, monospace" },
      ],
      series: [
        {},
        { stroke: color, width: 2, fill: props.fill ?? "rgba(70,201,171,0.14)", points: { show: false } },
      ],
    };
    if (plot) plot.destroy();
    plot = new uPlot(opts, [xs, props.data], host);
  };

  onMount(() => {
    build();
    const ro = new ResizeObserver(() => plot && plot.setSize({ width: host.clientWidth, height: props.height ?? 150 }));
    ro.observe(host);
    onCleanup(() => { ro.disconnect(); plot?.destroy(); plot = null; });
  });

  // rebuild when the data identity changes
  createEffect(() => { props.data; if (plot) build(); });

  return <div ref={host} style={{ width: "100%" }} />;
}
