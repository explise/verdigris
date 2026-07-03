/* ═══════════════════════════════════════════════════════════════════
   Chart renderer seam.

   Pages import chart components from HERE, never from a concrete renderer.
   Today low-density charts render as SVG (svg.tsx) and time-series render
   via uPlot/canvas (UPlotChart in uplot.tsx). When a chart hits the scale
   wall, swap its renderer here — pages don't change.

     Density rule of thumb:
       ≤ ~300 points / bars / arcs  → SVG (crisp, CSS-styleable, a11y-friendly)
       time-series, many points     → uPlot (canvas, real-time, millions)
       raw scatter / heatmap        → WebGL (future)
   ═══════════════════════════════════════════════════════════════════ */
export { AreaSVG, HBars, Donut } from "./svg";
export { UPlotChart } from "./uplot";

// The dashboard time-series use the canvas renderer by default (scale path).
export { UPlotChart as TimeSeries } from "./uplot";
