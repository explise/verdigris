/* ═══════════════════════════════════════════════════════════════════
   Arrow decode — the scale path for tabular results.

   The backend (DataFusion/Parquet) is Arrow-native. Tabular endpoints emit
   Arrow IPC; we decode columnar buffers with near-zero copy instead of
   parsing millions of JSON objects. Buffers can be handed straight to the
   virtualized table and (eventually) to uPlot/canvas.

   apache-arrow is heavy (~40KB gzip) and only needed on the tabular/Arrow
   path — which is NOT the default wire (JSON is). So it is loaded LAZILY via
   dynamic import(): Vite splits it into its own chunk, keeping it out of the
   initial bundle. The MIME constant below stays a plain string so the accept
   header can be built synchronously without pulling the library in.
   ═══════════════════════════════════════════════════════════════════ */

export const ARROW_MIME = "application/vnd.apache.arrow.stream";

/** Decode an Arrow IPC stream (bytes) into plain row objects for the
    row-oriented UI. Dynamically imports apache-arrow so it never lands in the
    initial chunk; the first Arrow response pays a one-time async load.

    For very large result sets, prefer reading columns directly off the Table
    rather than materializing every row — kept simple here for the scaffold. */
export async function decodeArrowRows<T = Record<string, unknown>>(
  buf: ArrayBuffer | Uint8Array,
): Promise<T[]> {
  const { tableFromIPC } = await import("apache-arrow");
  const table = tableFromIPC(buf instanceof Uint8Array ? buf : new Uint8Array(buf));
  const out: T[] = new Array(table.numRows);
  for (let i = 0; i < table.numRows; i++) {
    out[i] = table.get(i)!.toJSON() as T;
  }
  return out;
}
