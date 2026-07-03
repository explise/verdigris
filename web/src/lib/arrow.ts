/* ═══════════════════════════════════════════════════════════════════
   Arrow decode — the scale path for tabular results.

   The backend (DuckDB/Parquet) is Arrow-native. Tabular endpoints emit
   Arrow IPC; we decode columnar buffers with near-zero copy instead of
   parsing millions of JSON objects. Buffers can be handed straight to the
   virtualized table and (eventually) to uPlot/canvas.
   ═══════════════════════════════════════════════════════════════════ */
import { tableFromIPC, Table } from "apache-arrow";

export const ARROW_MIME = "application/vnd.apache.arrow.stream";

/** Decode an Arrow IPC stream (bytes) into an Arrow Table. */
export function decodeArrow(buf: ArrayBuffer | Uint8Array): Table {
  return tableFromIPC(buf instanceof Uint8Array ? buf : new Uint8Array(buf));
}

/** Materialize an Arrow Table into plain row objects (for the row-oriented UI).
    For very large result sets, prefer reading columns directly off the Table
    rather than materializing every row — kept simple here for the scaffold. */
export function tableToRows<T = Record<string, unknown>>(table: Table): T[] {
  const out: T[] = new Array(table.numRows);
  for (let i = 0; i < table.numRows; i++) {
    out[i] = table.get(i)!.toJSON() as T;
  }
  return out;
}
