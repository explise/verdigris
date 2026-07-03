export const fmtBytes = (gb: number) =>
  gb < 1 ? `${Math.round(gb * 1000)} MB` : gb >= 1000 ? `${(gb / 1000).toFixed(1)} TB` : `${gb % 1 === 0 ? gb : gb.toFixed(1)} GB`;

export const usd = (n: number) => "$" + n.toFixed(2);

export const esc = (s: unknown) =>
  String(s).replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c] as string));
