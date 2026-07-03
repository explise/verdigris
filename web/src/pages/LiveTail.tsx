/* Live tail — streaming via the api.tail seam (mock ticker now, SSE/WS later).
   Caps DOM nodes at 200, auto-follows unless the user scrolls up. */
import { createSignal, onCleanup, For } from "solid-js";
import { useApi } from "@/store";
import { ViewHead } from "@/ui/primitives";
import type { LogRow, Level } from "@/lib/types";

export default function LiveTail() {
  const api = useApi();
  const [lines, setLines] = createSignal<LogRow[]>([]);
  const [filter, setFilter] = createSignal<"all" | Level>("all");
  const [paused, setPaused] = createSignal(false);
  let wrap!: HTMLDivElement;
  let follow = true;

  // buffer ALL levels; the rendered list is a filtered view so changing the
  // level applies retroactively to lines already on screen, not just new ones.
  const visible = () => (filter() === "all" ? lines() : lines().filter((e) => e.level === filter()));

  const handle = api().tail((e) => {
    if (paused()) return;
    setLines((cur) => [...cur.slice(-199), e]);
    queueMicrotask(() => { if (follow && wrap) wrap.scrollTop = wrap.scrollHeight; });
  });
  onCleanup(() => handle.stop());

  return (
    <>
      <ViewHead title="Live tail" sub="Streaming from the hot tier · last 200 lines"
        actions={<>
          <div class="seg">
            <For each={["all", "ERROR", "WARN", "INFO"] as const}>{(l) => (
              <button class={filter() === l ? "on" : ""} onClick={() => setFilter(l as any)}>{l === "all" ? "all" : l.toLowerCase()}</button>
            )}</For>
          </div>
          <button class="btn" onClick={() => setPaused(!paused())}>
            <span class={`pulse-dot ${paused() ? "" : "live"}`} style={paused() ? { background: "var(--ink-faint)" } : {}} /> {paused() ? "resume" : "pause"}
          </button>
        </>} />
      <div class="tail-wrap" ref={wrap} onScroll={() => (follow = wrap.scrollTop + wrap.clientHeight >= wrap.scrollHeight - 40)}>
        <For each={visible()}>{(e) => (
          <div class="tail-line" data-lvl={e.level}>
            <span class="t">{e.ts}</span><span class="l">{e.level}</span><span class="s">{e.service}</span><span class="m">{e.message}</span>
          </div>
        )}</For>
      </div>
    </>
  );
}
