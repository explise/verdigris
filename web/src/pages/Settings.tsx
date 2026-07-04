import { createResource, createSignal, For, Show } from "solid-js";
import { useApi } from "@/store";
import { ViewHead, Card, CardHead } from "@/ui/primitives";

function Row(props: { label: string; hint?: string; children: any }) {
  return (
    <div class="form-row">
      <div><div class="flabel">{props.label}</div>{props.hint && <div class="fhint">{props.hint}</div>}</div>
      <div class="form-ctrl">{props.children}</div>
    </div>
  );
}

export default function Settings() {
  const api = useApi();
  const [s] = createResource(() => api(), (a) => a.settings());
  const [compute, setCompute] = createSignal(4);
  const [cold, setCold] = createSignal(true);

  return (
    <>
      <ViewHead title="Settings" sub="Bucket, retention, routing & query compute" actions={<button class="btn primary">Save changes</button>} />
      <Show when={s()} fallback={<div class="empty">loading…</div>}>
        {(() => { setCompute(s()!.queryCompute); setCold(s()!.confirmColdScans); return null; })()}
        <div class="view-body" style={{ "max-width": "920px" }}>
          <Card class="pad-lg"><div style={{ "margin-bottom": "10px" }}><CardHead title="Storage" /></div>
            <Row label="S3 bucket" hint="Source of truth. Verdigris never copies data out of it."><input class="input mono" value={s()!.bucket ?? ""} placeholder="not configured" style={{ width: "340px" }} /></Row>
            <Row label="Region"><input class="input mono" value={s()!.region ?? ""} placeholder="not configured" style={{ width: "200px" }} /></Row>
            <Row label="IAM role" hint="Assumed for query + lifecycle operations."><input class="input mono" value={s()!.iamRole ?? ""} placeholder="not configured" style={{ width: "420px" }} /></Row>
            <Row label="Retention" hint="After this, objects expire (delete) via S3 lifecycle."><div class="row"><input class="input mono" value={s()!.retentionDays ?? ""} style={{ width: "90px" }} /> <span class="muted">days</span></div></Row>
          </Card>

          <Card class="pad-lg"><div style={{ "margin": "16px 0 10px" }}><CardHead title="Query compute" hint="storage and compute are decoupled — this dial only changes speed" /></div>
            <Row label="Provisioned compute" hint="More compute = faster queries from colder tiers. Storage cost is unaffected.">
              <div class="dial" style={{ width: "380px" }}>
                <input type="range" min="1" max="16" value={compute()} style={{ "--p": `${(compute() / 16) * 100}%` } as any} onInput={(e) => setCompute(+e.currentTarget.value)} />
                <span class="dval">{compute()} vCPU · ~{(compute() * 0.4).toFixed(1)}s hot</span>
              </div>
            </Row>
          </Card>

          <Card class="pad-lg"><div style={{ "margin": "16px 0 10px" }}><CardHead title="Severity routing" hint="decides which prefix / storage class a log lands in at write time" /></div>
            <For each={s()!.routing}>{(r) => (
              <div class="rule"><span class="mono">{r.match}</span><span class="arrow">→</span><span><span class={`badge ${r.tier}`}>{r.tier}</span></span><button class="btn ghost sm">✕</button></div>
            )}</For>
            <button class="btn sm" style={{ "margin-top": "6px" }}>+ Add rule</button>
            <p class="fhint" style={{ "margin-top": "12px" }}>Routing is a storage-placement hint, <b>not</b> a price lever — severity never changes what a GB costs.</p>
          </Card>

          <Card class="pad-lg"><div style={{ "margin": "16px 0 10px" }}><CardHead title="Safety" /></div>
            <Row label="Confirm cold-tier scans" hint="Show a cost + restore-time gate before any query that touches Glacier Flexible.">
              <div class={`toggle ${cold() ? "on" : ""}`} onClick={() => setCold(!cold())} />
            </Row>
          </Card>
        </div>
      </Show>
    </>
  );
}
