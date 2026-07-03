/* Small presentational primitives shared by pages. Keep pages declarative. */
import { JSX, Show } from "solid-js";

export function ViewHead(props: { title: string; sub?: string; actions?: JSX.Element }) {
  return (
    <div class="view-head">
      <div class="titles">
        <h1>{props.title}</h1>
        <Show when={props.sub}><div class="sub">{props.sub}</div></Show>
      </div>
      <div class="actions">{props.actions}</div>
    </div>
  );
}

export function Card(props: { children: JSX.Element; class?: string }) {
  return <div class={`card ${props.class ?? ""}`}>{props.children}</div>;
}

export function CardHead(props: { title: string; hint?: string; right?: JSX.Element }) {
  return (
    <div class="card-head">
      <h3>{props.title}</h3>
      <Show when={props.hint}><span class="hint">{props.hint}</span></Show>
      <Show when={props.right}><span class="right">{props.right}</span></Show>
    </div>
  );
}

export function Stat(props: { label: string; value: JSX.Element; unit?: string; delta?: JSX.Element; class?: string }) {
  return (
    <div class={`card stat ${props.class ?? ""}`}>
      <div class="label">{props.label}</div>
      <div class="value">{props.value}{props.unit && <small> {props.unit}</small>}</div>
      <Show when={props.delta}><div class="delta">{props.delta}</div></Show>
    </div>
  );
}

export function Badge(props: { kind: string; children: JSX.Element }) {
  return <span class={`badge ${props.kind}`}><span class="dot" style={{ background: "currentColor" }} />{props.children}</span>;
}
