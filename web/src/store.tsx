/* App-wide context: runtime config + a scope-bound api factory.
   Scope (org/env) comes from the route, so the api is always tenant-correct. */
import { createContext, useContext, ParentProps, createMemo } from "solid-js";
import { useParams } from "@solidjs/router";
import { getConfig, RuntimeConfig } from "@/config/runtime";
import { createApi, Api } from "@/lib/api";

const Ctx = createContext<{ config: RuntimeConfig }>();

export function AppProvider(props: ParentProps) {
  return <Ctx.Provider value={{ config: getConfig() }}>{props.children}</Ctx.Provider>;
}

export function useConfig(): RuntimeConfig {
  const c = useContext(Ctx);
  return c ? c.config : getConfig();
}

/** Tenant-scoped api bound to the current /:org/:env route params. */
export function useApi(): () => Api {
  const p = useParams();
  return createMemo(() => createApi({ org: p.org ?? "default", env: p.env ?? "default" }));
}
