/* Entry point. Load runtime deployment config BEFORE first render so getConfig()
   is populated everywhere (tenancy, mocks, wire format, feature flags). */
import { render } from "solid-js/web";
import { loadRuntimeConfig } from "@/config/runtime";
import App from "@/App";

import "@/ui/app.css";
import "@/ui/extra.css";

async function boot() {
  await loadRuntimeConfig();
  render(() => <App />, document.getElementById("root")!);
}

boot();
