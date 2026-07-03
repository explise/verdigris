/* Router + layout. Tenancy lives in the path: /:org/:env/:page.
   `/` redirects to the first org/env from runtime config (single-org on-prem
   still works — it just has one option). */
import { Router, Route, Navigate, RouteSectionProps } from "@solidjs/router";
import { ParentProps } from "solid-js";
import { AppProvider } from "@/store";
import { Sidebar } from "@/shell/Sidebar";
import { getConfig } from "@/config/runtime";

import Logs from "@/pages/Logs";
import LiveTail from "@/pages/LiveTail";
import Dashboards from "@/pages/Dashboards";
import Alerts from "@/pages/Alerts";
import Storage from "@/pages/Storage";
import Cost from "@/pages/Cost";
import Pipelines from "@/pages/Pipelines";
import Settings from "@/pages/Settings";

function Layout(props: RouteSectionProps) {
  return (
    <AppProvider>
      <div class="app">
        <Sidebar />
        <main class="main" id="view">{props.children}</main>
      </div>
    </AppProvider>
  );
}

function defaultPath() {
  const c = getConfig();
  return `/${c.orgs[0]?.id ?? "default"}/${c.environments[0]?.id ?? "default"}/logs`;
}

export default function App() {
  return (
    <Router root={Layout}>
      <Route path="/:org/:env/logs" component={Logs} />
      <Route path="/:org/:env/tail" component={LiveTail} />
      <Route path="/:org/:env/dashboards" component={Dashboards} />
      <Route path="/:org/:env/alerts" component={Alerts} />
      <Route path="/:org/:env/storage" component={Storage} />
      <Route path="/:org/:env/cost" component={Cost} />
      <Route path="/:org/:env/pipelines" component={Pipelines} />
      <Route path="/:org/:env/settings" component={Settings} />
      <Route path="*" component={() => <Navigate href={defaultPath()} />} />
    </Router>
  );
}
