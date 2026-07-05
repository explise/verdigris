/* Token gate — the login surface for token-auth deployments.

   Shown when /config.json says `auth.kind === "token"` and no token is stored,
   or when the backend answers 401 (transport dispatches `auth-required`, e.g.
   the token was revoked). Stores the pasted token in localStorage and reloads
   so every page refetches authenticated. Tokens are issued by an admin
   (`POST /v1/auth/tokens`); the SPA only carries one. */
import { createSignal, onCleanup, Show } from "solid-js";
import { getConfig } from "@/config/runtime";
import { getToken, setToken, clearToken, onAuthRequired } from "@/lib/session";

export function TokenGate() {
  const cfg = getConfig();
  const [needed, setNeeded] = createSignal(cfg.auth.kind === "token" && !getToken());
  const [rejected, setRejected] = createSignal(false);
  const [value, setValue] = createSignal("");

  const off = onAuthRequired(() => {
    clearToken();
    setRejected(true);
    setNeeded(true);
  });
  onCleanup(off);

  const submit = (e: Event) => {
    e.preventDefault();
    const t = value().trim();
    if (!t) return;
    setToken(t);
    location.reload();
  };

  return (
    <Show when={needed()}>
      <div class="token-gate" role="dialog" aria-modal="true" aria-label="API token required">
        <form class="token-gate-card" onSubmit={submit}>
          <h2>API token required</h2>
          <p class="token-gate-msg">
            {rejected()
              ? "That token was rejected or has been revoked. Paste a valid API token to continue."
              : "This Verdigris deployment has authentication enabled. Paste your API token to continue."}
          </p>
          <input
            class="input mono"
            type="password"
            placeholder="paste token…"
            autocomplete="off"
            value={value()}
            onInput={(e) => setValue(e.currentTarget.value)}
            autofocus
          />
          <button class="btn primary" type="submit" disabled={!value().trim()}>
            Continue
          </button>
          <p class="token-gate-hint">
            Tokens are issued by an administrator (<code>POST /v1/auth/tokens</code>) and stored
            only in this browser.
          </p>
        </form>
      </div>
    </Show>
  );
}
