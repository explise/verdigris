/* ═══════════════════════════════════════════════════════════════════
   Session — where the user's API token lives (this browser only).

   transport.ts reads the token on every request and dispatches
   `auth-required` on a 401; the TokenGate (shell/TokenGate.tsx) listens,
   collects a token, and stores it here. Tokens are issued/revoked by an
   admin on the backend (`POST /v1/auth/tokens`) — the SPA never mints one.
   ═══════════════════════════════════════════════════════════════════ */

const KEY = "vdg.apiToken";
const EVT = "vdg:auth-required";

export function getToken(): string | null {
  try {
    return localStorage.getItem(KEY);
  } catch {
    return null; // storage disabled (private mode) → behave as signed out
  }
}

export function setToken(token: string): void {
  try {
    localStorage.setItem(KEY, token);
  } catch {
    /* storage disabled — the gate will reappear next load */
  }
}

export function clearToken(): void {
  try {
    localStorage.removeItem(KEY);
  } catch {
    /* ignore */
  }
}

/** Fired by the transport when the backend answers 401 (missing/revoked token). */
export function authRequired(): void {
  window.dispatchEvent(new CustomEvent(EVT));
}

/** Subscribe to auth-required; returns the unsubscribe function. */
export function onAuthRequired(fn: () => void): () => void {
  window.addEventListener(EVT, fn);
  return () => window.removeEventListener(EVT, fn);
}
