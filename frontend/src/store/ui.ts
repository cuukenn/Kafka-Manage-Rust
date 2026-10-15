// T041 UI store: view routing (no vue-router, R19) + theme persistence +
// connection badge state. Wire facts verified against the Rust side:
//  - config.get / config.set payload {key, value} (webview-bridge handlers.rs
//    ConfigPayload; tests use key "ui.theme"); config.get data = stored JSON | null.
//  - cluster.list rows are ClusterView (kafka-app services/clusters.rs):
//    ClusterConfig flattened camelCase (id, name, bootstrapServers, ...) plus
//    `state`: "disconnected"|"connecting"|"connected"|"degraded"|"error"
//    (ConnectionState, snake_case) and `driver`: "mock"|"real"|null.
//  - cluster events: kafka-core protocol.rs `event::CONNECTION_STATE` =
//    "connection.state", payload {clusterId, state, reason?}.

import { reactive } from 'vue';
import { rpc } from '../bridge/rpc';
import { on } from '../bridge/events';

export type ViewName = 'clusters' | 'groups' | 'logs';
export type Theme = 'light' | 'dark';

/** One cluster.list row, only the fields the shell renders. */
interface ClusterRow {
  id?: string;
  name?: string;
  state?: string;
}

const THEME_KEY = 'ui.theme';
const CONNECTED_STATES = new Set(['connected', 'degraded']);

export const ui = reactive({
  currentView: 'clusters' as ViewName,
  theme: 'dark' as Theme,
  /** null = not connected; otherwise the connected cluster's display name. */
  connectedCluster: null as string | null,
  badgeLoading: true,
});

export function setView(view: ViewName): void {
  ui.currentView = view;
}

function applyTheme(): void {
  document.documentElement.classList.toggle('dark', ui.theme === 'dark');
}

/** Load the persisted theme (config.get) and apply it. Best-effort. */
export async function initTheme(): Promise<void> {
  try {
    const stored = await rpc<Theme | null>('config.get', { key: THEME_KEY });
    if (stored === 'light' || stored === 'dark') ui.theme = stored;
  } catch {
    // First run or bridge hiccup: keep the default (dark, matching index.html).
  }
  applyTheme();
}

export function toggleTheme(): void {
  ui.theme = ui.theme === 'dark' ? 'light' : 'dark';
  applyTheme();
  void rpc('config.set', { key: THEME_KEY, value: ui.theme }).catch(() => {
    // Theme stays applied locally; persistence retried on next toggle.
  });
}

/** Re-pull cluster.list and point the badge at the first connected cluster. */
export async function refreshBadge(): Promise<void> {
  try {
    const rows = await rpc<ClusterRow[]>('cluster.list', {});
    const conn = (Array.isArray(rows) ? rows : []).find(
      (r) => typeof r.state === 'string' && CONNECTED_STATES.has(r.state),
    );
    ui.connectedCluster = conn ? conn.name || conn.id || 'cluster' : null;
  } catch {
    ui.connectedCluster = null;
  } finally {
    ui.badgeLoading = false;
  }
}

/**
 * Initial cluster.list pull + event-driven refresh on "connection.state".
 * Returns the unsubscribe function. Late-subscriber batch loss in events.ts
 * is covered by the mount-time pull above (state is live server-side).
 */
export function startBadge(): () => void {
  const off = on('connection.state', () => {
    void refreshBadge();
  });
  void refreshBadge();
  return off;
}
