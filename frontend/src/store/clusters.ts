// T042 cluster store: cluster.* wiring + coalesced refresh on connection.state.
// Wire facts verified against Rust: cluster.list {} -> ClusterView[] (kafka-app
// services/clusters.rs: ClusterConfig flattened camelCase + state + driver);
// cluster.save payload = ClusterConfig body (omit id for new rows: serde default
// mints a ClusterId); connect/disconnect/delete payload {clusterId}.
import { reactive } from 'vue';
import { on } from '../bridge/events';
import { RpcError, rpc } from '../bridge/rpc';

/** One cluster.list row: flattened ClusterConfig + live state + driver. */
export interface ClusterRow {
  id: string;
  name: string;
  bootstrapServers: string[];
  mock: boolean;
  clientId: string;
  securityProtocol: string;
  auth: { mechanism: string; username: string; password: string; saslProtocol?: string | null } | null;
  ssl: { caPem?: string | null; caPath?: string | null; certPem?: string | null; keyPem?: string | null; verifyCertificate: boolean; verifyHostname: boolean } | null;
  state: string;
  driver: string | null;
}

export const clusters = reactive({
  rows: [] as ClusterRow[],
  loading: false,
  error: null as string | null,
});

function message(e: unknown): string {
  return e instanceof RpcError ? `${e.kind}: ${e.message}` : String(e);
}

let inflight: Promise<void> | null = null;
let pending = false;

async function pull(): Promise<void> {
  clusters.loading = true;
  try {
    clusters.rows = await rpc<ClusterRow[]>('cluster.list', {});
    clusters.error = null;
  } catch (e) {
    clusters.error = message(e);
  } finally {
    clusters.loading = false;
  }
}

/** Coalesced re-pull: triggers arriving during an in-flight pull schedule exactly
 *  one follow-up, so connection.state events cannot race overlapping pulls (T041 review). */
export function refresh(): Promise<void> {
  if (inflight) {
    pending = true;
    return inflight;
  }
  inflight = pull().finally(() => {
    inflight = null;
    if (pending) {
      pending = false;
      void refresh();
    }
  });
  return inflight;
}

async function act(cmd: string, payload: unknown): Promise<boolean> {
  try {
    await rpc(cmd, payload);
    clusters.error = null;
    void refresh();
    return true;
  } catch (e) {
    clusters.error = message(e);
    return false;
  }
}

export const saveCluster = (cfg: Record<string, unknown>): Promise<boolean> => act('cluster.save', cfg);
export const deleteCluster = (id: string): Promise<boolean> => act('cluster.delete', { clusterId: id });
export const connectCluster = (id: string): Promise<boolean> => act('cluster.connect', { clusterId: id });
export const disconnectCluster = (id: string): Promise<boolean> => act('cluster.disconnect', { clusterId: id });

/** Page-lifetime subscription: connection.state -> coalesced cluster.list pull. */
export function startClusterEvents(): () => void {
  const off = on('connection.state', () => void refresh());
  void refresh();
  return off;
}
