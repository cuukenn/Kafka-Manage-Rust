// T043 cluster-tree store: lazy topics/partitions caches + event invalidation.
// Wire facts verified against Rust (narrow greps):
//  - "topic.list" payload {clusterId} -> TopicInfo[] camelCase (kafka-core topic.rs:
//    name, internal, partitionCount, replicationFactor, configs).
//  - "topic.describe" payload {clusterId, names: string[], includeInternal} ->
//    TopicDetail[] (name, internal, partitions: PartitionDetail[] camelCase: id,
//    leaderId, replicaIds, isrIds, beginningOffset, endOffset); response shape per
//    webview-bridge tests/router_topic_produce.rs (data array, details[0].partitions).
//  - events (protocol.rs `pub mod event`): "connection.state" {clusterId, state,
//    reason?}; "topic.changed" {clusterId, topic, change} (kafka-app bus.rs
//    AppEvent::TopicChanged, camelCase). No other topic.* event names exist.
import { reactive } from 'vue';
import { on } from '../bridge/events';
import { refresh as refreshClusters } from './clusters';
import { rpc } from '../bridge/rpc';

export interface TopicRow {
  name: string;
  internal: boolean;
  partitionCount: number;
  replicationFactor: number;
}
export interface PartitionRow {
  id: number;
  leaderId: number;
  replicaIds: number[];
  isrIds: number[];
  beginningOffset: number;
  endOffset: number;
}

/** Cache key for a topic node (partitions / expanded / loading / errors). */
export const partKey = (clusterId: string, topic: string): string => `${clusterId}\u0000${topic}`;

/** topic.changed kinds that justify dropping tree caches (see startTreeEvents A1 note). */
const STRUCTURAL_CHANGES = new Set(['created', 'deleted', 'config_changed']);

export const tree = reactive({
  /** clusterId -> topics; undefined = never fetched (lazy). */
  topics: {} as Record<string, TopicRow[] | undefined>,
  /** partKey(clusterId, topic) -> partitions; undefined = never fetched. */
  partitions: {} as Record<string, PartitionRow[] | undefined>,
  expanded: {} as Record<string, boolean>,
  loading: {} as Record<string, boolean>,
  errors: {} as Record<string, string | undefined>,
});

const errText = (e: unknown): string => (e instanceof Error ? e.message : String(e));

// T043 review I2: per-key request generation + in-flight dedup. clearCache bumps
// generations, so responses issued before an invalidation/disconnect are discarded
// at commit time instead of repopulating cleared caches (last-arrival-wins).
const gen = new Map<string, number>();
const inflight = new Map<string, Promise<void>>();

/** One fetch per key at a time; `fresh()` is false once the request is superseded. */
function fetchDeduped(key: string, call: (fresh: () => boolean) => Promise<void>): Promise<void> {
  const active = inflight.get(key);
  if (active) return active;
  const seq = (gen.get(key) ?? 0) + 1;
  gen.set(key, seq);
  tree.loading[key] = true;
  const fresh = (): boolean => gen.get(key) === seq;
  const done = call(fresh).finally(() => {
    if (fresh()) tree.loading[key] = false;
    if (inflight.get(key) === done) inflight.delete(key);
  });
  inflight.set(key, done);
  return done;
}

function loadTopics(clusterId: string): Promise<void> {
  return fetchDeduped(clusterId, async (fresh) => {
    try {
      const rows = await rpc<TopicRow[]>('topic.list', { clusterId });
      if (fresh()) {
        tree.topics[clusterId] = rows;
        tree.errors[clusterId] = undefined;
      }
    } catch (e) {
      if (fresh()) tree.errors[clusterId] = errText(e);
    }
  });
}

function loadPartitions(clusterId: string, topic: string): Promise<void> {
  const key = partKey(clusterId, topic);
  return fetchDeduped(key, async (fresh) => {
    try {
      const details = await rpc<{ partitions?: PartitionRow[] }[]>('topic.describe', {
        clusterId, names: [topic], includeInternal: true,
      });
      if (fresh()) {
        tree.partitions[key] = details?.[0]?.partitions ?? [];
        tree.errors[key] = undefined;
      }
    } catch (e) {
      if (fresh()) tree.errors[key] = errText(e);
    }
  });
}

function clearCache(clusterId: string): void {
  const prefix = `${clusterId}\u0000`;
  const supersede = (key: string): void => {
    gen.set(key, (gen.get(key) ?? 0) + 1);
    inflight.delete(key);
    tree.loading[key] = false;
  };
  delete tree.topics[clusterId];
  supersede(clusterId);
  for (const key of Object.keys(tree.partitions)) {
    if (key.startsWith(prefix)) {
      delete tree.partitions[key];
      supersede(key);
    }
  }
}

/**
 * topic.changed: drop the cluster's caches; refetch open nodes immediately —
 * topics if the cluster node is open AND partitions of every currently-expanded
 * topic node (T043 review I1: otherwise open topics silently show empty lists).
 */
export function invalidateCluster(clusterId: string): void {
  const prefix = `${clusterId}\u0000`;
  const openParts = tree.expanded[clusterId]
    ? Object.keys(tree.expanded).filter((k) => k.startsWith(prefix) && tree.expanded[k])
    : [];
  clearCache(clusterId);
  if (tree.expanded[clusterId]) {
    void loadTopics(clusterId);
    for (const key of openParts) void loadPartitions(clusterId, key.slice(prefix.length));
  }
}

export function toggleCluster(clusterId: string, live: boolean): void {
  const open = !tree.expanded[clusterId];
  tree.expanded[clusterId] = open;
  if (open && live && tree.topics[clusterId] === undefined) void loadTopics(clusterId);
}

export function toggleTopic(clusterId: string, topic: string): void {
  const key = partKey(clusterId, topic);
  const open = !tree.expanded[key];
  tree.expanded[key] = open;
  if (open && tree.partitions[key] === undefined) void loadPartitions(clusterId, topic);
}

/** Refresh buttons: force a re-pull even when a cache exists. */
export const reloadCluster = (clusterId: string): void => { void loadTopics(clusterId); };
export const reloadTopic = (clusterId: string, topic: string): void => { void loadPartitions(clusterId, topic); };

/**
 * Tree-lifetime subscriptions: keep cluster rows live even when the clusters view
 * is unmounted (TreeView lives in the shell sidebar), grey/clear caches on
 * connection loss, invalidate topic caches on "topic.changed".
 */
export function startTreeEvents(): () => void {
  const offConn = on('connection.state', (e) => {
    const p = (e.payload ?? {}) as { clusterId?: string; state?: string };
    void refreshClusters();
    if (p.clusterId && p.state !== 'connected' && p.state !== 'degraded') {
      tree.expanded[p.clusterId] = false;
      // Collapse this cluster's topic nodes too: an already-expanded topic node
      // would never re-trigger loadPartitions on reconnect (expand path is the
      // toggleTopic guard), leaving it silently empty (T043 review I1/I2).
      const prefix = `${p.clusterId}\u0000`;
      for (const key of Object.keys(tree.expanded)) {
        if (key.startsWith(prefix)) delete tree.expanded[key];
      }
      clearCache(p.clusterId);
    }
  });
  const offTopic = on('topic.changed', (e) => {
    // A1 (stage-4 final review): invalidate only on explicitly STRUCTURAL change
    // kinds. Wire kinds (kafka-app bus.rs TopicChange, serde snake_case):
    // "created" | "deleted" | "config_changed" | "messages". "messages" fires on
    // every produce burst; clearing + refetching the whole cluster tree for it
    // caused flicker + an RPC storm in this shell-lifetime component. Unknown
    // kinds deliberately do NOT invalidate (explicit-structural rule).
    const p = (e.payload ?? {}) as { clusterId?: string; change?: string };
    if (p.clusterId && STRUCTURAL_CHANGES.has(p.change ?? '')) invalidateCluster(p.clusterId);
  });
  void refreshClusters();
  return () => { offConn(); offTopic(); };
}
