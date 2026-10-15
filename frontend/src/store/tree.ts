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

async function loadTopics(clusterId: string): Promise<void> {
  tree.loading[clusterId] = true;
  try {
    tree.topics[clusterId] = await rpc<TopicRow[]>('topic.list', { clusterId });
    tree.errors[clusterId] = undefined;
  } catch (e) {
    tree.errors[clusterId] = errText(e);
  } finally {
    tree.loading[clusterId] = false;
  }
}

async function loadPartitions(clusterId: string, topic: string): Promise<void> {
  const key = partKey(clusterId, topic);
  tree.loading[key] = true;
  try {
    const details = await rpc<{ partitions?: PartitionRow[] }[]>('topic.describe', {
      clusterId, names: [topic], includeInternal: true,
    });
    tree.partitions[key] = details?.[0]?.partitions ?? [];
    tree.errors[key] = undefined;
  } catch (e) {
    tree.errors[key] = errText(e);
  } finally {
    tree.loading[key] = false;
  }
}

function clearCache(clusterId: string): void {
  delete tree.topics[clusterId];
  const prefix = `${clusterId}\u0000`;
  for (const key of Object.keys(tree.partitions)) {
    if (key.startsWith(prefix)) delete tree.partitions[key];
  }
}

/** topic.changed: drop the cluster's caches; refetch now if its node is open. */
export function invalidateCluster(clusterId: string): void {
  clearCache(clusterId);
  if (tree.expanded[clusterId]) void loadTopics(clusterId);
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
      clearCache(p.clusterId);
    }
  });
  const offTopic = on('topic.changed', (e) => {
    const p = (e.payload ?? {}) as { clusterId?: string };
    if (p.clusterId) invalidateCluster(p.clusterId);
  });
  void refreshClusters();
  return () => { offConn(); offTopic(); };
}
