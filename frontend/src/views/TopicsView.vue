<script setup lang="ts">
// T044a topic table: topic.list for the current cluster (tree selection wins,
// else first connected cluster from clusters store), create modal, row delete
// with confirm + tree invalidation, inline error bars. Wire facts (narrow greps
// kafka-core protocol.rs/topic.rs, webview-bridge tests/router_topic_produce.rs):
// topic.list {clusterId} -> TopicInfo[] camelCase; topic.create {clusterId,
// name, numPartitions, replicationFactor}; topic.delete {clusterId, names[]}
// -> [{name, error}]; topic.changed {clusterId, topic, change} -> coalesced re-pull.
import { computed, onMounted, onUnmounted, ref } from 'vue';
import TopicModal from '../components/TopicModal.vue';
import { on } from '../bridge/events';
import { rpc } from '../bridge/rpc';
import { clusters } from '../store/clusters';
import { invalidateCluster, type TopicRow } from '../store/tree';
import { clearTopicSelection, ui } from '../store/ui';

const CONNECTED = new Set(['connected', 'degraded']);

/** Cluster whose topics the table shows: selected node's cluster, else first connected. */
const targetCluster = computed(() => {
  const sel = ui.selectedTopic;
  if (sel) return clusters.rows.find((c) => c.id === sel.clusterId) ?? null;
  return clusters.rows.find((c) => CONNECTED.has(c.state)) ?? null;
});

const rows = ref<TopicRow[]>([]);
const loading = ref(false);
const tableError = ref<string | null>(null);
const showCreate = ref(false);

// T042/T043 coalesced re-pull: arrivals during an in-flight pull schedule
// exactly one follow-up, so topic.changed bursts cannot race overlapping pulls.
let inflight: Promise<void> | null = null;
let pending = false;

async function pull(): Promise<void> {
  const cluster = targetCluster.value;
  if (!cluster) {
    rows.value = [];
    return;
  }
  loading.value = true;
  try {
    rows.value = await rpc<TopicRow[]>('topic.list', { clusterId: cluster.id });
    tableError.value = null;
  } catch (e) {
    tableError.value = e instanceof Error ? e.message : String(e);
  } finally {
    loading.value = false;
  }
}

function refresh(): void {
  if (inflight) {
    pending = true;
    return;
  }
  inflight = pull().finally(() => {
    inflight = null;
    if (pending) {
      pending = false;
      refresh();
    }
  });
}

async function removeTopic(name: string): Promise<void> {
  const cluster = targetCluster.value;
  if (!cluster || !window.confirm(`删除 topic「${name}」？`)) return;
  try {
    const results = await rpc<{ name: string; error: string | null }[]>('topic.delete', {
      clusterId: cluster.id,
      names: [name],
    });
    const failed = (results ?? []).find((r) => r.error != null);
    tableError.value = failed ? `删除 ${failed.name} 失败：${failed.error}` : null;
  } catch (e) {
    tableError.value = e instanceof Error ? e.message : String(e);
    return;
  }
  if (ui.selectedTopic?.topic === name) clearTopicSelection();
  invalidateCluster(cluster.id); // T043 tree store: drop caches + refetch open nodes
  refresh();
}

let off: (() => void) | null = null;
onMounted(() => {
  off = on('topic.changed', (e) => {
    const p = (e.payload ?? {}) as { clusterId?: string };
    if (p.clusterId && p.clusterId === targetCluster.value?.id) refresh();
  });
  refresh();
});
onUnmounted(() => off?.());
</script>

<template>
  <section>
    <div class="mb-2 flex items-center gap-3">
      <h2 class="text-lg font-semibold">
        {{ targetCluster ? `Topics · ${targetCluster.name}` : 'Topics' }}
      </h2>
      <button v-if="ui.selectedTopic" type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="clearTopicSelection">返回集群列表</button>
      <div class="ml-auto flex gap-2">
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" :disabled="!targetCluster || loading" @click="refresh">刷新</button>
        <button type="button" class="rounded bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50" :disabled="!targetCluster" @click="showCreate = true">新建 Topic</button>
      </div>
    </div>

    <p v-if="tableError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ tableError }}</p>

    <p v-if="!targetCluster" class="text-sm text-slate-500 dark:text-slate-400">
      未找到已连接集群：请先在集群页连接，或在左侧树中选择一个 topic 节点。
    </p>

    <table v-else class="w-full border-collapse text-left text-xs">
      <thead>
        <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
          <th class="px-2 py-1.5 font-medium">名称</th>
          <th class="px-2 py-1.5 font-medium">Internal</th>
          <th class="px-2 py-1.5 font-medium">分区数</th>
          <th class="px-2 py-1.5 font-medium">副本因子</th>
          <th class="px-2 py-1.5 text-right font-medium">操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in rows"
          :key="row.name"
          class="border-b border-slate-100 dark:border-slate-800"
          :class="
            ui.selectedTopic && ui.selectedTopic.clusterId === targetCluster.id && ui.selectedTopic.topic === row.name
              ? 'bg-emerald-50 dark:bg-emerald-900/40'
              : ''
          "
        >
          <td class="px-2 py-1.5 font-medium text-slate-800 dark:text-slate-100">{{ row.name }}</td>
          <td class="px-2 py-1.5">{{ row.internal ? '是' : '—' }}</td>
          <td class="px-2 py-1.5">{{ row.partitionCount }}</td>
          <td class="px-2 py-1.5">{{ row.replicationFactor }}</td>
          <td class="px-2 py-1.5 text-right">
            <button
              type="button"
              class="rounded border border-rose-300 px-2 py-0.5 text-rose-700 hover:bg-rose-50 dark:border-rose-700 dark:text-rose-300 dark:hover:bg-rose-900/40"
              @click="removeTopic(row.name)"
            >
              删除
            </button>
          </td>
        </tr>
        <tr v-if="!loading && rows.length === 0">
          <td colspan="5" class="px-2 py-3 text-slate-500 dark:text-slate-400">该集群暂无 topic。</td>
        </tr>
      </tbody>
    </table>

    <TopicModal
      v-if="showCreate && targetCluster"
      :cluster-id="targetCluster.id"
      @close="showCreate = false"
      @created="showCreate = false; refresh()"
    />
  </section>
</template>
