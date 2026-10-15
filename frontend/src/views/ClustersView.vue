<script setup lang="ts">
// T042 cluster management view: cluster.list table (name / bootstrap / Mock /
// state badge / driver / actions) + new-cluster button + edit modal. Rows
// refresh on connection.state via the coalesced pull in store/clusters.ts;
// RpcError surfaces as an inline error bar (toast lands with T049).
import { onMounted, onUnmounted, ref } from 'vue';
import ClusterModal from '../components/ClusterModal.vue';
import { clusters, connectCluster, deleteCluster, disconnectCluster, startClusterEvents, type ClusterRow } from '../store/clusters';

const modal = ref<{ initial: ClusterRow | null } | null>(null);
let stop: (() => void) | undefined;

onMounted(() => { stop = startClusterEvents(); });
onUnmounted(() => stop?.());

const btn = 'rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700';

function stateClass(state: string): string {
  if (state === 'connected') return 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200';
  if (state === 'degraded' || state === 'connecting') return 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200';
  if (state === 'error') return 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200';
  return 'bg-slate-200 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
}

function remove(row: ClusterRow): void {
  if (window.confirm(`删除集群「${row.name}」？`)) void deleteCluster(row.id);
}
</script>

<template>
  <section>
    <div class="mb-3 flex items-center gap-3">
      <h2 class="text-lg font-semibold">集群</h2>
      <button type="button" class="ml-auto rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700" @click="modal = { initial: null }">
        新建集群
      </button>
    </div>
    <p v-if="clusters.error" class="mb-3 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">
      {{ clusters.error }}
    </p>
    <table class="w-full border-collapse text-left text-xs">
      <thead>
        <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
          <th class="py-2 pr-2 font-medium">名称</th>
          <th class="py-2 pr-2 font-medium">Bootstrap</th>
          <th class="py-2 pr-2 font-medium">Mock</th>
          <th class="py-2 pr-2 font-medium">连接状态</th>
          <th class="py-2 pr-2 font-medium">Driver</th>
          <th class="py-2 font-medium">操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in clusters.rows" :key="row.id" class="border-b border-slate-100 dark:border-slate-700/60">
          <td class="py-2 pr-2 font-medium">{{ row.name }}</td>
          <td class="py-2 pr-2 text-slate-500 dark:text-slate-400">{{ row.bootstrapServers.join(', ') }}</td>
          <td class="py-2 pr-2">{{ row.mock ? 'Mock' : 'Real' }}</td>
          <td class="py-2 pr-2">
            <span class="rounded-full px-2 py-0.5 text-[11px] font-medium" :class="stateClass(row.state)">{{ row.state }}</span>
          </td>
          <td class="py-2 pr-2">{{ row.driver ?? '—' }}</td>
          <td class="flex flex-wrap gap-2 py-2">
            <button v-if="row.state === 'connected' || row.state === 'degraded' || row.state === 'connecting'" type="button" :class="btn" @click="disconnectCluster(row.id)">断开</button>
            <button v-else type="button" :class="btn" @click="connectCluster(row.id)">连接</button>
            <button type="button" :class="btn" @click="modal = { initial: row }">编辑</button>
            <button type="button" :class="btn" @click="remove(row)">删除</button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="clusters.rows.length === 0" class="text-sm text-slate-500 dark:text-slate-400">
      {{ clusters.loading ? '加载中…' : '暂无集群，点击「新建集群」添加。' }}
    </p>
    <ClusterModal v-if="modal" :initial="modal.initial" @close="modal = null" />
  </section>
</template>
