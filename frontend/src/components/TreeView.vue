<script setup lang="ts">
// T043 sidebar cluster tree: saved clusters -> topics -> partitions, lazy-loaded and
// cached in store/tree.ts. Cluster badges reuse the T041/T042 state classes from
// ClustersView.vue. Clicking a topic records ui.selectedTopic (T044+ consumes it).
import { computed, onMounted, onUnmounted } from 'vue';
import { clusters } from '../store/clusters';
import { partKey, reloadCluster, reloadTopic, startTreeEvents, toggleCluster, toggleTopic, tree } from '../store/tree';
import { selectTopic, ui } from '../store/ui';

const LIVE = new Set(['connected', 'degraded']);
const rows = computed(() => [...clusters.rows].sort((a, b) => a.name.localeCompare(b.name)));

// Same state-badge classes as ClustersView.vue (T041/T042 style).
function stateClass(state: string): string {
  if (state === 'connected') return 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200';
  if (state === 'degraded' || state === 'connecting') return 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200';
  if (state === 'error') return 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200';
  return 'bg-slate-200 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
}

const chevron = 'w-4 shrink-0 text-slate-400 hover:text-slate-700 dark:hover:text-slate-200';
const refreshBtn = 'shrink-0 text-slate-400 hover:text-slate-700 dark:hover:text-slate-200';
const hint = 'py-0.5 pl-6 text-[11px] text-slate-400';
const errText = 'py-0.5 pl-6 text-[11px] text-rose-500';

const pick = (clusterId: string, clusterName: string, topic: string): void => {
  selectTopic({ clusterId, clusterName, topic });
};
const selected = (clusterId: string, topic: string): boolean =>
  ui.selectedTopic?.clusterId === clusterId && ui.selectedTopic.topic === topic;

let stop: (() => void) | null = null;
onMounted(() => { stop = startTreeEvents(); });
onUnmounted(() => stop?.());
</script>

<template>
  <div class="mt-2 min-h-0 flex-1 overflow-auto border-t border-slate-200 pt-2 dark:border-slate-700">
    <p v-if="!clusters.rows.length" :class="hint">{{ clusters.loading ? '加载集群…' : '暂无已保存集群' }}</p>
    <ul v-else class="space-y-0.5 text-xs">
      <li v-for="row in rows" :key="row.id">
        <div class="flex items-center gap-1">
          <button type="button" :class="chevron" @click="toggleCluster(row.id, LIVE.has(row.state))">{{ tree.expanded[row.id] ? '▾' : '▸' }}</button>
          <span class="truncate font-medium" :class="LIVE.has(row.state) ? 'text-slate-800 dark:text-slate-100' : 'text-slate-400 dark:text-slate-500'" :title="row.bootstrapServers.join(', ')">{{ row.name }}</span>
          <span class="ml-auto shrink-0 rounded-full px-1.5 py-px text-[10px] font-medium" :class="stateClass(row.state)">{{ row.state }}</span>
          <button v-if="tree.expanded[row.id] && LIVE.has(row.state)" type="button" :class="refreshBtn" title="重新拉取 topics" @click="reloadCluster(row.id)">↻</button>
        </div>
        <p v-if="tree.expanded[row.id] && !LIVE.has(row.state)" :class="hint">请先连接该集群</p>
        <p v-else-if="tree.expanded[row.id] && tree.loading[row.id]" :class="hint">加载 topics…</p>
        <p v-else-if="tree.expanded[row.id] && tree.errors[row.id]" :class="errText">{{ tree.errors[row.id] }}</p>
        <ul v-else-if="tree.expanded[row.id]" class="pl-4">
          <li v-for="t in tree.topics[row.id] ?? []" :key="t.name">
            <div class="flex items-center gap-1">
              <button type="button" :class="chevron" @click="toggleTopic(row.id, t.name)">{{ tree.expanded[partKey(row.id, t.name)] ? '▾' : '▸' }}</button>
              <button type="button" class="truncate text-left" :class="selected(row.id, t.name) ? 'font-medium text-emerald-600 dark:text-emerald-400' : 'text-slate-700 hover:text-emerald-600 dark:text-slate-200 dark:hover:text-emerald-400'" :title="t.name" @click="pick(row.id, row.name, t.name)">{{ t.name }}</button>
              <span class="ml-auto shrink-0 text-[10px] text-slate-400">{{ t.partitionCount }}p</span>
              <button v-if="tree.expanded[partKey(row.id, t.name)]" type="button" :class="refreshBtn" title="重新拉取 partitions" @click="reloadTopic(row.id, t.name)">↻</button>
            </div>
            <p v-if="tree.loading[partKey(row.id, t.name)]" :class="hint">加载 partitions…</p>
            <p v-else-if="tree.errors[partKey(row.id, t.name)]" :class="errText">{{ tree.errors[partKey(row.id, t.name)] }}</p>
            <ul v-else-if="tree.expanded[partKey(row.id, t.name)]" class="pl-6">
              <li v-for="p in tree.partitions[partKey(row.id, t.name)] ?? []" :key="p.id" class="py-px text-[11px] text-slate-500 dark:text-slate-400">
                P{{ p.id }} · leader {{ p.leaderId }} · ISR {{ p.isrIds.length }}/{{ p.replicaIds.length }} <span v-if="p.isrIds.length < p.replicaIds.length" class="text-amber-500" title="副本未同步">⚠</span>
              </li>
            </ul>
          </li>
        </ul>
      </li>
    </ul>
  </div>
</template>
