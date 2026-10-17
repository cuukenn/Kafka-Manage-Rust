<script setup lang="ts">
// T041 app shell: left nav + top bar (status badge, theme toggle) + main view.
// T042 wired the clusters view in; T043 added the sidebar cluster tree +
// topic placeholder; T048a wired the groups view in; logs placeholder remains (T049).
import { computed, onBeforeUnmount, onMounted } from 'vue';
import ClustersView from '../views/ClustersView.vue';
import ConsumeView from '../views/ConsumeView.vue';
import GroupsView from '../views/GroupsView.vue';
import LogView from '../views/LogView.vue';
import ProduceView from '../views/ProduceView.vue';
import TopicsView from '../views/TopicsView.vue';
import Toast from '../components/Toast.vue';
import TreeView from '../components/TreeView.vue';
import { on } from '../bridge/events';
import { cancelInflight, pending } from '../bridge/rpc';
import { clusters } from '../store/clusters';
import { push } from '../store/toast';
import { ui, setView, toggleTheme, type ViewName } from '../store/ui';

// T049a: cluster connect failures surface here (and via TreeView) as the
// connection.state event; toast them globally. Inline view error bars stay.
const prevState = new Map<string, string>();
on('connection.state', (ev) => {
  const d = ((ev as { payload?: unknown })?.payload ?? {}) as {
    clusterId?: string;
    state?: string;
    reason?: string;
  };
  const id = d.clusterId ?? '';
  const prev = prevState.get(id);
  prevState.set(id, d.state ?? '');
  if (d.state === prev) return;
  const name = clusters.rows.find((r) => r.id === id)?.name ?? id;
  if (d.state === 'error') push('error', `连接集群「${name}」失败：${d.reason ?? '未知原因'}`);
  else if (d.state === 'connected') push('info', `已连接：${name}`);
});

const NAV: { view: ViewName; label: string }[] = [
  { view: 'clusters', label: '集群' },
  { view: 'produce', label: '生产' },
  { view: 'consume', label: '消费' },
  { view: 'groups', label: '消费组' },
  { view: 'logs', label: '日志' },
];

// T050b: Alt+1..N switches views in NAV order. Guards: ignore form fields /
// contentEditable targets and ctrl/meta combos (also excludes Windows AltGr,
// which reports ctrlKey+altKey).
function onKeydown(e: KeyboardEvent): void {
  const t = e.target as HTMLElement | null;
  if (t && (t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' || t.tagName === 'SELECT' || t.isContentEditable)) return;
  if (!e.altKey || e.ctrlKey || e.metaKey) return;
  const idx = Number(e.key) - 1;
  if (Number.isInteger(idx) && idx >= 0 && idx < NAV.length) {
    e.preventDefault();
    setView(NAV[idx].view);
  }
}
onMounted(() => window.addEventListener('keydown', onKeydown));
onBeforeUnmount(() => window.removeEventListener('keydown', onKeydown));

const badgeText = computed(() => {
  if (ui.badgeLoading) return '检查连接中…';
  return ui.connectedCluster ? `已连接：${ui.connectedCluster}` : '未连接';
});

// T082c: the way out of a request that is taking too long. The runners-up are
// T082a's 30 s deadline on the Rust side and the views' own refresh guards;
// this one is the user's own decision, so it says how many it dropped.
function onCancel(): void {
  const dropped = cancelInflight();
  if (dropped > 0) push('info', `已取消 ${dropped} 个在途请求`);
}
</script>

<template>
  <div class="flex h-screen flex-col">
    <!-- Top bar -->
    <header
      class="flex h-12 shrink-0 items-center gap-4 border-b border-slate-200 bg-white px-4 dark:border-slate-700 dark:bg-slate-800"
    >
      <span class="text-sm font-semibold tracking-wide text-emerald-600 dark:text-emerald-400">
        rs-kafka-ui-client
      </span>
      <span
        class="rounded-full px-3 py-1 text-xs font-medium"
        :class="
          ui.connectedCluster
            ? 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200'
            : 'bg-slate-200 text-slate-600 dark:bg-slate-700 dark:text-slate-300'
        "
      >
        {{ badgeText }}
      </span>
      <!-- T049a: pending-RPC spinner next to the connection badge -->
      <span
        v-if="pending.count > 0"
        class="inline-block h-4 w-4 shrink-0 animate-spin rounded-full border-2 border-slate-300 border-t-emerald-600 dark:border-slate-600 dark:border-t-emerald-400"
        :title="`处理中：${pending.count} 个请求`"
        aria-label="请求处理中"
      />
      <!-- T082c: the user's way out of a slow request (T082b drops it for real) -->
      <button
        v-if="pending.count > 0"
        type="button"
        class="rounded border border-amber-300 px-2 py-1 text-xs font-medium text-amber-700 hover:bg-amber-50 dark:border-amber-600 dark:text-amber-200 dark:hover:bg-amber-900"
        title="取消所有在途请求"
        @click="onCancel()"
      >
        取消
      </button>
      <button
        type="button"
        class="ml-auto rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        :title="ui.theme === 'dark' ? '切换到浅色主题' : '切换到深色主题'"
        @click="toggleTheme"
      >
        {{ ui.theme === 'dark' ? '☀ 浅色' : '🌙 深色' }}
      </button>
    </header>

    <div class="flex min-h-0 flex-1">
      <!-- Left nav -->
      <nav
        class="flex min-h-0 w-44 shrink-0 flex-col gap-1 border-r border-slate-200 bg-white p-2 dark:border-slate-700 dark:bg-slate-800"
      >
        <button
          v-for="(item, i) in NAV"
          :key="item.view"
          type="button"
          class="rounded px-3 py-2 text-left text-sm"
          :title="`${item.label}（Alt+${i + 1}）`"
          :class="
            ui.currentView === item.view
              ? 'bg-emerald-600 font-medium text-white'
              : 'text-slate-700 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-700'
          "
          @click="setView(item.view)"
        >
          {{ item.label }}
        </button>
        <!-- T043: lazy cluster tree (clusters -> topics -> partitions) -->
        <p class="mt-3 px-1 text-[11px] font-semibold uppercase tracking-wide text-slate-400">
          集群树
        </p>
        <TreeView />
      </nav>

      <!-- Main view area -->
      <main class="min-w-0 flex-1 overflow-auto p-6">
        <!-- T044a: tree topic selection lands on the topic table (row highlighted) -->
        <TopicsView v-if="ui.currentView === 'clusters' && ui.selectedTopic" />
        <ClustersView v-else-if="ui.currentView === 'clusters'" />
        <ProduceView v-else-if="ui.currentView === 'produce'" />
        <ConsumeView v-else-if="ui.currentView === 'consume'" />
        <GroupsView v-else-if="ui.currentView === 'groups'" />
        <!-- T049b: log panel (ring buffer fed by event/RPC summaries) -->
        <LogView v-else-if="ui.currentView === 'logs'" />
      </main>
    </div>

    <!-- T049a: global toast stack (bottom-right) -->
    <Toast />
  </div>
</template>
