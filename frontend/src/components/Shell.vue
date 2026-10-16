<script setup lang="ts">
// T041 app shell: left nav + top bar (status badge, theme toggle) + main view.
// T042 wired the clusters view in; T043 added the sidebar cluster tree +
// topic placeholder; T048a wired the groups view in; logs placeholder remains (T049).
import { computed } from 'vue';
import ClustersView from '../views/ClustersView.vue';
import ConsumeView from '../views/ConsumeView.vue';
import GroupsView from '../views/GroupsView.vue';
import ProduceView from '../views/ProduceView.vue';
import TopicsView from '../views/TopicsView.vue';
import TreeView from '../components/TreeView.vue';
import { ui, setView, toggleTheme, type ViewName } from '../store/ui';

const NAV: { view: ViewName; label: string }[] = [
  { view: 'clusters', label: '集群' },
  { view: 'produce', label: '生产' },
  { view: 'consume', label: '消费' },
  { view: 'groups', label: '消费组' },
  { view: 'logs', label: '日志' },
];

const badgeText = computed(() => {
  if (ui.badgeLoading) return '检查连接中…';
  return ui.connectedCluster ? `已连接：${ui.connectedCluster}` : '未连接';
});
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
          v-for="item in NAV"
          :key="item.view"
          type="button"
          class="rounded px-3 py-2 text-left text-sm"
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
        <section v-else>
          <h2 class="mb-2 text-lg font-semibold">日志</h2>
          <p class="text-sm text-slate-500 dark:text-slate-400">
            日志页将在 T049 落地；当前外壳仅占位。
          </p>
        </section>
      </main>
    </div>
  </div>
</template>
