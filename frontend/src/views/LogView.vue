<script setup lang="ts">
// T049b: log panel — ring-buffer table (时间/级别/来源/内容) fed by store/logs.
// Newest on top, level filter (全部/error/warn/info), clear button, 500 cap.
import { computed, ref } from 'vue';
import { logState, clearLogs, type LogLevel } from '../store/logs';
import { downloadText } from '../utils/render';

const filter = ref<'all' | LogLevel>('all');

const rows = computed(() =>
  filter.value === 'all'
    ? logState.entries
    : logState.entries.filter((e) => e.level === filter.value),
);

function fmtTime(ts: number): string {
  return new Date(ts).toLocaleTimeString('zh-CN', { hour12: false });
}

// T083b: what is on screen is what gets exported (R90), oldest first (R91).
function exportLogs(): void {
  const tail = [...rows.value].reverse().map(
    (e) => `[${new Date(e.ts).toISOString()}] ${e.level.toUpperCase()} ${e.source} ${e.text}`,
  );
  downloadText(`kafka-logs-${stamp()}.txt`, `${tail.join('\n')}\n`);
}

function stamp(): string {
  const d = new Date();
  const pad = (n: number): string => String(n).padStart(2, '0');
  const date = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
  return `${date}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

const LEVEL_CLASS: Record<LogLevel, string> = {
  info: 'bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200',
  warn: 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200',
  error: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
};
</script>

<template>
  <section>
    <div class="mb-3 flex items-center gap-3">
      <h2 class="text-lg font-semibold">日志</h2>
      <select
        v-model="filter"
        class="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
        aria-label="级别过滤"
      >
        <option value="all">全部</option>
        <option value="error">error</option>
        <option value="warn">warn</option>
        <option value="info">info</option>
      </select>
      <button
        type="button"
        class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        @click="clearLogs"
      >
        清空
      </button>
      <button
        type="button"
        class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        :disabled="rows.length === 0"
        title="导出当前筛选结果（最旧在前）"
        @click="exportLogs"
      >
        导出
      </button>
      <span class="ml-auto text-xs text-slate-500 dark:text-slate-400">
        {{ rows.length }} / 500
      </span>
    </div>

    <table class="w-full border-collapse text-xs">
      <thead>
        <tr class="border-b border-slate-300 text-left text-slate-600 dark:border-slate-600 dark:text-slate-300">
          <th class="py-1 pr-3 font-medium">时间</th>
          <th class="py-1 pr-3 font-medium">级别</th>
          <th class="py-1 pr-3 font-medium">来源</th>
          <th class="py-1 font-medium">内容</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(e, i) in rows"
          :key="`${e.ts}-${i}`"
          class="border-b border-slate-100 align-top dark:border-slate-800"
        >
          <td class="py-1 pr-3 whitespace-nowrap text-slate-500 dark:text-slate-400">
            {{ fmtTime(e.ts) }}
          </td>
          <td class="py-1 pr-3">
            <span :class="['rounded px-2 py-0.5 font-medium', LEVEL_CLASS[e.level]]">
              {{ e.level }}
            </span>
          </td>
          <td class="py-1 pr-3 whitespace-nowrap text-slate-500 dark:text-slate-400">
            {{ e.source }}
          </td>
          <td class="py-1 break-all text-slate-700 dark:text-slate-200">{{ e.text }}</td>
        </tr>
      </tbody>
    </table>

    <p v-if="rows.length === 0" class="mt-4 text-sm text-slate-500 dark:text-slate-400">
      暂无日志。
    </p>
  </section>
</template>
