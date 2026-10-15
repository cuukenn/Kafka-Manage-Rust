<script setup lang="ts">
// T044b topic config editor. Wire facts (verified against kafka-core
// protocol.rs / topic.rs and webview-bridge tests/router_topic_produce.rs):
// read  topic.describe {clusterId, names:[topic], includeInternal:false}
//       -> TopicDetail[] with configs: Record<string,string> (camelCase).
// write topic.updateConfig {clusterId, topic, set:{k:v}, unset:[k]} -> null.
// The modal diffs its rows against the configs loaded on mount and sends
// only the delta: new/changed entries in `set`, removed keys in `unset`.
import { onMounted, reactive, ref } from 'vue';
import { rpc } from '../bridge/rpc';

const props = defineProps<{ clusterId: string; topic: string }>();
const emit = defineEmits<{ (e: 'saved'): void; (e: 'close'): void }>();

interface ConfigRow {
  key: string;
  value: string;
}

const rows = reactive<ConfigRow[]>([]);
const original = ref<Record<string, string>>({});
const error = ref<string | null>(null);
const busy = ref(false);
const loading = ref(true);

const inp =
  'w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-100';

onMounted(async () => {
  try {
    const details = await rpc<{ name: string; configs?: Record<string, string> }[]>(
      'topic.describe',
      { clusterId: props.clusterId, names: [props.topic], includeInternal: false },
    );
    const detail = (details ?? []).find((d) => d.name === props.topic) ?? details?.[0];
    original.value = detail?.configs ?? {};
    for (const [key, value] of Object.entries(original.value)) rows.push({ key, value });
    error.value = null;
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e);
  } finally {
    loading.value = false;
  }
});

function addRow(): void {
  rows.push({ key: '', value: '' });
}

function removeRow(i: number): void {
  rows.splice(i, 1);
}

async function submit(): Promise<void> {
  error.value = null;
  const set: Record<string, string> = {};
  const seen = new Set<string>();
  for (const row of rows) {
    const key = row.key.trim();
    if (!key) {
      error.value = '配置项 key 不能为空。';
      return;
    }
    if (seen.has(key)) {
      error.value = `配置项 key 重复：${key}`;
      return;
    }
    seen.add(key);
    if (original.value[key] !== row.value) set[key] = row.value;
  }
  const unset = Object.keys(original.value).filter((k) => !seen.has(k));
  if (Object.keys(set).length === 0 && unset.length === 0) {
    emit('close');
    return;
  }
  busy.value = true;
  try {
    await rpc('topic.updateConfig', {
      clusterId: props.clusterId,
      topic: props.topic,
      set,
      unset,
    });
    emit('saved');
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e);
    busy.value = false;
  }
}
</script>

<template>
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" @click.self="emit('close')">
    <form class="w-full max-w-lg rounded-lg bg-white p-4 shadow-xl dark:bg-slate-800" @submit.prevent="submit">
      <div class="mb-3 flex items-center justify-between">
        <h3 class="text-base font-semibold text-slate-800 dark:text-slate-100">配置 · {{ topic }}</h3>
        <button type="button" class="text-slate-400 hover:text-slate-600" @click="emit('close')">✕</button>
      </div>
      <p v-if="error" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ error }}</p>
      <p v-if="loading" class="mb-2 text-xs text-slate-500 dark:text-slate-400">加载配置中…</p>

      <div v-else class="max-h-72 space-y-1.5 overflow-y-auto">
        <div v-for="(row, i) in rows" :key="i" class="flex items-center gap-2">
          <input v-model="row.key" :class="inp" placeholder="key，如 retention.ms" class="flex-1" />
          <input v-model="row.value" :class="inp" placeholder="value，如 604800000" class="flex-1" />
          <button
            type="button"
            class="rounded border border-rose-300 px-2 py-1 text-rose-700 hover:bg-rose-50 dark:border-rose-700 dark:text-rose-300 dark:hover:bg-rose-900/40"
            @click="removeRow(i)"
          >
            删除
          </button>
        </div>
        <p v-if="rows.length === 0" class="text-xs text-slate-500 dark:text-slate-400">该 topic 暂无非默认配置。</p>
      </div>

      <div class="mt-3 flex items-center justify-between">
        <button
          type="button"
          class="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          @click="addRow"
        >
          添加配置项
        </button>
        <div class="flex gap-2">
          <button
            type="button"
            class="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            @click="emit('close')"
          >
            取消
          </button>
          <button
            type="submit"
            class="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
            :disabled="busy || loading"
          >
            {{ busy ? '保存中…' : '保存' }}
          </button>
        </div>
      </div>
    </form>
  </div>
</template>
