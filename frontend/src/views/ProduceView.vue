<script setup lang="ts">
// T045 produce panel: send messages to a topic of the current cluster (tree
// selection wins, else first connected cluster — same rule as T044a). Wire
// facts (narrow greps kafka-core/src/message.rs, tests/router_topic_produce.rs):
//  produce.send {clusterId, requests:[ProduceReq]} -> data = {results: ProduceResult[]}.
//  ProduceReq camelCase {topic, messages[]}; ProduceMessage {key?: number[]|null,
//  value: number[], headers[], partition?: number|null, timestamp?: number|null}
//  — Vec<u8> is a JSON NUMBER ARRAY (no base64 serde); MessageHeader {key: string,
//  value?: number[]|null}; ProduceResult {topic, partition, offset, timestamp,
//  error: string|null} — a batch may partially fail, so each row renders on its own.
import { computed, onMounted, ref, watch } from 'vue';
import { rpc } from '../bridge/rpc';
import { clusters } from '../store/clusters';
import type { TopicRow } from '../store/tree';
import { ui } from '../store/ui';

interface ProduceResultDto {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  error: string | null;
}
interface HeaderRow {
  key: string;
  value: string;
}

const CONNECTED = new Set(['connected', 'degraded']);

/** Cluster we produce to: selected tree node's cluster, else first connected. */
const targetCluster = computed(() => {
  const sel = ui.selectedTopic;
  if (sel) return clusters.rows.find((c) => c.id === sel.clusterId) ?? null;
  return clusters.rows.find((c) => CONNECTED.has(c.state)) ?? null;
});

const topics = ref<string[]>([]);
const topicName = ref('');
const topicsError = ref<string | null>(null);

async function loadTopics(): Promise<void> {
  const cluster = targetCluster.value;
  if (!cluster) {
    topics.value = [];
    return;
  }
  try {
    const rows = await rpc<TopicRow[]>('topic.list', { clusterId: cluster.id });
    topics.value = (rows ?? []).map((r) => r.name);
    topicsError.value = null;
    if (!topics.value.includes(topicName.value)) topicName.value = topics.value[0] ?? '';
  } catch (e) {
    topicsError.value = e instanceof Error ? e.message : String(e);
  }
}
watch(() => targetCluster.value?.id, () => void loadTopics());
onMounted(() => {
  if (ui.selectedTopic && !topicName.value) topicName.value = ui.selectedTopic.topic;
  void loadTopics();
});

// ---- form state ----
const keyText = ref('');
const valueText = ref('');
const mode = ref<'text' | 'json'>('text');
const jsonError = ref<string | null>(null);
const partitionText = ref('');
const headers = ref<HeaderRow[]>([{ key: '', value: '' }]);
const sending = ref(false);
const sendError = ref<string | null>(null);
const results = ref<(ProduceResultDto & { seq: number })[]>([]);
let seq = 0;

function addHeader(): void {
  headers.value.push({ key: '', value: '' });
}
function removeHeader(i: number): void {
  headers.value.splice(i, 1);
}

const enc = new TextEncoder();
const toBytes = (s: string): number[] => Array.from(enc.encode(s));

/** JSON mode: validate + compact; text mode: pass through. Null = invalid. */
function valuePayload(): string | null {
  if (mode.value !== 'json') return valueText.value;
  try {
    return JSON.stringify(JSON.parse(valueText.value));
  } catch (e) {
    jsonError.value = `JSON 无效：${e instanceof Error ? e.message : String(e)}`;
    return null;
  }
}

async function send(): Promise<void> {
  const cluster = targetCluster.value;
  if (!cluster || !topicName.value) {
    sendError.value = '请先选择已连接集群与目标 topic。';
    return;
  }
  jsonError.value = null;
  sendError.value = null;
  const payload = valuePayload();
  if (payload === null) return;
  const partition = partitionText.value.trim();
  const message = {
    key: keyText.value ? toBytes(keyText.value) : null,
    value: toBytes(payload),
    headers: headers.value
      .filter((h) => h.key.trim() !== '')
      .map((h) => ({ key: h.key, value: toBytes(h.value) })),
    partition: partition !== '' && Number.isInteger(Number(partition)) ? Number(partition) : null,
    timestamp: null,
  };
  sending.value = true;
  try {
    const report = await rpc<{ results: ProduceResultDto[] }>('produce.send', {
      clusterId: cluster.id,
      requests: [{ topic: topicName.value, messages: [message] }],
    });
    const list = (report?.results ?? []).map((r) => ({ ...r, seq: ++seq }));
    results.value = [...list, ...results.value];
    const failed = list.find((r) => r.error != null);
    sendError.value = failed ? `部分失败：${failed.error}` : null;
  } catch (e) {
    sendError.value = e instanceof Error ? e.message : String(e);
  } finally {
    sending.value = false;
  }
}
</script>

<template>
  <section class="max-w-3xl">
    <h2 class="mb-2 text-lg font-semibold">
      {{ targetCluster ? `生产消息 · ${targetCluster.name}` : '生产消息' }}
    </h2>

    <p v-if="!targetCluster" class="text-sm text-slate-500 dark:text-slate-400">
      未找到已连接集群：请先在集群页连接，或在左侧树中选择一个 topic 节点。
    </p>

    <template v-else>
      <div class="mb-3 flex items-center gap-2">
        <label class="text-xs font-medium text-slate-600 dark:text-slate-300">目标 Topic</label>
        <select
          v-model="topicName"
          class="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100"
        >
          <option v-if="topics.length === 0" value="">（无 topic）</option>
          <option v-for="t in topics" :key="t" :value="t">{{ t }}</option>
        </select>
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="loadTopics">刷新</button>
      </div>
      <p v-if="topicsError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ topicsError }}</p>

      <div class="mb-2 flex items-center gap-2">
        <label class="text-xs font-medium text-slate-600 dark:text-slate-300">Key（可空）</label>
        <input v-model="keyText" type="text" placeholder="消息 key" class="flex-1 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100" />
        <label class="text-xs font-medium text-slate-600 dark:text-slate-300">Partition（可空）</label>
        <input v-model="partitionText" type="number" min="0" placeholder="自动" class="w-20 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100" />
      </div>

      <div class="mb-1 flex items-center gap-2">
        <label class="text-xs font-medium text-slate-600 dark:text-slate-300">Value</label>
        <div class="ml-auto flex gap-1 text-xs">
          <button type="button" class="rounded px-2 py-0.5" :class="mode === 'text' ? 'bg-emerald-600 text-white' : 'border border-slate-300 text-slate-600 dark:border-slate-600 dark:text-slate-300'" @click="mode = 'text'">文本</button>
          <button type="button" class="rounded px-2 py-0.5" :class="mode === 'json' ? 'bg-emerald-600 text-white' : 'border border-slate-300 text-slate-600 dark:border-slate-600 dark:text-slate-300'" @click="mode = 'json'">JSON</button>
        </div>
      </div>
      <textarea v-model="valueText" rows="6" spellcheck="false" placeholder="消息内容（UTF-8）" class="mb-1 w-full rounded border border-slate-300 bg-white px-2 py-1 font-mono text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100" />
      <p v-if="jsonError" class="mb-2 text-xs text-rose-600 dark:text-rose-400">{{ jsonError }}</p>

      <p class="mb-1 mt-3 text-xs font-medium text-slate-600 dark:text-slate-300">Headers</p>
      <div v-for="(h, i) in headers" :key="i" class="mb-1 flex gap-2">
        <input v-model="h.key" type="text" placeholder="header key" class="flex-1 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100" />
        <input v-model="h.value" type="text" placeholder="header value" class="flex-1 rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100" />
        <button type="button" class="rounded border border-rose-300 px-2 py-0.5 text-xs text-rose-700 hover:bg-rose-50 dark:border-rose-700 dark:text-rose-300 dark:hover:bg-rose-900/40" @click="removeHeader(i)">删除</button>
      </div>
      <button type="button" class="mt-1 rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="addHeader">添加 Header</button>

      <div class="mt-4">
        <button type="button" class="rounded bg-emerald-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50" :disabled="sending || !topicName" @click="send">
          {{ sending ? '发送中…' : '发送' }}
        </button>
      </div>
      <p v-if="sendError" class="mt-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ sendError }}</p>

      <template v-if="results.length > 0">
        <p class="mb-1 mt-4 text-xs font-semibold uppercase tracking-wide text-slate-400">发送回执</p>
        <ul class="space-y-1 text-xs">
          <li
            v-for="r in results"
            :key="r.seq"
            class="rounded border px-3 py-1.5"
            :class="r.error ? 'border-rose-300 bg-rose-50 text-rose-800 dark:border-rose-700 dark:bg-rose-900/40 dark:text-rose-200' : 'border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200'"
          >
            <span class="font-mono">#{{ r.seq }}</span>
            {{ r.topic }} · partition {{ r.partition }} · offset {{ r.offset }}
            <span v-if="r.timestamp"> · ts {{ r.timestamp }}</span>
            <span v-if="r.error" class="ml-2 font-medium">失败：{{ r.error }}</span>
          </li>
        </ul>
      </template>
    </template>
  </section>
</template>
