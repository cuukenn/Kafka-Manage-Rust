<script setup lang="ts">
// T046a consume panel: subscribe form + live message table + unsubscribe.
// Wire facts (grep-verified, do not change without re-grepping):
//  - consume.subscribe payload {clusterId, topic, start, follow} — SubscribeReq fields
//    sit FLAT beside clusterId (webview-bridge handlers_consume.rs SubscribePayload);
//    data = SubscriptionInfo {subscriptionId, clusterId, topic, partitions, state, delivered}.
//  - OffsetStart is adjacently tagged (kafka-core message.rs: tag="type", content="value"):
//    {"type":"earliest"} | {"type":"latest"} | {"type":"offset","value":N}.
//  - consume.unsubscribe payload is {subscriptionId} ONLY (handlers_consume.rs; no clusterId).
//  - Push event "message.received" payload = {subscriptionId, message} (kafka-app bus.rs);
//    `message` is a raw MessageEvent: key/value are NUMBER ARRAYS (Vec<u8> via serde_json),
//    NOT the MessageDto base64 dual channel — the push path never converts to DTO
//    (services/consume.rs publishes MessageEvent directly). We decode bytes client-side.
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { rpc } from '../bridge/rpc';
import { on, type BridgeEvent } from '../bridge/events';
import { clusters } from '../store/clusters';
import { ui } from '../store/ui';
import type { TopicRow } from '../store/tree';

type StartMode = 'earliest' | 'latest' | 'offset';
interface MessageRow { seq: number; partition: number; offset: number; key: string; value: string; isUtf8: boolean; size: number; timestamp: number }
interface MessageEventDto { topic: string; partition: number; offset: number; key: number[] | null; value: number[]; timestamp: number }

const CONNECTED = new Set(['connected', 'degraded']);
const MAX_ROWS = 500;
const INP = 'rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100';
const LBL = 'text-xs font-medium text-slate-600 dark:text-slate-300';

/** Cluster we consume from: selected tree node's cluster, else first connected. */
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
  if (!cluster) { topics.value = []; return; }
  try {
    const rows = await rpc<TopicRow[]>('topic.list', { clusterId: cluster.id });
    topics.value = (rows ?? []).map((r) => r.name);
    topicsError.value = null;
    if (!topics.value.includes(topicName.value)) topicName.value = topics.value[0] ?? '';
  } catch (e) {
    topicsError.value = e instanceof Error ? e.message : String(e);
  }
}

// ---- subscription state ----
const subId = ref<string | null>(null);
const subState = ref('');
const subscribing = ref(false);
const subError = ref<string | null>(null);
const startMode = ref<StartMode>('earliest');
const offsetText = ref('');
const follow = ref(true);
const rows = ref<MessageRow[]>([]);
let seq = 0;

function decodeBytes(bytes: number[] | null): { text: string; isUtf8: boolean; size: number } {
  if (!bytes) return { text: '', isUtf8: true, size: 0 };
  const arr = Uint8Array.from(bytes);
  try {
    return { text: new TextDecoder('utf-8', { fatal: true }).decode(arr), isUtf8: true, size: arr.length };
  } catch {
    let bin = '';
    for (let i = 0; i < arr.length; i += 0x8000) bin += String.fromCharCode(...arr.subarray(i, i + 0x8000));
    const b64 = btoa(bin);
    return { text: `base64(${b64.length > 48 ? `${b64.slice(0, 48)}…` : b64})`, isUtf8: false, size: arr.length };
  }
}

function onMessage(e: BridgeEvent): void {
  const p = e.payload as { subscriptionId?: string; message?: MessageEventDto } | undefined;
  if (!p || !p.message || p.subscriptionId !== subId.value) return;
  const m = p.message;
  const key = decodeBytes(m.key ?? null);
  const val = decodeBytes(m.value ?? []);
  rows.value.unshift({
    seq: ++seq, partition: m.partition ?? 0, offset: m.offset ?? 0,
    key: key.text || (m.key ? '(binary)' : ''),
    value: val.text, isUtf8: val.isUtf8, size: val.size, timestamp: m.timestamp ?? 0,
  });
  if (rows.value.length > MAX_ROWS) rows.value.length = MAX_ROWS; // newest-on-top, hard cap
}

async function unsubscribe(): Promise<void> {
  if (!subId.value) return;
  const id = subId.value;
  subId.value = null; // clear first: duplicate-guard + no double-send on unmount
  try {
    await rpc('consume.unsubscribe', { subscriptionId: id });
  } catch {
    // Already gone server-side (bounded read finished); nothing to do.
  }
}

async function subscribe(): Promise<void> {
  const cluster = targetCluster.value;
  subError.value = null;
  if (!cluster || !topicName.value || subId.value) return;
  let start: Record<string, unknown>;
  if (startMode.value === 'offset') {
    const n = Number(offsetText.value);
    if (!Number.isInteger(n) || n < 0) { subError.value = '指定 offset 必须是非负整数'; return; }
    start = { type: 'offset', value: n };
  } else {
    start = { type: startMode.value };
  }
  subscribing.value = true;
  try {
    const info = await rpc<{ subscriptionId: string; state: string }>('consume.subscribe', {
      clusterId: cluster.id, topic: topicName.value, start, follow: follow.value,
    });
    rows.value = [];
    subId.value = info.subscriptionId;
    subState.value = info.state ?? 'running';
  } catch (e) {
    subError.value = e instanceof Error ? e.message : String(e);
  } finally {
    subscribing.value = false;
  }
}

let offMessage: (() => void) | null = null;
watch(() => targetCluster.value?.id, () => void loadTopics());
onMounted(() => {
  if (ui.selectedTopic && !topicName.value) topicName.value = ui.selectedTopic.topic;
  void loadTopics();
  offMessage = on('message.received', onMessage);
});
onBeforeUnmount(() => {
  offMessage?.(); // drop the event handler with the view
  void unsubscribe(); // leave page -> no leaked subscription
});

function fmtTs(ts: number): string {
  return ts > 0 ? new Date(ts).toLocaleTimeString() : '—';
}
</script>

<template>
  <section class="max-w-5xl">
    <h2 class="mb-2 text-lg font-semibold">{{ targetCluster ? `消费消息 · ${targetCluster.name}` : '消费消息' }}</h2>
    <p v-if="!targetCluster" class="text-sm text-slate-500 dark:text-slate-400">未找到已连接集群：请先在集群页连接，或在左侧树中选择一个 topic 节点。</p>
    <template v-else>
      <div class="mb-3 flex flex-wrap items-center gap-2">
        <label :class="LBL">Topic</label>
        <select v-model="topicName" :class="INP" :disabled="!!subId">
          <option v-if="topics.length === 0" value="">（无 topic）</option>
          <option v-for="t in topics" :key="t" :value="t">{{ t }}</option>
        </select>
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="loadTopics">刷新</button>
        <label :class="`${LBL} ml-2`">起点</label>
        <select v-model="startMode" :class="INP" :disabled="!!subId">
          <option value="earliest">earliest</option><option value="latest">latest</option><option value="offset">指定 offset</option>
        </select>
        <input v-model="offsetText" type="number" min="0" placeholder="offset" :class="`${INP} w-24`" :disabled="startMode !== 'offset' || !!subId" />
        <label :class="`${LBL} ml-2 flex items-center gap-1`"><input v-model="follow" type="checkbox" :disabled="!!subId" />follow（持续接收）</label>
      </div>
      <p v-if="topicsError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ topicsError }}</p>
      <div class="mb-3 flex items-center gap-2">
        <button type="button" class="rounded bg-emerald-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50" :disabled="subscribing || !!subId || !topicName" @click="subscribe">{{ subscribing ? '订阅中…' : subId ? '已订阅' : '订阅' }}</button>
        <button type="button" class="rounded border border-rose-300 px-4 py-1.5 text-sm font-medium text-rose-700 hover:bg-rose-50 disabled:opacity-50 dark:border-rose-700 dark:text-rose-300 dark:hover:bg-rose-900/40" :disabled="!subId" @click="unsubscribe">退订</button>
        <span v-if="subId" class="text-xs text-slate-500 dark:text-slate-400">订阅 {{ subId }} · {{ subState }} · {{ rows.length }} 条</span>
      </div>
      <p v-if="subError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ subError }}</p>
      <p class="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-400">消息（最新在上，上限 {{ MAX_ROWS }}）</p>
      <table class="w-full border-collapse text-xs">
        <thead>
          <tr class="border-b border-slate-300 text-left text-slate-500 dark:border-slate-600 dark:text-slate-400">
            <th class="px-2 py-1">Partition</th><th class="px-2 py-1">Offset</th><th class="px-2 py-1">Key</th><th class="px-2 py-1">Value</th><th class="px-2 py-1">Timestamp</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="rows.length === 0"><td colspan="5" class="px-2 py-3 text-slate-400 dark:text-slate-500">暂无消息</td></tr>
          <tr v-for="r in rows" :key="r.seq" class="border-b border-slate-200 align-top dark:border-slate-700">
            <td class="px-2 py-1 font-mono">{{ r.partition }}</td>
            <td class="px-2 py-1 font-mono">{{ r.offset }}</td>
            <td class="max-w-40 truncate px-2 py-1 font-mono">{{ r.key || '—' }}</td>
            <td class="max-w-md break-all px-2 py-1 font-mono" :class="r.isUtf8 ? '' : 'text-amber-600 dark:text-amber-400'">{{ r.value || '（空）' }}<span v-if="!r.isUtf8" class="text-slate-400"> · {{ r.size }}B</span></td>
            <td class="whitespace-nowrap px-2 py-1">{{ fmtTs(r.timestamp) }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>
