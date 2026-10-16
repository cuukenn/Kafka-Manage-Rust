<script setup lang="ts">
// T046a consume panel: subscribe form + live message table + unsubscribe.
// T046b adds the control bar: pause/resume toggle + state badge + seek (per-partition raw offset).
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
import MessageCell from '../components/MessageCell.vue';
import EmptyState from '../components/EmptyState.vue';
import { copyText, downloadJson, renderBytes, type RenderMode } from '../utils/render';
import type { TopicRow } from '../store/tree';

type StartMode = 'earliest' | 'latest' | 'offset';
interface MessageRow { seq: number; partition: number; offset: number; keyBytes: number[] | null; valueBytes: number[]; timestamp: number }
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

// ---- T047: render mode + copy + export ----
// One global decode-mode selector (component state). Key column always renders auto;
// value column follows the selector. Copy sends the value as current-mode text.
const renderMode = ref<RenderMode>('auto');
const copySeq = ref<number | null>(null);
const copyOk = ref(false);

async function copyRow(r: MessageRow): Promise<void> {
  const ok = await copyText(renderBytes(r.valueBytes, renderMode.value).text);
  copySeq.value = r.seq;
  copyOk.value = ok;
  window.setTimeout(() => { if (copySeq.value === r.seq) copySeq.value = null; }, 1500);
}

function exportRows(): void {
  if (rows.value.length === 0) return;
  const safe = topicName.value.replace(/[^\w.-]+/g, '_') || 'topic';
  const stamp = new Date().toISOString().replace(/[:T]/g, '').slice(0, 14);
  downloadJson(`${safe}-${stamp}.json`, rows.value.slice(0, MAX_ROWS).map((r) => ({
    partition: r.partition,
    offset: r.offset,
    key: renderBytes(r.keyBytes, 'auto').text,
    value: renderBytes(r.valueBytes, renderMode.value).text,
    timestamp: r.timestamp,
  })));
}

// ---- T047b: client-side search filter over arrived rows (UI-only mirror of kafka-core
// SearchFilter {query, matchValue=true, matchKey=false, caseSensitive=false}; regex omitted).
// NOTE (fact only, not wired): SubscribeReq.filter: Option<SearchFilter> exists wire-side
// (kafka-core message.rs:274) — that is subscribe-time filtering; this card filters the
// client-side ≤500-row buffer via a computed display list, no resubscribe, no wire commands.
const filterQuery = ref('');
const matchValue = ref(true);
const matchKey = ref(false);
const caseSensitive = ref(false);
const filterActive = computed(() => filterQuery.value.trim() !== '');

function rowMatches(r: MessageRow): boolean {
  const q = filterQuery.value.trim();
  const hay = (bytes: number[] | null): string => {
    const t = renderBytes(bytes, 'auto').text;
    return caseSensitive.value ? t : t.toLowerCase();
  };
  const needle = caseSensitive.value ? q : q.toLowerCase();
  if (matchValue.value && hay(r.valueBytes).includes(needle)) return true;
  return matchKey.value && hay(r.keyBytes).includes(needle);
}

const displayRows = computed(() =>
  filterActive.value ? rows.value.filter(rowMatches) : rows.value,
);

function onMessage(e: BridgeEvent): void {
  const p = e.payload as { subscriptionId?: string; message?: MessageEventDto } | undefined;
  if (!p || !p.message || p.subscriptionId !== subId.value || subDone.value) return; // done → stop appending
  const m = p.message;
  rows.value.unshift({
    seq: ++seq, partition: m.partition ?? 0, offset: m.offset ?? 0,
    keyBytes: m.key ?? null, valueBytes: m.value ?? [], timestamp: m.timestamp ?? 0,
  });
  if (rows.value.length > MAX_ROWS) rows.value.length = MAX_ROWS; // newest-on-top, hard cap
}

async function unsubscribe(): Promise<void> {
  if (!subId.value) return;
  const id = subId.value;
  subId.value = null; // clear first: duplicate-guard + no double-send on unmount
  subClusterId = null;
  try {
    await rpc('consume.unsubscribe', { subscriptionId: id });
  } catch {
    // Already gone server-side (bounded read finished); nothing to do.
  }
}

// ---- T046c server-side subscription events (badge truth) ----
// Wire facts (grep-verified, kafka-app bus.rs:68-81 + SubscriptionStatus bus.rs:28-35 snake_case):
//  - "subscription.state" payload {subscriptionId, state: "running"|"paused"|"done", reason?}
//  - "subscription.done" payload {subscriptionId, delivered: number, reason?}
// Bounded (follow=false) reads auto-stop server-side: done is the truth, no user action needed.
const subDone = ref(false);

function markDone(): void {
  subDone.value = true;
  subState.value = 'done';
}

function onSubState(e: BridgeEvent): void {
  const p = e.payload as { subscriptionId?: string; state?: string } | undefined;
  if (!p || p.subscriptionId !== subId.value) return;
  if (p.state === 'done') markDone();
  else if (p.state) subState.value = p.state;
}

function onSubDone(e: BridgeEvent): void {
  const p = e.payload as { subscriptionId?: string } | undefined;
  if (!p || p.subscriptionId !== subId.value) return;
  markDone();
}

// ---- T046b controls: pause / resume / seek ----
// Wire facts (grep-verified, handlers_consume.rs L31-46 + tests/router_consume.rs):
//  - consume.pause / consume.resume payload {subscriptionId} ONLY; success = ok flag, data:null;
//    subscription state flips to "paused" / "running" (router test pause_resume_seek_drive_state_transitions).
//  - consume.seek payload {subscriptionId, topic, partition, offset} — SeekPayload takes a RAW i64
//    offset per partition, NOT the OffsetStart tagged shape that consume.subscribe uses.
//  - Router tests assert no invalid_state kind; failures arrive as error.kind via service_dto,
//    so buttons are state-gated AND errors surface in the inline subError bar.
// Pause is a Rust StreamControls semantic: events simply stop arriving; the frontend only sends
// control commands and reflects state — rows already received stay in the table.
const controlBusy = ref(false);
const seekPartition = ref('0');
const seekOffset = ref('');

async function setPaused(paused: boolean): Promise<void> {
  if (!subId.value || controlBusy.value) return;
  subError.value = null;
  controlBusy.value = true;
  try {
    await rpc(paused ? 'consume.pause' : 'consume.resume', { subscriptionId: subId.value });
    subState.value = paused ? 'paused' : 'running';
  } catch (e) {
    subError.value = e instanceof Error ? e.message : String(e);
  } finally {
    controlBusy.value = false;
  }
}

async function seek(): Promise<void> {
  if (!subId.value || controlBusy.value) return;
  subError.value = null;
  const part = Number(seekPartition.value);
  const off = Number(seekOffset.value);
  if (!Number.isInteger(part) || part < 0) { subError.value = 'partition 必须是非负整数'; return; }
  if (seekOffset.value === '' || !Number.isInteger(off) || off < 0) { subError.value = 'seek offset 必须是非负整数'; return; }
  controlBusy.value = true;
  try {
    await rpc('consume.seek', { subscriptionId: subId.value, topic: topicName.value, partition: part, offset: off });
    rows.value = []; // replay from the seek point: drop old rows so the replay is visible
  } catch (e) {
    subError.value = e instanceof Error ? e.message : String(e);
  } finally {
    controlBusy.value = false;
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
    subDone.value = false;
    subId.value = info.subscriptionId;
    subClusterId = cluster.id; // B2: record which cluster this subscription lives on
    subState.value = info.state ?? 'running';
  } catch (e) {
    subError.value = e instanceof Error ? e.message : String(e);
  } finally {
    subscribing.value = false;
  }
}

// B2 (stage-4 final review): a subscription belongs to the cluster it was created
// on. If that cluster disconnects while this view is mounted, targetCluster
// silently falls back to another connected cluster — the old subscription must not
// keep appending rows under the wrong cluster. Track the owning cluster and tear
// the subscription down (fire-and-forget unsubscribe + state/form reset) whenever
// the effective target id no longer matches it.
let subClusterId: string | null = null;

function resetSubscription(): void {
  const id = subId.value;
  subId.value = null; // clear first: no double-send (same discipline as unsubscribe())
  subClusterId = null;
  if (id) void rpc('consume.unsubscribe', { subscriptionId: id }).catch(() => {}); // fire-and-forget: cluster may already be gone
  rows.value = [];
  subState.value = '';
  subDone.value = false;
  subError.value = null;
  topicName.value = '';
  startMode.value = 'earliest';
  offsetText.value = '';
  follow.value = true;
}

let offMessage: (() => void) | null = null;
let offSubState: (() => void) | null = null;
let offSubDone: (() => void) | null = null;
watch(() => targetCluster.value?.id, () => {
  void loadTopics();
  if (subId.value && targetCluster.value?.id !== subClusterId) resetSubscription();
});
onMounted(() => {
  if (ui.selectedTopic && !topicName.value) topicName.value = ui.selectedTopic.topic;
  void loadTopics();
  offMessage = on('message.received', onMessage);
  offSubState = on('subscription.state', onSubState);
  offSubDone = on('subscription.done', onSubDone);
});
onBeforeUnmount(() => {
  offMessage?.(); // drop the event handlers with the view
  offSubState?.();
  offSubDone?.();
  void unsubscribe(); // leave page -> no leaked subscription
});

function fmtTs(ts: number): string {
  return ts > 0 ? new Date(ts).toLocaleTimeString() : '—';
}
</script>

<template>
  <section class="max-w-5xl">
    <h2 class="mb-2 text-lg font-semibold">{{ targetCluster ? `消费消息 · ${targetCluster.name}` : '消费消息' }}</h2>
    <!-- T050b: shared EmptyState when no connected cluster -->
    <EmptyState v-if="!targetCluster" title="未找到已连接集群" hint="去集群页连接，或在左侧树中选择一个 topic 节点。" />
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
      <div v-if="subId" class="mb-3 flex flex-wrap items-center gap-2 rounded border border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-700 dark:bg-slate-800/60">
        <span class="rounded-full px-2 py-0.5 text-xs font-semibold" :class="subDone ? 'bg-slate-200 text-slate-500 dark:bg-slate-700 dark:text-slate-400' : subState === 'paused' ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/60 dark:text-amber-200' : 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/60 dark:text-emerald-200'">{{ subDone ? '已结束' : subState }}</span>
        <span v-if="subDone" class="text-xs font-medium text-slate-500 dark:text-slate-400">订阅已结束</span>
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" :disabled="controlBusy || subDone || subState !== 'running'" @click="setPaused(true)">暂停</button>
        <button type="button" class="rounded border border-emerald-300 px-3 py-1 text-xs font-medium text-emerald-700 hover:bg-emerald-50 disabled:opacity-50 dark:border-emerald-700 dark:text-emerald-300 dark:hover:bg-emerald-900/40" :disabled="controlBusy || subDone || subState !== 'paused'" @click="setPaused(false)">恢复</button>
        <label :class="`${LBL} ml-2`">Seek</label>
        <input v-model="seekPartition" type="number" min="0" placeholder="partition" :class="`${INP} w-24`" :disabled="subDone" />
        <input v-model="seekOffset" type="number" min="0" placeholder="offset" :class="`${INP} w-28`" :disabled="subDone" />
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" :disabled="controlBusy || subDone || seekOffset === ''" @click="seek">确认</button>
      </div>
      <p v-if="subError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ subError }}</p>
      <div class="mb-1 flex flex-wrap items-center gap-2">
        <p class="text-xs font-semibold uppercase tracking-wide text-slate-400">消息（最新在上，上限 {{ MAX_ROWS }}）</p>
        <label :class="`${LBL} ml-2`">解码</label>
        <select v-model="renderMode" :class="INP">
          <option value="auto">自动</option><option value="json">JSON</option><option value="base64">Base64</option><option value="hex">Hex</option>
        </select>
        <button type="button" class="rounded border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-100 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" :disabled="rows.length === 0" @click="exportRows">导出 JSON</button>
        <span v-if="copySeq !== null" class="text-xs font-medium" :class="copyOk ? 'text-emerald-600 dark:text-emerald-400' : 'text-rose-600 dark:text-rose-400'">{{ copyOk ? '已复制' : '复制失败' }}</span>
      </div>
      <div class="mb-2 flex flex-wrap items-center gap-2">
        <input v-model="filterQuery" type="search" placeholder="搜索（客户端过滤）" :class="`${INP} w-56`" />
        <label :class="`${LBL} flex items-center gap-1`"><input v-model="matchValue" type="checkbox" />匹配 Value</label>
        <label :class="`${LBL} flex items-center gap-1`"><input v-model="matchKey" type="checkbox" />匹配 Key</label>
        <label :class="`${LBL} flex items-center gap-1`"><input v-model="caseSensitive" type="checkbox" />区分大小写</label>
        <span v-if="filterActive" class="text-xs font-medium text-slate-500 dark:text-slate-400">{{ displayRows.length }}/{{ rows.length }}</span>
      </div>
      <table class="w-full border-collapse text-xs">
        <thead>
          <tr class="border-b border-slate-300 text-left text-slate-500 dark:border-slate-600 dark:text-slate-400">
            <th class="px-2 py-1">Partition</th><th class="px-2 py-1">Offset</th><th class="px-2 py-1">Key</th><th class="px-2 py-1">Value</th><th class="px-2 py-1">Timestamp</th><th class="px-2 py-1">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="displayRows.length === 0">
            <td colspan="6" class="px-2 py-3">
              <!-- T050b: pre-subscription empty state; subscribed/filter rows keep old text -->
              <EmptyState v-if="!subId" title="暂无消息" hint="选择 topic 后点订阅" />
              <span v-else class="text-slate-400 dark:text-slate-500">{{ filterActive ? '无匹配消息' : '暂无消息' }}</span>
            </td>
          </tr>
          <tr v-for="r in displayRows" :key="r.seq" class="border-b border-slate-200 align-top dark:border-slate-700">
            <td class="px-2 py-1 font-mono">{{ r.partition }}</td>
            <td class="px-2 py-1 font-mono">{{ r.offset }}</td>
            <td class="max-w-40 px-2 py-1"><MessageCell :bytes="r.keyBytes" mode="auto" /></td>
            <td class="max-w-md px-2 py-1"><MessageCell :bytes="r.valueBytes" :mode="renderMode" /></td>
            <td class="whitespace-nowrap px-2 py-1">{{ fmtTs(r.timestamp) }}</td>
            <td class="whitespace-nowrap px-2 py-1">
              <button type="button" class="rounded border border-slate-300 px-2 py-0.5 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700" @click="copyRow(r)">复制</button>
            </td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>
