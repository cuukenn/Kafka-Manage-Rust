<script setup lang="ts">
// T048a consumer groups view: group.list table (组名/状态/成员数/协议/分配器/本地)
// + row-click inline detail panel below the table: members table (group.describe
// {clusterId, ids:[groupId]} -> GroupDetail{info, members[memberId/clientId/
// clientHost/assignments]}) and committed-offsets table with lag (group.offsets
// {clusterId, groupId, withLag:true} -> GroupOffsetEntry[]; lag is wire-provided,
// kafka-core group.rs GroupOffsetEntry.lag = endOffset - committed).
// Wire facts verified against kafka-core protocol.rs GROUP_* + group.rs DTOs
// (camelCase serde) and webview-bridge tests/router_group.rs payload keys.
// No group events on the event stream -> mount pull + manual refresh only.
// 位点重置 (group.resetOffsets) lands in T048b — button intentionally disabled.
import { computed, onMounted, reactive } from 'vue';
import { RpcError, rpc } from '../bridge/rpc';
import { clusters, refresh as refreshClusters } from '../store/clusters';

interface TopicPartition { topic: string; partition: number }
/** GroupInfo (kafka-core group.rs, camelCase): state is snake_case on the wire. */
interface GroupInfo {
  groupId: string;
  state: string;
  memberCount: number;
  protocolType: string;
  partitionAssignor: string;
  coordinatorId: number | null;
  isLocal: boolean;
}
interface GroupMember { memberId: string; clientId: string; clientHost: string; assignments: TopicPartition[] }
interface GroupDetail { info: GroupInfo; members: GroupMember[] }
interface OffsetRow { topic: string; partition: number; offset: number; metadata: string; committedAt: number | null; lag: number | null }

const CONNECTED = new Set(['connected', 'degraded']);

const panel = reactive({
  rows: [] as GroupInfo[],
  loading: false,
  error: null as string | null,
  selected: null as string | null,
  detail: null as GroupDetail | null,
  offsets: [] as OffsetRow[],
  detailLoading: false,
  detailError: null as string | null,
});

/** Groups are per-cluster: use the first connected cluster (nav clears tree selection). */
const targetCluster = computed(() => clusters.rows.find((c) => CONNECTED.has(c.state)) ?? null);

function message(e: unknown): string {
  return e instanceof RpcError ? `${e.kind}: ${e.message}` : String(e);
}

function stateClass(state: string): string {
  if (state === 'stable') return 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-200';
  if (state === 'preparing_rebalance' || state === 'completing_rebalance') return 'bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200';
  if (state === 'dead') return 'bg-rose-100 text-rose-800 dark:bg-rose-900 dark:text-rose-200';
  return 'bg-slate-200 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
}

function closeDetail(): void {
  panel.selected = null;
  panel.detail = null;
  panel.offsets = [];
  panel.detailError = null;
}

async function pull(): Promise<void> {
  const cluster = targetCluster.value;
  if (!cluster) {
    panel.rows = [];
    panel.error = null;
    closeDetail();
    return;
  }
  panel.loading = true;
  try {
    panel.rows = await rpc<GroupInfo[]>('group.list', { clusterId: cluster.id });
    panel.error = null;
    if (panel.selected && !panel.rows.some((g) => g.groupId === panel.selected)) closeDetail();
  } catch (e) {
    panel.error = message(e);
  } finally {
    panel.loading = false;
  }
}

async function toggleDetail(row: GroupInfo): Promise<void> {
  if (panel.selected === row.groupId) {
    closeDetail();
    return;
  }
  const cluster = targetCluster.value;
  if (!cluster) return;
  panel.selected = row.groupId;
  panel.detail = null;
  panel.offsets = [];
  panel.detailError = null;
  panel.detailLoading = true;
  try {
    const [details, offs] = await Promise.all([
      rpc<GroupDetail[]>('group.describe', { clusterId: cluster.id, ids: [row.groupId] }),
      rpc<OffsetRow[]>('group.offsets', { clusterId: cluster.id, groupId: row.groupId, withLag: true }),
    ]);
    const match = (Array.isArray(details) ? details : []).find((d) => d.info.groupId === row.groupId);
    panel.detail = match ?? null;
    panel.offsets = Array.isArray(offs) ? offs : [];
  } catch (e) {
    panel.detailError = message(e);
  } finally {
    panel.detailLoading = false;
  }
}

onMounted(() => {
  void refreshClusters(); // rows may be cold if the user landed here directly
  void pull();
});
</script>

<template>
  <section>
    <div class="mb-3 flex items-center gap-3">
      <h2 class="text-lg font-semibold">消费组</h2>
      <span v-if="targetCluster" class="text-xs text-slate-500 dark:text-slate-400">{{ targetCluster.name }}</span>
      <button type="button" class="ml-auto rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" :disabled="panel.loading || !targetCluster" @click="pull()">
        {{ panel.loading ? '刷新中…' : '刷新' }}
      </button>
    </div>
    <p v-if="panel.error" class="mb-3 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">
      {{ panel.error }}
    </p>
    <p v-if="!targetCluster" class="mb-3 text-sm text-slate-500 dark:text-slate-400">
      未找到已连接的集群——请先在「集群」页连接一个集群。
    </p>
    <table class="w-full border-collapse text-left text-xs">
      <thead>
        <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
          <th class="py-2 pr-2 font-medium">组名</th>
          <th class="py-2 pr-2 font-medium">状态</th>
          <th class="py-2 pr-2 font-medium">成员数</th>
          <th class="py-2 pr-2 font-medium">协议类型</th>
          <th class="py-2 pr-2 font-medium">分配器</th>
          <th class="py-2 font-medium">来源</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in panel.rows"
          :key="row.groupId"
          class="cursor-pointer border-b border-slate-100 hover:bg-slate-50 dark:border-slate-700/60 dark:hover:bg-slate-800"
          :class="panel.selected === row.groupId ? 'bg-slate-100 dark:bg-slate-800' : ''"
          @click="toggleDetail(row)"
        >
          <td class="py-2 pr-2 font-medium">{{ row.groupId }}</td>
          <td class="py-2 pr-2">
            <span class="rounded-full px-2 py-0.5 text-[11px] font-medium" :class="stateClass(row.state)">{{ row.state }}</span>
          </td>
          <td class="py-2 pr-2">{{ row.memberCount }}</td>
          <td class="py-2 pr-2 text-slate-500 dark:text-slate-400">{{ row.protocolType || '—' }}</td>
          <td class="py-2 pr-2 text-slate-500 dark:text-slate-400">{{ row.partitionAssignor || '—' }}</td>
          <td class="py-2">{{ row.isLocal ? '本地订阅' : '外部' }}</td>
        </tr>
      </tbody>
    </table>
    <p v-if="panel.rows.length === 0" class="text-sm text-slate-500 dark:text-slate-400">
      {{ panel.loading ? '加载中…' : (targetCluster ? '该集群暂无消费组。' : '暂无消费组。') }}
    </p>

    <!-- T048a row-click inline detail: members + committed offsets (lag from wire) -->
    <div v-if="panel.selected" class="mt-4 rounded border border-slate-200 p-3 dark:border-slate-700">
      <div class="mb-2 flex items-center gap-3">
        <h3 class="text-sm font-semibold">{{ panel.selected }}</h3>
        <span v-if="panel.detail" class="text-xs text-slate-500 dark:text-slate-400">
          协调者 {{ panel.detail.info.coordinatorId ?? '—' }} · 订阅分配见成员表
        </span>
        <button type="button" class="ml-auto rounded border border-slate-300 px-2 py-1 text-xs text-slate-400 dark:border-slate-600" disabled title="T048b 落地">重置位点</button>
        <button type="button" class="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="closeDetail()">收起</button>
      </div>
      <p v-if="panel.detailError" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">
        {{ panel.detailError }}
      </p>
      <p v-if="panel.detailLoading" class="text-xs text-slate-500 dark:text-slate-400">详情加载中…</p>
      <template v-else-if="panel.detail">
        <h4 class="mb-1 text-xs font-semibold text-slate-600 dark:text-slate-300">成员（{{ panel.detail.members.length }}）</h4>
        <table class="mb-4 w-full border-collapse text-left text-xs">
          <thead>
            <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
              <th class="py-1.5 pr-2 font-medium">memberId</th>
              <th class="py-1.5 pr-2 font-medium">clientId</th>
              <th class="py-1.5 pr-2 font-medium">host</th>
              <th class="py-1.5 font-medium">分配分区</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="m in panel.detail.members" :key="m.memberId" class="border-b border-slate-100 dark:border-slate-700/60">
              <td class="py-1.5 pr-2 font-mono">{{ m.memberId }}</td>
              <td class="py-1.5 pr-2">{{ m.clientId }}</td>
              <td class="py-1.5 pr-2 text-slate-500 dark:text-slate-400">{{ m.clientHost }}</td>
              <td class="py-1.5 text-slate-500 dark:text-slate-400">
                {{ m.assignments.length ? m.assignments.map((a) => `${a.topic}-${a.partition}`).join(', ') : '—' }}
              </td>
            </tr>
          </tbody>
        </table>
        <p v-if="panel.detail.members.length === 0" class="mb-4 text-xs text-slate-500 dark:text-slate-400">该组当前没有成员。</p>
        <h4 class="mb-1 text-xs font-semibold text-slate-600 dark:text-slate-300">提交位点（{{ panel.offsets.length }}）</h4>
        <table class="w-full border-collapse text-left text-xs">
          <thead>
            <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
              <th class="py-1.5 pr-2 font-medium">Topic</th>
              <th class="py-1.5 pr-2 font-medium">分区</th>
              <th class="py-1.5 pr-2 font-medium">Offset</th>
              <th class="py-1.5 font-medium">Lag</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="o in panel.offsets" :key="`${o.topic}-${o.partition}`" class="border-b border-slate-100 dark:border-slate-700/60">
              <td class="py-1.5 pr-2 font-medium">{{ o.topic }}</td>
              <td class="py-1.5 pr-2">{{ o.partition }}</td>
              <td class="py-1.5 pr-2 font-mono">{{ o.offset }}</td>
              <td class="py-1.5 font-mono">{{ o.lag ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="panel.offsets.length === 0" class="text-xs text-slate-500 dark:text-slate-400">该组没有已提交位点。</p>
      </template>
    </div>
  </section>
</template>
