<script setup lang="ts">
// T048b offset-reset modal (style skeleton reused from ClusterModal.vue).
// Wire facts verified against kafka-core group.rs + webview-bridge tests/router_group.rs:
//   cmd  group.resetOffsets, flat payload {clusterId, groupId, topics, partitions, spec, dryRun}
//   spec is PER-REQUEST (ResetSpec, #[serde(tag="type", content="value")], camelCase):
//        {"type":"earliest"} | {"type":"latest"} | {"type":"toOffset","value":N}
//   answer data = ResetOffsetsResult {groupId, applied, dryRun, error}; error non-null -> inline bar.
// One request per distinct spec (partitions grouped), so per-row modes stay strictly wire-legal.
// Second-confirm bar lives inside the modal (T044a lesson: never window.confirm).
import { onMounted, onUnmounted, reactive } from 'vue';
import { RpcError, rpc } from '../bridge/rpc';

interface TopicPartition { topic: string; partition: number }
interface OffsetRow { topic: string; partition: number; offset: number; lag: number | null }
interface ResetResult { groupId: string; applied: unknown[]; dryRun: boolean; error: string | null }
type Mode = 'earliest' | 'latest' | 'toOffset';

const props = defineProps<{ clusterId: string; groupId: string; offsets: OffsetRow[] }>();
const emit = defineEmits<{ (e: 'close'): void; (e: 'updated'): void }>();

const rows = reactive(props.offsets.map((o) => ({
  topic: o.topic,
  partition: o.partition,
  offset: o.offset,
  lag: o.lag,
  enabled: true,
  mode: 'earliest' as Mode,
  value: '',
})));

const s = reactive({ busy: false, error: null as string | null, confirming: false });

// T050c: Esc closes via the same emit('close') path as 取消, guarded by s.busy.
const onKey = (e: KeyboardEvent): void => { if (e.key === 'Escape' && !s.busy) emit('close'); };
onMounted(() => window.addEventListener('keydown', onKey));
onUnmounted(() => window.removeEventListener('keydown', onKey));

const inp = 'mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-100';

function message(e: unknown): string {
  return e instanceof RpcError ? `${e.kind}: ${e.message}` : String(e);
}

function specOf(r: (typeof rows)[number]): Record<string, unknown> {
  return r.mode === 'toOffset' ? { type: 'toOffset', value: Number(r.value) } : { type: r.mode };
}

// Shared by submit() and execute(): inputs stay editable while the amber confirm
// bar is armed, so execute() re-validates before any destructive request.
function validateSelected(): boolean {
  const sel = rows.filter((r) => r.enabled);
  if (sel.length === 0) { s.error = '请至少选择一个分区。'; return false; }
  if (sel.some((r) => r.mode === 'toOffset' && !/^\d+$/.test(r.value.trim()))) {
    s.error = '「指定 Offset」必须是非负整数。';
    return false;
  }
  return true;
}

async function submit(): Promise<void> {
  if (s.confirming) { await execute(); return; }
  if (!validateSelected()) return;
  s.error = null;
  s.confirming = true; // in-modal second-confirm bar, not window.confirm
}

async function execute(): Promise<void> {
  if (!validateSelected()) return; // re-validate: inputs stay editable while the confirm bar is armed
  const sel = rows.filter((r) => r.enabled);
  const bySpec = new Map<string, { spec: Record<string, unknown>; parts: TopicPartition[] }>();
  for (const r of sel) {
    const spec = specOf(r);
    const g = bySpec.get(JSON.stringify(spec)) ?? { spec, parts: [] };
    g.parts.push({ topic: r.topic, partition: r.partition });
    bySpec.set(JSON.stringify(spec), g);
  }
  s.confirming = false;
  s.busy = true;
  s.error = null;
  let appliedGroups = 0; // spec groups that applied successfully before any failure
  try {
    for (const g of bySpec.values()) {
      const res = await rpc<ResetResult>('group.resetOffsets', {
        clusterId: props.clusterId,
        groupId: props.groupId,
        topics: [],
        partitions: g.parts,
        spec: g.spec,
        dryRun: false,
      });
      if (res?.error) {
        s.error = `重置失败：${res.error}`;
        if (appliedGroups > 0) emit('updated'); // partial write is real — refresh table, keep error bar
        return;
      }
      appliedGroups++;
    }
    emit('updated');
  } catch (e) {
    s.error = message(e);
    if (appliedGroups > 0) emit('updated'); // partial write before the throw — refresh table
  } finally {
    s.busy = false;
  }
}
</script>

<template>
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" @click.self="emit('close')">
    <form class="max-h-full w-full max-w-lg overflow-auto rounded-lg bg-white p-4 shadow-xl dark:bg-slate-800" @submit.prevent="submit">
      <div class="mb-3 flex items-center justify-between">
        <h3 class="text-base font-semibold text-slate-800 dark:text-slate-100">重置位点 · {{ groupId }}</h3>
        <button type="button" class="text-slate-400 hover:text-slate-600" @click="emit('close')">✕</button>
      </div>
      <p v-if="s.error" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ s.error }}</p>
      <p v-if="rows.length === 0" class="mb-3 text-xs text-slate-500 dark:text-slate-400">该组没有已提交位点，无法按分区重置。</p>
      <table v-else class="w-full border-collapse text-left text-xs text-slate-700 dark:text-slate-200">
        <thead>
          <tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-400">
            <th class="py-1.5 pr-2 font-medium">选择</th>
            <th class="py-1.5 pr-2 font-medium">Topic-分区</th>
            <th class="py-1.5 pr-2 font-medium">当前 Offset</th>
            <th class="py-1.5 pr-2 font-medium">方式</th>
            <th class="py-1.5 font-medium">新 Offset</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="`${r.topic}-${r.partition}`" class="border-b border-slate-100 dark:border-slate-700/60">
            <td class="py-1.5 pr-2"><input type="checkbox" v-model="r.enabled" /></td>
            <td class="py-1.5 pr-2 font-mono">{{ r.topic }}-{{ r.partition }}<span class="ml-1 text-slate-400">（lag {{ r.lag ?? '—' }}）</span></td>
            <td class="py-1.5 pr-2 font-mono">{{ r.offset }}</td>
            <td class="py-1.5 pr-2">
              <select v-model="r.mode" :class="inp">
                <option value="earliest">earliest</option>
                <option value="latest">latest</option>
                <option value="toOffset">指定 Offset</option>
              </select>
            </td>
            <td class="py-1.5">
              <input v-if="r.mode === 'toOffset'" v-model="r.value" type="text" inputmode="numeric" :class="inp" placeholder="例如 1024" />
              <span v-else class="text-slate-400">—</span>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="s.confirming" class="mt-3 flex items-center gap-2 rounded bg-amber-100 px-3 py-2 text-xs text-amber-900 dark:bg-amber-900 dark:text-amber-100">
        <span>确认将所选 {{ rows.filter((r) => r.enabled).length }} 个分区的提交位点重置？该操作会直接写入消费组。</span>
        <button type="button" class="ml-auto rounded border border-amber-400 px-2 py-1 hover:bg-amber-200 dark:border-amber-600 dark:hover:bg-amber-800" @click="s.confirming = false">返回编辑</button>
        <button type="button" class="rounded bg-rose-600 px-3 py-1 font-medium text-white hover:bg-rose-700" :disabled="s.busy" @click="execute()">{{ s.busy ? '提交中…' : '确认重置' }}</button>
      </div>
      <div class="mt-4 flex justify-end gap-2">
        <button type="button" class="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700" @click="emit('close')">取消</button>
        <button type="submit" class="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700" :disabled="s.busy || rows.length === 0">重置位点</button>
      </div>
    </form>
  </div>
</template>
