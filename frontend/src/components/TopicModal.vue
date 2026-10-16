<script setup lang="ts">
// T044a create-topic modal: name / numPartitions / replicationFactor ->
// "topic.create" payload {clusterId, name, numPartitions, replicationFactor}
// (keys verified against webview-bridge tests/router_topic_produce.rs
// create_payload; kafka-core CreateTopicReq has serde defaults, so the form
// only requires a name and sends numbers as typed).
import { onMounted, onUnmounted, reactive, ref } from 'vue';
import { rpc } from '../bridge/rpc';
import { invalidateCluster } from '../store/tree';

const props = defineProps<{ clusterId: string }>();
const emit = defineEmits<{ (e: 'created'): void; (e: 'close'): void }>();

const f = reactive({ name: '', partitions: 1, rf: 1 });
const error = ref<string | null>(null);
const busy = ref(false);

// T050c: Esc closes via the same emit('close') path as 取消, guarded by busy.
const onKey = (e: KeyboardEvent): void => { if (e.key === 'Escape' && !busy.value) emit('close'); };
onMounted(() => window.addEventListener('keydown', onKey));
onUnmounted(() => window.removeEventListener('keydown', onKey));

const inp =
  'mt-1 w-full rounded border border-slate-300 bg-white px-2 py-1.5 text-xs text-slate-800 dark:border-slate-600 dark:bg-slate-900 dark:text-slate-100';

async function submit(): Promise<void> {
  busy.value = true;
  error.value = null;
  try {
    await rpc('topic.create', {
      clusterId: props.clusterId,
      name: f.name.trim(),
      numPartitions: f.partitions,
      replicationFactor: f.rf,
    });
    invalidateCluster(props.clusterId); // keep the T043 sidebar tree in sync
    emit('created');
  } catch (e) {
    error.value = e instanceof Error ? e.message : String(e);
  } finally {
    busy.value = false;
  }
}
</script>

<template>
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" @click.self="emit('close')">
    <form class="w-full max-w-sm rounded-lg bg-white p-4 shadow-xl dark:bg-slate-800" @submit.prevent="submit">
      <div class="mb-3 flex items-center justify-between">
        <h3 class="text-base font-semibold text-slate-800 dark:text-slate-100">新建 Topic</h3>
        <button type="button" class="text-slate-400 hover:text-slate-600" @click="emit('close')">✕</button>
      </div>
      <p v-if="error" class="mb-2 rounded bg-rose-100 px-3 py-2 text-xs text-rose-800 dark:bg-rose-900 dark:text-rose-200">{{ error }}</p>
      <div class="grid grid-cols-2 gap-x-3 gap-y-2 text-xs text-slate-700 dark:text-slate-200">
        <label class="col-span-2 block">名称<input v-model="f.name" required :class="inp" placeholder="orders" /></label>
        <label class="block">分区数<input v-model.number="f.partitions" type="number" min="1" :class="inp" /></label>
        <label class="block">副本因子<input v-model.number="f.rf" type="number" min="1" :class="inp" /></label>
      </div>
      <div class="mt-4 flex justify-end gap-2">
        <button
          type="button"
          class="rounded border border-slate-300 px-3 py-1.5 text-xs text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          @click="emit('close')"
        >
          取消
        </button>
        <button type="submit" class="rounded bg-emerald-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50" :disabled="busy">
          {{ busy ? '创建中…' : '创建' }}
        </button>
      </div>
    </form>
  </div>
</template>
