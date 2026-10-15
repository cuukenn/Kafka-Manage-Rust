<script setup lang="ts">
// T040 minimal shell: proves the RPC round-trip (rpc('cluster.list', {})) and
// mounts with Tailwind styling active. Shell/navigation lands in T041.
import { onMounted, ref } from 'vue';
import { rpc, RpcError } from './bridge/rpc';

interface ClusterRow {
  id?: string;
  name?: string;
  bootstrapServers?: string;
}

type Phase = 'loading' | 'ok' | 'error';

const phase = ref<Phase>('loading');
const clusters = ref<ClusterRow[]>([]);
const errorKind = ref('');
const errorMessage = ref('');

async function load(): Promise<void> {
  phase.value = 'loading';
  errorKind.value = '';
  errorMessage.value = '';
  try {
    const data = await rpc<ClusterRow[]>('cluster.list', {});
    clusters.value = Array.isArray(data) ? data : [];
    phase.value = 'ok';
  } catch (e) {
    phase.value = 'error';
    if (e instanceof RpcError) {
      errorKind.value = e.kind;
      errorMessage.value = e.message;
    } else {
      errorKind.value = 'unknown';
      errorMessage.value = String(e);
    }
  }
}

onMounted(load);

function rowLabel(c: ClusterRow, i: number): string {
  const name = c.name ?? c.id ?? `cluster #${i + 1}`;
  return c.bootstrapServers ? `${name} — ${c.bootstrapServers}` : name;
}
</script>

<template>
  <main class="mx-auto max-w-xl p-6 font-sans">
    <h1 class="mb-1 text-2xl font-semibold text-emerald-400">rs-kafka-ui-client</h1>
    <p class="mb-4 text-sm text-slate-400">
      T040 scaffold — RPC round-trip probe via <code>cluster.list</code>
    </p>

    <div v-if="phase === 'loading'" class="mb-4 rounded bg-slate-800 p-4 text-sm text-slate-300">
      Loading clusters…
    </div>

    <template v-else-if="phase === 'ok'">
      <ul v-if="clusters.length" class="mb-4 space-y-2">
        <li
          v-for="(c, i) in clusters"
          :key="c.id ?? i"
          class="rounded border border-slate-700 bg-slate-800 px-3 py-2 text-sm"
        >
          {{ rowLabel(c, i) }}
        </li>
      </ul>
      <p v-else class="mb-4 rounded bg-slate-800 p-4 text-sm text-slate-400">
        RPC ok — no clusters yet (empty list).
      </p>
    </template>

    <div v-else class="mb-4 rounded border border-rose-700 bg-rose-950 p-4 text-sm text-rose-200">
      <span class="font-semibold">{{ errorKind }}</span>: {{ errorMessage }}
    </div>

    <button
      type="button"
      class="rounded bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-500 disabled:opacity-50"
      :disabled="phase === 'loading'"
      @click="load"
    >
      Retry
    </button>
  </main>
</template>
