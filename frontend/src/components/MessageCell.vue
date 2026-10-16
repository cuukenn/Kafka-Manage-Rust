<script setup lang="ts">
// T047: one key/value cell — renders number[] bytes in the chosen decode mode
// (auto/json/base64/hex via render.ts) and collapses text over ~200 chars behind a click.
import { computed, ref } from 'vue';
import { renderBytes, type RenderMode } from '../utils/render';

const props = defineProps<{ bytes: number[] | null; mode: RenderMode }>();
const COLLAPSE_AT = 200;
const expanded = ref(false);

const r = computed(() => renderBytes(props.bytes, props.mode));
const long = computed(() => r.value.text.length > COLLAPSE_AT);
const shown = computed(() => (long.value && !expanded.value ? `${r.value.text.slice(0, COLLAPSE_AT)}…` : r.value.text));
const toggleLabel = computed(() => (expanded.value ? '收起' : `展开（共 ${r.value.text.length} 字符）`));
</script>

<template>
  <div class="whitespace-pre-wrap break-all font-mono">
    <span v-if="bytes === null || bytes.length === 0" class="text-slate-400">—</span>
    <template v-else>
      <span :class="r.decoded ? '' : 'text-amber-600 dark:text-amber-400'">{{ shown }}</span>
      <button v-if="long" type="button" class="ml-1 align-baseline text-[11px] text-sky-600 hover:underline dark:text-sky-400" @click="expanded = !expanded">{{ toggleLabel }}</button>
      <span v-if="!r.decoded" class="text-slate-400"> · {{ bytes.length }}B</span>
    </template>
  </div>
</template>
