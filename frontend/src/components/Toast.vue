<script setup lang="ts">
// T049a bottom-right toast stack; level colors, light/dark, manual close.
import { close, toasts } from '../store/toast';

const LEVEL_CLASS: Record<string, string> = {
  error:
    'border-red-300 bg-red-50 text-red-800 dark:border-red-700 dark:bg-red-950 dark:text-red-200',
  warn: 'border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-200',
  info: 'border-sky-300 bg-sky-50 text-sky-900 dark:border-sky-700 dark:bg-sky-950 dark:text-sky-200',
};
</script>

<template>
  <div
    class="pointer-events-none fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2"
    role="status"
    aria-live="polite"
  >
    <div
      v-for="t in toasts.items"
      :key="t.id"
      class="pointer-events-auto flex items-start gap-2 rounded border px-3 py-2 text-xs shadow-lg"
      :class="LEVEL_CLASS[t.level]"
    >
      <span class="flex-1 whitespace-pre-wrap break-words">{{ t.text }}</span>
      <button
        type="button"
        class="shrink-0 opacity-60 hover:opacity-100"
        aria-label="关闭"
        @click="close(t.id)"
      >
        ✕
      </button>
    </div>
  </div>
</template>
