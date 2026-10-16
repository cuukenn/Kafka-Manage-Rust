<script setup lang="ts">
// T050a shared empty state: title (prop or slot) + optional hint slot +
// optional action slot. Icon-less minimal block, Tailwind light/dark.
defineProps<{ title?: string; hint?: string }>();
</script>

<template>
  <div class="py-8 text-center">
    <p class="text-sm font-medium text-slate-600 dark:text-slate-300">
      <slot name="title">{{ title }}</slot>
    </p>
    <p v-if="hint || $slots.hint" class="mt-1 text-xs text-slate-500 dark:text-slate-400">
      <slot name="hint">{{ hint }}</slot>
    </p>
    <div v-if="$slots.action" class="mt-3">
      <slot name="action" />
    </div>
  </div>
</template>
