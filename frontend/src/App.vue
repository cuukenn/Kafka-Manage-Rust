<script setup lang="ts">
// T041 app root: mounts the shell, restores the persisted theme, and starts
// the connection badge (mount-time cluster.list pull + "connection.state"
// event refresh). View switching itself lives in store/ui.ts — no router.
import { onMounted, onUnmounted } from 'vue';
import Shell from './components/Shell.vue';
import { initTheme, startBadge } from './store/ui';

let stopBadge: (() => void) | undefined;

onMounted(async () => {
  await initTheme();
  stopBadge = startBadge();
});

onUnmounted(() => stopBadge?.());
</script>

<template>
  <Shell />
</template>
