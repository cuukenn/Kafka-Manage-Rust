// T049a global toast store: push(level, text) with auto-dismiss
// (error 6s, warn/info 3s), manual close, cap 5 (oldest dropped).
import { reactive } from 'vue';

export type ToastLevel = 'error' | 'warn' | 'info';

export interface ToastItem {
  id: number;
  level: ToastLevel;
  text: string;
}

const MAX = 5;
const LIFETIME: Record<ToastLevel, number> = { error: 6000, warn: 3000, info: 3000 };

export const toasts = reactive({ items: [] as ToastItem[] });

let seq = 0;

export function push(level: ToastLevel, text: string): void {
  const item: ToastItem = { id: ++seq, level, text };
  toasts.items.push(item);
  if (toasts.items.length > MAX) toasts.items.shift();
  setTimeout(() => close(item.id), LIFETIME[level]);
}

export function close(id: number): void {
  const i = toasts.items.findIndex((t) => t.id === id);
  if (i >= 0) toasts.items.splice(i, 1);
}
