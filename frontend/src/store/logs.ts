// T049b: app log ring buffer (cap 500, newest first) fed from event/RPC/UI
// sources. events.ts exposes onAll(); we subscribe here at module load so the
// buffer fills even while LogView is not mounted. rpc.ts records its own
// response/error summaries. Never log whole payloads — event name plus a
// whitelisted key-field summary only.
import { reactive } from 'vue';
import { onAll, type BridgeEvent } from '../bridge/events';

export type LogLevel = 'info' | 'warn' | 'error';
export type LogSource = 'event' | 'rpc' | 'ui';

export interface LogEntry {
  ts: number;
  level: LogLevel;
  source: LogSource;
  text: string;
}

const CAP = 500;

export const logState = reactive({ entries: [] as LogEntry[] });

export function record(level: LogLevel, source: LogSource, text: string): void {
  logState.entries.unshift({ ts: Date.now(), level, source, text });
  if (logState.entries.length > CAP) logState.entries.length = CAP;
}

export function clearLogs(): void {
  logState.entries.length = 0;
}

/** Scalar fields worth surfacing in an event summary line. */
const KEY_FIELDS = [
  'clusterId',
  'topic',
  'partition',
  'groupId',
  'state',
  'offset',
  'key',
  'name',
] as const;

function summarizeEvent(e: BridgeEvent): string {
  const p = e.payload;
  if (p && typeof p === 'object' && !Array.isArray(p)) {
    const parts: string[] = [];
    for (const k of KEY_FIELDS) {
      const v = (p as Record<string, unknown>)[k];
      if (v !== undefined && v !== null && typeof v !== 'object') {
        parts.push(`${k}=${String(v)}`);
      }
    }
    return parts.length ? `${e.event} ${parts.join(' ')}` : e.event;
  }
  return e.event;
}

function levelFor(e: BridgeEvent): LogLevel {
  const p = e.payload;
  if (p && typeof p === 'object' && (p as { state?: string }).state === 'error') {
    return 'error';
  }
  return e.event.includes('error') ? 'error' : 'info';
}

// One summary line per pushed event (module-load side effect; Shell.vue
// statically imports the view chain, so this runs at app start).
onAll((e) => record(levelFor(e), 'event', summarizeEvent(e)));
