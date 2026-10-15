// Event-batch bridge (T040). Rust evaluates
// `window.__bridgeEvents && window.__bridgeEvents(batch)` where batch is a JSON
// array of {event,payload,ts}. index.html defines a queueing hook first, since
// batches can arrive before this module runs; here we drain that queue, then
// install our own dispatching handler and expose the subscriber API.

export interface BridgeEvent {
  event: string;
  payload?: unknown;
  ts?: number;
}

declare global {
  interface Window {
    __bridgeEvents?: (batch: BridgeEvent[]) => void;
    __bridgeQueue?: BridgeEvent[][];
  }
}

type Handler = (e: BridgeEvent) => void;

const handlers = new Map<string, Set<Handler>>();

function dispatchBatch(batch: BridgeEvent[]): void {
  for (const e of batch) {
    const set = handlers.get(e.event);
    if (!set) continue;
    for (const h of set) {
      try {
        h(e);
      } catch (err) {
        console.error('[bridge] event handler error for', e.event, err);
      }
    }
  }
}

/** Subscribe to one event name; returns an unsubscribe function. */
export function on(event: string, handler: Handler): () => void {
  let set = handlers.get(event);
  if (!set) {
    set = new Set();
    handlers.set(event, set);
  }
  set.add(handler);
  const bucket = set;
  return () => bucket.delete(handler);
}

// Drain batches queued by the inline index.html hook, then take over the hook.
const queued = window.__bridgeQueue ?? [];
window.__bridgeQueue = [];
window.__bridgeEvents = dispatchBatch;
for (const batch of queued) {
  dispatchBatch(batch);
}
