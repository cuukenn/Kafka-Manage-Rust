// RPC bridge for the wry custom-protocol shell (T040, R14).
// MUST use the relative path '/rpc': the page is served from
// http://app.localhost by wry's protocol handler, and Chromium rejects
// absolute app:// fetches. Envelope: request {id,cmd,payload}; response
// {id,ok,data} | {id,ok:false,error:{kind,message,retryable}}.
import { reactive } from 'vue';
import { record } from '../store/logs';

export interface RpcRequest {
  id: string;
  cmd: string;
  payload?: unknown;
}

export interface RpcErrorBody {
  kind: string;
  message: string;
  retryable: boolean;
  // T081b: the driver's one line of advice, when the kind has one.
  hint?: string;
}

interface RpcOk<T> {
  id: string;
  ok: true;
  data: T;
}

interface RpcFail {
  id: string;
  ok: false;
  error: RpcErrorBody;
}

type RpcResponse<T> = RpcOk<T> | RpcFail;

export class RpcError extends Error {
  // T081b: kept apart from `message` so a caller that wants the driver's text and
  // the advice as two elements can still get them separately.
  readonly hint?: string;

  constructor(
    readonly kind: string,
    message: string,
    readonly retryable: boolean,
    hint?: string,
  ) {
    // R79/R80: every catch site renders `e.message`, so the advice rides along in it
    // (second line; plain `\n`, which pre-wrap text keeps and plain text folds).
    super(hint ? `${message}\n${hint}` : message);
    this.name = 'RpcError';
    this.hint = hint;
  }
}

let seq = 0;
function nextId(): string {
  seq += 1;
  return `${Date.now().toString(36)}.${seq.toString(36)}`;
}

// T049a: in-flight RPC count (reactive); Shell topbar spinner shows when > 0.
// T049b: each call also records a {cmd, ok, kind?} summary line in the log ring.
export const pending = reactive({ count: 0 });
export async function rpc<T = unknown>(cmd: string, payload?: unknown): Promise<T> {
  pending.count += 1;
  try {
    const data = await rpcInner<T>(cmd, payload);
    record('info', 'rpc', `${cmd} ok`);
    return data;
  } catch (e) {
    const kind = e instanceof RpcError ? ` (${e.kind})` : '';
    record('error', 'rpc', `${cmd} failed${kind}: ${String((e as Error)?.message ?? e)}`);
    throw e;
  } finally {
    pending.count -= 1;
  }
}
async function rpcInner<T>(cmd: string, payload?: unknown): Promise<T> {
  const id = nextId();
  const controller = new AbortController();
  inflight.set(id, controller);
  try {
    return await rpcSend<T>(id, cmd, payload, controller.signal);
  } finally {
    inflight.delete(id);
  }
}

// T082c: in-flight requests by envelope id. Dropping one does two things — it
// posts `request.cancel` (T082b) so the Rust handler is aborted, and it aborts
// the local fetch so the page stops waiting for an answer nobody wants.
const inflight = new Map<string, AbortController>();

/** Drop everything still in flight; returns how many were dropped (T082c). */
export function cancelInflight(): number {
  const dropped = [...inflight];
  inflight.clear();
  for (const [id, controller] of dropped) {
    controller.abort();
    // Best effort: the reply races the aborted fetch above and is not awaited.
    void fetch('/rpc', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        id: `cancel.${id}`,
        cmd: 'request.cancel',
        payload: { id },
      } satisfies RpcRequest),
    }).catch(() => {});
  }
  return dropped.length;
}

async function rpcSend<T>(
  id: string,
  cmd: string,
  payload: unknown,
  signal: AbortSignal,
): Promise<T> {
  let res: Response;
  try {
    res = await fetch('/rpc', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ id, cmd, payload } satisfies RpcRequest),
      signal,
    });
  } catch (e) {
    if (e instanceof DOMException && e.name === 'AbortError') {
      throw new RpcError('cancelled', `${cmd} was cancelled`, false);
    }
    throw new RpcError('network', `fetch /rpc failed: ${String(e)}`, true);
  }
  if (!res.ok) {
    throw new RpcError('http', `rpc endpoint returned HTTP ${res.status}`, false);
  }
  let body: RpcResponse<T>;
  try {
    body = (await res.json()) as RpcResponse<T>;
  } catch (e) {
    throw new RpcError('protocol', `invalid rpc response: ${String(e)}`, false);
  }
  if (body.ok === true) {
    return body.data;
  }
  if (body.ok === false) {
    throw new RpcError(
      body.error.kind,
      body.error.message,
      body.error.retryable,
      body.error.hint,
    );
  }
  throw new RpcError('protocol', 'malformed rpc envelope (missing ok)', false);
}
