// RPC bridge for the wry custom-protocol shell (T040, R14).
// MUST use the relative path '/rpc': the page is served from
// http://app.localhost by wry's protocol handler, and Chromium rejects
// absolute app:// fetches. Envelope: request {id,cmd,payload}; response
// {id,ok,data} | {id,ok:false,error:{kind,message,retryable}}.

export interface RpcRequest {
  id: string;
  cmd: string;
  payload?: unknown;
}

export interface RpcErrorBody {
  kind: string;
  message: string;
  retryable: boolean;
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
  constructor(
    readonly kind: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = 'RpcError';
  }
}

let seq = 0;
function nextId(): string {
  seq += 1;
  return `${Date.now().toString(36)}.${seq.toString(36)}`;
}

export async function rpc<T = unknown>(cmd: string, payload?: unknown): Promise<T> {
  let res: Response;
  try {
    res = await fetch('/rpc', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ id: nextId(), cmd, payload } satisfies RpcRequest),
    });
  } catch (e) {
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
    throw new RpcError(body.error.kind, body.error.message, body.error.retryable);
  }
  throw new RpcError('protocol', 'malformed rpc envelope (missing ok)', false);
}
