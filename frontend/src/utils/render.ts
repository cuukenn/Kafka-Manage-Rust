// T047: byte-array decode/render helpers + clipboard/export for the consume table.
// Wire shape (T046a-verified): push-path key/value are number[] (Vec<u8> via serde_json).
export type RenderMode = 'auto' | 'json' | 'base64' | 'hex';

export function bytesToBase64(arr: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < arr.length; i += 0x8000) bin += String.fromCharCode(...arr.subarray(i, i + 0x8000));
  return btoa(bin);
}

function utf8OrNone(arr: Uint8Array): string | null {
  try {
    return new TextDecoder('utf-8', { fatal: true }).decode(arr);
  } catch {
    return null;
  }
}

export function bytesToHex(arr: Uint8Array): string {
  let out = '';
  for (let i = 0; i < arr.length; i++) out += arr[i].toString(16).padStart(2, '0');
  return out;
}

export interface Rendered {
  text: string;
  /** false = bytes are not valid utf-8 and auto mode fell back to base64 */
  decoded: boolean;
}

/** Render bytes per mode. json: pretty when parseable (utf8), else falls back to auto. */
export function renderBytes(bytes: number[] | null | undefined, mode: RenderMode): Rendered {
  if (!bytes || bytes.length === 0) return { text: '', decoded: true };
  const arr = Uint8Array.from(bytes);
  if (mode === 'hex') return { text: bytesToHex(arr), decoded: true };
  if (mode === 'base64') return { text: bytesToBase64(arr), decoded: true };
  const utf = utf8OrNone(arr);
  if (mode === 'json' && utf !== null) {
    try {
      return { text: JSON.stringify(JSON.parse(utf), null, 2), decoded: true };
    } catch {
      /* not JSON: fall through to auto */
    }
  }
  if (utf !== null) return { text: utf, decoded: true };
  return { text: bytesToBase64(arr), decoded: false };
}

/** Clipboard with execCommand fallback: WebView2 pages may run in a non-secure context. */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to execCommand */
  }
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand('copy');
    ta.remove();
    return ok;
  } catch {
    return false;
  }
}

/** Blob + a[download] download. WebView2's download UI is a manual-verify item. */
export function downloadText(filename: string, text: string, mime = 'text/plain'): void {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 4000);
}

/** Pretty JSON through the same path (T083b: `downloadText` is the shared half). */
export function downloadJson(filename: string, data: unknown): void {
  downloadText(filename, JSON.stringify(data, null, 2), 'application/json');
}
