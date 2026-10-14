import { decode as cborDecode, encode as cborEncode } from 'cbor-x';
import { createHash } from 'node:crypto';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export const newRunId = () => `wrun_${ulid()}`;
export const newMessageId = () => `msg_${ulid()}`;
export const newChunkSeq = () => ulid();

export function encodeBlob(value: unknown): string {
  return Buffer.from(cborEncode(value)).toString('base64');
}

export function decodeBlob<T = unknown>(b64: string | null | undefined): T | undefined {
  if (!b64) return undefined;
  return cborDecode(Buffer.from(b64, 'base64')) as T;
}

export function bytesToBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('base64');
}

export function base64ToBytes(b64: string): Uint8Array {
  return new Uint8Array(Buffer.from(b64, 'base64'));
}

export function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

const SAFE_ID = /^[A-Za-z0-9_-]+$/;
export function assertSafeEntityId(name: string, value: string): void {
  if (!SAFE_ID.test(value)) {
    throw new Error(
      `[world-edgeone/kv] Invalid ${name} "${value}": must match ${SAFE_ID}`
    );
  }
}

/**
 * Pad a numeric timestamp (ms since epoch) to 13 zero-padded digits so
 * lexicographic ordering of keys preserves chronological order. 13 digits
 * covers years up to ~5138 AD — plenty.
 */
export function padTs(ms: number): string {
  return ms.toString().padStart(13, '0');
}

/** Pad an integer chunk index to 10 digits for the same lex-sort reason. */
export function padIdx(n: number): string {
  return n.toString().padStart(10, '0');
}
