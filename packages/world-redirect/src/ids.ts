import { createHash } from 'node:crypto';
import { monotonicFactory } from 'ulid';

const ulid = monotonicFactory();

export function monotonicUlid(): string {
  return ulid();
}

export function newRunId(): string {
  return `wrun_${ulid()}`;
}

export function newMessageId(): string {
  return `msg_${ulid()}`;
}

/** SHA-256 hex of a hook token, used as a fixed-length key component. */
export function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex');
}

const SAFE_ID = /^[A-Za-z0-9_-]+$/;

/**
 * Reject identifiers that could break out of the key namespace (':' is the
 * key separator) or inject control characters. runIds and correlationIds
 * are ULID-based (`prefix_<Crockford base32>`) and eventIds are slot
 * positions (`evnt_<zero-padded digits>`), so the safe set is
 * alphanumerics plus `_` and `-`.
 */
export function assertSafeEntityId(name: string, value: string): void {
  if (!SAFE_ID.test(value)) {
    throw new Error(
      `[world-redirect] Invalid ${name} "${value}": must match ${SAFE_ID}`
    );
  }
}
