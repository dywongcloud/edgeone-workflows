/**
 * KV-backed queue + in-process dispatcher for the EdgeOne KV world.
 *
 * Schedule layout:
 *   {p}/job/<paddedRunAtMs>/<messageId>   → CBOR(QueueJob)
 *
 * The padded `runAtMs` in the key means `kv.list({prefix: '{p}/job/'})` returns
 * jobs already in chronological order, so the dispatcher can iterate until it
 * hits a key whose timestamp prefix is > now and stop early.
 *
 * Concurrency / multi-host safety:
 *   KV has no atomic compare-and-swap. To prevent two hosts dispatching the
 *   same job, we acquire a short-TTL lease via `put({ifNotExists: true})` on
 *   `{p}/lease/<messageId>`. EdgeOne / Workers KV require TTL ≥ 60s, so the
 *   lease window is 60s minimum — long enough for any reasonable HTTP
 *   dispatch, but means a crashed dispatcher delays re-delivery by up to 60s.
 *   Workflow handlers are idempotent (event-sourced replay), so a
 *   double-dispatch on lease expiry is safe, just wasteful.
 */

import {
  MessageId,
  type Queue,
  type QueuePayload,
  ValidQueueName,
} from '@workflow/world';
import { decodeBlob, encodeBlob, newMessageId, padTs } from './codec.js';
import type { Keys } from './keys.js';
import type { KV } from './types.js';

interface QueueJob {
  messageId: string;
  queueName: string;
  route: 'flow' | 'step';
  runId: string;
  attempt: number;
  runAtMs: number;
  body: unknown; // decoded message payload — stored CBOR-encoded
}

interface StoredJob {
  messageId: string;
  queueName: string;
  route: 'flow' | 'step';
  runId: string;
  attempt: number;
  runAtMs: number;
  bodyB64: string;
}

interface QueueConfig {
  readonly baseUrl: string;
  readonly deploymentId: string;
  readonly maxAttempts: number;
  readonly retryBaseMs: number;
  readonly dispatcherPollMs: number;
  readonly dispatchBatch: number;
  readonly leaseSeconds: number;
}

function backoffMs(attempt: number, base: number): number {
  if (attempt <= 6) return base;
  return Math.min(base * 2 ** (attempt - 6), 5 * 60_000);
}

export interface KVQueue extends Queue {
  startDispatcher(): void;
  stopDispatcher(): Promise<void>;
  drainOnce(): Promise<number>;
}

export function createKVQueue(
  kv: KV,
  keys: Keys,
  config: QueueConfig
): KVQueue {
  let running = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let inFlight = 0;

  const leaseKey = (messageId: string) => `${keys.root}/lease/${messageId}`;

  async function storeJob(job: StoredJob): Promise<void> {
    await kv.put(keys.jobBlob(job.runAtMs, job.messageId), encodeBlob(job));
  }

  async function readJobByKey(jobKey: string): Promise<StoredJob | null> {
    const s = await kv.get(jobKey);
    if (!s) return null;
    return decodeBlob<StoredJob>(s) ?? null;
  }

  async function findJobByMessageId(
    messageId: string
  ): Promise<{ key: string; job: StoredJob } | null> {
    // The messageId is the last segment of the key. Without a secondary index
    // we list the job prefix and match — for the createQueueHandler lookup
    // only, not the hot dispatch path. Bounded by listing the schedule, which
    // is at most "queued jobs" — typically a handful at a time.
    let cursor: string | undefined;
    do {
      const listOpts: { prefix: string; cursor?: string; limit?: number } = {
        prefix: keys.jobPrefix(),
        limit: 1000,
      };
      if (cursor) listOpts.cursor = cursor;
      const page = await kv.list(listOpts);
      for (const k of page.keys) {
        if (k.endsWith(`/${messageId}`)) {
          const job = await readJobByKey(k);
          if (job) return { key: k, job };
        }
      }
      cursor = page.complete ? undefined : page.cursor;
    } while (cursor);
    return null;
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    const messageId = newMessageId();
    // Workflow orchestration and queued step invocations share the combined
    // flow handler.
    const route = 'flow' as const;
    const runAtMs =
      Date.now() + Math.max(0, (opts?.delaySeconds ?? 0) * 1000);
    const runId = (message as any)?.runId ?? (message as any)?.workflowRunId ?? '';
    await storeJob({
      messageId,
      queueName,
      route,
      runId,
      attempt: 1,
      runAtMs,
      bodyB64: encodeBlob(message),
    });
    return { messageId: MessageId.parse(messageId) };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => {
    return async (req: Request): Promise<Response> => {
      const url = new URL(req.url);
      const msgId = url.searchParams.get('msg');
      if (!msgId) {
        return Response.json({ error: 'missing ?msg' }, { status: 400 });
      }

      const hit = await findJobByMessageId(msgId);
      if (!hit) return Response.json({ ok: true }); // idempotent no-op

      const { key: jobKey, job } = hit;
      if (!job.queueName.startsWith(prefix)) {
        return Response.json({ error: 'Unhandled queue' }, { status: 400 });
      }

      const message = decodeBlob<QueuePayload>(job.bodyB64);
      if (message === undefined) {
        return Response.json({ error: 'empty body' }, { status: 400 });
      }

      let result: unknown;
      try {
        result = await handler(message, {
          attempt: job.attempt,
          queueName: ValidQueueName.parse(job.queueName),
          messageId: MessageId.parse(msgId),
          requestId: req.headers.get('x-request-id') ?? undefined,
        });
      } catch (err) {
        const nextAttempt = job.attempt + 1;
        await kv.delete(jobKey);
        if (nextAttempt <= config.maxAttempts) {
          const nextRunAt =
            Date.now() + backoffMs(nextAttempt, config.retryBaseMs);
          await storeJob({
            ...job,
            attempt: nextAttempt,
            runAtMs: nextRunAt,
          });
        }
        await kv.delete(leaseKey(msgId));
        return Response.json(String(err), { status: 500 });
      }

      await kv.delete(jobKey);
      await kv.delete(leaseKey(msgId));
      const timeoutSeconds =
        typeof result === 'object' && result !== null
          ? (result as { timeoutSeconds?: unknown }).timeoutSeconds
          : undefined;
      if (typeof timeoutSeconds === 'number') {
        const sec = Math.max(0, timeoutSeconds);
        await storeJob({
          ...job,
          runAtMs: Date.now() + sec * 1000,
        });
        return Response.json({ timeoutSeconds: sec });
      }

      return Response.json({ ok: true });
    };
  };

  const getDeploymentId: Queue['getDeploymentId'] = async () =>
    config.deploymentId;

  async function dispatchOne(jobKey: string, job: StoredJob): Promise<void> {
    const target = `${config.baseUrl}/.well-known/workflow/v1/${job.route}?msg=${encodeURIComponent(job.messageId)}`;
    try {
      const res = await fetch(target, {
        method: 'POST',
        redirect: 'follow',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': job.queueName,
          'x-vqs-message-id': job.messageId,
          'x-vqs-message-attempt': String(job.attempt),
        },
        body: '',
      });
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        console.error(
          `[world-edgeone/kv] dispatch ${job.messageId} -> HTTP ${res.status}: ${text.slice(0, 200)}`
        );
      }
      // Note: the queue handler itself deletes the job key on success / failure,
      // so we do nothing here on 2xx. On 5xx, the handler also rescheduled.
    } catch (err) {
      console.error(
        `[world-edgeone/kv] dispatch ${job.messageId} network error:`,
        err
      );
      try {
        const nextAttempt = job.attempt + 1;
        await kv.delete(jobKey);
        if (nextAttempt <= config.maxAttempts) {
          await storeJob({
            ...job,
            attempt: nextAttempt,
            runAtMs: Date.now() + backoffMs(nextAttempt, config.retryBaseMs),
          });
        }
        await kv.delete(leaseKey(job.messageId));
      } catch {
        // best effort
      }
    }
  }

  async function drainOnce(): Promise<number> {
    const now = Date.now();
    const upToPrefix = padTs(now);
    let dispatched = 0;
    let cursor: string | undefined;
    let scanned = 0;
    const cap = config.dispatchBatch;
    pageLoop: while (dispatched < cap) {
      const listOpts: { prefix: string; cursor?: string; limit?: number } = {
        prefix: keys.jobPrefix(),
        limit: 100,
      };
      if (cursor) listOpts.cursor = cursor;
      const page = await kv.list(listOpts);
      for (const k of page.keys) {
        // Key shape: <root>/job/<paddedTs>/<msgId>
        // Extract paddedTs and short-circuit when past `now`.
        const rest = k.slice(keys.jobPrefix().length); // "<paddedTs>/<msgId>"
        const slash = rest.indexOf('/');
        const tsPart = slash >= 0 ? rest.slice(0, slash) : rest;
        if (tsPart > upToPrefix) {
          // Lex-greater means runAt > now; nothing else is due in this scan.
          break pageLoop;
        }
        scanned++;
        const job = await readJobByKey(k);
        if (!job) continue;
        // Lease the message so concurrent dispatchers don't pick it up.
        const claim = await kv.put(leaseKey(job.messageId), '1', {
          ifNotExists: true,
          ttlSeconds: config.leaseSeconds,
        });
        if (!claim.ok) continue;
        inFlight++;
        dispatched++;
        void dispatchOne(k, job).finally(() => {
          inFlight--;
        });
        if (dispatched >= cap) break pageLoop;
      }
      cursor = page.complete ? undefined : page.cursor;
      if (!cursor) break;
      if (scanned > 5000) break; // safety
    }
    return dispatched;
  }

  function startDispatcher(): void {
    if (running) return;
    running = true;
    const tick = async () => {
      if (!running) return;
      try {
        await drainOnce();
      } catch (err) {
        console.error('[world-edgeone/kv] dispatcher tick error:', err);
      }
      if (!running) return;
      timer = setTimeout(tick, config.dispatcherPollMs);
    };
    void tick();
  }

  async function stopDispatcher(): Promise<void> {
    running = false;
    if (timer) clearTimeout(timer);
    const deadline = Date.now() + 5000;
    while (inFlight > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  return {
    queue,
    createQueueHandler,
    getDeploymentId,
    startDispatcher,
    stopDispatcher,
    drainOnce,
  };
}
