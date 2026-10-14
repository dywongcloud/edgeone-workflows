import { AsyncLocalStorage } from 'node:async_hooks';
import { handleHealthCheckMessage } from '@workflow/core/runtime/helpers';
import {
  MessageId,
  type Queue,
  type QueuePayload,
  ValidQueueName,
} from '@workflow/world';
import type { RedisClient } from './client/types.js';
import { decodeBlob, encodeBlob } from './codec.js';
import type { ResolvedRedisWorldConfig } from './config.js';
import { newMessageId } from './ids.js';
import type { Keys } from './keys.js';

/**
 * The HTTP route a job is delivered to. Workflow orchestration and queued
 * step invocations share the combined flow handler, so every job goes there;
 * `step` is only read back from job records written by older versions.
 */
type Route = 'flow' | 'step';

interface JobRecord {
  queueName: string;
  body: string; // base64(CBOR(message))
  attempt: number;
  headers: Record<string, string>;
  route: Route;
}

interface HandlerContext {
  /** Immediate (delay=0) self-enqueues recorded during a handler invocation. */
  immediate: { messageId: string; route: Route }[];
}

/**
 * Maximum 307 hops in a single dispatch chain. Kept safely below undici's
 * default redirect limit (20); when exceeded, the wrapper stops redirecting
 * and lets the dispatcher pick the next job up on its own poll.
 */
const MAX_HOPS = 15;

/**
 * Grace window applied to in-handler immediate self-continuations so the
 * polling dispatcher does not race the 307 trampoline for them. The 307
 * wrapper claims the job well within this window; if the chain breaks, the
 * job still fires after it for durability.
 */
const IMMEDIATE_GRACE_MS = 1000;

function routeFor(_queueName: string): Route {
  return 'flow';
}

function backoffMs(attempt: number, base: number): number {
  // Linear base for the first attempts, then capped exponential.
  if (attempt <= 6) return base;
  return Math.min(base * 2 ** (attempt - 6), 5 * 60_000);
}

export interface RedisQueue extends Queue {
  /** Start the in-process 307 dispatch pump. */
  startDispatcher(): void;
  /** Stop the pump. */
  stopDispatcher(): Promise<void>;
  /**
   * Drain all currently-due jobs once (used by external triggers / tests).
   * Returns the number of jobs dispatched.
   */
  drainOnce(): Promise<number>;
  /** Resolves once the schedule is empty (used by tests). */
  waitForIdle(timeoutMs?: number): Promise<void>;
}

export function createQueue(
  redis: RedisClient,
  keys: Keys,
  config: ResolvedRedisWorldConfig,
  specVersion: number
): RedisQueue {
  const als = new AsyncLocalStorage<HandlerContext>();
  let dispatcherTimer: ReturnType<typeof setTimeout> | null = null;
  let running = false;
  let inFlight = 0;
  const MAX_CONCURRENCY = 100;
  let wake: (() => void) | null = null;

  async function writeJob(messageId: string, job: JobRecord): Promise<void> {
    await redis.hsetMany(keys.job(messageId), {
      queueName: job.queueName,
      body: job.body,
      attempt: String(job.attempt),
      headers: encodeBlob(job.headers),
      route: job.route,
    });
  }

  async function readJob(messageId: string): Promise<JobRecord | null> {
    const raw = await redis.hgetAll(keys.job(messageId));
    if (!raw || !raw.queueName) return null;
    return {
      queueName: raw.queueName,
      body: raw.body ?? '',
      attempt: Number(raw.attempt ?? '1'),
      headers: raw.headers ? decodeBlob<Record<string, string>>(raw.headers) : {},
      route: routeFor(raw.queueName),
    };
  }

  async function deleteJob(messageId: string): Promise<void> {
    await redis.del(keys.job(messageId));
    await redis.zrem(keys.schedule(), messageId);
  }

  async function scheduleJob(messageId: string, runAt: number): Promise<void> {
    await redis.zadd(keys.schedule(), runAt, messageId);
  }

  function ensureDispatcher(): void {
    // Auto-start the pump on first use so the world "just works" inside any
    // server (Next.js, standalone, etc.) without an explicit start() call.
    if (config.startDispatcher && !running) startDispatcher();
  }

  const queue: Queue['queue'] = async (queueName, message, opts) => {
    ensureDispatcher();
    // Idempotency: collapse duplicate enqueues sharing a key.
    if (opts?.idempotencyKey) {
      const idemKey = `${keys.job('idem')}:${opts.idempotencyKey}`;
      const placeholder = newMessageId();
      const claimed = await redis.set(idemKey, placeholder, {
        nx: true,
        pxMs: 3_600_000,
      });
      if (!claimed) {
        const existing = await redis.get(idemKey);
        return { messageId: MessageId.parse(existing ?? placeholder) };
      }
    }

    const messageId = newMessageId();
    const route = routeFor(queueName);
    const delayMs = (opts?.delaySeconds ?? 0) * 1000;

    // 307 hot path: a no-delay enqueue *inside* a handler is a self-
    // continuation. We schedule it slightly in the future (a grace window) so
    // the polling dispatcher won't steal it, record it for the wrapper to
    // claim and continue via a 307 redirect in the same dispatch chain, and
    // do NOT wake the dispatcher. If the chain breaks (crash / hop limit), the
    // job still fires after the grace window — durability is preserved.
    const ctx = delayMs === 0 ? als.getStore() : undefined;
    const inHandlerImmediate = !!ctx;
    const runAt =
      Date.now() + (inHandlerImmediate ? IMMEDIATE_GRACE_MS : delayMs);

    await writeJob(messageId, {
      queueName,
      body: encodeBlob(message),
      attempt: 1,
      headers: opts?.headers ?? {},
      route,
    });
    await scheduleJob(messageId, runAt);

    if (inHandlerImmediate) {
      ctx.immediate.push({ messageId, route });
    } else if (wake) {
      wake();
    }
    return { messageId: MessageId.parse(messageId) };
  };

  const createQueueHandler: Queue['createQueueHandler'] = (prefix, handler) => {
    return async (req: Request): Promise<Response> => {
      ensureDispatcher();
      const url = new URL(req.url);
      const msgId = url.searchParams.get('msg');
      const hop = Number(url.searchParams.get('hop') ?? '0');

      let job: JobRecord | null = null;
      let messageId: string;

      if (msgId) {
        job = await readJob(msgId);
        // Already processed/claimed by another chain — idempotent no-op.
        if (!job) return Response.json({ ok: true });
        messageId = msgId;
      } else {
        // Fallback: direct invocation with x-vqs-* headers + body
        // (matches the standard queue transport). No durable job record.
        const headers = req.headers;
        const queueName = headers.get('x-vqs-queue-name') ?? '';
        messageId = headers.get('x-vqs-message-id') ?? newMessageId();
        const attempt = Number(headers.get('x-vqs-message-attempt') ?? '1');
        if (!queueName || !req.body) {
          return Response.json({ error: 'Missing headers or body' }, {
            status: 400,
          });
        }
        const buf = new Uint8Array(await req.arrayBuffer());
        job = {
          queueName,
          body: Buffer.from(buf).toString('base64'),
          attempt,
          headers: {},
          route: routeFor(queueName),
        };
      }

      const message = decodeBlob<QueuePayload>(job.body);

      // Health-check probe: satisfy it directly by calling the runtime
      // helper, which writes the response stream that healthCheck() is
      // waiting on.
      const hc = (message as any)?.__healthCheck === true ? (message as any) : null;
      if (hc) {
        await handleHealthCheckMessage(hc, specVersion);
        if (msgId) await deleteJob(msgId);
        return Response.json({ ok: true });
      }

      if (!job.queueName.startsWith(prefix)) {
        return Response.json({ error: 'Unhandled queue' }, { status: 400 });
      }

      const runId =
        (message as any)?.runId ?? (message as any)?.workflowRunId;

      const ctx: HandlerContext = { immediate: [] };
      let result: unknown;
      try {
        result = await als.run(ctx, () =>
          handler(message, {
            attempt: job.attempt,
            queueName: ValidQueueName.parse(job.queueName),
            messageId: MessageId.parse(messageId),
            requestId: req.headers.get('x-request-id') ?? undefined,
          })
        );
      } catch (error) {
        // Reschedule with backoff for the durable path; surface 500 so the
        // dispatcher (or an external pump) retries.
        if (msgId) {
          const nextAttempt = job.attempt + 1;
          if (nextAttempt <= config.maxAttempts) {
            await redis.hset(keys.job(msgId), 'attempt', String(nextAttempt));
            await scheduleJob(msgId, Date.now() + backoffMs(nextAttempt, config.retryBaseMs));
          } else {
            console.error(
              `[world-redirect] job ${msgId} (${job.queueName}) exhausted ${config.maxAttempts} attempts; dropping`,
              { runId }
            );
            await deleteJob(msgId);
          }
        }
        return Response.json(String(error), { status: 500 });
      }

      // Re-delivery requested (sleep / wait / long step): durable reschedule.
      const timeoutSeconds =
        typeof result === 'object' && result !== null
          ? (result as { timeoutSeconds?: unknown }).timeoutSeconds
          : undefined;
      if (typeof timeoutSeconds === 'number') {
        const seconds = Math.max(0, timeoutSeconds);
        if (msgId) await scheduleJob(msgId, Date.now() + seconds * 1000);
        return Response.json({ timeoutSeconds: seconds });
      }

      // Delivery complete.
      if (msgId) await deleteJob(msgId);

      // Promote any extra immediate self-enqueues (e.g. parallel fan-out)
      // to fire now so the dispatcher handles them promptly.
      const [next, ...rest] = ctx.immediate;
      for (const r of rest) await scheduleJob(r.messageId, Date.now());
      if (rest.length && wake) wake();

      // 307 trampoline: continue the primary self-continuation in this chain.
      if (next && hop + 1 < MAX_HOPS) {
        // Claim it for this redirect chain (it was scheduled with a grace
        // window, so the dispatcher hasn't taken it).
        await redis.zrem(keys.schedule(), next.messageId);
        const location = `${config.baseUrl}/.well-known/workflow/v1/${next.route}?msg=${encodeURIComponent(next.messageId)}&hop=${hop + 1}`;
        return new Response(null, { status: 307, headers: { location } });
      }
      // Hop limit reached (or nothing to continue): promote the primary job to
      // fire now and let the dispatcher pick it up.
      if (next) {
        await scheduleJob(next.messageId, Date.now());
        if (wake) wake();
      }
      return Response.json({ ok: true });
    };
  };

  const getDeploymentId: Queue['getDeploymentId'] = async () =>
    config.deploymentId;

  // ---------------- dispatcher (the 307 pump) ----------------

  async function dispatchOne(messageId: string): Promise<void> {
    const job = await readJob(messageId);
    if (!job) return;
    const target = `${config.baseUrl}/.well-known/workflow/v1/${job.route}?msg=${encodeURIComponent(messageId)}`;
    try {
      const res = await fetch(target, {
        method: 'POST',
        redirect: 'follow',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': job.queueName,
          'x-vqs-message-id': messageId,
          'x-vqs-message-attempt': String(job.attempt),
        },
        body: '',
      });
      // The handler wrapper owns its own job's retry/timeout/completion
      // lifecycle (it has the durable msgId). Log every non-2xx so failures
      // between dispatcher and handler are never silent — a 404 on the flow
      // route, a 400 from a queue-name mismatch, etc.
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        console.error(
          `[world-redirect] dispatch ${messageId} (${job.queueName}) -> HTTP ${res.status} ${res.url}: ${text.slice(0, 300)}`
        );
      }
    } catch (err) {
      // True network error: the wrapper never ran, so nobody rescheduled.
      // Reschedule with backoff so the job is not lost.
      const nextAttempt = job.attempt + 1;
      if (nextAttempt <= config.maxAttempts) {
        await redis.hset(keys.job(messageId), 'attempt', String(nextAttempt));
        await scheduleJob(
          messageId,
          Date.now() + backoffMs(nextAttempt, config.retryBaseMs)
        );
      } else {
        await deleteJob(messageId);
      }
      console.error(`[world-redirect] dispatch ${messageId} network error:`, err);
    }
  }

  async function drainOnce(): Promise<number> {
    const now = Date.now();
    const capacity = MAX_CONCURRENCY - inFlight;
    if (capacity <= 0) return 0;
    const due = await redis.zrangeByScore(keys.schedule(), 0, now, {
      offset: 0,
      count: capacity,
    });
    let dispatched = 0;
    const tasks: Promise<void>[] = [];
    for (const messageId of due) {
      // Atomic claim: only the dispatcher that removes it proceeds.
      const removed = await redis.zrem(keys.schedule(), messageId);
      if (removed === 0) continue;
      inFlight++;
      dispatched++;
      tasks.push(
        dispatchOne(messageId).finally(() => {
          inFlight--;
        })
      );
    }
    // Don't await tasks fully — let them run concurrently; just kick them off.
    void Promise.allSettled(tasks);
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
        console.error('[world-redirect] dispatcher tick error:', err);
      }
      if (!running) return;
      await new Promise<void>((resolve) => {
        wake = () => {
          wake = null;
          resolve();
        };
        dispatcherTimer = setTimeout(() => {
          wake = null;
          resolve();
        }, config.dispatcherPollMs);
      });
      void tick();
    };
    void tick();
  }

  async function stopDispatcher(): Promise<void> {
    running = false;
    if (dispatcherTimer) clearTimeout(dispatcherTimer);
    if (wake) wake();
    // Allow in-flight dispatches a brief moment to settle.
    const deadline = Date.now() + 2000;
    while (inFlight > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  async function waitForIdle(timeoutMs = 15_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const pending = await redis.zcard(keys.schedule());
      if (pending === 0 && inFlight === 0) return;
      await new Promise((r) => setTimeout(r, 25));
    }
  }

  return {
    queue,
    createQueueHandler,
    getDeploymentId,
    startDispatcher,
    stopDispatcher,
    drainOnce,
    waitForIdle,
  };
}
