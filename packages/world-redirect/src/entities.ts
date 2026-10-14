import type {
  Event,
  Hook,
  PaginationOptions,
  Step,
  Wait,
  WorkflowRun,
} from '@workflow/world';
import {
  EventSchema,
  eventIdToSlot,
  HookSchema,
  StepSchema,
  slotToEventId,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import type { RedisClient } from './client/types.js';
import { decodeBlob, encodeBlob } from './codec.js';
import type { Keys } from './keys.js';

export interface Page<T> {
  data: T[];
  cursor: string | null;
  hasMore: boolean;
}

function encodeCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ o: offset })).toString('base64url');
}

function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    return typeof parsed?.o === 'number' && parsed.o >= 0 ? parsed.o : 0;
  } catch {
    return 0;
  }
}

/**
 * Appends one event to a run's log and returns the id it landed on. Event ids
 * are positions in the run's log (slot 1, 2, 3, ...), so the slot is chosen by
 * the same atomic script that commits the event: Redis runs a script without
 * interleaving any other command, which makes the slot unique (no two appends
 * read the same length), dense (each append takes the next index) and
 * allocated at the commit (nothing can land behind a slot a reader has
 * passed). The event blob is stored without its id, which is the hash field
 * it is stored under.
 *
 * KEYS: [eventList, eventData, corrIndex]
 * ARGV: [idPrefix, idDigits, blob, hasCorrelationId]
 */
const APPEND_EVENT_SCRIPT = `-- world-redirect:append-event
local slot = redis.call('LLEN', KEYS[1]) + 1
local digits = tostring(slot)
local id = ARGV[1] .. string.rep('0', tonumber(ARGV[2]) - #digits) .. digits
redis.call('HSET', KEYS[2], id, ARGV[3])
redis.call('RPUSH', KEYS[1], id)
if ARGV[4] == '1' then
  redis.call('RPUSH', KEYS[3], id)
end
return id`;

// Derived from the SDK's own formatter so the script pads exactly as
// `slotToEventId()` does; `appendEvent` re-checks every id it gets back.
const EVENT_ID_PREFIX = slotToEventId(1).replace(/0*1$/, '');
const EVENT_ID_DIGITS = slotToEventId(1).length - EVENT_ID_PREFIX.length;

/** An event as built before the store assigns its slot. */
export type UnslottedEvent = Omit<Event, 'eventId'>;

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

function clampLimit(limit: number | undefined, fallback = DEFAULT_LIMIT) {
  if (!limit || limit <= 0) return fallback;
  return Math.min(limit, MAX_LIMIT);
}

/**
 * Low-level entity persistence + secondary indexes over Redis. Stateless;
 * all mutation ordering/atomicity is handled by the caller (RunLocks) in
 * storage.ts. A single global monotonic sequence supplies stable total
 * ordering for index ZSETs.
 */
export class EntityStore {
  constructor(
    private readonly redis: RedisClient,
    private readonly keys: Keys
  ) {}

  // Index scores use the entity's immutable createdAt timestamp, so
  // re-writing an entity (status updates) keeps its position stable. ULID
  // ids break same-millisecond ties in creation order.

  // ---------------- runs ----------------

  async getRun(runId: string): Promise<WorkflowRun | null> {
    const raw = await this.redis.get(this.keys.run(runId));
    if (!raw) return null;
    return WorkflowRunSchema.parse(decodeBlob(raw));
  }

  async writeRun(run: WorkflowRun, prevStatus?: string): Promise<void> {
    const score = run.createdAt.getTime();
    await this.redis.set(this.keys.run(run.runId), encodeBlob(run));
    await this.redis.zadd(this.keys.runsIndex(), score, run.runId);
    if (prevStatus && prevStatus !== run.status) {
      await this.redis.zrem(this.keys.runsByStatus(prevStatus), run.runId);
    }
    await this.redis.zadd(this.keys.runsByStatus(run.status), score, run.runId);
  }

  async listRuns(params: {
    status?: string;
    pagination?: PaginationOptions;
  }): Promise<Page<WorkflowRun>> {
    const key = params.status
      ? this.keys.runsByStatus(params.status)
      : this.keys.runsIndex();
    const ids = await this.pageIds(key, params.pagination, 'desc');
    const runs = await Promise.all(ids.data.map((id) => this.getRun(id)));
    return { ...ids, data: runs.filter((r): r is WorkflowRun => r !== null) };
  }

  /**
   * Runs whose status is any of `statuses`, newest first. Merges the
   * per-status indexes into one sorted page; the cursor is the offset into
   * that merged order, like every other listing here.
   */
  async listRunsByStatuses(params: {
    statuses: string[];
    pagination?: PaginationOptions;
  }): Promise<Page<WorkflowRun>> {
    const statuses = [...new Set(params.statuses)];
    if (statuses.length === 0) return { data: [], cursor: null, hasMore: false };
    if (statuses.length === 1) {
      return this.listRuns({ status: statuses[0], pagination: params.pagination });
    }
    const limit = clampLimit(params.pagination?.limit);
    const offset = decodeCursor(params.pagination?.cursor);
    const asc = params.pagination?.sortOrder === 'asc';
    const ids = (
      await Promise.all(
        statuses.map((s) =>
          this.redis.zrangeByRank(this.keys.runsByStatus(s), 0, -1)
        )
      )
    ).flat();
    const runs = (await Promise.all(ids.map((id) => this.getRun(id)))).filter(
      (r): r is WorkflowRun => r !== null && statuses.includes(r.status)
    );
    runs.sort((a, b) => {
      const d = a.createdAt.getTime() - b.createdAt.getTime() ||
        a.runId.localeCompare(b.runId);
      return asc ? d : -d;
    });
    const data = runs.slice(offset, offset + limit);
    const consumed = offset + data.length;
    const hasMore = consumed < runs.length;
    return { data, cursor: hasMore ? encodeCursor(consumed) : null, hasMore };
  }

  // ---------------- steps ----------------

  async getStep(runId: string, stepId: string): Promise<Step | null> {
    const raw = await this.redis.get(this.keys.step(runId, stepId));
    if (!raw) return null;
    return StepSchema.parse(decodeBlob(raw));
  }

  async writeStep(step: Step): Promise<void> {
    const score = step.createdAt.getTime();
    await this.redis.set(
      this.keys.step(step.runId, step.stepId),
      encodeBlob(step)
    );
    await this.redis.zadd(this.keys.stepsIndex(step.runId), score, step.stepId);
  }

  async listSteps(params: {
    runId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Step>> {
    const ids = await this.pageIds(
      this.keys.stepsIndex(params.runId),
      params.pagination,
      'desc'
    );
    const steps = await Promise.all(
      ids.data.map((id) => this.getStep(params.runId, id))
    );
    return { ...ids, data: steps.filter((s): s is Step => s !== null) };
  }

  // ---------------- hooks ----------------

  async getHook(hookId: string): Promise<Hook | null> {
    const raw = await this.redis.get(this.keys.hook(hookId));
    if (!raw) return null;
    return HookSchema.parse(decodeBlob(raw)) as Hook;
  }

  async writeHook(hook: Hook): Promise<void> {
    const score = hook.createdAt.getTime();
    await this.redis.set(this.keys.hook(hook.hookId), encodeBlob(hook));
    await this.redis.zadd(this.keys.hooksIndex(), score, hook.hookId);
    await this.redis.zadd(this.keys.hooksByRun(hook.runId), score, hook.hookId);
  }

  async deleteHook(hook: Hook, tokenHash: string): Promise<void> {
    await this.redis.del(this.keys.hook(hook.hookId));
    await this.redis.zrem(this.keys.hooksIndex(), hook.hookId);
    await this.redis.zrem(this.keys.hooksByRun(hook.runId), hook.hookId);
    await this.redis.del(this.keys.hookToken(tokenHash));
  }

  async listHooks(params: {
    runId?: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Hook>> {
    const key = params.runId
      ? this.keys.hooksByRun(params.runId)
      : this.keys.hooksIndex();
    const ids = await this.pageIds(key, params.pagination, 'desc');
    const hooks = await Promise.all(ids.data.map((id) => this.getHook(id)));
    return { ...ids, data: hooks.filter((h): h is Hook => h !== null) };
  }

  // ---------------- waits ----------------

  async getWait(runId: string, correlationId: string): Promise<Wait | null> {
    const raw = await this.redis.get(this.keys.wait(runId, correlationId));
    if (!raw) return null;
    return WaitSchema.parse(decodeBlob(raw));
  }

  async writeWait(wait: Wait, correlationId: string): Promise<void> {
    const score = wait.createdAt.getTime();
    await this.redis.set(
      this.keys.wait(wait.runId, correlationId),
      encodeBlob(wait)
    );
    await this.redis.zadd(
      this.keys.waitsByRun(wait.runId),
      score,
      correlationId
    );
  }

  // ---------------- terminal cleanup ----------------

  async deleteAllHooksForRun(runId: string): Promise<void> {
    const ids = await this.redis.zrangeByRank(
      this.keys.hooksByRun(runId),
      0,
      -1
    );
    for (const id of ids) {
      const hook = await this.getHook(id);
      if (hook) {
        const { hashToken } = await import('./ids.js');
        await this.deleteHook(hook, hashToken(hook.token));
      }
    }
    await this.redis.del(this.keys.hooksByRun(runId));
  }

  async deleteAllWaitsForRun(runId: string): Promise<void> {
    const corrIds = await this.redis.zrangeByRank(
      this.keys.waitsByRun(runId),
      0,
      -1
    );
    for (const cid of corrIds) {
      await this.redis.del(this.keys.wait(runId, cid));
    }
    await this.redis.del(this.keys.waitsByRun(runId));
  }

  // ---------------- events ----------------

  /** Commits `event` at the run's next free slot and returns it with its id. */
  async appendEvent(event: UnslottedEvent): Promise<Event> {
    const { eventId: _ignored, ...blob } = event as Event;
    const correlationId = event.correlationId ?? '';
    const id = await this.redis.eval(
      APPEND_EVENT_SCRIPT,
      [
        this.keys.eventList(event.runId),
        this.keys.eventData(event.runId),
        this.keys.corrIndex(event.runId, correlationId || '_'),
      ],
      [
        EVENT_ID_PREFIX,
        String(EVENT_ID_DIGITS),
        encodeBlob(blob),
        correlationId ? '1' : '0',
      ]
    );
    const slot = typeof id === 'string' ? eventIdToSlot(id) : null;
    if (slot === null || slotToEventId(slot) !== id) {
      throw new Error(
        `[world-redirect] append-event script returned ${String(id)}, which is not a canonical event id`
      );
    }
    return { ...event, eventId: id } as Event;
  }

  async getEvent(runId: string, eventId: string): Promise<Event | null> {
    const raw = await this.redis.hget(this.keys.eventData(runId), eventId);
    if (!raw) return null;
    return EventSchema.parse({
      ...decodeBlob<Record<string, unknown>>(raw),
      eventId,
    });
  }

  async listEvents(params: {
    runId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Event>> {
    const listKey = this.keys.eventList(params.runId);
    const total = await this.redis.llen(listKey);
    const limit = clampLimit(params.pagination?.limit, MAX_LIMIT);
    const offset = decodeCursor(params.pagination?.cursor);
    const sortOrder = params.pagination?.sortOrder ?? 'asc';

    let ids: string[];
    if (sortOrder === 'asc') {
      ids = await this.redis.lrange(listKey, offset, offset + limit - 1);
    } else {
      // newest first: walk from the end
      const start = Math.max(0, total - offset - limit);
      const end = total - offset - 1;
      ids = end < 0 ? [] : await this.redis.lrange(listKey, start, end);
      ids.reverse();
    }

    const events = await Promise.all(
      ids.map((id) => this.getEvent(params.runId, id))
    );
    const data = events.filter((e): e is Event => e !== null);
    const consumed = offset + ids.length;
    const hasMore = consumed < total;
    // The event log always returns a resumable tail cursor when there are
    // events, so the runtime can incrementally fetch events appended later
    // (not just the currently-available pages). This differs from standard
    // entity pagination, which returns null at the end.
    return {
      data,
      cursor: total === 0 ? null : encodeCursor(consumed),
      hasMore,
    };
  }

  async listEventsByCorrelationId(params: {
    runId: string;
    correlationId: string;
    pagination?: PaginationOptions;
  }): Promise<Page<Event>> {
    const listKey = this.keys.corrIndex(params.runId, params.correlationId);
    const refs = await this.redis.lrange(listKey, 0, -1);
    const limit = clampLimit(params.pagination?.limit, MAX_LIMIT);
    const offset = decodeCursor(params.pagination?.cursor);
    const ordered =
      (params.pagination?.sortOrder ?? 'asc') === 'asc'
        ? refs
        : [...refs].reverse();
    const slice = ordered.slice(offset, offset + limit);
    const events = await Promise.all(
      slice.map((eventId) => this.getEvent(params.runId, eventId))
    );
    const data = events.filter((e): e is Event => e !== null);
    const consumed = offset + slice.length;
    const hasMore = consumed < ordered.length;
    return {
      data,
      cursor: ordered.length === 0 ? null : encodeCursor(consumed),
      hasMore,
    };
  }

  // ---------------- helpers ----------------

  private async pageIds(
    zsetKey: string,
    pagination: PaginationOptions | undefined,
    defaultOrder: 'asc' | 'desc'
  ): Promise<Page<string>> {
    const total = await this.redis.zcard(zsetKey);
    const limit = clampLimit(pagination?.limit);
    const offset = decodeCursor(pagination?.cursor);
    const rev = (pagination?.sortOrder ?? defaultOrder) === 'desc';
    const data = await this.redis.zrangeByRank(
      zsetKey,
      offset,
      offset + limit - 1,
      rev
    );
    const consumed = offset + data.length;
    const hasMore = consumed < total;
    return { data, cursor: hasMore ? encodeCursor(consumed) : null, hasMore };
  }
}
