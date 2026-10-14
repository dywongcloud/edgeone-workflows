/**
 * Event-sourced storage layer for the EdgeOne KV-backed world.
 *
 * Same semantics as world-redirect / world-sheets — every state transition
 * is recorded as an immutable event, and the entity blob is updated in the
 * same logical operation. KV gives us point lookups (no row scans), but no
 * cross-key transactions, so we serialise per-run writes with an in-process
 * RunMutex and document the cross-instance-race caveat in the README.
 *
 * Indexes are realised as zero-value "presence" keys with shapes designed so
 * `list({prefix})` returns the rows we want already sorted:
 *
 *   {p}/idx-run-status/<status>/<runId>   → presence  (runId sorts by ULID)
 *   {p}/idx-step/<runId>/<stepId>         → presence
 *   {p}/evt/<runId>/<eventId>             → CBOR(event)        (eventId = slot, sorts by position)
 *
 * Event ids are positions in the run's log (`slotToEventId(1)`, 2, 3, ...).
 * An append claims the next slot with `put({ifNotExists: true})` and, if the
 * slot is taken, moves to the next one, so slots stay unique and dense only
 * when the backend enforces put-if-absent atomically. Where it does not, two
 * hosts can claim the same slot: run a single dispatcher, as for every other
 * cross-instance race in this World.
 *   {p}/idx-hook-run/<runId>/<hookId>     → presence
 *
 * Hook token claim:
 *   {p}/tok/<sha256(token)>               → hookId
 * Created with `put({ifNotExists: true})`. Losers emit a `hook_conflict`
 * event identical to world-redirect's flow.
 */

import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import { globalSingleton } from '@workflow/utils';
import type {
  CreateEventParams,
  Event,
  EventResult,
  Hook,
  PaginationOptions,
  Step,
  Storage,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  applyAttributeChanges,
  eventIdToSlot,
  FIRST_EVENT_SLOT,
  isChildEntityCreationEvent,
  SPEC_VERSION_SUPPORTS_SLOT_IDENTITY,
  slotToEventId,
  stripEventDataRefs,
  validateAttributeChanges,
  validateUlidTimestamp,
} from '@workflow/world';
import {
  assertSafeEntityId,
  decodeBlob,
  encodeBlob,
  hashToken,
  newRunId,
} from './codec.js';
import type { Keys } from './keys.js';
import type { KV } from './types.js';

const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
const isTerminal = (s: string) => TERMINAL.has(s);

class RunMutex {
  private inflight = new Map<string, Promise<unknown>>();
  async run<T>(runId: string, fn: () => Promise<T>): Promise<T> {
    const prev = this.inflight.get(runId);
    const box: { task?: Promise<T> } = {};
    const task = (async () => {
      if (prev) await prev.catch(() => undefined);
      try {
        return await fn();
      } finally {
        if (this.inflight.get(runId) === box.task) this.inflight.delete(runId);
      }
    })();
    box.task = task;
    this.inflight.set(runId, task);
    return task;
  }
}

/** An event as built before it is assigned its slot. */
type UnslottedEvent = Omit<Event, 'eventId'>;

/**
 * Highest slot this process has committed per run, keyed by the run's event
 * prefix. A starting point for the next append, never the authority: the
 * `ifNotExists` claim decides. Held on globalThis so module copies a bundler
 * creates share it.
 */
const slotHints = globalSingleton('@open-workflow/world-edgeone//kvSlotHints', 1, () => ({
  byRun: new Map<string, number>(),
}));

/** The longest run of slots 1, 2, 3, ... at the start of a sorted log. */
function densePrefix(events: Event[]): Event[] {
  let n = 0;
  while (
    n < events.length &&
    eventIdToSlot(events[n]!.eventId) === FIRST_EVENT_SLOT + n
  ) {
    n++;
  }
  return n === events.length ? events : events.slice(0, n);
}

type StoredRun = WorkflowRun;
type StoredStep = Step;

async function listAll(
  kv: KV,
  prefix: string,
  limit = 1000
): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | undefined;
  do {
    const listOpts: { prefix: string; cursor?: string; limit?: number } = {
      prefix,
      limit,
    };
    if (cursor) listOpts.cursor = cursor;
    const page = await kv.list(listOpts);
    out.push(...page.keys);
    cursor = page.complete ? undefined : page.cursor;
  } while (cursor);
  return out;
}

async function getBlob<T>(kv: KV, key: string): Promise<T | null> {
  const s = await kv.get(key);
  if (s == null) return null;
  return decodeBlob<T>(s) ?? null;
}

async function putBlob(kv: KV, key: string, value: unknown): Promise<void> {
  await kv.put(key, encodeBlob(value));
}

function stripRunData(run: WorkflowRun): WorkflowRunWithoutData {
  return {
    ...run,
    input: undefined,
    output: undefined,
    dynamicWorkflowCode: undefined,
  } as WorkflowRunWithoutData;
}

function paginateBy<T>(
  items: T[],
  pagination: PaginationOptions | undefined,
  defaultOrder: 'asc' | 'desc' = 'desc'
): { data: T[]; cursor: string | null; hasMore: boolean } {
  const order = pagination?.sortOrder ?? defaultOrder;
  const ordered = order === 'desc' ? [...items].reverse() : items;
  const limit = Math.min(Math.max(1, pagination?.limit ?? 100), 1000);
  const offset = pagination?.cursor
    ? Number.parseInt(
        Buffer.from(pagination.cursor, 'base64url').toString(),
        10
      ) || 0
    : 0;
  const page = ordered.slice(offset, offset + limit);
  const consumed = offset + page.length;
  const hasMore = consumed < ordered.length;
  return {
    data: page,
    cursor: hasMore
      ? Buffer.from(String(consumed)).toString('base64url')
      : null,
    hasMore,
  };
}

export interface KVStorage extends Storage {
  readonly raw: {
    runsForStatus: (status: WorkflowRun['status']) => Promise<WorkflowRun[]>;
  };
}

export function createKVStorage(
  kv: KV,
  keys: Keys,
  owner: { ownerId: string; projectId: string; environment: string },
  specVersion: number
): KVStorage {
  const mutex = new RunMutex();

  async function readRun(runId: string): Promise<StoredRun | null> {
    return getBlob<StoredRun>(kv, keys.runBlob(runId));
  }

  async function writeRun(run: StoredRun, prevStatus?: string): Promise<void> {
    // Maintain status index — drop old, set new.
    if (prevStatus && prevStatus !== run.status) {
      await kv.delete(keys.runStatusIdx(prevStatus, run.runId));
    }
    await putBlob(kv, keys.runBlob(run.runId), run);
    await kv.put(keys.runStatusIdx(run.status, run.runId), '');
  }

  async function readStep(runId: string, stepId: string): Promise<StoredStep | null> {
    return getBlob<StoredStep>(kv, keys.stepBlob(runId, stepId));
  }

  async function writeStep(step: StoredStep): Promise<void> {
    await putBlob(kv, keys.stepBlob(step.runId, step.stepId), step);
    await kv.put(keys.stepIdx(step.runId, step.stepId), '');
  }

  /** Commits `event` at the run's next free slot and returns it with its id. */
  async function appendEvent(event: UnslottedEvent): Promise<Event> {
    const prefix = keys.evtPrefix(event.runId);
    let slot =
      (slotHints.byRun.get(prefix) ??
        (await listAll(kv, prefix)).length) + 1;
    // Each failed claim proves the slot is taken, so moving up one never
    // skips a free slot. The bound only stops a broken adapter looping.
    for (let attempt = 0; attempt < 10_000; attempt++, slot++) {
      const eventId = slotToEventId(slot);
      const committed = { ...event, eventId } as Event;
      const { ok } = await kv.put(
        keys.evtBlob(event.runId, eventId),
        encodeBlob(committed),
        { ifNotExists: true }
      );
      if (ok) {
        slotHints.byRun.set(prefix, slot);
        return committed;
      }
    }
    throw new WorkflowWorldError(
      `Could not claim an event slot for run "${event.runId}"`
    );
  }

  /** The run's log in slot order, cut at the first slot not yet visible. */
  async function readLog(runId: string): Promise<Event[]> {
    const evtKeys = await listAll(kv, keys.evtPrefix(runId));
    const items: Event[] = [];
    for (const k of evtKeys) {
      const e = await getBlob<Event>(kv, k);
      if (e) items.push(e);
    }
    items.sort((a, b) => (a.eventId < b.eventId ? -1 : a.eventId > b.eventId ? 1 : 0));
    // An eventually consistent list can show slot n+1 before slot n. A reader
    // must get a prefix of the log, so stop at the first missing slot; the
    // rest arrives on a later read.
    return densePrefix(items);
  }

  async function readHook(hookId: string): Promise<Hook | null> {
    return getBlob<Hook>(kv, keys.hookBlob(hookId));
  }

  async function readWait(runId: string, correlationId: string): Promise<Wait | null> {
    return getBlob<Wait>(kv, keys.waitBlob(runId, correlationId));
  }

  async function createEvent(
    runId: string | null,
    data: any,
    params?: CreateEventParams
  ): Promise<EventResult> {
    if (runId != null && runId !== '') assertSafeEntityId('runId', runId);
    if (typeof data.correlationId === 'string') {
      assertSafeEntityId('correlationId', data.correlationId);
    }
    let effectiveRunId: string;
    if (data.eventType === 'run_created' && (!runId || runId === '')) {
      effectiveRunId = newRunId();
    } else if (!runId) {
      throw new Error('runId is required for non-run_created events');
    } else {
      effectiveRunId = runId;
    }
    if (data.eventType === 'run_created' && runId && runId !== '') {
      const err = validateUlidTimestamp(effectiveRunId, 'wrun_');
      if (err) throw new WorkflowWorldError(err);
    }
    const result = await mutex.run(effectiveRunId, () =>
      createImpl(effectiveRunId, data, params)
    );
    if (params?.eventCount === undefined) return result;
    return reportSkippedSlots(result, params.eventCount);
  }

  /**
   * Bump-and-report: when a write expecting slot `askedFor + 1` landed
   * higher, return the events in the slots it skipped. No cursor, since the
   * report is not a page that moves the caller's read position.
   */
  async function reportSkippedSlots(
    result: EventResult,
    askedFor: number
  ): Promise<EventResult> {
    const committed = result.event ? eventIdToSlot(result.event.eventId) : null;
    if (
      !result.event ||
      committed === null ||
      askedFor < FIRST_EVENT_SLOT ||
      committed <= askedFor + 1
    ) {
      return result;
    }
    const span = committed - askedFor - 1;
    const log = await readLog(result.event.runId);
    const events = log.slice(askedFor, askedFor + span);
    return { ...result, events, cursor: null, hasMore: events.length < span };
  }

  async function createImpl(
    effectiveRunId: string,
    data: any,
    params?: CreateEventParams
  ): Promise<EventResult> {
    const now = new Date();
    const resolveData =
      params?.resolveData === 'none' ? 'none' : ('all' as const);
    const effectiveSpecVersion: number = data.specVersion ?? specVersion;

    let currentRun: WorkflowRun | null = null;
    const skipRunRead =
      data.eventType === 'step_completed' || data.eventType === 'step_retrying';
    if (data.eventType !== 'run_created' && !skipRunRead) {
      currentRun = await readRun(effectiveRunId);
    }
    if (data.eventType === 'run_failed' && !currentRun) {
      throw new WorkflowRunNotFoundError(effectiveRunId);
    }
    // Runs created before slot-numbered event ids hold ULID-numbered logs,
    // which the runtime cannot replay.
    if (
      currentRun &&
      (currentRun.specVersion ?? 0) < SPEC_VERSION_SUPPORTS_SLOT_IDENTITY
    ) {
      throw new WorkflowWorldError(
        `world-edgeone/kv cannot append to run "${effectiveRunId}": it was created at spec version ${currentRun.specVersion}, before slot-numbered event ids (${SPEC_VERSION_SUPPORTS_SLOT_IDENTITY}), and its log cannot be replayed by this runtime`
      );
    }

    if (currentRun && isTerminal(currentRun.status)) {
      if (data.eventType === 'run_cancelled' && currentRun.status === 'cancelled') {
        const event = await appendEvent(
          mkEvent(data, effectiveRunId, now, effectiveSpecVersion)
        );
        return { event: stripEventDataRefs(event, resolveData), run: currentRun };
      }
      if (data.eventType === 'run_started') {
        throw new RunExpiredError(
          `Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
        );
      }
      if (
        ['run_started', 'run_completed', 'run_failed', 'run_cancelled'].includes(
          data.eventType
        )
      ) {
        throw new EntityConflictError(
          `Cannot transition run from terminal state "${currentRun.status}"`
        );
      }
      // A lazy step_started creates its step, so it counts as a creation.
      if (isChildEntityCreationEvent(data)) {
        throw new EntityConflictError(
          `Cannot create new entities on run in terminal state "${currentRun.status}"`
        );
      }
      if (data.eventType === 'attr_set') {
        throw new EntityConflictError(
          `Cannot set attributes on run in terminal state "${currentRun.status}"`
        );
      }
    }

    // Lazy step start: a step_started carrying the step's name and input
    // creates the step itself. Whoever creates it owns running its body, so
    // a lazy start for a step that already exists lost that race.
    const lazyStepStart =
      data.eventType === 'step_started' && isChildEntityCreationEvent(data);

    let validatedStep: Step | null = null;
    if (
      ['step_started', 'step_completed', 'step_failed', 'step_retrying'].includes(
        data.eventType
      ) &&
      data.correlationId
    ) {
      validatedStep = await readStep(effectiveRunId, data.correlationId);
      if (lazyStepStart && validatedStep) {
        throw new EntityConflictError(
          `Step "${data.correlationId}" already created`
        );
      }
      if (!validatedStep && !lazyStepStart) {
        throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
      }
      if (validatedStep && isTerminal(validatedStep.status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${validatedStep.status}"`
        );
      }
      // No work begins on a finished run, even for a step that still reads
      // `running` (a redelivered start). An in-flight step may still record
      // its outcome.
      if (validatedStep && currentRun && isTerminal(currentRun.status)) {
        if (
          data.eventType === 'step_started' ||
          validatedStep.status !== 'running'
        ) {
          throw new RunExpiredError(
            `Cannot ${data.eventType === 'step_started' ? 'start' : 'modify non-running'} step on run in terminal state "${currentRun.status}"`
          );
        }
      }
    }

    if (
      (data.eventType === 'hook_disposed' || data.eventType === 'hook_received') &&
      data.correlationId
    ) {
      const h = await readHook(data.correlationId);
      if (!h) throw new HookNotFoundError(data.correlationId);
    }

    const event = mkEvent(data, effectiveRunId, now, effectiveSpecVersion);
    if (data.eventType === 'run_started' && 'eventData' in event) {
      delete (event as any).eventData;
    }

    let run: WorkflowRun | undefined;
    let step: Step | undefined;
    let hook: Hook | undefined;
    let wait: Wait | undefined;
    let stepCreated = false;

    switch (data.eventType) {
      case 'run_created': {
        const rd = data.eventData;
        if (await readRun(effectiveRunId)) {
          throw new EntityConflictError(
            `Workflow run "${effectiveRunId}" already exists`
          );
        }
        run = {
          runId: effectiveRunId,
          deploymentId: rd.deploymentId,
          status: 'pending',
          workflowName: rd.workflowName,
          specVersion: effectiveSpecVersion,
          executionContext: rd.executionContext,
          input: rd.input,
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          // Carries the reserved lineage attributes ($parentRunId,
          // $rootRunId) a child run is started with.
          attributes: rd.attributes ?? {},
          createdAt: now,
          updatedAt: now,
        };
        await writeRun(run);
        break;
      }
      case 'run_started': {
        if (currentRun) {
          if (currentRun.status === 'running') return { run: currentRun };
          run = {
            ...currentRun,
            status: 'running',
            startedAt: currentRun.startedAt ?? now,
            completedAt: undefined,
            error: undefined,
            output: undefined,
            updatedAt: now,
          };
          await writeRun(run, currentRun.status);
        }
        break;
      }
      case 'run_completed':
      case 'run_failed':
      case 'run_cancelled': {
        if (currentRun) {
          const next: any = {
            ...currentRun,
            status:
              data.eventType === 'run_completed'
                ? 'completed'
                : data.eventType === 'run_failed'
                  ? 'failed'
                  : 'cancelled',
            completedAt: now,
            updatedAt: now,
          };
          if (data.eventType === 'run_completed') {
            next.output = data.eventData?.output;
            next.error = undefined;
          } else if (data.eventType === 'run_failed') {
            next.error = data.eventData.error;
            next.errorCode = data.eventData.errorCode;
            next.output = undefined;
          } else {
            next.output = undefined;
            next.error = undefined;
          }
          run = next as WorkflowRun;
          await writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'step_created': {
        if (await readStep(effectiveRunId, data.correlationId)) {
          throw new EntityConflictError(
            `Step "${data.correlationId}" already created`
          );
        }
        step = {
          runId: effectiveRunId,
          stepId: data.correlationId,
          stepName: data.eventData.stepName,
          status: 'pending',
          input: data.eventData.input,
          output: undefined,
          error: undefined,
          attempt: 0,
          startedAt: undefined,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
          specVersion: effectiveSpecVersion,
        };
        await writeStep(step);
        break;
      }
      case 'step_started': {
        if (lazyStepStart && !validatedStep) {
          const lazyData = (data as any).eventData as {
            stepName: string;
            input: unknown;
          } & Record<string, unknown>;
          validatedStep = {
            runId: effectiveRunId,
            stepId: data.correlationId,
            stepName: lazyData.stepName,
            status: 'pending',
            input: lazyData.input,
            output: undefined,
            error: undefined,
            attempt: 0,
            startedAt: undefined,
            completedAt: undefined,
            createdAt: now,
            updatedAt: now,
            specVersion: effectiveSpecVersion,
          } as Step;
          await writeStep(validatedStep);
          // Replay has to see step_created before step_started.
          await appendEvent({
            eventType: 'step_created',
            runId: effectiveRunId,
            correlationId: data.correlationId,
            createdAt: now,
            specVersion: effectiveSpecVersion,
            eventData: { stepName: lazyData.stepName, input: lazyData.input },
          } as UnslottedEvent);
          stepCreated = true;
          const { stepName: _n, input: _i, ...rest } = lazyData;
          (event as any).eventData = rest;
        }
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'running',
            attempt: validatedStep.attempt + 1,
            startedAt: validatedStep.startedAt ?? now,
            retryAfter: undefined,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_completed': {
        validatedStep ??= await readStep(effectiveRunId, data.correlationId);
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError(
              'Cannot modify step in terminal state'
            );
          }
          step = {
            ...validatedStep,
            status: 'completed',
            output: data.eventData.result,
            completedAt: now,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_failed': {
        validatedStep ??= await readStep(effectiveRunId, data.correlationId);
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError(
              'Cannot modify step in terminal state'
            );
          }
          step = {
            ...validatedStep,
            status: 'failed',
            error: data.eventData.error,
            completedAt: now,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'step_retrying': {
        validatedStep ??= await readStep(effectiveRunId, data.correlationId);
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'pending',
            error: data.eventData.error,
            retryAfter: data.eventData.retryAfter,
            updatedAt: now,
          };
          await writeStep(step);
        }
        break;
      }
      case 'hook_created': {
        const hd = data.eventData;
        const tokenHash = hashToken(hd.token);
        // NX-claim. EdgeOne KV has no atomic put-if-absent so the adapter
        // does a check-then-put — losers fall through to the conflict event,
        // matching world-redirect / world-sheets semantics.
        const claim = await kv.put(
          keys.tokenBlob(tokenHash),
          data.correlationId,
          { ifNotExists: true }
        );
        if (!claim.ok) {
          const conflictingHookId = await kv.get(keys.tokenBlob(tokenHash));
          let conflictingRunId: string | undefined;
          if (conflictingHookId) {
            const existing = await readHook(conflictingHookId);
            conflictingRunId = existing?.runId;
          }
          const conflictEvent = await appendEvent({
            eventType: 'hook_conflict',
            correlationId: data.correlationId,
            runId: effectiveRunId,
            createdAt: now,
            specVersion: effectiveSpecVersion,
            eventData: {
              token: hd.token,
              ...(conflictingRunId ? { conflictingRunId } : {}),
            },
          } as UnslottedEvent);
          return {
            event: stripEventDataRefs(conflictEvent, resolveData),
            hook: undefined,
          };
        }
        hook = {
          hookId: data.correlationId,
          runId: effectiveRunId,
          token: hd.token,
          metadata: hd.metadata,
          ownerId: owner.ownerId,
          projectId: owner.projectId,
          environment: owner.environment,
          createdAt: now,
          specVersion: effectiveSpecVersion,
          isWebhook: hd.isWebhook ?? false,
          isSystem: hd.isSystem ?? false,
        };
        await putBlob(kv, keys.hookBlob(hook.hookId), hook);
        await kv.put(keys.hookRunIdx(effectiveRunId, hook.hookId), '');
        break;
      }
      case 'hook_disposed': {
        const existing = await readHook(data.correlationId);
        if (existing) {
          await kv.delete(keys.hookBlob(existing.hookId));
          await kv.delete(keys.hookRunIdx(existing.runId, existing.hookId));
          await kv.delete(keys.tokenBlob(hashToken(existing.token)));
        }
        break;
      }
      case 'wait_created': {
        if (await readWait(effectiveRunId, data.correlationId)) {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already exists`
          );
        }
        wait = {
          waitId: `${effectiveRunId}-${data.correlationId}`,
          runId: effectiveRunId,
          status: 'waiting',
          resumeAt: data.eventData.resumeAt,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
          specVersion: effectiveSpecVersion,
        };
        await putBlob(kv, keys.waitBlob(effectiveRunId, data.correlationId), wait);
        break;
      }
      case 'wait_completed': {
        const existing = await readWait(effectiveRunId, data.correlationId);
        if (!existing) {
          throw new WorkflowWorldError(`Wait "${data.correlationId}" not found`);
        }
        if (existing.status === 'completed') {
          throw new EntityConflictError(
            `Wait "${data.correlationId}" already completed`
          );
        }
        wait = {
          ...existing,
          status: 'completed',
          completedAt: now,
          updatedAt: now,
        };
        await putBlob(kv, keys.waitBlob(effectiveRunId, data.correlationId), wait);
        break;
      }
      case 'attr_set': {
        if (!currentRun) throw new WorkflowRunNotFoundError(effectiveRunId);
        validateAttributeChanges(data.eventData.changes, {
          existingKeys: Object.keys(currentRun.attributes ?? {}),
          allowReservedAttributes:
            data.eventData.allowReservedAttributes === true,
        });
        // A workflow-body attr_set replays deterministically, so a second
        // write with its correlationId is a duplicate. Claimed only after
        // validation, so a rejected write can be retried.
        if (data.correlationId && data.eventData.writer?.type === 'workflow') {
          const claim = await kv.put(
            keys.attrClaim(effectiveRunId, data.correlationId),
            '',
            { ifNotExists: true }
          );
          if (!claim.ok) {
            throw new EntityConflictError(
              `Attribute event "${data.correlationId}" already exists`
            );
          }
        }
        run = {
          ...currentRun,
          attributes: applyAttributeChanges(
            currentRun.attributes,
            data.eventData.changes
          ),
          updatedAt: now,
        };
        await writeRun(run, currentRun.status);
        break;
      }
      default:
        break;
    }

    const committed = await appendEvent(event);

    // Preload the log on run_started so the runtime can skip events.list.
    let preload: { events: Event[]; cursor: string | null; hasMore: boolean } | undefined;
    if (data.eventType === 'run_started' && run) {
      const log = await readLog(effectiveRunId);
      preload = {
        events: log,
        cursor:
          log.length === 0 ? null : Buffer.from(String(log.length)).toString('base64url'),
        hasMore: false,
      };
    }
    return {
      event: stripEventDataRefs(committed, resolveData),
      run,
      step,
      hook,
      wait,
      ...(stepCreated ? { stepCreated: true as const } : {}),
      ...(preload ?? {}),
    } as EventResult;
  }

  async function terminalCleanup(runId: string): Promise<void> {
    // Delete hooks for the run + their token keys.
    const hookIdxKeys = await listAll(kv, keys.hookRunIdxPrefix(runId));
    for (const idxKey of hookIdxKeys) {
      const hookId = idxKey.slice(keys.hookRunIdxPrefix(runId).length);
      const h = await readHook(hookId);
      if (h) {
        await kv.delete(keys.hookBlob(hookId));
        await kv.delete(keys.tokenBlob(hashToken(h.token)));
      }
      await kv.delete(idxKey);
    }
    // Delete waits.
    const waitKeys = await listAll(kv, keys.waitPrefix(runId));
    for (const wk of waitKeys) await kv.delete(wk);
  }

  // ----- public Storage surface -----

  const storage: KVStorage = {
    raw: {
      async runsForStatus(status) {
        const idxKeys = await listAll(kv, keys.runStatusIdxPrefix(status));
        const runs: WorkflowRun[] = [];
        for (const k of idxKeys) {
          const runId = k.slice(keys.runStatusIdxPrefix(status).length);
          const r = await readRun(runId);
          if (r) runs.push(r);
        }
        return runs;
      },
    },
    runs: {
      get: (async (id: string, params?: { resolveData?: 'none' | 'all' }) => {
        assertSafeEntityId('runId', id);
        const r = await readRun(id);
        if (!r) throw new WorkflowRunNotFoundError(id);
        return params?.resolveData === 'none' ? stripRunData(r) : r;
      }) as Storage['runs']['get'],
      list: (async (params?: {
        status?: WorkflowRun['status'] | WorkflowRun['status'][];
        pagination?: PaginationOptions;
        resolveData?: 'none' | 'all';
      }) => {
        let runs: WorkflowRun[];
        if (Array.isArray(params?.status)) {
          // Any of the listed statuses; an empty array matches nothing, like
          // SQL `IN ()`.
          runs = [];
          for (const s of new Set(params.status)) {
            runs.push(...(await storage.raw.runsForStatus(s)));
          }
          runs.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
        } else if (params?.status) {
          runs = await storage.raw.runsForStatus(params.status);
        } else {
          // Across all statuses — concatenate each status prefix.
          const statuses: WorkflowRun['status'][] = [
            'pending',
            'running',
            'completed',
            'failed',
            'cancelled',
          ];
          runs = [];
          for (const s of statuses) {
            runs.push(...(await storage.raw.runsForStatus(s)));
          }
          runs.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
        }
        const page = paginateBy(runs, params?.pagination);
        const data =
          params?.resolveData === 'none' ? page.data.map(stripRunData) : page.data;
        return { data, cursor: page.cursor, hasMore: page.hasMore };
      }) as Storage['runs']['list'],
    },
    steps: {
      get: (async (
        runId: string,
        stepId: string,
        params?: { resolveData?: 'none' | 'all' }
      ) => {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('stepId', stepId);
        const s = await readStep(runId, stepId);
        if (!s) {
          throw new WorkflowWorldError(`Step ${stepId} in run ${runId} not found`);
        }
        if (params?.resolveData === 'none') {
          return { ...s, input: undefined, output: undefined } as Step;
        }
        return s;
      }) as Storage['steps']['get'],
      list: (async (params: {
        runId: string;
        pagination?: PaginationOptions;
        resolveData?: 'none' | 'all';
      }) => {
        assertSafeEntityId('runId', params.runId);
        const idxKeys = await listAll(kv, keys.stepIdxPrefix(params.runId));
        const steps: Step[] = [];
        for (const k of idxKeys) {
          const stepId = k.slice(keys.stepIdxPrefix(params.runId).length);
          const s = await readStep(params.runId, stepId);
          if (s) steps.push(s);
        }
        steps.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
        const page = paginateBy(steps, params.pagination);
        const data =
          params.resolveData === 'none'
            ? page.data.map((s) => ({ ...s, input: undefined, output: undefined }))
            : page.data;
        return {
          data: data as any,
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      }) as Storage['steps']['list'],
    },
    events: {
      create: createEvent as Storage['events']['create'],
      async get(runId, eventId, params) {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('eventId', eventId);
        const e = await getBlob<Event>(kv, keys.evtBlob(runId, eventId));
        if (!e) {
          throw new WorkflowWorldError(
            `Event ${eventId} in run ${runId} not found`
          );
        }
        return stripEventDataRefs(e, params?.resolveData ?? 'all');
      },
      async list(params) {
        assertSafeEntityId('runId', params.runId);
        const items = await readLog(params.runId);
        const page = paginateBy(items, params.pagination, 'asc');
        const resolveData = params.resolveData ?? 'all';
        const data =
          resolveData === 'none'
            ? page.data.map((e) => stripEventDataRefs(e, resolveData))
            : page.data;
        return {
          data,
          cursor:
            items.length === 0
              ? null
              : (page.cursor ??
                Buffer.from(String(items.length)).toString('base64url')),
          hasMore: page.hasMore,
        };
      },
      async listByCorrelationId(params) {
        assertSafeEntityId('runId', params.runId);
        assertSafeEntityId('correlationId', params.correlationId);
        // A correlation id is only unique within its run, so this scans that
        // run's log rather than the whole event namespace.
        const items = (await readLog(params.runId)).filter(
          (e) => (e as any).correlationId === params.correlationId
        );
        const page = paginateBy(items, params.pagination, 'asc');
        const resolveData = params.resolveData ?? 'all';
        const data =
          resolveData === 'none'
            ? page.data.map((e) => stripEventDataRefs(e, resolveData))
            : page.data;
        return {
          data,
          cursor:
            items.length === 0
              ? null
              : (page.cursor ??
                Buffer.from(String(items.length)).toString('base64url')),
          hasMore: page.hasMore,
        };
      },
    },
    hooks: {
      async get(hookId) {
        assertSafeEntityId('hookId', hookId);
        const h = await readHook(hookId);
        if (!h) throw new HookNotFoundError(hookId);
        return h;
      },
      async getByToken(token) {
        const hookId = await kv.get(keys.tokenBlob(hashToken(token)));
        if (!hookId) throw new HookNotFoundError(token);
        const h = await readHook(hookId);
        if (!h) throw new HookNotFoundError(token);
        return h;
      },
      async list(params) {
        let hooks: Hook[] = [];
        if (params?.runId) {
          const idxKeys = await listAll(kv, keys.hookRunIdxPrefix(params.runId));
          for (const k of idxKeys) {
            const hookId = k.slice(keys.hookRunIdxPrefix(params.runId).length);
            const h = await readHook(hookId);
            if (h) hooks.push(h);
          }
        } else {
          const allKeys = await listAll(kv, `${keys.root}/hook/`);
          for (const k of allKeys) {
            const h = await getBlob<Hook>(kv, k);
            if (h) hooks.push(h);
          }
        }
        hooks.sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime());
        return paginateBy(hooks, params?.pagination);
      },
    },
  };
  return storage;
}

function mkEvent(
  data: any,
  runId: string,
  createdAt: Date,
  specVersion: number
): UnslottedEvent {
  return { ...data, runId, createdAt, specVersion } as UnslottedEvent;
}
