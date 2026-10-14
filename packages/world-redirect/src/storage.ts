import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  CreateEventParams,
  Event,
  EventResult,
  Hook,
  SerializedData,
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
  isLegacySpecVersion,
  requiresNewerWorld,
  SPEC_VERSION_MAX_SUPPORTED,
  SPEC_VERSION_SUPPORTS_SLOT_IDENTITY,
  stripEventDataRefs,
  validateAttributeChanges,
  validateUlidTimestamp,
} from '@workflow/world';
import type { RedisClient } from './client/types.js';
import { EntityStore, type UnslottedEvent } from './entities.js';
import { assertSafeEntityId, hashToken, newRunId } from './ids.js';
import type { Keys } from './keys.js';
import type { RunLocks } from './lock.js';

const TERMINAL = new Set(['completed', 'failed', 'cancelled']);
const isTerminal = (status: string) => TERMINAL.has(status);

export interface HookOwnerConfig {
  ownerId: string;
  projectId: string;
  environment: string;
}

function stripRunData(run: WorkflowRun): WorkflowRunWithoutData {
  return {
    ...run,
    input: undefined,
    output: undefined,
    dynamicWorkflowCode: undefined,
  } as WorkflowRunWithoutData;
}

function stripStepData(step: Step): Step {
  return { ...step, input: undefined, output: undefined };
}

export function createStorage(
  redis: RedisClient,
  keys: Keys,
  store: EntityStore,
  locks: RunLocks,
  owner: HookOwnerConfig,
  specVersion: number
): Storage {
  async function createEvent(
    runId: string | null,
    data: any,
    params?: CreateEventParams
  ): Promise<EventResult> {
    // Path-safety: validate ids before they become Redis key components.
    if (runId != null && runId !== '') assertSafeEntityId('runId', runId);
    if (typeof data.correlationId === 'string') {
      assertSafeEntityId('correlationId', data.correlationId);
    }

    // Resolve / generate the effective run id up front so we can lock on it.
    let effectiveRunId: string;
    if (data.eventType === 'run_created' && (!runId || runId === '')) {
      effectiveRunId = newRunId();
    } else if (!runId) {
      throw new Error('runId is required for non-run_created events');
    } else {
      effectiveRunId = runId;
    }

    if (data.eventType === 'run_created' && runId && runId !== '') {
      const validationError = validateUlidTimestamp(effectiveRunId, 'wrun_');
      if (validationError) throw new WorkflowWorldError(validationError);
    }

    const result = await locks.withLock(effectiveRunId, () =>
      createImpl(effectiveRunId, data, params)
    );
    if (params?.eventCount === undefined || result.events !== undefined) {
      return result;
    }
    return reportSkippedSlots(result, params.eventCount);
  }

  /**
   * Bump-and-report. A writer that replayed `askedFor` events expected its
   * event at slot `askedFor + 1`; when it landed higher, the events in the
   * slots in between come back with the result so the writer learns what it
   * had not seen. The report carries no cursor: it is a lower bound on what
   * the write skipped over, not a page that moves the caller's read position.
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
    const page = await store.listEvents({
      runId: result.event.runId,
      pagination: {
        // Slot n sits at list index n - 1, so the offset of slot
        // `askedFor + 1` is `askedFor`.
        cursor: Buffer.from(JSON.stringify({ o: askedFor })).toString(
          'base64url'
        ),
        limit: span,
        sortOrder: 'asc',
      },
    });
    const committedId = result.event.eventId;
    const events = page.data.filter((e) => e.eventId < committedId);
    return {
      ...result,
      events,
      cursor: null,
      hasMore: events.length < span,
    };
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

    // Load current run for validation (except run_created and the two
    // runtime events that only touch running steps).
    let currentRun: WorkflowRun | null = null;
    const skipRunValidation =
      data.eventType === 'step_completed' ||
      data.eventType === 'step_retrying';
    if (data.eventType !== 'run_created' && !skipRunValidation) {
      currentRun = await store.getRun(effectiveRunId);

      // Resilient start: run_started on a non-existent run can bootstrap it.
      if (
        data.eventType === 'run_started' &&
        !currentRun &&
        data.eventData?.deploymentId &&
        data.eventData?.workflowName &&
        data.eventData?.input !== undefined
      ) {
        const created: WorkflowRun = {
          runId: effectiveRunId,
          deploymentId: data.eventData.deploymentId,
          status: 'pending',
          workflowName: data.eventData.workflowName,
          specVersion: effectiveSpecVersion,
          executionContext: data.eventData.executionContext,
          input: data.eventData.input,
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          attributes: data.eventData.attributes ?? {},
          createdAt: now,
          updatedAt: now,
        };
        await store.writeRun(created);
        const runCreatedEvent = {
          eventType: 'run_created',
          runId: effectiveRunId,
          createdAt: now,
          specVersion: effectiveSpecVersion,
          eventData: {
            deploymentId: data.eventData.deploymentId,
            workflowName: data.eventData.workflowName,
            input: data.eventData.input,
            executionContext: data.eventData.executionContext,
            ...(data.eventData.attributes
              ? { attributes: data.eventData.attributes }
              : {}),
          },
        } as UnslottedEvent;
        await store.appendEvent(runCreatedEvent);
        currentRun = created;
      }
    }

    if (data.eventType === 'run_failed' && !currentRun) {
      throw new WorkflowRunNotFoundError(effectiveRunId);
    }

    // Spec-version compatibility.
    if (currentRun) {
      if (requiresNewerWorld(currentRun.specVersion)) {
        throw new RunNotSupportedError(
          currentRun.specVersion!,
          SPEC_VERSION_MAX_SUPPORTED
        );
      }
      if (isLegacySpecVersion(currentRun.specVersion)) {
        throw new WorkflowWorldError(
          `world-redirect does not support legacy (specVersion 1) run "${effectiveRunId}"`
        );
      }
      // Runs created before slot-numbered event ids hold ULID-numbered logs,
      // which the runtime cannot replay. Refuse them here with a clear error
      // rather than appending a slot-numbered event to a ULID log.
      if ((currentRun.specVersion ?? 0) < SPEC_VERSION_SUPPORTS_SLOT_IDENTITY) {
        throw new WorkflowWorldError(
          `world-redirect cannot append to run "${effectiveRunId}": it was created at spec version ${currentRun.specVersion}, before slot-numbered event ids (${SPEC_VERSION_SUPPORTS_SLOT_IDENTITY}), and its log cannot be replayed by this runtime`
        );
      }
    }

    // -------- run terminal-state validation --------
    if (currentRun && isTerminal(currentRun.status)) {
      if (
        data.eventType === 'run_cancelled' &&
        currentRun.status === 'cancelled'
      ) {
        const event = await store.appendEvent(
          mkEvent(data, effectiveRunId, now, effectiveSpecVersion)
        );
        return {
          event: stripEventDataRefs(event, resolveData),
          run: currentRun,
        };
      }
      if (data.eventType === 'run_started') {
        throw new RunExpiredError(
          `Workflow run "${effectiveRunId}" is already in terminal state "${currentRun.status}"`
        );
      }
      if (
        data.eventType === 'run_started' ||
        data.eventType === 'run_completed' ||
        data.eventType === 'run_failed' ||
        data.eventType === 'run_cancelled'
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
    // creates the step itself, with no step_created before it. Whoever
    // creates the step owns running its body, so a lazy start for a step that
    // already exists lost that race and must not start it again.
    const lazyStepStart =
      data.eventType === 'step_started' && isChildEntityCreationEvent(data);

    // -------- step ordering / terminal validation --------
    let validatedStep: Step | null = null;
    const stepEvents = new Set([
      'step_started',
      'step_completed',
      'step_failed',
      'step_retrying',
    ]);
    if (stepEvents.has(data.eventType) && data.correlationId) {
      validatedStep = await store.getStep(effectiveRunId, data.correlationId);
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
      if (validatedStep && currentRun && isTerminal(currentRun.status)) {
        // A step_started begins work, and no work begins on a finished run,
        // even when the step still reads `running`: that is a redelivery of
        // a start an earlier delivery already claimed. An in-flight step may
        // still record its outcome.
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

    // -------- hook ordering validation --------
    if (
      (data.eventType === 'hook_disposed' ||
        data.eventType === 'hook_received') &&
      data.correlationId
    ) {
      const existingHook = await store.getHook(data.correlationId);
      if (!existingHook) throw new HookNotFoundError(data.correlationId);
    }

    const event = mkEvent(data, effectiveRunId, now, effectiveSpecVersion);
    // run_started eventData belongs on run_created only.
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
        if (await store.getRun(effectiveRunId)) {
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
        await store.writeRun(run);
        break;
      }
      case 'run_started': {
        if (currentRun) {
          if (currentRun.status === 'running') return { run: currentRun };
          run = {
            ...currentRun,
            status: 'running',
            startedAt: currentRun.startedAt ?? now,
            output: undefined,
            error: undefined,
            completedAt: undefined,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
        }
        break;
      }
      case 'run_completed': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'completed',
            output: data.eventData?.output,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'run_failed': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'failed',
            output: undefined,
            error: data.eventData.error as Uint8Array,
            errorCode: data.eventData.errorCode,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'run_cancelled': {
        if (currentRun) {
          run = {
            ...currentRun,
            status: 'cancelled',
            output: undefined,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeRun(run, currentRun.status);
          await terminalCleanup(effectiveRunId);
        }
        break;
      }
      case 'step_created': {
        // Dedup: serialized by the per-run lock; a duplicate create on an
        // existing step is a conflict (matches snapshot-replay determinism).
        if (await store.getStep(effectiveRunId, data.correlationId)) {
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
        await store.writeStep(step);
        break;
      }
      case 'step_started': {
        if (lazyStepStart && !validatedStep) {
          const lazyData = (data as any).eventData as {
            stepName: string;
            input: unknown;
          } & Record<string, unknown>;
          // Replay still has to see step_created before step_started, so the
          // lazy start writes it first, under the same run lock.
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
          };
          await store.writeStep(validatedStep);
          await store.appendEvent({
            eventType: 'step_created',
            runId: effectiveRunId,
            correlationId: data.correlationId,
            createdAt: now,
            specVersion: effectiveSpecVersion,
            eventData: {
              stepName: lazyData.stepName,
              input: lazyData.input,
            },
          } as UnslottedEvent);
          stepCreated = true;
          // The step's input lives on step_created; step_started keeps only
          // what the runtime attached beyond it.
          const { stepName: _n, input: _i, ...rest } = lazyData;
          (event as any).eventData = rest;
        }
        if (validatedStep) {
          if (
            validatedStep.retryAfter &&
            validatedStep.retryAfter.getTime() > Date.now()
          ) {
            throw new TooEarlyError(
              `Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
              {
                retryAfter: Math.ceil(
                  (validatedStep.retryAfter.getTime() - Date.now()) / 1000
                ),
              }
            );
          }
          step = {
            ...validatedStep,
            status: 'running',
            startedAt: validatedStep.startedAt ?? now,
            attempt: validatedStep.attempt + 1,
            retryAfter: undefined,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'step_completed': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
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
          await store.writeStep(step);
        }
        break;
      }
      case 'step_failed': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
        if (validatedStep) {
          if (isTerminal(validatedStep.status)) {
            throw new EntityConflictError(
              'Cannot modify step in terminal state'
            );
          }
          step = {
            ...validatedStep,
            status: 'failed',
            error: data.eventData.error as Uint8Array,
            completedAt: now,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'step_retrying': {
        validatedStep ??= await store.getStep(
          effectiveRunId,
          data.correlationId
        );
        if (validatedStep) {
          step = {
            ...validatedStep,
            status: 'pending',
            error: data.eventData.error as Uint8Array,
            retryAfter: data.eventData.retryAfter,
            updatedAt: now,
          };
          await store.writeStep(step);
        }
        break;
      }
      case 'hook_created': {
        const hd = data.eventData;
        const tokenHash = hashToken(hd.token);
        const claimed = await redis.set(
          keys.hookToken(tokenHash),
          `${effectiveRunId}|${data.correlationId}`,
          { nx: true }
        );
        if (!claimed) {
          const existing = await redis.get(keys.hookToken(tokenHash));
          const conflictingRunId = existing?.split('|')[0];
          const conflictEvent = await store.appendEvent({
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
            run,
            step,
            hook: undefined,
          };
        }
        hook = {
          runId: effectiveRunId,
          hookId: data.correlationId,
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
        await store.writeHook(hook);
        break;
      }
      case 'hook_disposed': {
        const existing = await store.getHook(data.correlationId);
        if (existing) {
          await store.deleteHook(existing, hashToken(existing.token));
        }
        break;
      }
      case 'wait_created': {
        if (await store.getWait(effectiveRunId, data.correlationId)) {
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
        await store.writeWait(wait, data.correlationId);
        break;
      }
      case 'wait_completed': {
        const existing = await store.getWait(effectiveRunId, data.correlationId);
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
        await store.writeWait(wait, data.correlationId);
        break;
      }
      case 'attr_set': {
        if (!currentRun) throw new WorkflowRunNotFoundError(effectiveRunId);
        validateAttributeChanges(data.eventData.changes, {
          existingKeys: Object.keys(currentRun.attributes ?? {}),
          allowReservedAttributes:
            data.eventData.allowReservedAttributes === true,
        });
        // A workflow-body attr_set is replayed deterministically, so a second
        // write with its correlationId is a duplicate. Claimed only after
        // validation, so a rejected write can be retried.
        if (data.correlationId && data.eventData.writer?.type === 'workflow') {
          const claimed = await redis.set(
            keys.attrClaim(effectiveRunId, data.correlationId),
            '1',
            { nx: true }
          );
          if (!claimed) {
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
        await store.writeRun(run, currentRun.status);
        break;
      }
      // hook_received / hook_conflict: stored in the log, no entity mutation.
      default:
        break;
    }

    const committed = await store.appendEvent(event);

    // Preload events on run_started so the runtime can skip events.list.
    let events: Event[] | undefined;
    let cursor: string | null | undefined;
    let hasMore: boolean | undefined;
    if (data.eventType === 'run_started' && run) {
      const page = await store.listEvents({
        runId: effectiveRunId,
        pagination: { sortOrder: 'asc', limit: 1000 },
      });
      events = page.data;
      cursor = page.cursor;
      hasMore = page.hasMore;
    }

    return {
      event: stripEventDataRefs(committed, resolveData),
      run,
      step,
      hook,
      wait,
      ...(stepCreated ? { stepCreated: true as const } : {}),
      ...(events !== undefined
        ? { events, cursor: cursor ?? null, hasMore: hasMore ?? false }
        : {}),
    } as EventResult;
  }

  async function terminalCleanup(runId: string): Promise<void> {
    await Promise.all([
      store.deleteAllHooksForRun(runId),
      store.deleteAllWaitsForRun(runId),
    ]);
  }

  return {
    runs: {
      get: (async (id: string, params?: { resolveData?: 'none' | 'all' }) => {
        assertSafeEntityId('runId', id);
        const run = await store.getRun(id);
        if (!run) throw new WorkflowRunNotFoundError(id);
        return params?.resolveData === 'none' ? stripRunData(run) : run;
      }) as Storage['runs']['get'],
      list: (async (params?: {
        status?: string | string[];
        pagination?: any;
        resolveData?: 'none' | 'all';
      }) => {
        // An array matches a run whose status is any of the listed ones; an
        // empty array matches nothing, like SQL `IN ()`.
        const page = Array.isArray(params?.status)
          ? await store.listRunsByStatuses({
              statuses: params.status,
              pagination: params?.pagination,
            })
          : await store.listRuns({
              status: params?.status,
              pagination: params?.pagination,
            });
        const data =
          params?.resolveData === 'none'
            ? page.data.map(stripRunData)
            : page.data;
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
        const step = await store.getStep(runId, stepId);
        if (!step) {
          throw new WorkflowWorldError(`Step ${stepId} in run ${runId} not found`);
        }
        return params?.resolveData === 'none' ? stripStepData(step) : step;
      }) as Storage['steps']['get'],
      list: (async (params: {
        runId: string;
        pagination?: any;
        resolveData?: 'none' | 'all';
      }) => {
        assertSafeEntityId('runId', params.runId);
        const page = await store.listSteps({
          runId: params.runId,
          pagination: params.pagination,
        });
        const data =
          params.resolveData === 'none'
            ? page.data.map(stripStepData)
            : page.data;
        return { data, cursor: page.cursor, hasMore: page.hasMore };
      }) as Storage['steps']['list'],
    },
    events: {
      create: createEvent as Storage['events']['create'],
      async get(runId, eventId, params) {
        assertSafeEntityId('runId', runId);
        assertSafeEntityId('eventId', eventId);
        const event = await store.getEvent(runId, eventId);
        if (!event) {
          throw new WorkflowWorldError(
            `Event ${eventId} in run ${runId} not found`
          );
        }
        return stripEventDataRefs(event, params?.resolveData ?? 'all');
      },
      async list(params) {
        assertSafeEntityId('runId', params.runId);
        const page = await store.listEvents({
          runId: params.runId,
          pagination: params.pagination,
        });
        const resolveData = params.resolveData ?? 'all';
        return {
          data:
            resolveData === 'none'
              ? page.data.map((e) => stripEventDataRefs(e, resolveData))
              : page.data,
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
      async listByCorrelationId(params) {
        assertSafeEntityId('runId', params.runId);
        assertSafeEntityId('correlationId', params.correlationId);
        const page = await store.listEventsByCorrelationId({
          runId: params.runId,
          correlationId: params.correlationId,
          pagination: params.pagination,
        });
        const resolveData = params.resolveData ?? 'all';
        return {
          data:
            resolveData === 'none'
              ? page.data.map((e) => stripEventDataRefs(e, resolveData))
              : page.data,
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },
    hooks: {
      async get(hookId, _params) {
        assertSafeEntityId('hookId', hookId);
        const hook = await store.getHook(hookId);
        if (!hook) throw new HookNotFoundError(hookId);
        return hook;
      },
      async getByToken(token, _params) {
        const ref = await redis.get(keys.hookToken(hashToken(token)));
        if (!ref) throw new HookNotFoundError(token);
        const hookId = ref.split('|')[1];
        if (!hookId) throw new HookNotFoundError(token);
        const hook = await store.getHook(hookId);
        if (!hook) throw new HookNotFoundError(token);
        return hook;
      },
      async list(params) {
        const page = await store.listHooks({
          runId: params?.runId,
          pagination: params?.pagination,
        });
        return { data: page.data, cursor: page.cursor, hasMore: page.hasMore };
      },
    },
  };
}

function mkEvent(
  data: any,
  runId: string,
  createdAt: Date,
  specVersion: number
): UnslottedEvent {
  return {
    ...data,
    runId,
    createdAt,
    specVersion,
  } as UnslottedEvent;
}

export type { SerializedData };
