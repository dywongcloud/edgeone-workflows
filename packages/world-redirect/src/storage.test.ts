import {
  EntityConflictError,
  HookNotFoundError,RunExpiredError, 
  WorkflowRunNotFoundError
} from '@workflow/errors';
import { eventIdToSlot, FIRST_EVENT_SLOT, slotToEventId } from '@workflow/world';
import { afterAll, describe, expect, it } from 'vitest';
import { MemoryRedisClient } from './client/memory.js';
import { NodeRedisClient } from './client/node-redis.js';
import type { RedisClient } from './client/types.js';
import { createRedisWorld, type RedisWorld } from './index.js';

const ser = (o: unknown) => new TextEncoder().encode(JSON.stringify(o));

// The in-memory client always runs. Set TEST_REDIS_URL to additionally run the
// full suite against a real Redis (e.g. redis://127.0.0.1:6379).
const backends: { name: string; make: () => RedisClient }[] = [
  { name: 'memory', make: () => new MemoryRedisClient() },
  ...(process.env.TEST_REDIS_URL
    ? [
        {
          name: 'node-redis',
          make: () => new NodeRedisClient(process.env.TEST_REDIS_URL as string),
        },
      ]
    : []),
];

let prefixCounter = 0;
function makeWorld(client: RedisClient): RedisWorld {
  prefixCounter += 1;
  return createRedisWorld({
    client,
    keyPrefix: `test${Date.now()}_${prefixCounter}`,
    startDispatcher: false,
    recoverActiveRuns: false,
  });
}

const runCreated = (w: RedisWorld, name = 'workflow//./wf//demo') =>
  w.events.create(null, {
    eventType: 'run_created',
    specVersion: w.specVersion,
    eventData: {
      deploymentId: 'dpl_test',
      workflowName: name,
      input: ser({ hello: 'world' }),
    },
  } as any);

describe.each(backends)('storage event-sourcing [$name]', ({ make }) => {
  const client = make();
  afterAll(async () => {
    await client.close();
  });

  it('creates a run and reads it back', async () => {
    const w = makeWorld(client);
    const res = await runCreated(w);
    expect(res.run?.status).toBe('pending');
    const runId = res.run!.runId;
    const fetched = await w.runs.get(runId);
    expect(fetched.runId).toBe(runId);
    expect(fetched.workflowName).toBe('workflow//./wf//demo');
    expect(fetched.input).toBeInstanceOf(Uint8Array);
  });

  it('rejects run_failed for an unknown run', async () => {
    const w = makeWorld(client);
    await expect(
      w.events.create('wrun_01HZZZZZZZZZZZZZZZZZZZZZZZ', {
        eventType: 'run_failed',
        specVersion: w.specVersion,
        eventData: { error: ser('boom'), errorCode: 'USER_ERROR' },
      } as any)
    ).rejects.toBeInstanceOf(WorkflowRunNotFoundError);
  });

  it('runs the full run/step lifecycle', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;

    const started = await w.events.create(runId, {
      eventType: 'run_started',
      specVersion: w.specVersion,
    } as any);
    expect(started.run?.status).toBe('running');
    // run_started preloads events.
    expect(started.events && started.events.length).toBeGreaterThanOrEqual(2);

    const stepId = 'step_01HZSTEPAAAAAAAAAAAAAAAAAA';
    const created = await w.events.create(runId, {
      eventType: 'step_created',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { stepName: 'step//./wf//doThing', input: ser([1, 2]) },
    } as any);
    expect(created.step?.status).toBe('pending');
    expect(created.step?.attempt).toBe(0);

    const startedStep = await w.events.create(runId, {
      eventType: 'step_started',
      correlationId: stepId,
      specVersion: w.specVersion,
    } as any);
    expect(startedStep.step?.status).toBe('running');
    expect(startedStep.step?.attempt).toBe(1);

    const completed = await w.events.create(runId, {
      eventType: 'step_completed',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { result: ser({ ok: true }) },
    } as any);
    expect(completed.step?.status).toBe('completed');
    expect(completed.step?.output).toBeInstanceOf(Uint8Array);

    const done = await w.events.create(runId, {
      eventType: 'run_completed',
      specVersion: w.specVersion,
      eventData: { output: ser('final') },
    } as any);
    expect(done.run?.status).toBe('completed');

    const finalRun = await w.runs.get(runId);
    expect(finalRun.status).toBe('completed');
    expect(finalRun.output).toBeInstanceOf(Uint8Array);
  });

  it('supports step retry (step_retrying then step_started increments attempt)', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const stepId = 'step_01HZRETRYAAAAAAAAAAAAAAAAA';
    await w.events.create(runId, {
      eventType: 'step_created',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { stepName: 'step//./wf//retryMe', input: ser(null) },
    } as any);
    await w.events.create(runId, {
      eventType: 'step_started',
      correlationId: stepId,
      specVersion: w.specVersion,
    } as any);
    const retry = await w.events.create(runId, {
      eventType: 'step_retrying',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { error: ser('transient') },
    } as any);
    expect(retry.step?.status).toBe('pending');
    const restart = await w.events.create(runId, {
      eventType: 'step_started',
      correlationId: stepId,
      specVersion: w.specVersion,
    } as any);
    expect(restart.step?.attempt).toBe(2);
  });

  it('rejects duplicate run_created', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await expect(
      w.events.create(runId, {
        eventType: 'run_created',
        specVersion: w.specVersion,
        eventData: {
          deploymentId: 'dpl_test',
          workflowName: 'workflow//./wf//demo',
          input: ser({}),
        },
      } as any)
    ).rejects.toBeInstanceOf(EntityConflictError);
  });

  it('rejects creating entities on a terminal run', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    await w.events.create(runId, {
      eventType: 'run_completed',
      specVersion: w.specVersion,
      eventData: { output: ser('x') },
    } as any);
    await expect(
      w.events.create(runId, {
        eventType: 'step_created',
        correlationId: 'step_01HZLATEAAAAAAAAAAAAAAAAAA',
        specVersion: w.specVersion,
        eventData: { stepName: 'step//./wf//late', input: ser(null) },
      } as any)
    ).rejects.toBeInstanceOf(EntityConflictError);
  });

  it('claims hook tokens and emits hook_conflict on collision', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const token = `tok_${Date.now()}`;
    const hookId = 'hook_01HZHOOKAAAAAAAAAAAAAAAAAA';
    const created = await w.events.create(runId, {
      eventType: 'hook_created',
      correlationId: hookId,
      specVersion: w.specVersion,
      eventData: { token, isWebhook: true },
    } as any);
    expect(created.hook?.token).toBe(token);

    const byToken = await w.hooks.getByToken(token);
    expect(byToken.hookId).toBe(hookId);

    // Same token, different hook -> conflict event, no hook entity.
    const conflict = await w.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'hook_01HZHOOKBBBBBBBBBBBBBBBBBB',
      specVersion: w.specVersion,
      eventData: { token },
    } as any);
    expect(conflict.event?.eventType).toBe('hook_conflict');
    expect(conflict.hook).toBeUndefined();
  });

  it('throws HookNotFoundError for unknown token', async () => {
    const w = makeWorld(client);
    await expect(w.hooks.getByToken('nope')).rejects.toBeInstanceOf(
      HookNotFoundError
    );
  });

  it('creates and completes waits, rejecting double completion', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const waitCorr = 'wait_01HZWAITAAAAAAAAAAAAAAAAAA';
    const created = await w.events.create(runId, {
      eventType: 'wait_created',
      correlationId: waitCorr,
      specVersion: w.specVersion,
      eventData: { resumeAt: new Date(Date.now() + 60_000) },
    } as any);
    expect(created.wait?.status).toBe('waiting');
    const done = await w.events.create(runId, {
      eventType: 'wait_completed',
      correlationId: waitCorr,
      specVersion: w.specVersion,
    } as any);
    expect(done.wait?.status).toBe('completed');
    await expect(
      w.events.create(runId, {
        eventType: 'wait_completed',
        correlationId: waitCorr,
        specVersion: w.specVersion,
      } as any)
    ).rejects.toBeInstanceOf(EntityConflictError);
  });

  it('lists events chronologically and paginates', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const page1 = await w.events.list({
      runId,
      pagination: { limit: 1, sortOrder: 'asc' },
    });
    expect(page1.data[0]?.eventType).toBe('run_created');
    expect(page1.hasMore).toBe(true);
    const page2 = await w.events.list({
      runId,
      pagination: { limit: 1, cursor: page1.cursor!, sortOrder: 'asc' },
    });
    expect(page2.data[0]?.eventType).toBe('run_started');
  });

  it('lists runs filtered by status', async () => {
    const w = makeWorld(client);
    const r1 = (await runCreated(w, 'workflow//./wf//a')).run!.runId;
    await runCreated(w, 'workflow//./wf//b');
    await w.events.create(r1, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const pending = await w.runs.list({ status: 'pending', resolveData: 'none' });
    const running = await w.runs.list({ status: 'running', resolveData: 'none' });
    expect(pending.data.some((r) => r.runId === r1)).toBe(false);
    expect(running.data.some((r) => r.runId === r1)).toBe(true);
    expect(running.data[0]?.input).toBeUndefined();
  });

  it('numbers events by dense slot under concurrent appends', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const width = 25;
    await Promise.all(
      Array.from({ length: width }, (_, i) =>
        w.events.create(
          runId,
          {
            eventType: 'step_created',
            correlationId: `step_01HZPAR${String(i).padStart(19, '0')}`,
            specVersion: w.specVersion,
            eventData: { stepName: 'step//./wf//fan', input: ser([i]) },
          } as any,
          // Every writer replayed the same two-event log.
          { eventCount: 2 }
        )
      )
    );
    const page = await w.events.list({
      runId,
      pagination: { sortOrder: 'asc', limit: 1000 },
    });
    const slots = page.data.map((e) => eventIdToSlot(e.eventId));
    expect(slots).toEqual(
      Array.from({ length: width + 2 }, (_, i) => FIRST_EVENT_SLOT + i)
    );
    expect(page.data.map((e) => e.eventId)).toEqual(
      slots.map((n) => slotToEventId(n!))
    );
  });

  it('reports the slots a stale write skipped over', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const mkStep = (n: number) => ({
      eventType: 'step_created',
      correlationId: `step_01HZBUMP${String(n).padStart(18, '0')}`,
      specVersion: w.specVersion,
      eventData: { stepName: 'step//./wf//s', input: ser([n]) },
    });
    const first = await w.events.create(runId, mkStep(1) as any, { eventCount: 2 });
    expect(first.events).toBeUndefined();
    // Still believes the log holds two events: it is one behind.
    const stale = await w.events.create(runId, mkStep(2) as any, { eventCount: 2 });
    expect(eventIdToSlot(stale.event!.eventId)).toBe(4);
    expect(stale.events?.map((e) => e.eventId)).toEqual([first.event!.eventId]);
    expect(stale.cursor).toBeNull();
    expect(stale.hasMore).toBe(false);
    // A create with no eventCount is accepted and reports nothing.
    const blind = await w.events.create(runId, mkStep(3) as any);
    expect(blind.events).toBeUndefined();
  });

  it('creates the step on a lazy step_started, exactly once', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const stepId = 'step_01HZLAZYAAAAAAAAAAAAAAAAAA';
    const lazy = {
      eventType: 'step_started',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { stepName: 'step//./wf//lazy', input: ser([1]) },
    };
    const results = await Promise.allSettled([
      w.events.create(runId, lazy as any),
      w.events.create(runId, lazy as any),
    ]);
    const won = results.filter((r) => r.status === 'fulfilled');
    const lost = results.filter((r) => r.status === 'rejected');
    expect(won).toHaveLength(1);
    expect(lost).toHaveLength(1);
    expect((lost[0] as PromiseRejectedResult).reason).toBeInstanceOf(EntityConflictError);
    const winner = (won[0] as PromiseFulfilledResult<any>).value;
    expect(winner.stepCreated).toBe(true);
    expect(winner.step.status).toBe('running');
    expect(winner.step.attempt).toBe(1);
    const page = await w.events.list({ runId, pagination: { sortOrder: 'asc' } });
    expect(page.data.map((e) => e.eventType)).toEqual([
      'run_created',
      'run_started',
      'step_created',
      'step_started',
    ]);
    expect((await w.steps.get(runId, stepId)).input).toBeInstanceOf(Uint8Array);
  });

  it('refuses step_started on a terminal run even when the step is running', async () => {
    const w = makeWorld(client);
    const runId = (await runCreated(w)).run!.runId;
    await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
    const stepId = 'step_01HZTERMAAAAAAAAAAAAAAAAAA';
    await w.events.create(runId, {
      eventType: 'step_created',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { stepName: 'step//./wf//t', input: ser([]) },
    } as any);
    await w.events.create(runId, { eventType: 'step_started', correlationId: stepId, specVersion: w.specVersion } as any);
    await w.events.create(runId, { eventType: 'run_cancelled', specVersion: w.specVersion } as any);
    await expect(
      w.events.create(runId, { eventType: 'step_started', correlationId: stepId, specVersion: w.specVersion } as any)
    ).rejects.toBeInstanceOf(RunExpiredError);
    // The in-flight step can still record its outcome.
    const done = await w.events.create(runId, {
      eventType: 'step_completed',
      correlationId: stepId,
      specVersion: w.specVersion,
      eventData: { result: ser(1) },
    } as any);
    expect(done.step?.status).toBe('completed');
  });

  it('lists runs matching any of several statuses', async () => {
    const w = makeWorld(client);
    const r1 = (await runCreated(w, 'workflow//./wf//a')).run!.runId;
    const r2 = (await runCreated(w, 'workflow//./wf//b')).run!.runId;
    const r3 = (await runCreated(w, 'workflow//./wf//c')).run!.runId;
    await w.events.create(r1, { eventType: 'run_started', specVersion: w.specVersion } as any);
    await w.events.create(r3, { eventType: 'run_cancelled', specVersion: w.specVersion } as any);
    const both = await w.runs.list({ status: ['pending', 'running'], resolveData: 'none' });
    const ids = both.data.map((r) => r.runId);
    expect(ids).toContain(r1);
    expect(ids).toContain(r2);
    expect(ids).not.toContain(r3);
    const none = await w.runs.list({ status: [], resolveData: 'none' });
    expect(none.data).toEqual([]);
  });

  it('scopes listByCorrelationId to its run', async () => {
    const w = makeWorld(client);
    const shared = 'step_01HZSHAREDAAAAAAAAAAAAAAA';
    const runIds: string[] = [];
    for (const name of ['workflow//./wf//x', 'workflow//./wf//y']) {
      const runId = (await runCreated(w, name)).run!.runId;
      runIds.push(runId);
      await w.events.create(runId, { eventType: 'run_started', specVersion: w.specVersion } as any);
      await w.events.create(runId, {
        eventType: 'step_created',
        correlationId: shared,
        specVersion: w.specVersion,
        eventData: { stepName: 'step//./wf//s', input: ser([]) },
      } as any);
    }
    for (const runId of runIds) {
      const page = await w.events.listByCorrelationId({ runId, correlationId: shared });
      expect(page.data).toHaveLength(1);
      expect(page.data[0]?.runId).toBe(runId);
    }
  });
});
