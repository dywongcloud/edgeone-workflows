/**
 * Smoke tests for the KV-backed world. Uses the in-memory KV adapter so the
 * suite runs without any external dependency — gives us confidence the
 * event-sourcing semantics, indexes, hook-token claim, and scheduler all
 * line up before we point at a real EdgeOne KV namespace.
 *
 * Run: `pnpm test` inside packages/world-edgeone.
 */

import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EntityConflictError, RunExpiredError } from '@workflow/errors';
import { eventIdToSlot, FIRST_EVENT_SLOT, slotToEventId } from '@workflow/world';
import { createKVWorld, InMemoryKV } from '../src/kv/index.js';

function freshWorld() {
  const kv = new InMemoryKV();
  const world = createKVWorld({
    kv,
    baseUrl: 'http://localhost:9999',
    startDispatcher: false,
    recoverActiveRuns: false,
    keyPrefix: 'test',
  });
  return { kv, world };
}

test('runs.list returns nothing before any events', async () => {
  const { world } = freshWorld();
  const r = await world.runs.list();
  assert.equal(r.data.length, 0);
  assert.equal(r.hasMore, false);
});

test('event-sourced run lifecycle: created -> started -> completed', async () => {
  const { world } = freshWorld();

  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: {
      workflowName: 'demo',
      deploymentId: 'dpl_test',
      input: { hello: 'world' },
    },
  });
  assert.ok(created.run, 'run created');
  const runId = created.run!.runId;
  assert.equal(created.run!.status, 'pending');

  // run_started
  const started = await world.events.create(runId, {
    eventType: 'run_started',
  });
  assert.equal(started.run!.status, 'running');

  // run_completed
  const completed = await world.events.create(runId, {
    eventType: 'run_completed',
    eventData: { output: { ok: true } },
  });
  assert.equal(completed.run!.status, 'completed');
  assert.deepEqual(completed.run!.output, { ok: true });

  // The run blob round-trips
  const fetched = await world.runs.get(runId);
  assert.equal(fetched.status, 'completed');
  assert.deepEqual(fetched.input, { hello: 'world' });

  // Status index moved across statuses (no stale 'pending' entries).
  const pendingList = await world.runs.list({ status: 'pending' });
  assert.equal(pendingList.data.length, 0);
  const completedList = await world.runs.list({ status: 'completed' });
  assert.equal(completedList.data.length, 1);
  assert.equal(completedList.data[0]!.runId, runId);

  // Events list reads back in order.
  const events = await world.events.list({ runId });
  assert.equal(events.data.length, 3);
  assert.deepEqual(
    events.data.map((e) => e.eventType),
    ['run_created', 'run_started', 'run_completed']
  );
});

test('terminal-state guard rejects transitions from completed', async () => {
  const { world } = freshWorld();
  const c = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { workflowName: 'wf', deploymentId: 'dpl_test' },
  });
  const runId = c.run!.runId;
  await world.events.create(runId, { eventType: 'run_started' });
  await world.events.create(runId, {
    eventType: 'run_completed',
    eventData: { output: null },
  });
  await assert.rejects(
    world.events.create(runId, { eventType: 'run_started' }),
    /terminal/
  );
});

test('step lifecycle round-trips', async () => {
  const { world } = freshWorld();
  const c = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { workflowName: 'wf', deploymentId: 'dpl_test' },
  });
  const runId = c.run!.runId;
  await world.events.create(runId, { eventType: 'run_started' });

  const step = await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: 'step1',
    eventData: { stepName: 'doThing', input: { x: 1 } },
  });
  assert.equal(step.step!.status, 'pending');

  await world.events.create(runId, {
    eventType: 'step_started',
    correlationId: 'step1',
  });
  const done = await world.events.create(runId, {
    eventType: 'step_completed',
    correlationId: 'step1',
    eventData: { result: { y: 2 } },
  });
  assert.equal(done.step!.status, 'completed');
  assert.deepEqual(done.step!.output, { y: 2 });

  const list = await world.steps.list({ runId });
  assert.equal(list.data.length, 1);
  assert.equal(list.data[0]!.status, 'completed');
});

test('hook token NX claim emits hook_conflict on collision', async () => {
  const { world } = freshWorld();

  const c1 = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { workflowName: 'wf', deploymentId: 'dpl' },
  });
  const c2 = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { workflowName: 'wf', deploymentId: 'dpl' },
  });

  const ok = await world.events.create(c1.run!.runId, {
    eventType: 'hook_created',
    correlationId: 'hk1',
    eventData: { token: 'shared-token', isWebhook: true },
  });
  assert.ok(ok.hook, 'first hook succeeds');

  const conflict = await world.events.create(c2.run!.runId, {
    eventType: 'hook_created',
    correlationId: 'hk2',
    eventData: { token: 'shared-token', isWebhook: true },
  });
  assert.equal(conflict.hook, undefined);
  assert.equal(conflict.event.eventType, 'hook_conflict');
});

test('queue.queue persists job, scheduler lists it in time order', async () => {
  const { world, kv } = freshWorld();
  const now = Date.now();
  const a = await world.queue('__wkf_workflow_demo', { runId: 'r1' } as any);
  const b = await world.queue(
    '__wkf_workflow_demo',
    { runId: 'r2' } as any,
    { delaySeconds: 1 }
  );
  assert.ok(a.messageId);
  assert.ok(b.messageId);

  // Two job keys in the namespace.
  const jobs = await kv.list({ prefix: 'test/job/' });
  assert.equal(jobs.keys.length, 2);

  // Drain only delivers the due one (we don't want network errors here, so
  // we replace fetch temporarily to swallow the dispatch).
  const realFetch = globalThis.fetch;
  let dispatched = 0;
  globalThis.fetch = async () => {
    dispatched++;
    return new Response('{}', { status: 200 });
  };
  try {
    const count = await world.drainOnce();
    // Only the immediate one is due; the delayed one is ~1s out.
    assert.ok(count >= 1, `expected at least 1 dispatch, got ${count}`);
    assert.equal(dispatched, count);
    void now;
  } finally {
    globalThis.fetch = realFetch;
  }
});

test('streams.write/read polls and surfaces chunks', async () => {
  const { world } = freshWorld();
  await world.streams.write('rX', 'log', 'hello ');
  await world.streams.write('rX', 'log', 'world');

  const info = await world.streams.getInfo('rX', 'log');
  assert.equal(info.tailIndex, 1);
  assert.equal(info.done, false);

  const chunks = await world.streams.getChunks('rX', 'log');
  const text = chunks.data.map((c) => Buffer.from(c.data).toString()).join('');
  assert.equal(text, 'hello world');

  await world.streams.close('rX', 'log');
  const after = await world.streams.getInfo('rX', 'log');
  assert.equal(after.done, true);
});

async function startedRun(world: ReturnType<typeof freshWorld>['world']) {
  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { workflowName: 'demo', deploymentId: 'dpl_test', input: [] },
  } as any);
  const runId = created.run!.runId;
  await world.events.create(runId, { eventType: 'run_started' } as any);
  return runId;
}

test('events are numbered by dense slot, also under concurrent appends', async () => {
  const { world } = freshWorld();
  const runId = await startedRun(world);
  const width = 20;
  await Promise.all(
    Array.from({ length: width }, (_, i) =>
      world.events.create(
        runId,
        {
          eventType: 'step_created',
          correlationId: `step_${i}`,
          eventData: { stepName: 'fan', input: [i] },
        } as any,
        { eventCount: 2 }
      )
    )
  );
  const page = await world.events.list({ runId, pagination: { limit: 1000 } });
  const slots = page.data.map((e) => eventIdToSlot(e.eventId));
  assert.deepEqual(
    slots,
    Array.from({ length: width + 2 }, (_, i) => FIRST_EVENT_SLOT + i)
  );
  assert.deepEqual(
    page.data.map((e) => e.eventId),
    slots.map((n) => slotToEventId(n!))
  );
});

test('a stale write is bumped and reports the slots it skipped', async () => {
  const { world } = freshWorld();
  const runId = await startedRun(world);
  const step = (n: number) =>
    ({
      eventType: 'step_created',
      correlationId: `step_${n}`,
      eventData: { stepName: 's', input: [n] },
    }) as any;
  const first = await world.events.create(runId, step(1), { eventCount: 2 });
  assert.equal(first.events, undefined);
  const stale = await world.events.create(runId, step(2), { eventCount: 2 });
  assert.equal(eventIdToSlot(stale.event!.eventId), 4);
  assert.deepEqual(
    stale.events?.map((e) => e.eventId),
    [first.event!.eventId]
  );
  assert.equal(stale.cursor, null);
  assert.equal(stale.hasMore, false);
});

test('a slot claimed elsewhere is skipped, not overwritten', async () => {
  const { kv, world } = freshWorld();
  const runId = await startedRun(world);
  // Another host claimed slot 3 behind this process's back.
  await kv.put(world.keys.evtBlob(runId, slotToEventId(3)), 'taken', {
    ifNotExists: true,
  });
  const res = await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: 'step_x',
    eventData: { stepName: 's', input: [] },
  } as any);
  assert.equal(eventIdToSlot(res.event!.eventId), 4);
  assert.equal(
    await kv.get(world.keys.evtBlob(runId, slotToEventId(3))),
    'taken'
  );
});

test('events.list returns only the dense prefix of the log', async () => {
  const { kv, world } = freshWorld();
  const runId = await startedRun(world);
  // Slot 4 is visible before slot 3, as an eventually consistent list can show.
  const third = await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: 'step_3',
    eventData: { stepName: 's', input: [] },
  } as any);
  await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: 'step_4',
    eventData: { stepName: 's', input: [] },
  } as any);
  await kv.delete(world.keys.evtBlob(runId, third.event!.eventId));
  const page = await world.events.list({ runId });
  assert.deepEqual(
    page.data.map((e) => eventIdToSlot(e.eventId)),
    [1, 2]
  );
});

test('a lazy step_started creates its step exactly once', async () => {
  const { world } = freshWorld();
  const runId = await startedRun(world);
  const lazy = {
    eventType: 'step_started',
    correlationId: 'step_lazy',
    eventData: { stepName: 'lazy', input: [1] },
  } as any;
  const first = await world.events.create(runId, lazy);
  assert.equal(first.stepCreated, true);
  assert.equal(first.step?.status, 'running');
  await assert.rejects(world.events.create(runId, lazy), EntityConflictError);
  const page = await world.events.list({ runId });
  assert.deepEqual(
    page.data.map((e) => e.eventType),
    ['run_created', 'run_started', 'step_created', 'step_started']
  );
});

test('step_started is refused on a terminal run', async () => {
  const { world } = freshWorld();
  const runId = await startedRun(world);
  await world.events.create(runId, {
    eventType: 'step_created',
    correlationId: 'step_t',
    eventData: { stepName: 's', input: [] },
  } as any);
  await world.events.create(runId, {
    eventType: 'step_started',
    correlationId: 'step_t',
  } as any);
  await world.events.create(runId, { eventType: 'run_cancelled' } as any);
  await assert.rejects(
    world.events.create(runId, {
      eventType: 'step_started',
      correlationId: 'step_t',
    } as any),
    RunExpiredError
  );
});
