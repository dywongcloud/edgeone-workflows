# @open-workflow/world-redirect

## Unreleased

### Changed

- **Workflow SDK v5 (spec version 8).** Targets `workflow` 5.2.2 / `@workflow/world` 5.0.4 and declares `specVersion` via `mintedSpecVersion()` (8, or 6 with `WORKFLOW_SEALED_LOG=0`). `workflow` 5.2.x refuses a World below spec version 6 at startup, which this package was (it declared 2).
- **Event ids are slot positions.** `evnt_` plus the run-log position (`slotToEventId()`), allocated by one Lua script that reads the log length, stores the event and appends it atomically. A write whose `eventCount` was stale lands on the next free slot and returns the events it skipped (`events` / `hasMore`, no cursor).
- Lazy `step_started` (carrying `stepName` + `input`) creates the step and its `step_created` event; a second lazy start for the same step is an `EntityConflictError`.
- `step_started` is refused with `RunExpiredError` on a terminal run, even when the step still reads `running`.
- `runs.list({ status: [...] })` matches any listed status (`[]` matches nothing). Startup recovery relies on this.
- `events.listByCorrelationId` is scoped to `runId`; the correlation index key now includes the run.
- `run_created` keeps `eventData.attributes` (child-run lineage), and `attr_set` updates run attributes.
- Queued steps are delivered to the flow route; the `__wkf_step_*` / `step` route is gone.
- The `createWorld()` cache lives on `globalThis`, so bundled copies of the package share one world.
- Adds `@workflow/world-testing` conformance (`src/conformance.test.ts`, runs with `TEST_REDIS_URL`).

### Upgrading

Runs created by 0.2.x hold ULID-numbered event logs that the v5 runtime cannot replay. Drain in-flight runs on the old version before deploying this one: a pre-slot run gets a `WorkflowWorldError` on its next write.

## 0.2.0 — 2026-06-02

### Changed

- **Renamed `@open-workflow/world-redis` → `@open-workflow/world-redirect`.** The original name described the storage tier, but the defining feature of this World is the **307-redirect dispatch trampoline** that replaces a queue broker on the hot path — Redis is just the durable backing. Renaming makes the dispatch model explicit at the package level. Internal class names (`NodeRedisClient`, `UpstashRedisClient`, `MemoryRedisClient`, `RedisClient`) keep the `Redis` prefix because they remain accurate descriptions of what they implement.

  **Migration:**
  - `WORKFLOW_TARGET_WORLD=@open-workflow/world-redis` → `WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect`
  - `import { ... } from '@open-workflow/world-redis'` → `import { ... } from '@open-workflow/world-redirect'`
  - Log prefix changed from `[world-redis]` to `[world-redirect]`.

  No API changes; this is a pure rename. Pin `0.1.x` if you need the old name.

## 0.1.2 — 2026-06-01

### Fixed

- **Upstash REST: `hgetAll` returning wrong shape with `automaticDeserialization: false`.** The Upstash client's per-command HGETALL deserializer is bypassed when `automaticDeserialization` is disabled — instead of an object map, it returns the raw RESP shape (a flat array `[f1, v1, f2, v2, ...]`). The adapter previously trusted that the Upstash typings always returned `Record<string, string>` and handed the array through untouched.

  **User-visible effect:** every queued job hash *appeared* empty on read (`raw.queueName === undefined`), so the dispatcher's `readJob` returned `null` and the wrapper short-circuited with an "idempotent no-op" 200. Runs were created with `run_created` events but never progressed past `pending` — and the failure left no error in the log, because the 200 looked like a successful delivery for an already-processed job.

  The adapter now feature-detects the array shape and pivots it back into a field map. RESP-mode (`node-redis`) and the in-memory client were unaffected.

### How we missed it

The unit suite parameterised over `MemoryRedisClient` and `NodeRedisClient` — both return `hgetAll` as an object map, so the bug only surfaced against a real Upstash REST endpoint. Added to the testing playbook: any time we depend on Upstash's command-level deserialization, exercise the path against a real Upstash instance, not just the in-memory mock.

## 0.1.1 — 2026-05-29

### Added

- **Dispatcher: verbose non-2xx logging.** The dispatcher previously only logged HTTP `>= 500` responses from the flow endpoint. Any 4xx (a missing route, a queue-name prefix mismatch, a 401 from middleware) failed silently. Now every non-OK response is logged with the queue name, status, and response body, so the failure mode is always visible in the host log.
- **Health-check probe round-trip on both prefixes.** In the V2 combined-handler model only the `__wkf_workflow_*` prefix has a registered handler (steps run inline). The dashboard's health check probes both queues and was timing out on the `step` side. The queue wrapper now detects `__healthCheck` payloads and calls `handleHealthCheckMessage` from `@workflow/core/runtime/helpers` directly, satisfying the probe on either prefix.

## 0.1.0 — 2026-05-28

- Initial release: Redis storage (event log + materialized run/step/hook/wait entities), 307 redirect trampoline + sorted-set scheduler, streamer over Redis lists + pub/sub, three clients (`MemoryRedisClient`, `NodeRedisClient`, `UpstashRedisClient`).
