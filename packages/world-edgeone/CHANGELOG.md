# @open-workflow/world-edgeone

## Unreleased

### Changed

- **Workflow SDK v5 (spec version 8).** Requires `workflow` ^5.2.2; the default export picks up `@open-workflow/world-redirect`'s v5 migration.
- `/kv` World: declares `mintedSpecVersion()`, numbers events by slot (`slotToEventId()`), claiming each slot with `put({ ifNotExists: true })` and moving up on a taken slot. `events.list` returns only the dense prefix of the log, so an eventually consistent listing never hands the runtime a log with a hole. Bump-and-report, lazy `step_started`, terminal-run `step_started` refusal, multi-status `runs.list`, run-scoped `listByCorrelationId`, run attributes and `attr_set` are implemented as in world-redirect.
- Slot uniqueness across hosts depends on the KV backend enforcing `ifNotExists` atomically. Where it does not, run a single dispatcher (unchanged guidance).
- Adds an opt-in conformance run (`WORLD_CONFORMANCE=1`, `src/kv/conformance.test.ts`).

### Upgrading

Runs created before this version hold ULID-numbered event logs that the v5 runtime cannot replay. Drain them before deploying.

## 0.2.0 — 2026-06-02

Adds an **EdgeOne Pages KV** backend on the `/kv` subpath. The default
`.` subpath is unchanged — still re-exports the Redis-backed
`world-redirect` world plus the `withEdgeOneWorkflow` helper.

Selected by setting
`WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone/kv`. The function only
needs a KV namespace binding (default name `EDGEONE_KV`, override via
`WORKFLOW_EDGEONE_KV_BINDING`); storage, queue, and stream chunks all live
in that one KV. No Redis dependency at all.

Layout / mechanics:

- One key per entity (`run/<runId>`, `step/<runId>/<stepId>`,
  `hook/<hookId>`, `wait/<runId>/<corr>`). Status / list indexes are
  realised as zero-value "presence" keys with prefix-sortable names so
  `list({prefix})` returns rows already ordered.
- Events stored as `evt/<runId>/<eventId>` — list-by-prefix yields the
  per-run event log in chronological order (eventId is ULID).
- Scheduler: jobs at `job/<paddedRunAtMs>/<msgId>`, list-by-prefix returns
  due jobs in time order; the dispatcher iterates until it passes `now` and
  stops early. Claim lease via `put({ifNotExists: true, ttlSeconds: 60})`
  on `lease/<msgId>`.
- Hook token NX-claim via `put({ifNotExists: true})` on `tok/<sha256>`;
  losers fall back to a `hook_conflict` event (matches `world-redirect`).
- Polling streamer — KV has no pub/sub, so live `get()` polls (default
  1000ms, tune via `WORKFLOW_EDGEONE_KV_STREAM_FLUSH_MS`).

Includes an `InMemoryKV` adapter used both by the smoke-test suite (full
event-sourced run lifecycle, terminal-state guards, step lifecycle,
hook-conflict, scheduler, streams) and as a local-dev fallback via
`WORKFLOW_EDGEONE_KV_MEMORY=1`.

Caveats documented in README: lower throughput than Redis, eventual
consistency, 60-second claim-lease minimum, full-namespace scan for
`events.listByCorrelationId`. Designed for ops/admin workflows running on
EdgeOne where pulling in a separate Redis service is unwanted overhead.

Programmatic API: `createKVWorld({ kv?, bindingName?, keyPrefix?, baseUrl?,
… })`. Re-exported from the package root as `createKVWorld`, `InMemoryKV`,
`adaptNamespace`, `resolveEdgeOneKV` for callers that prefer the named
import over the subpath.

## 0.1.0 — 2026-06-02

Initial release. Bundles the EdgeOne Pages / OpenNext-specific workarounds
discovered while shipping a real bot to production:

- `withEdgeOneWorkflow(nextConfig, options?)` — Next.js config helper that
  purges Vercel-detection env vars, pins `WORKFLOW_TARGET_WORLD` +
  `WORKFLOW_NEXT_LAZY_DISCOVERY=0`, maps `UPSTASH_REDIS_REST_*` onto
  `WORKFLOW_REDIS_REST_*`, adds `@open-workflow/world-{edgeone,redirect}` to
  `serverExternalPackages`, installs `beforeFiles` rewrites mapping
  `/.well-known/workflow/v1/*` → `/api/wf/*`, then wraps with the upstream
  `withWorkflow`.

- `templates/vendor/world-vercel-shim/` — a thin `@workflow/world-vercel`
  shim that redirects to `@open-workflow/world-redirect`, for consumers to
  vendor and reference via `package.json#resolutions` (or `overrides`).

- `templates/app/api/wf/{flow,webhook/[token]}/route.ts` — mirror route
  files that re-export the eager-generated `.well-known/workflow/v1/*`
  handlers from a non-dot-prefix path that survives OpenNext's deploy
  artifact strip.

The World implementation itself (Redis storage + 307-redirect dispatch) is
identical to `@open-workflow/world-redirect@0.2.0` and is re-exported from
this package so consumers can name one world at runtime
(`WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone`) and at build
(`withEdgeOneWorkflow`).
