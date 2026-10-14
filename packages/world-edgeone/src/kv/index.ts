/**
 * @open-workflow/world-edgeone/kv — Workflow World backed by EdgeOne Pages KV.
 *
 * Zero external dependencies on EdgeOne: storage, queue and streams all live
 * in a single KV namespace. Selected by setting
 *
 *   WORKFLOW_TARGET_WORLD=@open-workflow/world-edgeone/kv
 *
 * and binding a KV namespace named `EDGEONE_KV` (or the name in
 * `WORKFLOW_EDGEONE_KV_BINDING`) to your EdgeOne Pages function.
 *
 * Trade-offs vs the Redis-backed default world:
 *   + No Redis to provision / pay for / rotate creds on
 *   + Bounded blast radius — KV is per-project, not shared infrastructure
 *   - Lower throughput. KV ops are slower than Redis, and there's no pub/sub
 *     so live streams poll
 *   - List-by-prefix for indexes — fine for tens of thousands of entities,
 *     not for millions
 *   - Eventual consistency. KV writes propagate to read nodes within ~60s.
 *     Hot loops that read-then-act on freshly-written state may see stale
 *     data. The in-process RunMutex covers the typical case; multi-host
 *     deployments should pin a single dispatcher.
 *
 * Comparison snapshot:
 *
 *   | concern         | world-redirect (Redis)     | world-edgeone/kv (EdgeOne KV) |
 *   | --------------- | -------------------------- | ----------------------------- |
 *   | entity get      | O(1) HGETALL                | O(1) GET                      |
 *   | events.list     | LRANGE                      | list-by-prefix + N GETs       |
 *   | scheduler       | ZRANGEBYSCORE               | list-by-prefix + ts compare   |
 *   | claim / lease   | atomic Lua                  | put-if-not-exists + TTL ≥60s  |
 *   | streams.live    | pub/sub                     | poll                          |
 *   | hook NX claim   | SETNX                       | put-if-not-exists w/ retry    |
 */

import { globalSingleton } from '@workflow/utils';
import { mintedSpecVersion, type World } from '@workflow/world';
import { resolveEdgeOneKV } from './binding.js';
import { type KVWorldConfig, resolveKVConfig } from './config.js';
import { type Keys, keys as makeKeys } from './keys.js';
import { createKVQueue, type KVQueue } from './queue.js';
import { createKVStorage, type KVStorage } from './storage.js';
import { createKVStreamer } from './streamer.js';
import type { KV } from './types.js';

export { adaptNamespace, InMemoryKV } from './adapters.js';
export { resolveEdgeOneKV } from './binding.js';
export type { KVWorldConfig } from './config.js';
export type { KV, KVNamespaceLike } from './types.js';

export interface KVWorld extends World {
  start(): Promise<void>;
  close(): Promise<void>;
  drainOnce(): Promise<number>;
  readonly kv: KV;
  readonly keys: Keys;
  readonly storage: KVStorage;
}

function ownerConfig() {
  return {
    ownerId: process.env.WORKFLOW_OWNER_ID ?? 'owf-owner',
    projectId: process.env.WORKFLOW_PROJECT_ID ?? 'owf-project',
    environment: process.env.WORKFLOW_ENVIRONMENT ?? 'development',
  };
}

export function createKVWorld(config: KVWorldConfig = {}): KVWorld {
  const resolved = resolveKVConfig(config);
  const kv: KV =
    resolved.kv ??
    resolveEdgeOneKV({
      bindingName: resolved.bindingName,
      allowInMemoryFallback: resolved.allowInMemoryFallback,
    });
  const keys = makeKeys(resolved.keyPrefix);
  // Read per world rather than at module load, so WORKFLOW_SEALED_LOG=0 is
  // honoured by every world a process creates.
  const specVersion = mintedSpecVersion();
  const storage = createKVStorage(kv, keys, ownerConfig(), specVersion);
  const streamer = createKVStreamer(kv, keys, resolved.streamFlushIntervalMs);
  const queue: KVQueue = createKVQueue(kv, keys, {
    baseUrl: resolved.baseUrl,
    deploymentId: resolved.deploymentId,
    maxAttempts: resolved.maxAttempts,
    retryBaseMs: resolved.retryBaseMs,
    dispatcherPollMs: resolved.dispatcherPollMs,
    dispatchBatch: resolved.dispatchBatch,
    leaseSeconds: resolved.leaseSeconds,
  });

  const world: KVWorld = {
    specVersion,
    queue: queue.queue,
    createQueueHandler: queue.createQueueHandler,
    getDeploymentId: queue.getDeploymentId,
    runs: storage.runs,
    steps: storage.steps,
    events: storage.events,
    hooks: storage.hooks,
    streams: streamer.streams,
    streamFlushIntervalMs: streamer.streamFlushIntervalMs,
    async start() {
      if (resolved.startDispatcher) queue.startDispatcher();
      if (resolved.recoverActiveRuns) {
        for (const status of ['pending', 'running'] as const) {
          const runs = await storage.raw.runsForStatus(status);
          for (const r of runs) {
            await queue.queue(
              `__wkf_workflow_${r.workflowName}`,
              { runId: r.runId } as any
            );
          }
        }
      }
    },
    async close() {
      await queue.stopDispatcher();
    },
    drainOnce: queue.drainOnce,
    kv,
    keys,
    storage,
  };
  return world;
}

// ---- singleton entry for WORKFLOW_TARGET_WORLD ----

// Held on globalThis rather than at module scope: a host that bundles this
// package into several module graphs would otherwise build one world (and one
// dispatcher and per-run mutex) per graph.
const { cache } = globalSingleton('@open-workflow/world-edgeone//kvWorlds', 1, () => ({
  cache: new Map<string, KVWorld>(),
}));
function cacheKey(): string {
  return [
    process.env.WORKFLOW_EDGEONE_KV_BINDING ?? 'EDGEONE_KV',
    process.env.WORKFLOW_EDGEONE_KV_PREFIX ?? 'owf',
    process.env.WORKFLOW_BASE_URL ?? '',
  ].join('|');
}

export function createWorld(): KVWorld {
  const key = cacheKey();
  let world = cache.get(key);
  if (!world) {
    world = createKVWorld();
    cache.set(key, world);
  }
  return world;
}

export default createWorld;
