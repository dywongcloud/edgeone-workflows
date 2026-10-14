import { globalSingleton } from '@workflow/utils';
import type { World } from '@workflow/world';
import { mintedSpecVersion, reenqueueActiveRuns } from '@workflow/world';
import { createRedisClient } from './client/index.js';
import type { RedisClient } from './client/types.js';
import {
  type RedisWorldConfig,
  resolveConfig,
} from './config.js';
import { EntityStore } from './entities.js';
import { Keys } from './keys.js';
import { RunLocks } from './lock.js';
import { createQueue, type RedisQueue } from './queue.js';
import { createStorage, type HookOwnerConfig } from './storage.js';
import { createStreamer } from './streamer.js';

export { NodeRedisClient, UpstashRedisClient } from './client/index.js';
export type { RedisClient } from './client/types.js';
export type { RedisWorldConfig } from './config.js';

/**
 * A World instance with the in-process 307 dispatcher controls surfaced for
 * hosts (start/stop the pump) and tests (drain/wait).
 */
export interface RedisWorld extends World {
  /** Launch the 307 dispatch pump and recover active runs. */
  start(): Promise<void>;
  /** Stop the pump and release the Redis connection. */
  close(): Promise<void>;
  /** Manually drain all currently-due jobs once. Returns count dispatched. */
  drainOnce(): Promise<number>;
  /** Resolve once the schedule is empty. */
  waitForIdle(timeoutMs?: number): Promise<void>;
  /** The underlying client (escape hatch / tests). */
  readonly redis: RedisClient;
}

function ownerConfig(): HookOwnerConfig {
  return {
    ownerId: process.env.WORKFLOW_OWNER_ID ?? 'owf-owner',
    projectId: process.env.WORKFLOW_PROJECT_ID ?? 'owf-project',
    environment:
      process.env.WORKFLOW_ENVIRONMENT ?? 'development',
  };
}

/**
 * Build a Redis-backed World from explicit config. Construction is
 * synchronous (the Redis connection is established lazily on first use);
 * call `start()` to launch the dispatcher and recover active runs.
 */
export function createRedisWorld(config: RedisWorldConfig = {}): RedisWorld {
  const resolved = resolveConfig(config);
  // Read per world rather than at module load, so WORKFLOW_SEALED_LOG=0 is
  // honoured by every world a process creates.
  const specVersion = mintedSpecVersion();
  const redis = createRedisClient(resolved);
  const keys = new Keys(resolved.keyPrefix);
  const store = new EntityStore(redis, keys);
  const locks = new RunLocks(redis, keys);
  const queue: RedisQueue = createQueue(redis, keys, resolved, specVersion);
  const storage = createStorage(
    redis,
    keys,
    store,
    locks,
    ownerConfig(),
    specVersion
  );
  const streamer = createStreamer(redis, keys, resolved.streamFlushIntervalMs);

  const world: RedisWorld = {
    specVersion,
    // queue
    queue: queue.queue,
    createQueueHandler: queue.createQueueHandler,
    getDeploymentId: queue.getDeploymentId,
    // storage
    runs: storage.runs,
    steps: storage.steps,
    events: storage.events,
    hooks: storage.hooks,
    // streamer
    ...streamer,
    // lifecycle
    async start() {
      if (resolved.startDispatcher) queue.startDispatcher();
      if (resolved.recoverActiveRuns) {
        await reenqueueActiveRuns(storage.runs, queue.queue, 'world-redirect');
      }
    },
    async close() {
      await queue.stopDispatcher();
      await redis.close();
    },
    // dispatcher controls
    drainOnce: queue.drainOnce,
    waitForIdle: queue.waitForIdle,
    redis,
  };
  return world;
}

// ---- singleton for the WORKFLOW_TARGET_WORLD loader ----

// Held on globalThis rather than at module scope: a host that bundles this
// package into several module graphs (Next.js does) would otherwise build one
// world, Redis connection and dispatcher per graph.
const { cache } = globalSingleton('@open-workflow/world-redirect//worlds', 1, () => ({
  cache: new Map<string, RedisWorld>(),
}));

function cacheKey(): string {
  const r = resolveConfig();
  return [r.redisUrl, r.upstashUrl, r.keyPrefix, r.baseUrl].join('|');
}

/**
 * Entry point used by the Workflow runtime when
 * `WORKFLOW_TARGET_WORLD=@open-workflow/world-redirect`. Returns a process-wide
 * singleton so the flow handler, the dispatcher and observability all share
 * one world (and one AsyncLocalStorage for the 307 hot path).
 */
export function createWorld(): RedisWorld {
  const key = cacheKey();
  let world = cache.get(key);
  if (!world) {
    world = createRedisWorld();
    cache.set(key, world);
  }
  return world;
}

export default createWorld;
