import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createTestSuite } from '@workflow/world-testing';
import { describe } from 'vitest';

// The Workflow SDK's World conformance suite against the KV World, backed by
// the in-memory KV adapter inside the suite's server process. Opt in with
// WORLD_CONFORMANCE=1 after `pnpm build`: it spawns a server per test and
// takes a few minutes.
if (process.env.WORLD_CONFORMANCE === '1') {
  process.env.WORKFLOW_EDGEONE_KV_MEMORY = '1';
  process.env.WORKFLOW_EDGEONE_KV_PREFIX = `conformance_${randomUUID()}`;
  process.env.WORKFLOW_EDGEONE_KV_DISPATCHER_POLL_MS ??= '50';
  createTestSuite(
    fileURLToPath(new URL('../../dist/kv/index.js', import.meta.url))
  );
} else {
  describe.skip('KV World conformance (set WORLD_CONFORMANCE=1)', () => {});
}
