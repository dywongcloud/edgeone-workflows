import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createTestSuite } from '@workflow/world-testing';
import { describe } from 'vitest';

// The Workflow SDK's World conformance suite: it spawns a server with
// WORKFLOW_TARGET_WORLD pointed at this package's build and runs real
// workflows against it. Needs a real Redis (TEST_REDIS_URL) and a prior
// `pnpm build`. Each test file gets its own key prefix, so servers running
// in parallel never recover each other's runs.
if (process.env.TEST_REDIS_URL) {
  process.env.WORKFLOW_REDIS_URL = process.env.TEST_REDIS_URL;
  process.env.WORKFLOW_REDIS_KEY_PREFIX = `conformance_${randomUUID()}`;
  createTestSuite(fileURLToPath(new URL('../dist/index.js', import.meta.url)));
} else {
  describe.skip('World conformance (set TEST_REDIS_URL)', () => {});
}
